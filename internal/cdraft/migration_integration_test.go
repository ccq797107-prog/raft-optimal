package cdraft

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/czq/cd-raft/internal/topology"
)

// Gate E: a Move (the GL-only administrative trigger an external controller uses)
// performs a safe, two-phase Global Leader handoff. The request is converted
// into a real higher-term Global Leader election ONLY after the target Domain
// Leader has been caught up to a frozen migration barrier. It MUST keep a unique
// Global Leader, preserve two-domain commit evidence and linearizability, and
// never edit the leader pointer directly.
//
// Load is NOT stopped before the handoff: the draining barrier lets the target
// converge while writes keep arriving, proving the catch-up phase does the work.
func TestRealGRPCMoveHandsOffGlobalLeaderViaCatchUpHandoff(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(latencyProfile())
		node.SetRPCPolicy(RPCPolicy{Deadline: 2 * time.Second, MaxRetries: 2})
		node.SetCatchUpTimeout(8 * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	target := mustNode(t, harness.config, "a1").ListenAddress
	initial, err := harness.client.Metrics(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	initialTerm := initial.GetGlobalTerm()

	// Keep load arriving from domain b throughout the handoff so the catch-up
	// phase, not a quiet log, is what makes the election succeed.
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	go driveLoad(loadCtx, harness, target, "b")

	// External controller decision: move the Global Leader to domain b. The Move
	// is sent to the current GL (a1) and only it executes the handoff.
	moveCtx, moveCancel := context.WithTimeout(ctx, 25*time.Second)
	resp, err := harness.client.Move(moveCtx, target, "b", "test move")
	moveCancel()
	if err != nil || !resp.GetAccepted() {
		t.Fatalf("move to b not accepted: resp=%+v err=%v", resp, err)
	}
	if resp.GetNewGlobalTerm() <= initialTerm {
		t.Fatalf("migration did not advance the global term: %d <= %d", resp.GetNewGlobalTerm(), initialTerm)
	}

	// Stop the load so the cluster can quiesce deterministically on one leader.
	stopLoad()

	var settledLeader string
	waitUntil(t, ctx, func() bool {
		agreed := ""
		leaders := 0
		for _, configured := range harness.config.Nodes {
			status, err := harness.client.Status(ctx, configured.ListenAddress)
			if err != nil {
				return false
			}
			leader := status.GetGlobalLeader().GetNodeId()
			if leader == "" || status.GetGlobalLeader().GetDomainId() != "b" {
				return false
			}
			if agreed == "" {
				agreed = leader
			} else if agreed != leader {
				return false
			}
			if status.GetGlobalRole() == string(Leader) {
				leaders++
			}
		}
		if leaders != 1 {
			return false
		}
		settledLeader = agreed
		return true
	}, "cluster did not quiesce on a unique migrated Global Leader")
	if settledLeader == "" {
		t.Fatal("no settled Global Leader after migration")
	}

	// Linearizability across the migration: a write committed through the new
	// Global Leader is visible to a subsequent read.
	writeCtx, writeCancel := context.WithTimeout(ctx, 6*time.Second)
	defer writeCancel()
	decision, err := harness.client.Write(writeCtx, target, ClientWriteRequest{
		RequestID: "post-migration", OriginDomain: "b",
		Command: Command{Key: "migrated", Value: "value"},
	})
	if err != nil || !decision.Result.Committed {
		t.Fatalf("write after migration did not commit: decision=%+v err=%v", decision, err)
	}
	value, err := harness.client.ReadFrom(writeCtx, target, "b", "migrated")
	if err != nil || value != "value" {
		t.Fatalf("post-migration read not linearizable: value=%q err=%v", value, err)
	}

	// All nodes must hold the core safety invariants after the handoff.
	for _, node := range harness.nodes {
		if err := node.AssertInvariants(); err != nil {
			t.Fatalf("invariant violated after migration: %v", err)
		}
	}
}

// Gate E rollback: if the target Domain Leader cannot be caught up to the
// migration barrier (its in-domain quorum is unavailable), the migration MUST
// abort WITHOUT fencing the incumbent. The original Global Leader keeps serving
// reads and writes uninterrupted; no migration is recorded.
func TestRealGRPCMigrationRollsBackWhenTargetCannotCatchUp(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(latencyProfile())
		node.SetRPCPolicy(RPCPolicy{Deadline: time.Second, MaxRetries: 2})
		// Short catch-up budget so the doomed attempt fails fast.
		node.SetCatchUpTimeout(1500 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	// Cripple domain b's in-domain quorum: with b2 and b3 down, the target b1 can
	// never advance DomainQuorumIndex[b] to the barrier, so catch-up must fail.
	harness.nodes["b2"].Stop()
	harness.nodes["b3"].Stop()

	target := mustNode(t, harness.config, "a1").ListenAddress

	// Commit a write FIRST so the migration barrier is non-zero. It commits via
	// domains a+c (b cannot reach an in-domain quorum), so DomainQuorumIndex[b]
	// stays behind the barrier and any catch-up to it is doomed.
	seedCtx, seedCancel := context.WithTimeout(ctx, 6*time.Second)
	seed, err := harness.client.Write(seedCtx, target, ClientWriteRequest{
		RequestID: "rollback-seed", OriginDomain: "a",
		Command: Command{Key: "seed", Value: "v"},
	})
	seedCancel()
	if err != nil || !seed.Result.Committed {
		t.Fatalf("seed write did not commit: decision=%+v err=%v", seed, err)
	}

	// External controller requests a move to the crippled domain b. The catch-up
	// to the non-zero barrier must time out, so the Move is REJECTED and the
	// incumbent is left untouched.
	moveCtx, moveCancel := context.WithTimeout(ctx, 6*time.Second)
	resp, err := harness.client.Move(moveCtx, target, "b", "doomed move")
	moveCancel()
	if err != nil {
		t.Fatalf("move RPC errored instead of returning a clean rejection: %v", err)
	}
	if resp.GetAccepted() {
		t.Fatalf("a doomed catch-up handoff was unexpectedly accepted: %+v", resp)
	}

	// The incumbent in domain a must still be the unique Global Leader and no
	// migration may have been recorded.
	metrics, err := harness.client.Metrics(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	if metrics.GetMigrations() != 0 {
		t.Fatalf("a doomed catch-up handoff was recorded as a migration: %d (note=%q)", metrics.GetMigrations(), metrics.GetLastMigration())
	}
	if metrics.GetGlobalLeader().GetDomainId() != "a" {
		t.Fatalf("incumbent Global Leader was disrupted by a failed handoff: %+v", metrics.GetGlobalLeader())
	}

	waitUntil(t, ctx, func() bool {
		m, err := harness.client.Metrics(ctx, target)
		return err == nil && m.GetGlobalLeader().GetDomainId() == "a"
	}, "incumbent Global Leader did not remain in domain a")
	time.Sleep(2 * time.Second)

	// And it must still be serving: a write (committed via domains a and c) is
	// visible to a subsequent read.
	writeCtx, writeCancel := context.WithTimeout(ctx, 8*time.Second)
	defer writeCancel()
	decision, err := harness.client.Write(writeCtx, target, ClientWriteRequest{
		RequestID: "rollback-still-serving", OriginDomain: "a",
		Command: Command{Key: "served", Value: "ok"},
	})
	if err != nil || !decision.Result.Committed {
		t.Fatalf("incumbent stopped serving after a failed handoff: decision=%+v err=%v", decision, err)
	}
	value, err := harness.client.ReadFrom(writeCtx, target, "a", "served")
	if err != nil || value != "ok" {
		t.Fatalf("incumbent read not linearizable after a failed handoff: value=%q err=%v", value, err)
	}
}

// Gate E window: a write committed before the handoff survives the migration and
// an idempotent retry of the SAME requestId against the new Global Leader returns
// the identical committed result (same global index) rather than re-applying it.
// This proves the migration window stays safe and linearizable and that retries
// (the client's only availability tool during the window) never double-commit.
func TestRealGRPCMigrationWindowIsIdempotentAndLinearizable(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(latencyProfile())
		node.SetRPCPolicy(RPCPolicy{Deadline: 2 * time.Second, MaxRetries: 2})
		node.SetCatchUpTimeout(8 * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	target := mustNode(t, harness.config, "a1").ListenAddress

	// Commit a tracked write through the incumbent Global Leader before the
	// handoff and remember its global index.
	preCtx, preCancel := context.WithTimeout(ctx, 6*time.Second)
	first, err := harness.client.Write(preCtx, target, ClientWriteRequest{
		RequestID: "window-write", OriginDomain: "b",
		Command: Command{Key: "window-key", Value: "window-value"},
	})
	preCancel()
	if err != nil || !first.Result.Committed {
		t.Fatalf("pre-migration write did not commit: decision=%+v err=%v", first, err)
	}
	committedIndex := first.Result.GlobalIndex

	// Drive b-origin load while the controller moves the GL to domain b.
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	go driveLoad(loadCtx, harness, target, "b")

	moveCtx, moveCancel := context.WithTimeout(ctx, 25*time.Second)
	resp, err := harness.client.Move(moveCtx, target, "b", "window move")
	moveCancel()
	if err != nil || !resp.GetAccepted() {
		t.Fatalf("move to b not accepted: resp=%+v err=%v", resp, err)
	}
	stopLoad()

	// Idempotent retry of the SAME requestId after the handoff: the new Global
	// Leader (reached via redirect) must return the identical committed result,
	// not a fresh commit, and the value must still be readable.
	retryCtx, retryCancel := context.WithTimeout(ctx, 8*time.Second)
	defer retryCancel()
	retry, err := harness.client.Write(retryCtx, target, ClientWriteRequest{
		RequestID: "window-write", OriginDomain: "b",
		Command: Command{Key: "window-key", Value: "window-value"},
	})
	if err != nil || !retry.Result.Committed {
		t.Fatalf("idempotent retry across the migration window failed: decision=%+v err=%v", retry, err)
	}
	if retry.Result.GlobalIndex != committedIndex {
		t.Fatalf("idempotent retry re-committed the write: index %d != %d", retry.Result.GlobalIndex, committedIndex)
	}
	value, err := harness.client.ReadFrom(retryCtx, target, "b", "window-key")
	if err != nil || value != "window-value" {
		t.Fatalf("write was not linearizable across the migration window: value=%q err=%v", value, err)
	}
}

// Regression for the post-migration leadership "flap". After a successful
// handoff the outgoing Global Leader relinquished its lease BEFORE the handoff
// round-trip, so the re-campaign timer it armed at revoke could fire mid-handoff.
// Leader-stickiness stops that stale campaign from WINNING, but the global term
// it bumps tears the freshly-elected new GL back down via the higher-term
// observation path — leadership flaps right back to the origin domain.
//
// The timing race is hard to reproduce deterministically in the in-process
// harness, so we assert the two invariants the fix relies on directly:
//  1. a node that is itself driving a handoff (migrating) must not start a
//     global re-campaign even with an expired deadline, and
//  2. on a successful handoff the outgoing GL adopts the new leader and re-arms
//     a fresh deadline instead of sitting leaderless with the stale timer.
func TestMigratingNodeDoesNotStartGlobalReCampaign(t *testing.T) {
	node := newMigrationUnitNode(t)
	past := time.Now().Add(-time.Hour)

	node.mu.Lock()
	node.domainRole = Leader
	node.globalRole = Follower
	node.globalCampaigning = false
	node.globalDeadline = past

	// Not migrating: an expired deadline with no GL should trigger a re-campaign.
	node.migrating = false
	if !node.needsGlobalElectionLocked(time.Now()) {
		node.mu.Unlock()
		t.Fatal("expected a global re-campaign when the deadline lapsed and we are not migrating")
	}

	// Migrating: the same expired deadline must be suppressed, otherwise the
	// outgoing GL's own timer flaps leadership back during its handoff.
	node.migrating = true
	got := node.needsGlobalElectionLocked(time.Now())
	node.mu.Unlock()
	if got {
		t.Fatal("a node driving a handoff must NOT start a global re-campaign (flap guard)")
	}
}

func TestAdoptHandedOffLeaderClearsStaleReCampaignTimer(t *testing.T) {
	node := newMigrationUnitNode(t)
	target := DomainLeaderIdentity{DomainID: "b", NodeID: "b1", DomainTerm: 1}

	node.mu.Lock()
	// Simulate the state right after revoke: leaderless, term behind the handoff,
	// and the re-campaign timer already armed (and about to fire).
	node.globalRole = Follower
	node.globalLeader = GlobalLeaderIdentity{}
	node.state.GlobalTerm = 3
	node.globalDeadline = time.Now().Add(-time.Hour)

	node.adoptHandedOffLeaderLocked(target, 4)

	adoptedTerm := node.state.GlobalTerm
	adoptedLeader := node.globalLeader
	deadline := node.globalDeadline
	node.mu.Unlock()

	if adoptedTerm != 4 {
		t.Fatalf("expected to adopt the handed-off global term 4, got %d", adoptedTerm)
	}
	if adoptedLeader.DomainLeader.NodeID != "b1" || adoptedLeader.GlobalTerm != 4 {
		t.Fatalf("expected to follow the new Global Leader b1@4, got %+v", adoptedLeader)
	}
	if !deadline.After(time.Now()) {
		t.Fatal("expected the global re-campaign deadline to be pushed into the future after adopting the new GL")
	}
}

// Gate E liveness: a Move handoff to a domain whose Domain Leader holds the full
// committed log but a LAGGING in-domain quorum mark (DomainQuorumIndex) must
// still succeed. This is the exact state a freshly (re)elected Domain Leader
// inherits — followers never advance DomainQuorumIndex, so a new leader's mark
// trails its already-replicated log until the next write drives an in-domain
// quorum. The catch-up phase must re-drive the in-domain quorum (re-stream the
// tail ABOVE the quorum mark) instead of computing an EMPTY tail off the
// already-full log and aborting with "did not reach barrier". Regression for the
// move-to-a-quiet-domain deadlock observed on the cloud (domain-c with a high
// domainTerm: a single unrelated write healed it and let the move through).
func TestRealGRPCMoveCatchesUpWhenLogLeadsQuorumMark(t *testing.T) {
	harness := newRPCHarness(t, true)
	harness.elect(t) // GL = a1, domain leaders a1/b1/c1
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(latencyProfile())
		node.SetRPCPolicy(RPCPolicy{Deadline: 2 * time.Second, MaxRetries: 2})
		node.SetCatchUpTimeout(8 * time.Second)
		// Disable the in-domain quorum self-heal so the wedged mark below is fixed
		// (or not) ONLY by the migration catch-up — this test guards runCatchUp.
		node.SetDomainQuorumReconfirmEnabled(false)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	target := mustNode(t, harness.config, "a1").ListenAddress

	// Seed committed writes so every domain (including c) replicates a non-empty
	// log and the migration barrier is non-zero.
	for i := 0; i < 4; i++ {
		wCtx, wCancel := context.WithTimeout(ctx, 6*time.Second)
		decision, err := harness.client.Write(wCtx, target, ClientWriteRequest{
			RequestID: fmt.Sprintf("seed-%d", i), OriginDomain: "b",
			Command: Command{Key: fmt.Sprintf("seed-%d", i), Value: "v"},
		})
		wCancel()
		if err != nil || !decision.Result.Committed {
			t.Fatalf("seed write %d did not commit: decision=%+v err=%v", i, decision, err)
		}
	}

	// Wait for the background far-domain replication to land c's full log, then
	// let any in-flight in-domain quorum drives settle so our wedge is not
	// immediately overwritten by a late ack.
	cLeader := harness.nodes["c1"]
	waitUntil(t, ctx, func() bool {
		cLeader.mu.Lock()
		logLen := uint64(len(cLeader.state.Log))
		cLeader.mu.Unlock()
		return logLen >= 4
	}, "domain-c did not replicate the seeded log")
	time.Sleep(time.Second)

	// Reproduce a freshly-elected leader's state: full log, but the in-domain
	// quorum mark wedged below the barrier (no further writes will bump it).
	cLeader.mu.Lock()
	barrier := cLeader.state.Summary().LastGlobalIndex
	cLeader.state.DomainQuorumIndex["c"] = 1
	cLeader.mu.Unlock()
	if barrier < 4 {
		t.Fatalf("expected domain-c to hold a barrier >= 4, got %d", barrier)
	}

	// Move the Global Leader to the quiet, quorum-lagging domain c. The catch-up
	// must re-drive c's in-domain quorum up to the barrier and the handoff must
	// be accepted (before the fix it aborted: the empty tail computed off the
	// full log left the quorum mark wedged until catchUpTimeout).
	moveCtx, moveCancel := context.WithTimeout(ctx, 20*time.Second)
	resp, err := harness.client.Move(moveCtx, target, "c", "move to quorum-lagging domain")
	moveCancel()
	if err != nil {
		t.Fatalf("move RPC errored: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("handoff to quorum-lagging domain c was rejected: %q", resp.GetError())
	}

	// The catch-up must have healed c's quorum mark up to (at least) the barrier.
	if got := domainQuorumIndexOf(cLeader, "c"); got < barrier {
		t.Fatalf("domain-c quorum mark not healed by catch-up: got %d want >= %d", got, barrier)
	}
}

// A long-running cloud cluster has log entries from older Global terms. Move
// catch-up must replay those historical entries even though the receiver already
// follows a newer Global Leader term. Regression for treating
// ReplicateEntry.GlobalTerm (the entry's term) as if it were the RPC sender's
// current term, which made every multi-term handoff abort at the barrier.
func TestRealGRPCMoveCatchUpReplaysHistoricalTerms(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t) // GL = a1, term 1
	for _, node := range harness.nodes {
		node.SetCatchUpTimeout(8 * time.Second)
		node.SetRPCPolicy(RPCPolicy{Deadline: 500 * time.Millisecond, MaxRetries: 2})
		node.SetDomainQuorumReconfirmEnabled(false)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	write := func(addr, requestID string) {
		t.Helper()
		wCtx, wCancel := context.WithTimeout(ctx, 6*time.Second)
		decision, err := harness.client.Write(wCtx, addr, ClientWriteRequest{
			RequestID: requestID, OriginDomain: "a",
			Command: Command{Key: requestID, Value: "v"},
		})
		wCancel()
		if err != nil || !decision.Result.Committed {
			t.Fatalf("write %s did not commit: decision=%+v err=%v", requestID, decision, err)
		}
	}

	aAddr := mustNode(t, harness.config, "a1").ListenAddress
	bAddr := mustNode(t, harness.config, "b1").ListenAddress
	write(aAddr, "historical-term-1")

	moveCtx, moveCancel := context.WithTimeout(ctx, 15*time.Second)
	resp, err := harness.client.Move(moveCtx, aAddr, "b", "first move creates term 2")
	moveCancel()
	if err != nil || !resp.GetAccepted() {
		t.Fatalf("first move to b not accepted: resp=%+v err=%v", resp, err)
	}
	waitUntil(t, ctx, func() bool {
		status, err := harness.client.Status(ctx, bAddr)
		return err == nil && status.GetGlobalLeader().GetNodeId() == "b1" && status.GetGlobalTerm() >= 2
	}, "b1 did not become the term-2 Global Leader")

	write(bAddr, "current-term-2")

	cLeader := harness.nodes["c1"]
	waitUntil(t, ctx, func() bool {
		cLeader.mu.Lock()
		defer cLeader.mu.Unlock()
		return cLeader.globalLeader.DomainLeader.NodeID == "b1" &&
			cLeader.state.GlobalTerm >= 2 &&
			cLeader.state.Summary().LastGlobalIndex >= 2
	}, "domain-c did not learn the multi-term log")

	cLeader.mu.Lock()
	barrier := cLeader.state.Summary().LastGlobalIndex
	cLeader.state.DomainQuorumIndex["c"] = 0
	cLeader.mu.Unlock()
	if barrier < 2 {
		t.Fatalf("expected a multi-entry barrier, got %d", barrier)
	}

	moveCtx, moveCancel = context.WithTimeout(ctx, 15*time.Second)
	resp, err = harness.client.Move(moveCtx, bAddr, "c", "move must replay historical terms")
	moveCancel()
	if err != nil {
		t.Fatalf("move RPC errored: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("handoff requiring historical-term catch-up was rejected: %q", resp.GetError())
	}
	if got := domainQuorumIndexOf(cLeader, "c"); got < barrier {
		t.Fatalf("domain-c quorum mark not healed across historical terms: got %d want >= %d", got, barrier)
	}
}

func newMigrationUnitNode(t *testing.T) *RPCNode {
	t.Helper()
	var configured []topology.Node
	for _, domain := range []string{"a", "b", "c"} {
		configured = append(configured, topology.Node{
			ID:            domain + "1",
			DomainCode:    domain,
			ListenAddress: freeAddress(t), InterDomainAddress: freeAddress(t),
		})
	}
	cfg := topology.Config{ApplicationGroup: topology.ApplicationGroup, Nodes: configured}
	node, err := NewRPCNode(cfg, "a1", NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	return node
}
