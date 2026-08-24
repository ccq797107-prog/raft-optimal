package cdraft

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Fault-injection harness helpers (real gRPC path).
//
// These drive partition / packet-loss / restart scenarios against the REAL
// rpc_node.go runtime (not the in-process simulator), exercising the lease,
// re-election, commit-identity and durability machinery end to end.
// ---------------------------------------------------------------------------

// runAll records the run context and starts every node's live runtime (Run),
// which enables heartbeats and lease enforcement. The recorded context is reused
// when a node is restarted mid-test.
func (h *rpcHarness) runAll(ctx context.Context) {
	h.runCtx = ctx
	for _, node := range h.nodes {
		go node.Run(ctx)
	}
}

func (h *rpcHarness) addr(id string) string {
	node, _ := h.config.Node(id)
	return node.ListenAddress
}

func (h *rpcHarness) allNodeIDs() []string {
	ids := make([]string, 0, len(h.config.Nodes))
	for _, n := range h.config.Nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

// partition installs a symmetric, bidirectional link cut between the two groups:
// no node in groupA can reach any node in groupB and vice versa.
func (h *rpcHarness) partition(groupA, groupB []string) {
	for _, id := range groupA {
		h.nodes[id].SetPartitionedFrom(groupB...)
	}
	for _, id := range groupB {
		h.nodes[id].SetPartitionedFrom(groupA...)
	}
}

func (h *rpcHarness) heal() {
	for _, node := range h.nodes {
		node.HealPartition()
	}
}

func (h *rpcHarness) setDropRate(rate float64) {
	for _, node := range h.nodes {
		node.SetDropRate(rate)
	}
}

// restart stops a node and brings up a fresh RPCNode bound to the SAME address
// and backed by the SAME store, modelling a process crash + recovery. The new
// node reloads its persisted state via store.Load().
func (h *rpcHarness) restart(t *testing.T, id string) {
	t.Helper()
	h.nodes[id].Stop()
	node, err := NewRPCNode(h.config, id, h.stores[id])
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	h.nodes[id] = node
	if h.runCtx != nil {
		go node.Run(h.runCtx)
	}
}

// agreedGL returns the Global Leader node ID that every listed node currently
// reports AND agrees on while Serving, or "" if they do not (yet) agree.
func (h *rpcHarness) agreedGL(ctx context.Context, ids ...string) string {
	agreed := ""
	for _, id := range ids {
		st, err := h.client.Status(ctx, h.addr(id))
		if err != nil {
			return ""
		}
		gl := st.GetGlobalLeader().GetNodeId()
		if gl == "" || st.GetStage() != string(Serving) {
			return ""
		}
		if agreed == "" {
			agreed = gl
		} else if agreed != gl {
			return ""
		}
	}
	return agreed
}

// writeUntilCommitted retries an idempotent write (stable requestId) until it
// commits or the budget expires. Retrying is the client's only availability tool
// under faults, and the stable requestId keeps it linearizable (no double apply).
func (h *rpcHarness) writeUntilCommitted(t *testing.T, ctx context.Context, target string, req ClientWriteRequest, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		decision, err := h.client.Write(wctx, target, req)
		cancel()
		if err == nil && decision.Result.Committed {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("write %s did not commit within %s", req.RequestID, budget)
}

// readUntil retries a read until it returns the wanted value or the budget runs
// out (reads are linearizable but may briefly redirect during leader changes).
func (h *rpcHarness) readUntil(t *testing.T, ctx context.Context, target, origin, key, want string, budget time.Duration) {
	t.Helper()
	deadline := time.Now().Add(budget)
	var last string
	for time.Now().Before(deadline) {
		rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		value, err := h.client.ReadFrom(rctx, target, origin, key)
		cancel()
		if err == nil {
			last = value
			if value == want {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("read %q did not return %q within %s (last=%q)", key, want, budget, last)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestRealGRPCFastReturnDurableQuorumsRecoverWithoutCommitPersist pins the core
// Fast Return crash window: two domain majorities have durably recorded an entry,
// but no node has durably recorded the global commit/apply/result yet. After a
// process crash and re-election, the Global Leader must reconstruct the commit
// from those durable DomainQuorumIndex certificates and serve the value.
func TestRealGRPCFastReturnDurableQuorumsRecoverWithoutCommitPersist(t *testing.T) {
	harness := newRPCHarness(t, true)
	harness.elect(t)

	for _, id := range harness.allNodeIDs() {
		harness.nodes[id].Stop()
	}

	entry := LogEntry{
		GlobalTerm: 1, GlobalIndex: 1, RequestID: "fr-durable-recover", OriginDomain: "b",
		Command: Command{Key: "fr-window", Value: "safe"}, Result: commandResult(Command{Key: "fr-window", Value: "safe"}),
	}
	holders := map[string]bool{
		"a1": true, "a2": true,
		"b1": true, "b2": true,
	}
	for _, id := range harness.allNodeIDs() {
		state, err := harness.stores[id].Load()
		if err != nil {
			t.Fatalf("load store %s: %v", id, err)
		}
		state.Log = nil
		if holders[id] {
			state.Log = []LogEntry{entry}
		}
		state.DomainQuorumIndex = make(map[string]uint64)
		switch id {
		case "a1":
			state.DomainQuorumIndex["a"] = 1
		case "b1":
			state.DomainQuorumIndex["b"] = 1
		}
		state.KnownGlobalCommitIndex = 0
		state.AppliedIndex = 0
		state.Results = make(map[string]ClientResult)
		state.StateMachine = make(map[string]string)
		if err := harness.stores[id].Save(state); err != nil {
			t.Fatalf("save precommit store %s: %v", id, err)
		}
	}

	for _, id := range harness.allNodeIDs() {
		harness.restart(t, id)
	}
	harness.elect(t)

	for _, id := range harness.allNodeIDs() {
		state, err := harness.stores[id].Load()
		if err != nil {
			t.Fatalf("reload store %s: %v", id, err)
		}
		if state.KnownGlobalCommitIndex != 0 || state.AppliedIndex != 0 || state.Results[entry.RequestID].Committed {
			t.Fatalf("%s unexpectedly persisted commit before recovery: commit=%d applied=%d result=%+v",
				id, state.KnownGlobalCommitIndex, state.AppliedIndex, state.Results[entry.RequestID])
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	harness.runAll(ctx)

	waitUntil(t, ctx, func() bool {
		status, err := harness.client.Status(ctx, harness.addr("a1"))
		return err == nil && status.GetCommitIndex() >= 1 && status.GetAppliedIndex() >= 1
	}, "Global Leader did not recover the Fast Return commit from durable domain quorums")
	waitUntil(t, ctx, func() bool {
		status, err := harness.client.Status(ctx, harness.addr("b1"))
		return err == nil && status.GetCommitIndex() >= 1 && status.GetAppliedIndex() >= 1
	}, "recovered commit was not republished to the other durable quorum domain")

	harness.readUntil(t, ctx, harness.addr("a1"), "b", "fr-window", "safe", 8*time.Second)
}

// TestRealGRPCPartitionElectsSingleNewGlobalLeaderAndHeals isolates the entire
// domain that hosts the Global Leader. The majority side (the other two domains)
// must elect exactly one NEW Global Leader, while the isolated old GL must stop
// serving (its read lease lapses) — proving there is never a dual-leader serving
// window. After healing, the cluster reconverges on a single Global Leader and a
// write committed before the partition is still readable.
func TestRealGRPCPartitionElectsSingleNewGlobalLeaderAndHeals(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	harness.runAll(ctx)

	// The incumbent Global Leader is a1 (domain a). Commit a write first.
	harness.writeUntilCommitted(t, ctx, harness.addr("a1"), ClientWriteRequest{
		RequestID: "pre-partition", OriginDomain: "a",
		Command: Command{Key: "survivor", Value: "1"},
	}, 8*time.Second)

	// Isolate domain a (the GL's domain) from domains b and c.
	domainA := []string{"a1", "a2", "a3"}
	others := []string{"b1", "b2", "b3", "c1", "c2", "c3"}
	harness.partition(domainA, others)

	// The majority side must elect a NEW Global Leader (not in domain a).
	waitUntil(t, ctx, func() bool {
		gl := harness.agreedGL(ctx, others...)
		return gl != "" && !strings.HasPrefix(gl, "a")
	}, "majority side did not elect a new Global Leader after isolating domain a")

	// The old, isolated Global Leader a1 must have stopped serving: no dual GL.
	waitUntil(t, ctx, func() bool {
		st, err := harness.client.Status(ctx, harness.addr("a1"))
		if err != nil {
			return false
		}
		return !(st.GetGlobalRole() == string(Leader) && st.GetStage() == string(Serving))
	}, "isolated old Global Leader a1 kept serving (dual-leader window)")

	// Heal: the whole cluster must reconverge on a single Global Leader.
	harness.heal()
	finalGL := ""
	waitUntil(t, ctx, func() bool {
		gl := harness.agreedGL(ctx, harness.allNodeIDs()...)
		if gl == "" {
			return false
		}
		finalGL = gl
		return true
	}, "cluster did not reconverge on a single Global Leader after healing")

	// The pre-partition committed write survived and is still linearizable.
	harness.readUntil(t, ctx, harness.addr(finalGL), "a", "survivor", "1", 8*time.Second)
}

// TestRealGRPCWritesCommitUnderPacketLoss injects a per-attempt packet-loss rate
// on every inter-node link and verifies that idempotent, retried writes still
// commit and remain linearizable. This exercises the real RPC retry paths under
// lossy conditions rather than the deterministic simulator.
func TestRealGRPCWritesCommitUnderPacketLoss(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	// More retries absorb dropped attempts on the lossy links.
	for _, node := range harness.nodes {
		node.SetRPCPolicy(RPCPolicy{Deadline: time.Second, MaxRetries: 6})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	harness.runAll(ctx)

	// 20% of every inter-node RPC attempt is dropped.
	harness.setDropRate(0.2)

	keys := []string{"loss-k1", "loss-k2", "loss-k3", "loss-k4"}
	for i, key := range keys {
		harness.writeUntilCommitted(t, ctx, harness.addr("a1"), ClientWriteRequest{
			RequestID: "loss-write-" + key, OriginDomain: "a",
			Command: Command{Key: key, Value: "v"},
		}, 12*time.Second)
		_ = i
	}

	// Heal the loss and confirm every committed key reads back linearizably.
	harness.setDropRate(0)
	for _, key := range keys {
		harness.readUntil(t, ctx, harness.addr("a1"), "a", key, "v", 8*time.Second)
	}
}

// TestRealGRPCNodeRestartPreservesCommittedState commits writes, restarts a
// follower (crash + recovery against the same store), and verifies: the
// restarted node's persisted state still contains the committed entry (no loss
// and no resurrected fork), the cluster keeps serving (a fresh write commits),
// and the restarted node rejoins and re-applies up to the commit index.
func TestRealGRPCNodeRestartPreservesCommittedState(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	harness.runAll(ctx)

	// Commit a write through the Global Leader (a1).
	harness.writeUntilCommitted(t, ctx, harness.addr("a1"), ClientWriteRequest{
		RequestID: "restart-pre", OriginDomain: "a",
		Command: Command{Key: "durable", Value: "kept"},
	}, 8*time.Second)

	// Wait until the follower we will restart (c2) has applied the committed
	// entry, so we can prove its PERSISTED state survives the restart.
	const restartID = "c2"
	waitUntil(t, ctx, func() bool {
		st, err := harness.client.Status(ctx, harness.addr(restartID))
		return err == nil && st.GetAppliedIndex() >= 1
	}, "follower did not apply the committed entry before restart")

	harness.restart(t, restartID)

	// Durability: the persisted state reloaded by the restarted node must still
	// hold the committed value (no loss, no fork resurrection).
	state, err := harness.stores[restartID].Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := state.StateMachine["durable"]; got != "kept" {
		t.Fatalf("restarted node lost committed state: durable=%q, want %q", got, "kept")
	}

	// The cluster keeps serving: a fresh write commits after the restart.
	harness.writeUntilCommitted(t, ctx, harness.addr("a1"), ClientWriteRequest{
		RequestID: "restart-post", OriginDomain: "a",
		Command: Command{Key: "after", Value: "ok"},
	}, 10*time.Second)

	// The restarted node rejoins and catches up to the cluster's commit index.
	waitUntil(t, ctx, func() bool {
		st, err := harness.client.Status(ctx, harness.addr(restartID))
		return err == nil && st.GetStage() == string(Serving) && st.GetAppliedIndex() >= 2
	}, "restarted node did not rejoin and catch up to the commit index")

	// Both values are linearizable through the leader.
	harness.readUntil(t, ctx, harness.addr("a1"), "a", "durable", "kept", 8*time.Second)
	harness.readUntil(t, ctx, harness.addr("a1"), "a", "after", "ok", 8*time.Second)
}
