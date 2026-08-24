package cdraft

import (
	"context"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"github.com/czq/cd-raft/internal/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type rpcHarness struct {
	config topology.Config
	nodes  map[string]*RPCNode
	stores map[string]Store
	client *RPCClient
	runCtx context.Context
}

func newRPCHarness(t *testing.T, fastReturn bool) *rpcHarness {
	t.Helper()
	var configured []topology.Node
	for _, domain := range []string{"a", "b", "c"} {
		for i := 1; i <= 3; i++ {
			id := fmt.Sprintf("%s%d", domain, i)
			configured = append(configured, topology.Node{
				ID: id, DomainCode: domain,
				ListenAddress: freeAddress(t), InterDomainAddress: freeAddress(t),
			})
		}
	}
	cfg := topology.Config{
		ApplicationGroup: topology.ApplicationGroup,
		Nodes:            configured,
		Features: topology.Features{
			FastReturnEnabled: fastReturn,
		},
	}
	harness := &rpcHarness{config: cfg, nodes: make(map[string]*RPCNode), stores: make(map[string]Store), client: NewRPCClient()}
	for _, configuredNode := range cfg.Nodes {
		store := NewMemoryStore()
		node, err := NewRPCNode(cfg, configuredNode.ID, store)
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		harness.nodes[configuredNode.ID] = node
		harness.stores[configuredNode.ID] = store
	}
	t.Cleanup(func() {
		harness.client.Close()
		for _, node := range harness.nodes {
			node.Stop()
		}
	})
	return harness
}

func (h *rpcHarness) elect(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, id := range []string{"a1", "b1", "c1"} {
		if err := h.nodes[id].CampaignDomain(ctx); err != nil {
			t.Fatalf("elect domain leader %s: %v", id, err)
		}
	}
	if err := h.nodes["a1"].CampaignGlobal(ctx); err != nil {
		t.Fatalf("elect global leader: %v", err)
	}
	// Wait for every node to cross the serving gate rather than checking once:
	// the post-election heartbeat/announce propagation to followers is
	// asynchronous, so a single-pass check is a convergence race under load.
	for _, configured := range h.config.Nodes {
		configured := configured
		waitUntil(t, ctx, func() bool {
			status, err := h.client.Status(ctx, configured.ListenAddress)
			return err == nil &&
				status.GetStage() == string(Serving) &&
				status.GetGlobalLeader().GetNodeId() == "a1"
		}, fmt.Sprintf("node %s did not cross real gRPC serving gate", configured.ID))
	}
}

func TestRealGRPCDomainElectionOnlyContactsSameDomain(t *testing.T) {
	harness := newRPCHarness(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := harness.nodes["a1"].CampaignDomain(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a1", "a2", "a3"} {
		node, _ := harness.config.Node(id)
		status, err := harness.client.Status(ctx, node.ListenAddress)
		if err != nil {
			t.Fatal(err)
		}
		if status.GetDomainTerm() == 0 || status.GetDomainLeader().GetNodeId() != "a1" {
			t.Fatalf("same-domain node %s missed election RPCs: %+v", id, status)
		}
	}
	for _, id := range []string{"b1", "c1"} {
		node, _ := harness.config.Node(id)
		status, err := harness.client.Status(ctx, node.ListenAddress)
		if err != nil {
			t.Fatal(err)
		}
		if status.GetDomainTerm() != 0 {
			t.Fatalf("cross-domain node %s received domain vote/heartbeat traffic: %+v", id, status)
		}
	}
}

func TestRealGRPCDomainElectionToleratesOneUnreachablePeer(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.nodes["a1"].SetRPCPolicy(RPCPolicy{Deadline: 50 * time.Millisecond, MaxRetries: 2})
	harness.nodes["a3"].Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := harness.nodes["a1"].CampaignDomain(ctx); err != nil {
		t.Fatalf("real RPC election did not use the remaining majority: %v", err)
	}
	status, err := harness.client.Status(ctx, mustNode(t, harness.config, "a2").ListenAddress)
	if err != nil {
		t.Fatal(err)
	}
	if status.GetDomainLeader().GetNodeId() != "a1" {
		t.Fatalf("reachable follower did not observe elected Domain Leader: %+v", status)
	}
}

func TestRealGRPCAutomaticDualLayerStartup(t *testing.T) {
	harness := newRPCHarness(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}
	var globalLeader string
	for {
		allServing := true
		globalLeader = ""
		for _, configured := range harness.config.Nodes {
			status, err := harness.client.Status(ctx, configured.ListenAddress)
			if err != nil || status.GetStage() != string(Serving) || status.GetGlobalLeader().GetNodeId() == "" {
				allServing = false
				break
			}
			if globalLeader == "" {
				globalLeader = status.GetGlobalLeader().GetNodeId()
			} else if globalLeader != status.GetGlobalLeader().GetNodeId() {
				allServing = false
				break
			}
		}
		if allServing {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("real gRPC nodes did not automatically complete dual-layer startup")
		case <-time.After(50 * time.Millisecond):
		}
	}
	if globalLeader == "" {
		t.Fatal("automatic startup produced no Global Leader")
	}
}

func TestRealGRPCFailureDetectionReelectsDomainAndGlobalLeader(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	harness.nodes["b1"].Stop()
	waitUntil(t, ctx, func() bool {
		var leader string
		for _, id := range []string{"b2", "b3"} {
			status, err := harness.client.Status(ctx, mustNode(t, harness.config, id).ListenAddress)
			if err != nil || status.GetDomainLeader().GetNodeId() == "" || status.GetDomainLeader().GetNodeId() == "b1" {
				return false
			}
			if leader == "" {
				leader = status.GetDomainLeader().GetNodeId()
			} else if leader != status.GetDomainLeader().GetNodeId() {
				return false
			}
		}
		return true
	}, "domain b did not re-elect through real RPC")

	for _, id := range []string{"a1", "a2", "a3"} {
		harness.nodes[id].Stop()
	}
	waitUntil(t, ctx, func() bool {
		var leader string
		for _, id := range []string{"b2", "b3", "c1", "c2", "c3"} {
			status, err := harness.client.Status(ctx, mustNode(t, harness.config, id).ListenAddress)
			if err != nil || status.GetStage() != string(Serving) ||
				status.GetGlobalLeader().GetNodeId() == "" || status.GetGlobalLeader().GetNodeId() == "a1" {
				return false
			}
			if leader == "" {
				leader = status.GetGlobalLeader().GetNodeId()
			} else if leader != status.GetGlobalLeader().GetNodeId() {
				return false
			}
		}
		return true
	}, "surviving N-1 domains did not re-elect Global Leader through real RPC")

	target := mustNode(t, harness.config, "c2")
	decision, err := harness.client.Write(ctx, target.ListenAddress, ClientWriteRequest{
		RequestID: "after-real-reelection", OriginDomain: "c",
		Command: Command{Key: "after-failure", Value: "committed"},
	})
	if err != nil || !decision.Result.Committed {
		t.Fatalf("real client could not write after Global Leader re-election: decision=%+v err=%v", decision, err)
	}
}

func TestRealGRPCDualLeaderStartupAndBaselineClientWriteRead(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	follower, _ := harness.config.Node("b2")
	decision, err := harness.client.Write(ctx, follower.ListenAddress, ClientWriteRequest{
		RequestID: "grpc-baseline", OriginDomain: "b",
		Command: Command{Key: "x", Value: "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Winner != GlobalResponse || !decision.Result.Committed {
		t.Fatalf("baseline client did not use normal Global Leader RPC: %+v", decision)
	}
	value, err := harness.client.Read(ctx, follower.ListenAddress, "x")
	if err != nil || value != "1" {
		t.Fatalf("real gRPC read did not observe committed write: value=%q err=%v", value, err)
	}
	for _, id := range []string{"a1", "b1", "c1"} {
		node, _ := harness.config.Node(id)
		status, err := harness.client.Status(ctx, node.ListenAddress)
		if err != nil {
			t.Fatal(err)
		}
		if status.GetCommitIndex() != 1 || status.GetAppliedIndex() != 1 {
			t.Fatalf("domain leader %s did not receive real commit RPC: %+v", id, status)
		}
	}
}

func TestRealGRPCFastReturnAndNormalResponseRace(t *testing.T) {
	t.Run("fast return first", func(t *testing.T) {
		harness := newRPCHarness(t, true)
		harness.elect(t)
		harness.nodes["a1"].SetResponseDelays(150*time.Millisecond, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		target, _ := harness.config.Node("a1")
		decision, err := harness.client.Write(ctx, target.ListenAddress, ClientWriteRequest{
			RequestID: "grpc-fast-first", OriginDomain: "b",
			Command: Command{Key: "fast", Value: "1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if decision.Winner != FastResponse {
			t.Fatalf("real Fast Return callback did not win: %+v", decision)
		}
		for lateErr := range decision.LateErrors {
			t.Fatalf("late normal gRPC response disagreed: %v", lateErr)
		}
	})

	t.Run("global response first and late callback deduped", func(t *testing.T) {
		harness := newRPCHarness(t, true)
		harness.elect(t)
		harness.nodes["b1"].SetResponseDelays(0, 150*time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		target, _ := harness.config.Node("a1")
		decision, err := harness.client.Write(ctx, target.ListenAddress, ClientWriteRequest{
			RequestID: "grpc-global-first", OriginDomain: "b",
			Command: Command{Key: "normal", Value: "1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if decision.Winner != GlobalResponse {
			t.Fatalf("normal unary gRPC did not win: %+v", decision)
		}
		for lateErr := range decision.LateErrors {
			t.Fatalf("late Fast Return callback disagreed: %v", lateErr)
		}
	})
}

func TestRealGRPCCallbackUnavailableSafelyFallsBack(t *testing.T) {
	harness := newRPCHarness(t, true)
	harness.elect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target, _ := harness.config.Node("a1")
	conn, err := grpc.NewClient(target.ListenAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	response, err := cdraftv1.NewClientClient(conn).Write(ctx, &cdraftv1.ClientWriteRequest{
		RequestId: "grpc-no-callback", OriginDomain: "b", ReplyRoute: "127.0.0.1:1", Key: "fallback", Value: "1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !response.GetCommitted() || response.GetSource() != string(GlobalResponse) {
		t.Fatalf("unreachable callback broke normal response: %+v", response)
	}
}

func TestRealGRPCLatencyExperiment(t *testing.T) {
	harness := newRPCHarness(t, true)
	harness.elect(t)
	target := mustNode(t, harness.config, "a1")

	// Warm the client connection before timing. The simulated topology uses a
	// 100 ms A<->B RTT and a slower 160 ms A<->C RTT.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if _, err := harness.client.Status(ctx, target.ListenAddress); err != nil {
		t.Fatal(err)
	}
	cancel()
	simulation := topology.NetworkSimulation{
		Enabled: true, LocalOneWayDelayMillis: 1,
		InterDomainOneWayMillis: map[string]int{
			"a->b": 50, "b->a": 50,
			"a->c": 80, "c->a": 80,
			"b->c": 60, "c->b": 60,
		},
	}
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(simulation)
		node.SetRPCPolicy(RPCPolicy{Deadline: time.Second, MaxRetries: 2})
	}

	sameDomain := measureWriteLatency(t, harness, target.ListenAddress, "a", "same", GlobalResponse, 5)
	crossDomainFast := measureWriteLatency(t, harness, target.ListenAddress, "b", "cross-fast", FastResponse, 5)
	farCrossDomainFast := measureWriteLatency(t, harness, target.ListenAddress, "c", "far-cross-fast", FastResponse, 5)
	for _, node := range harness.nodes {
		node.SetFastReturnEnabled(false)
	}
	crossDomainFallback := measureWriteLatency(t, harness, target.ListenAddress, "b", "cross-normal", GlobalResponse, 5)
	farCrossDomainFallback := measureWriteLatency(t, harness, target.ListenAddress, "c", "far-cross-normal", GlobalResponse, 5)

	rttAB := 100 * time.Millisecond
	rttAC := 160 * time.Millisecond
	t.Logf("simulated A<->B RTT=%v, local one-way=1ms, A<->C RTT=160ms", rttAB)
	t.Logf("client same domain as Global Leader: median=%v (%.2f RTT), winner=%s",
		sameDomain, float64(sameDomain)/float64(rttAB), GlobalResponse)
	t.Logf("client cross-domain with Fast Return: median=%v (%.2f RTT), winner=%s",
		crossDomainFast, float64(crossDomainFast)/float64(rttAB), FastResponse)
	t.Logf("client cross-domain without Fast Return: median=%v (%.2f RTT), winner=%s",
		crossDomainFallback, float64(crossDomainFallback)/float64(rttAB), GlobalResponse)
	t.Logf("far client cross-domain with Fast Return: median=%v (%.2f A-C RTT), winner=%s",
		farCrossDomainFast, float64(farCrossDomainFast)/float64(rttAC), FastResponse)
	t.Logf("far client cross-domain without Fast Return: median=%v (%.2f A-C RTT), winner=%s",
		farCrossDomainFallback, float64(farCrossDomainFallback)/float64(rttAC), GlobalResponse)

	if sameDomain < 80*time.Millisecond || sameDomain > 150*time.Millisecond {
		t.Fatalf("same-domain write is not approximately one nearest-domain RTT: %v", sameDomain)
	}
	if crossDomainFast < 80*time.Millisecond || crossDomainFast > 150*time.Millisecond {
		t.Fatalf("cross-domain Fast Return is not approximately one A-B RTT: %v", crossDomainFast)
	}
	if crossDomainFallback < 170*time.Millisecond || crossDomainFallback > 280*time.Millisecond {
		t.Fatalf("cross-domain normal response is not approximately two A-B RTTs: %v", crossDomainFallback)
	}
	if farCrossDomainFast < 140*time.Millisecond || farCrossDomainFast > 220*time.Millisecond {
		t.Fatalf("far cross-domain Fast Return is not approximately one A-C RTT: %v", farCrossDomainFast)
	}
	if farCrossDomainFallback < 230*time.Millisecond || farCrossDomainFallback > 340*time.Millisecond {
		t.Fatalf("far cross-domain normal response did not use the nearest commit domain: %v", farCrossDomainFallback)
	}
	if crossDomainFast*3 >= crossDomainFallback*2 {
		t.Fatalf("Fast Return did not materially beat normal cross-domain response: fast=%v normal=%v",
			crossDomainFast, crossDomainFallback)
	}
}

// TestFastReturnClientExitDoesNotStrandGlobalCommit reproduces a liveness bug
// that only surfaces under real latency: when Fast Return wins the race, a
// one-shot client exits immediately, tearing down the in-flight Global Leader
// Write RPC (cancelling its context). If the GL ties its cross-domain commit to
// that request context, it aborts before collecting its own second-domain
// evidence, leaving the entry uncommitted on the GL forever and blocking every
// subsequent linearizable read behind the read barrier. The GL MUST finish the
// commit independently of the client request lifetime.
func TestFastReturnClientExitDoesNotStrandGlobalCommit(t *testing.T) {
	harness := newRPCHarness(t, true)
	harness.elect(t)
	target := mustNode(t, harness.config, "a1").ListenAddress

	// Cross-domain latency so Fast Return reliably wins the race well before the
	// Global Leader has gathered its own second-domain ack; that gap is what
	// lets the early client exit cancel the GL Write before it commits.
	simulation := topology.NetworkSimulation{
		Enabled: true, LocalOneWayDelayMillis: 1,
		InterDomainOneWayMillis: map[string]int{
			"a->b": 50, "b->a": 50, "a->c": 80, "c->a": 80, "b->c": 60, "c->b": 60,
		},
	}
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(simulation)
		node.SetRPCPolicy(RPCPolicy{Deadline: time.Second, MaxRetries: 2})
	}

	writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	decision, err := harness.client.Write(writeCtx, target, ClientWriteRequest{
		RequestID: "fast-exit", OriginDomain: "b",
		Command: Command{Key: "fast-exit-key", Value: "v"},
	})
	if err != nil {
		writeCancel()
		t.Fatalf("cross-domain write failed: %v", err)
	}
	if decision.Winner != FastResponse {
		writeCancel()
		t.Fatalf("expected Fast Return to win the race, got %s", decision.Winner)
	}
	// Simulate the one-shot client process exiting the instant Fast Return wins:
	// this cancels the still-in-flight Global Leader Write RPC.
	writeCancel()

	// The Global Leader must still complete the two-domain commit on its own, so
	// this linearizable read observes the write instead of spinning on the read
	// barrier until its deadline.
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	value, err := harness.client.ReadFrom(readCtx, target, "b", "fast-exit-key")
	if err != nil || value != "v" {
		t.Fatalf("Global Leader stranded the commit after the client left on Fast Return: value=%q err=%v", value, err)
	}
}

func domainQuorumIndexOf(n *RPCNode, domain string) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.DomainQuorumIndex[domain]
}

// TestDomainQuorumIndexHealsAfterLag pins down a Fast Return liveness bug. The
// per-domain quorum high-water mark used to advance only when the new entry was
// exactly contiguous with it (`DomainQuorumIndex+1 >= GlobalIndex`). Because the
// Global Leader replicates to a non-committing (far) domain concurrently with
// later entries, two quorum updates can land out of order; once that happened
// the mark wedged permanently and every future write ORIGINATING in that domain
// silently fell back from Fast Return to the full two-domain commit.
//
// The log is strictly append-only (appendEntry requires GlobalIndex==len+1), so
// reaching quorum at index N PROVES the domain durably holds 1..N — the mark can
// always jump to N safely. This test forces the wedged state and asserts the
// very next replicated entry heals it.
func TestDomainQuorumIndexHealsAfterLag(t *testing.T) {
	harness := newRPCHarness(t, true)
	harness.elect(t) // GL = a1
	target := mustNode(t, harness.config, "a1").ListenAddress

	// Seed a few committed entries so every domain's log is populated.
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := harness.client.Write(ctx, target, ClientWriteRequest{
			RequestID: fmt.Sprintf("seed-%d", i), OriginDomain: "a",
			Command: Command{Key: fmt.Sprintf("seed-%d", i), Value: "v"},
		}); err != nil {
			cancel()
			t.Fatalf("seed write %d: %v", i, err)
		}
		cancel()
	}
	// Let any background far-domain replication settle before we tamper.
	time.Sleep(200 * time.Millisecond)

	c1 := harness.nodes["c1"]
	c1.mu.Lock()
	logLen := uint64(len(c1.state.Log))
	// Simulate the wedged state the old contiguity gate could get stuck in: the
	// domain holds a contiguous committed log but its quorum mark lags far behind.
	c1.state.DomainQuorumIndex["c"] = 0
	c1.mu.Unlock()
	if logLen == 0 {
		t.Fatal("expected domain-c to hold the seeded log")
	}

	// One more write replicates the next entry to domain-c. Reaching quorum there
	// must heal the mark up to (at least) the committed log length instead of
	// staying stuck at 0.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := harness.client.Write(ctx, target, ClientWriteRequest{
		RequestID: "heal-0", OriginDomain: "a",
		Command: Command{Key: "heal-0", Value: "v"},
	}); err != nil {
		cancel()
		t.Fatalf("heal write: %v", err)
	}
	cancel()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	waitUntil(t, waitCtx, func() bool {
		return domainQuorumIndexOf(c1, "c") >= logLen+1
	}, fmt.Sprintf("domain-c quorum index did not heal past the wedge: got %d want >= %d",
		domainQuorumIndexOf(c1, "c"), logLen+1))
}

// TestDomainQuorumReconfirmHealsStaleLeaderMarkWithoutWrite pins the secondary
// symptom of the migration deadlock: a Domain Leader whose log holds the full
// committed prefix but whose DomainQuorumIndex lags (the state a freshly elected
// leader inherits, since followers never advance the mark) must self-heal its
// quorum mark WITHOUT needing a client write — otherwise Fast Return for that
// domain stays degraded (and a GL move to it would need a write first).
func TestDomainQuorumReconfirmHealsStaleLeaderMarkWithoutWrite(t *testing.T) {
	harness := newRPCHarness(t, true)
	harness.elect(t) // GL = a1, domain leaders a1/b1/c1

	// Seed committed writes so domain-c replicates a non-empty log.
	target := mustNode(t, harness.config, "a1").ListenAddress
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := harness.client.Write(ctx, target, ClientWriteRequest{
			RequestID: fmt.Sprintf("seed-%d", i), OriginDomain: "b",
			Command: Command{Key: fmt.Sprintf("seed-%d", i), Value: "v"},
		}); err != nil {
			cancel()
			t.Fatalf("seed write %d: %v", i, err)
		}
		cancel()
	}
	time.Sleep(200 * time.Millisecond)

	c1 := harness.nodes["c1"]
	c1.mu.Lock()
	barrier := c1.state.Summary().LastGlobalIndex
	// Wedge the mark below the (already full) log, exactly like a new leader.
	c1.state.DomainQuorumIndex["c"] = 1
	c1.mu.Unlock()
	if barrier < 3 {
		t.Fatalf("expected domain-c to hold a barrier >= 3, got %d", barrier)
	}

	// The periodic self-heal must lift the mark back to the barrier with NO write
	// and NO migration — re-driving the in-domain quorum on the existing tail.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c1.reconfirmDomainQuorum(ctx)

	if got := domainQuorumIndexOf(c1, "c"); got < barrier {
		t.Fatalf("reconfirm did not heal the stale leader mark: got %d want >= %d", got, barrier)
	}
}

func measureWriteLatency(
	t *testing.T,
	harness *rpcHarness,
	target, origin, prefix string,
	expectedWinner ResultSource,
	samples int,
) time.Duration {
	t.Helper()
	durations := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		start := time.Now()
		decision, err := harness.client.Write(ctx, target, ClientWriteRequest{
			RequestID: fmt.Sprintf("%s-%d", prefix, i), OriginDomain: origin,
			Command: Command{Key: fmt.Sprintf("%s-key-%d", prefix, i), Value: "value"},
		})
		elapsed := time.Since(start)
		if err != nil {
			cancel()
			t.Fatalf("%s sample %d failed: %v", prefix, i, err)
		}
		if decision.Winner != expectedWinner {
			cancel()
			t.Fatalf("%s sample %d winner=%s, want %s", prefix, i, decision.Winner, expectedWinner)
		}
		durations = append(durations, elapsed)
		if expectedWinner == FastResponse {
			for lateErr := range decision.LateErrors {
				if lateErr != nil {
					cancel()
					t.Fatalf("%s late response disagreed: %v", prefix, lateErr)
				}
			}
		}
		cancel()
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	return durations[len(durations)/2]
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func mustNode(t *testing.T, cfg topology.Config, id string) topology.Node {
	t.Helper()
	node, ok := cfg.Node(id)
	if !ok {
		t.Fatalf("missing node %s", id)
	}
	return node
}

func waitUntil(t *testing.T, ctx context.Context, condition func() bool, message string) {
	t.Helper()
	for {
		if condition() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(message)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestIncrementalDomainStartupElectsGlobalLeader brings domains up one at a time
// (domain-a, then domain-b, then domain-c, each separated so no two domains boot
// together). This exercises two robustness guards:
//
//  1. Pre-vote guard: while only one domain is up (< N-1) the lone Domain Leader
//     MUST NOT elect a Global Leader and MUST NOT inflate the global term by
//     re-campaigning into the void.
//  2. Periodic discovery re-announce: the one-shot PublishReady fired at election
//     time is lost to peers that have not booted yet, and the cross-domain vote
//     handler rejects votes for unknown candidates. Without re-announcing, a
//     staggered start deadlocks Global Leader convergence forever. With it, a
//     single Global Leader MUST converge once enough domains are up.
//
// Nodes are constructed up front but their gRPC listeners (Start) and election
// loops (Run) are only brought online per phase, so an un-started domain is
// genuinely unreachable — matching a real "container not started yet" topology.
func TestIncrementalDomainStartupElectsGlobalLeader(t *testing.T) {
	var configured []topology.Node
	for _, domain := range []string{"a", "b", "c"} {
		for i := 1; i <= 3; i++ {
			configured = append(configured, topology.Node{
				ID: fmt.Sprintf("%s%d", domain, i), DomainCode: domain,
				ListenAddress: freeAddress(t), InterDomainAddress: freeAddress(t),
			})
		}
	}
	cfg := topology.Config{ApplicationGroup: topology.ApplicationGroup, Nodes: configured}

	nodes := make(map[string]*RPCNode, len(configured))
	for _, configuredNode := range cfg.Nodes {
		node, err := NewRPCNode(cfg, configuredNode.ID, NewMemoryStore())
		if err != nil {
			t.Fatal(err)
		}
		nodes[configuredNode.ID] = node
	}

	client := NewRPCClient()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(func() {
		cancel()
		client.Close()
		for _, node := range nodes {
			node.Stop()
		}
	})

	startDomain := func(ids ...string) {
		for _, id := range ids {
			if err := nodes[id].Start(); err != nil {
				t.Fatalf("start %s: %v", id, err)
			}
			go nodes[id].Run(ctx)
		}
	}
	statusOf := func(id string) *cdraftv1.NodeStatusResponse {
		s, err := client.Status(ctx, mustNode(t, cfg, id).ListenAddress)
		if err != nil {
			return nil
		}
		return s
	}

	// Phase 1: only domain-a.
	startDomain("a1", "a2", "a3")
	waitUntil(t, ctx, func() bool {
		s := statusOf("a1")
		return s != nil && s.GetDomainLeader().GetNodeId() != ""
	}, "domain-a did not elect a domain leader")

	// A single domain is below the N-1 threshold: no Global Leader may form, and
	// the global term must stay bounded (the pre-vote guard must stop the lone
	// leader from campaigning into the void and inflating the term).
	time.Sleep(3 * time.Second)
	for _, id := range []string{"a1", "a2", "a3"} {
		s := statusOf(id)
		if s == nil {
			t.Fatalf("status %s unavailable", id)
		}
		if s.GetGlobalLeader().GetNodeId() != "" {
			t.Fatalf("global leader %q elected with only one domain up", s.GetGlobalLeader().GetNodeId())
		}
		if s.GetGlobalTerm() > 3 {
			t.Fatalf("global term inflated while alone (pre-vote guard missing?): term=%d on %s", s.GetGlobalTerm(), id)
		}
	}

	// Phase 2: bring up domain-b, then domain-c, each after a gap so the initial
	// one-shot announces cannot cover them — only periodic re-discovery can.
	startDomain("b1", "b2", "b3")
	time.Sleep(2 * time.Second)
	startDomain("c1", "c2", "c3")

	// A single Global Leader must converge across every node.
	waitUntil(t, ctx, func() bool {
		gl := ""
		for _, configuredNode := range cfg.Nodes {
			s := statusOf(configuredNode.ID)
			if s == nil || s.GetStage() != string(Serving) || s.GetGlobalLeader().GetNodeId() == "" {
				return false
			}
			if gl == "" {
				gl = s.GetGlobalLeader().GetNodeId()
			} else if s.GetGlobalLeader().GetNodeId() != gl {
				return false
			}
		}
		return gl != ""
	}, "global leader did not converge after starting domains one at a time")
}
