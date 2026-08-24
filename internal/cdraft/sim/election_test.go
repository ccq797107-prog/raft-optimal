package sim

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/czq/cd-raft/internal/topology"

	. "github.com/czq/cd-raft/internal/cdraft"
)

func testTopology() topology.Config {
	var nodes []topology.Node
	for _, domain := range []string{"a", "b", "c"} {
		for i := 1; i <= 3; i++ {
			id := fmt.Sprintf("%s%d", domain, i)
			nodes = append(nodes, topology.Node{
				ID: id, DomainCode: domain,
				ListenAddress: id + "-client", InterDomainAddress: id + "-domain",
			})
		}
	}
	return topology.Config{ApplicationGroup: topology.ApplicationGroup, Nodes: nodes}
}

func newTestCluster(t *testing.T, stores map[string]Store) *Cluster {
	t.Helper()
	cluster, err := NewCluster(testTopology(), stores, NewTestNetwork(), NewManualClock(time.Unix(0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range testTopology().Nodes {
		if err := cluster.StartNode(node.ID); err != nil {
			t.Fatal(err)
		}
	}
	return cluster
}

func startTestCluster(t *testing.T) *Cluster {
	t.Helper()
	cluster := newTestCluster(t, nil)
	if err := cluster.StartAll(); err != nil {
		t.Fatal(err)
	}
	return cluster
}

func TestDualLayerStartupPassesServingGate(t *testing.T) {
	cluster := startTestCluster(t)
	for _, domain := range []string{"a", "b", "c"} {
		leader, ok := cluster.DomainLeader(domain)
		if !ok || !leader.Valid() {
			t.Fatalf("domain %s has no current leader", domain)
		}
	}
	global, ok := cluster.GlobalLeader()
	if !ok {
		t.Fatal("global leader was not elected")
	}
	for _, configured := range testTopology().Nodes {
		node, _ := cluster.Snapshot(configured.ID)
		if node.Stage != Serving || node.State.GlobalLeader != global {
			t.Fatalf("node %s crossed serving gate incorrectly: %+v", configured.ID, node)
		}
	}
	if err := cluster.AssertLeaderUniqueness(); err != nil {
		t.Fatal(err)
	}
}

func TestPhasedStartupDoesNotLetBootingNodesSkipStages(t *testing.T) {
	cluster, err := NewCluster(testTopology(), nil, NewTestNetwork(), NewManualClock(time.Unix(0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.ElectDomainLeader("a", "a1"); !errors.Is(err, ErrNotServing) {
		t.Fatalf("booting candidate participated in election: %v", err)
	}
	for _, domain := range []string{"a", "b"} {
		for _, node := range cluster.Config().DomainMembers(domain) {
			if err := cluster.StartNode(node.ID); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := cluster.ElectDomainLeader(domain, domain+"1"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cluster.ElectGlobalLeader("a1"); err != nil {
		t.Fatal(err)
	}
	for _, node := range cluster.Config().DomainMembers("c") {
		snapshot, _ := cluster.Snapshot(node.ID)
		if snapshot.Stage != Booting {
			t.Fatalf("booting node %s skipped directly to %s", node.ID, snapshot.Stage)
		}
	}
}

func TestDomainElectionOnlyUsesSameDomainAndMajority(t *testing.T) {
	cluster := newTestCluster(t, nil)
	cluster.Network().Partition([]string{"a1"}, []string{"a2", "a3"})
	if _, err := cluster.ElectDomainLeader("a", "a1"); !errors.Is(err, ErrNoDomainQuorum) {
		t.Fatalf("minority candidate won: %v", err)
	}
	for _, nodeID := range []string{"b1", "b2", "b3"} {
		node, _ := cluster.Snapshot(nodeID)
		if node.State.DomainTerm != 0 || node.State.DomainVotedFor != "" {
			t.Fatalf("cross-domain node %s participated in domain a election", nodeID)
		}
	}
}

func TestDomainVoteRejectsDuplicateStaleAndLogBehindCandidates(t *testing.T) {
	store := NewMemoryStore()
	_ = store.Save(PersistentState{
		DomainTerm:     3,
		DomainVotedFor: "a2",
		Log:            []LogEntry{{GlobalTerm: 4, GlobalIndex: 8}},
	})
	cluster := newTestCluster(t, map[string]Store{"a1": store})

	stale := cluster.HandleDomainVote("a1", DomainVoteRequest{
		DomainID: "a", Candidate: "a3", DomainTerm: 2, Log: LogSummary{LastGlobalTerm: 9, LastGlobalIndex: 99},
	})
	if stale.Granted || stale.DomainTerm != 3 {
		t.Fatalf("stale term was accepted: %+v", stale)
	}
	duplicate := cluster.HandleDomainVote("a1", DomainVoteRequest{
		DomainID: "a", Candidate: "a3", DomainTerm: 3, Log: LogSummary{LastGlobalTerm: 9, LastGlobalIndex: 99},
	})
	if duplicate.Granted {
		t.Fatal("node voted twice in the same domain term")
	}
	behind := cluster.HandleDomainVote("a1", DomainVoteRequest{
		DomainID: "a", Candidate: "a3", DomainTerm: 4, Log: LogSummary{LastGlobalTerm: 4, LastGlobalIndex: 7},
	})
	if behind.Granted {
		t.Fatal("log-behind domain candidate received a vote")
	}
}

func TestRandomizedDeadlinesAndHeartbeatReset(t *testing.T) {
	cluster := newTestCluster(t, nil)
	for _, id := range []string{"a1", "a2", "a3"} {
		if err := cluster.StartNode(id); err != nil {
			t.Fatal(err)
		}
	}
	a1, _ := cluster.Snapshot("a1")
	a2, _ := cluster.Snapshot("a2")
	if a1.ElectionDeadline.Equal(a2.ElectionDeadline) {
		t.Fatal("domain election deadlines were not randomized")
	}
	leader, err := cluster.ElectDomainLeader("a", "a1")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := cluster.Snapshot("a2")
	cluster.Advance(time.Second)
	if !cluster.SendDomainHeartbeat(leader) {
		t.Fatal("healthy domain heartbeat did not reach a majority")
	}
	after, _ := cluster.Snapshot("a2")
	if !after.ElectionDeadline.After(before.ElectionDeadline) {
		t.Fatal("heartbeat did not reset follower election timer")
	}
}

func TestDomainLeaderFailureReelectsAndFencesOldIdentity(t *testing.T) {
	cluster := startTestCluster(t)
	old, _ := cluster.DomainLeader("a")
	cluster.StopNode(old.NodeID)
	if err := cluster.DetectAndReelect(); err != nil {
		t.Fatal(err)
	}
	current, ok := cluster.DomainLeader("a")
	if !ok || current.NodeID == old.NodeID || current.DomainTerm <= old.DomainTerm {
		t.Fatalf("unsafe domain re-election: old=%+v current=%+v", old, current)
	}
	if cluster.SendDomainHeartbeat(old) {
		t.Fatal("old domain leader identity remained valid")
	}
	if _, ok := cluster.GlobalLeader(); !ok {
		t.Fatal("global leader was not safely re-elected after domain identity change")
	}
	if err := cluster.AssertLeaderUniqueness(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentDomainCandidatesStillProduceOneLeaderPerTerm(t *testing.T) {
	cluster := newTestCluster(t, nil)
	var wait sync.WaitGroup
	for _, candidate := range []string{"a1", "a2"} {
		wait.Add(1)
		go func(id string) {
			defer wait.Done()
			_, _ = cluster.ElectDomainLeader("a", id)
		}(candidate)
	}
	wait.Wait()
	if _, ok := cluster.DomainLeader("a"); !ok {
		t.Fatal("concurrent candidates produced no current domain leader")
	}
	if err := cluster.AssertLeaderUniqueness(); err != nil {
		t.Fatal(err)
	}
}

func TestRestartPreservesTermsAndDoesNotRestoreLeadershipDirectly(t *testing.T) {
	stores := make(map[string]Store)
	for _, node := range testTopology().Nodes {
		stores[node.ID] = NewMemoryStore()
	}
	cluster := newTestCluster(t, stores)
	if err := cluster.StartAll(); err != nil {
		t.Fatal(err)
	}
	before, _ := cluster.Snapshot("a1")
	cluster.StopNode("a1")
	if err := cluster.RestartNode("a1"); err != nil {
		t.Fatal(err)
	}
	after, _ := cluster.Snapshot("a1")
	if after.State.DomainTerm < before.State.DomainTerm || after.State.GlobalTerm < before.State.GlobalTerm {
		t.Fatalf("terms regressed after restart: before=%+v after=%+v", before.State, after.State)
	}
	if after.DomainRole == Leader || after.GlobalRole == Leader || after.Stage == Serving {
		t.Fatalf("restart directly restored leadership/service: %+v", after)
	}
}

func TestGlobalElectionRequiresCurrentDomainLeaderAndNMinusOneVotes(t *testing.T) {
	cluster := startTestCluster(t)
	if _, err := cluster.ElectGlobalLeader("a2"); !errors.Is(err, ErrNotDomainLeader) {
		t.Fatalf("ordinary domain node participated globally: %v", err)
	}

	candidate, _ := cluster.DomainLeader("b")
	a, _ := cluster.DomainLeader("a")
	c, _ := cluster.DomainLeader("c")
	cluster.Network().Drop(candidate.NodeID, a.NodeID, GlobalVoteMessage)
	cluster.Network().Drop(candidate.NodeID, c.NodeID, GlobalVoteMessage)
	if _, err := cluster.ElectGlobalLeader(candidate.NodeID); !errors.Is(err, ErrNoGlobalQuorum) {
		t.Fatalf("global candidate won below N-1 threshold: %v", err)
	}
}

func TestGlobalElectionFailsWithFewerThanNMinusOneDomainLeaders(t *testing.T) {
	cluster := newTestCluster(t, nil)
	a, err := cluster.ElectDomainLeader("a", "a1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.ElectGlobalLeader(a.NodeID); !errors.Is(err, ErrNoGlobalQuorum) {
		t.Fatalf("one domain leader elected global leader in a three-domain cluster: %v", err)
	}
	for _, node := range testTopology().Nodes {
		snapshot, _ := cluster.Snapshot(node.ID)
		if snapshot.Stage == Serving {
			t.Fatalf("node %s bypassed the serving gate", node.ID)
		}
	}
}

func TestHigherGlobalTermFencesOldLeader(t *testing.T) {
	cluster := startTestCluster(t)
	old, _ := cluster.GlobalLeader()
	challengerDomain := "b"
	if old.DomainLeader.DomainID == challengerDomain {
		challengerDomain = "c"
	}
	challenger, _ := cluster.DomainLeader(challengerDomain)
	response := cluster.HandleGlobalVote(old.DomainLeader.NodeID, GlobalVoteRequest{
		Candidate: challenger, GlobalTerm: old.GlobalTerm + 1, Log: LogSummary{},
	})
	if response.GlobalTerm != old.GlobalTerm+1 {
		t.Fatalf("higher term was not observed: %+v", response)
	}
	if _, ok := cluster.GlobalLeader(); ok {
		t.Fatal("old global leader remained current after observing a higher term")
	}
	if err := cluster.DetectAndReelect(); err != nil {
		t.Fatal(err)
	}
	current, ok := cluster.GlobalLeader()
	if !ok || current.GlobalTerm <= old.GlobalTerm {
		t.Fatalf("safe higher-term re-election did not occur: %+v", current)
	}
}

func TestGlobalVoteRejectsStaleDomainIdentityAndLogBehindCandidate(t *testing.T) {
	cluster := startTestCluster(t)
	oldA, _ := cluster.DomainLeader("a")
	cluster.StopNode(oldA.NodeID)
	if err := cluster.DetectAndReelect(); err != nil {
		t.Fatal(err)
	}
	b, _ := cluster.DomainLeader("b")
	staleIdentity := cluster.HandleGlobalVote(b.NodeID, GlobalVoteRequest{
		Candidate: oldA, GlobalTerm: 100, Log: LogSummary{LastGlobalTerm: 100, LastGlobalIndex: 100},
	})
	if staleIdentity.Granted {
		t.Fatal("vote from a stale domain leader identity was accepted")
	}

	bNode, _ := cluster.Snapshot(b.NodeID)
	behind := cluster.HandleGlobalVote(b.NodeID, GlobalVoteRequest{
		Candidate:  b,
		GlobalTerm: bNode.State.GlobalTerm + 1,
		Log:        LogSummary{},
	})
	// Empty logs are equal in a fresh cluster, so seed b's durable log and retry.
	if !behind.Granted {
		return
	}
	store := NewMemoryStore()
	_ = store.Save(PersistentState{Log: []LogEntry{{GlobalTerm: 5, GlobalIndex: 9}}})
	fresh := newTestCluster(t, map[string]Store{"b1": store})
	_, _ = fresh.ElectDomainLeader("a", "a1")
	_, _ = fresh.ElectDomainLeader("b", "b1")
	_, _ = fresh.ElectDomainLeader("c", "c1")
	bIdentity, _ := fresh.DomainLeader("b")
	response := fresh.HandleGlobalVote("b1", GlobalVoteRequest{
		Candidate: bIdentity, GlobalTerm: 1, Log: LogSummary{LastGlobalTerm: 5, LastGlobalIndex: 8},
	})
	if response.Granted {
		t.Fatal("log-behind global candidate received a vote")
	}
}

func TestGlobalLeaderFailureUsesNormalElectionPathAndNoFixedNode(t *testing.T) {
	cluster := startTestCluster(t)
	old, _ := cluster.GlobalLeader()
	cluster.StopNode(old.DomainLeader.NodeID)
	if err := cluster.DetectAndReelect(); err != nil {
		t.Fatal(err)
	}
	current, ok := cluster.GlobalLeader()
	if !ok || current.DomainLeader.NodeID == old.DomainLeader.NodeID || current.GlobalTerm <= old.GlobalTerm {
		t.Fatalf("unsafe global re-election: old=%+v current=%+v", old, current)
	}

	candidate, _ := cluster.DomainLeader("c")
	elected, err := cluster.ElectGlobalLeader(candidate.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if elected.DomainLeader.NodeID != candidate.NodeID {
		t.Fatalf("configured/fixed node bypassed requested election: %+v", elected)
	}
	if err := cluster.AssertLeaderUniqueness(); err != nil {
		t.Fatal(err)
	}
}

func TestContinuousGlobalLeaderFailuresRemainSafe(t *testing.T) {
	cluster := startTestCluster(t)
	var previousTerm uint64
	for i := 0; i < 2; i++ {
		current, ok := cluster.GlobalLeader()
		if !ok {
			t.Fatal("missing global leader before failure")
		}
		if current.GlobalTerm <= previousTerm {
			t.Fatalf("global term did not advance: %+v", current)
		}
		previousTerm = current.GlobalTerm
		cluster.StopNode(current.DomainLeader.NodeID)
		if err := cluster.DetectAndReelect(); err != nil {
			t.Fatal(err)
		}
		if err := cluster.AssertLeaderUniqueness(); err != nil {
			t.Fatal(err)
		}
	}
}
