package sim

import (
	"errors"
	"testing"

	. "github.com/czq/cd-raft/internal/cdraft"
)

func currentGlobalNode(t *testing.T, cluster *Cluster) string {
	t.Helper()
	leader, ok := cluster.GlobalLeader()
	if !ok {
		t.Fatal("missing global leader")
	}
	return leader.DomainLeader.NodeID
}

func writeRequest(id, origin, key, value string) ClientWriteRequest {
	return ClientWriteRequest{
		RequestID: id, OriginDomain: origin, ReplyRoute: origin + "-reply",
		Command: Command{Key: key, Value: value},
	}
}

func TestBaselineWriteUsesGlobalLeaderAndTwoDomains(t *testing.T) {
	cluster := startTestCluster(t)
	global := currentGlobalNode(t, cluster)
	outcome := cluster.Write(global, writeRequest("r1", "b", "x", "1"))
	if outcome.NormalErr != nil || outcome.Normal == nil || !outcome.Normal.Committed {
		t.Fatalf("normal write failed: %+v", outcome)
	}
	if outcome.Fast != nil {
		t.Fatal("Fast Return ran while feature gate was disabled")
	}
	if err := cluster.AssertCommitInvariants(); err != nil {
		t.Fatal(err)
	}
	value, err := cluster.Read(global, "x")
	if err != nil || value != "1" {
		t.Fatalf("write was not linearly readable: value=%q err=%v", value, err)
	}

	nonGlobal := "b2"
	if nonGlobal == global {
		nonGlobal = "c2"
	}
	if got := cluster.Write(nonGlobal, writeRequest("r2", "b", "y", "2")); !errors.Is(got.NormalErr, ErrNotGlobalLeader) {
		t.Fatalf("non-global write was not rejected: %+v", got)
	}
}

func TestSingleDomainCannotCommit(t *testing.T) {
	cluster := startTestCluster(t)
	globalIdentity, _ := cluster.GlobalLeader()
	globalDomain := globalIdentity.DomainLeader.DomainID
	for _, domain := range cluster.Config().Domains() {
		if domain == globalDomain {
			continue
		}
		for _, node := range cluster.Config().DomainMembers(domain) {
			cluster.StopNode(node.ID)
		}
	}
	outcome := cluster.Write(globalIdentity.DomainLeader.NodeID, writeRequest("r1", globalDomain, "x", "1"))
	if !errors.Is(outcome.NormalErr, ErrNoCommit) || outcome.Normal != nil {
		t.Fatalf("single-domain write committed: %+v", outcome)
	}
}

func TestDuplicateRequestReusesLogPositionAndResult(t *testing.T) {
	cluster := startTestCluster(t)
	global := currentGlobalNode(t, cluster)
	request := writeRequest("same-request", "b", "x", "1")
	first := cluster.Write(global, request)
	second := cluster.Write(global, request)
	if first.NormalErr != nil || second.NormalErr != nil ||
		first.Normal.GlobalIndex != second.Normal.GlobalIndex ||
		first.Normal.Result != second.Normal.Result {
		t.Fatalf("duplicate result changed: first=%+v second=%+v", first, second)
	}
	node, _ := cluster.Snapshot(global)
	if len(node.State.Log) != 1 {
		t.Fatalf("duplicate request appended another entry: %d", len(node.State.Log))
	}
	if got := cluster.Write(global, writeRequest("same-request", "b", "x", "different")); got.NormalErr == nil {
		t.Fatal("requestId reuse with a different command was accepted")
	}
}

func TestContinuousReplicationDoesNotSkipMissingEntry(t *testing.T) {
	cluster := startTestCluster(t)
	globalIdentity, _ := cluster.GlobalLeader()
	global := globalIdentity.DomainLeader.NodeID
	for _, domain := range cluster.Config().Domains() {
		if domain == globalIdentity.DomainLeader.DomainID {
			continue
		}
		domainLeader, _ := cluster.DomainLeader(domain)
		cluster.Network().Drop(global, domainLeader.NodeID, ReplicateEntryMessage)
	}
	first := cluster.Write(global, writeRequest("r1", "b", "x", "1"))
	if !errors.Is(first.NormalErr, ErrNoCommit) {
		t.Fatalf("write unexpectedly committed: %+v", first)
	}
	for _, domain := range cluster.Config().Domains() {
		if domain == globalIdentity.DomainLeader.DomainID {
			continue
		}
		domainLeader, _ := cluster.DomainLeader(domain)
		cluster.Network().Allow(global, domainLeader.NodeID, ReplicateEntryMessage)
	}
	second := cluster.Write(global, writeRequest("r2", "b", "y", "2"))
	if second.NormalErr == nil {
		t.Fatal("second entry skipped the absent first entry in remote domains")
	}
	if retry := cluster.Write(global, writeRequest("r1", "b", "x", "1")); retry.NormalErr != nil {
		t.Fatalf("first request did not recover: %+v", retry)
	}
	if retry := cluster.Write(global, writeRequest("r2", "b", "y", "2")); retry.NormalErr != nil {
		t.Fatalf("second request did not commit after prefix repair: %+v", retry)
	}
}

func TestOldGlobalLeaderCannotWriteAfterReelection(t *testing.T) {
	cluster := startTestCluster(t)
	old, _ := cluster.GlobalLeader()
	cluster.StopNode(old.DomainLeader.NodeID)
	if err := cluster.DetectAndReelect(); err != nil {
		t.Fatal(err)
	}
	cluster.Network().Recover(old.DomainLeader.NodeID)
	outcome := cluster.Write(old.DomainLeader.NodeID, writeRequest("stale", "b", "x", "1"))
	if !errors.Is(outcome.NormalErr, ErrNotGlobalLeader) && !errors.Is(outcome.NormalErr, ErrNotServing) {
		t.Fatalf("old global leader accepted a write: %+v", outcome)
	}
}

func TestFastReturnRequiresBothDomainQuorumsAndIndependentAck(t *testing.T) {
	cluster := startTestCluster(t)
	cluster.SetFastReturnEnabled(true)
	globalIdentity, _ := cluster.GlobalLeader()
	global := globalIdentity.DomainLeader.NodeID
	origin := "b"
	if globalIdentity.DomainLeader.DomainID == origin {
		origin = "c"
	}
	originLeader, _ := cluster.DomainLeader(origin)

	outcome := cluster.Write(global, writeRequest("fast", origin, "x", "1"))
	if outcome.FastErr != nil || outcome.Fast == nil || outcome.NormalErr != nil || outcome.Normal == nil {
		t.Fatalf("both independent success paths were not produced: %+v", outcome)
	}
	if outcome.Fast.Source != FastResponse || outcome.Normal.Source != GlobalResponse {
		t.Fatalf("response sources were conflated: %+v", outcome)
	}

	cluster.Network().Drop(global, originLeader.NodeID, GlobalDomainQuorumAckMessage)
	noGlobalAck := cluster.Write(global, writeRequest("no-global-ack", origin, "y", "2"))
	if noGlobalAck.Fast != nil || noGlobalAck.NormalErr != nil {
		t.Fatalf("missing global-domain ack did not safely fall back: %+v", noGlobalAck)
	}
}

func TestFastReturnSurvivesOriginAckLossButNormalPathUsesOtherDomain(t *testing.T) {
	cluster := startTestCluster(t)
	cluster.SetFastReturnEnabled(true)
	globalIdentity, _ := cluster.GlobalLeader()
	global := globalIdentity.DomainLeader.NodeID
	origin := "b"
	if globalIdentity.DomainLeader.DomainID == origin {
		origin = "c"
	}
	originLeader, _ := cluster.DomainLeader(origin)
	cluster.Network().Drop(originLeader.NodeID, global, DomainQuorumAckMessage)

	outcome := cluster.Write(global, writeRequest("ack-loss", origin, "x", "1"))
	if outcome.FastErr != nil || outcome.Fast == nil {
		t.Fatalf("origin domain could not Fast Return with a valid certificate: %+v", outcome)
	}
	if outcome.NormalErr != nil || outcome.Normal == nil {
		t.Fatalf("normal path did not independently commit through another domain: %+v", outcome)
	}
}

func TestFastReturnAfterImmediateReadWaitsForGlobalKnowledge(t *testing.T) {
	cluster := startTestCluster(t)
	cluster.SetFastReturnEnabled(true)
	globalIdentity, _ := cluster.GlobalLeader()
	global := globalIdentity.DomainLeader.NodeID
	origin := "b"
	if globalIdentity.DomainLeader.DomainID == origin {
		origin = "c"
	}
	originLeader, _ := cluster.DomainLeader(origin)
	cluster.Network().Drop(originLeader.NodeID, global, DomainQuorumAckMessage)
	for _, domain := range cluster.Config().Domains() {
		if domain == origin || domain == globalIdentity.DomainLeader.DomainID {
			continue
		}
		for _, node := range cluster.Config().DomainMembers(domain) {
			cluster.StopNode(node.ID)
		}
	}

	outcome := cluster.Write(global, writeRequest("fast-only", origin, "x", "1"))
	if outcome.FastErr != nil || outcome.Fast == nil || !errors.Is(outcome.NormalErr, ErrNoCommit) {
		t.Fatalf("expected Fast Return with delayed global knowledge: %+v", outcome)
	}
	if _, err := cluster.Read(global, "x"); !errors.Is(err, ErrReadBarrier) {
		t.Fatalf("read bypassed missing global commit knowledge: %v", err)
	}
	cluster.Network().Allow(originLeader.NodeID, global, DomainQuorumAckMessage)
	value, err := cluster.Read(global, "x")
	if err != nil || value != "1" {
		t.Fatalf("read did not become visible after evidence reconciliation: value=%q err=%v", value, err)
	}
}

func TestFastReturnFallsBackOnReplicationOrDomainFailure(t *testing.T) {
	tests := []struct {
		name      string
		breakPath func(*Cluster, GlobalLeaderIdentity, string, DomainLeaderIdentity)
	}{
		{
			name: "cross-domain replication loss",
			breakPath: func(c *Cluster, global GlobalLeaderIdentity, _ string, origin DomainLeaderIdentity) {
				c.Network().Drop(global.DomainLeader.NodeID, origin.NodeID, ReplicateEntryMessage)
			},
		},
		{
			name: "client domain majority loss",
			breakPath: func(c *Cluster, _ GlobalLeaderIdentity, originDomain string, origin DomainLeaderIdentity) {
				for _, node := range c.Config().DomainMembers(originDomain) {
					if node.ID != origin.NodeID {
						c.StopNode(node.ID)
					}
				}
			},
		},
		{
			name: "global domain majority loss",
			breakPath: func(c *Cluster, global GlobalLeaderIdentity, _ string, _ DomainLeaderIdentity) {
				for _, node := range c.Config().DomainMembers(global.DomainLeader.DomainID) {
					if node.ID != global.DomainLeader.NodeID {
						c.StopNode(node.ID)
					}
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := startTestCluster(t)
			cluster.SetFastReturnEnabled(true)
			global, _ := cluster.GlobalLeader()
			originDomain := "b"
			if global.DomainLeader.DomainID == originDomain {
				originDomain = "c"
			}
			origin, _ := cluster.DomainLeader(originDomain)
			test.breakPath(cluster, global, originDomain, origin)
			outcome := cluster.Write(global.DomainLeader.NodeID, writeRequest(test.name, originDomain, "x", "1"))
			if outcome.Fast != nil {
				t.Fatalf("unsafe Fast Return was produced: %+v", outcome)
			}
		})
	}
}
