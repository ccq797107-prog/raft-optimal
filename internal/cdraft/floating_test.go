package cdraft

import (
	"testing"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"github.com/czq/cd-raft/internal/topology"
)

func floatingTestConfig() topology.Config {
	return topology.Config{
		ApplicationGroup: topology.ApplicationGroup,
		Nodes: []topology.Node{
			{ID: "a1", DomainCode: "a", ListenAddress: "a1-client", InterDomainAddress: "a1-domain"},
			{ID: "b1", DomainCode: "b", ListenAddress: "b1-client", InterDomainAddress: "b1-domain"},
			{ID: "c1", DomainCode: "c", ListenAddress: "c1-client", InterDomainAddress: "c1-domain"},
		},
		Features: topology.Features{
			FastReturnEnabled:          true,
			FloatingDomains:            []string{"edge"},
			FloatingTelemetryTTLMillis: 5,
		},
		NetworkSimulation: topology.NetworkSimulation{
			Enabled:                 true,
			InterDomainOneWayMillis: map[string]int{"a->b": 20, "a->c": 80},
		},
	}
}

func servingFloatingTestNode(t *testing.T) *RPCNode {
	t.Helper()
	node, err := NewRPCNode(floatingTestConfig(), "a1", NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	a := DomainLeaderIdentity{DomainID: "a", NodeID: "a1", DomainTerm: 1}
	b := DomainLeaderIdentity{DomainID: "b", NodeID: "b1", DomainTerm: 1}
	c := DomainLeaderIdentity{DomainID: "c", NodeID: "c1", DomainTerm: 1}
	node.stage = Serving
	node.domainRole = Leader
	node.globalRole = Leader
	node.state.DomainLeader = a
	node.globalLeader = GlobalLeaderIdentity{DomainLeader: a, GlobalTerm: 1}
	node.state.GlobalLeader = node.globalLeader
	node.state.GlobalTerm = 1
	node.domainLeaders = map[string]DomainLeaderIdentity{"a": a, "b": b, "c": c}
	return node
}

func TestFloatingLatencyReportValidationAndTTL(t *testing.T) {
	node := servingFloatingTestNode(t)
	report := &cdraftv1.FloatingLatencyReport{
		OriginDomain: "edge",
		GlobalTerm:   1,
		Rtts: []*cdraftv1.FloatingDomainRtt{
			{ToDomain: "a", OneWayMillis: 100},
			{ToDomain: "b", OneWayMillis: 10},
		},
	}
	if err := node.ingestFloatingLatencyReport(report); err != nil {
		t.Fatal(err)
	}
	if got, ok := node.floatingLatency.oneWay("edge", "b", time.Now()); !ok || got != 10 {
		t.Fatalf("missing fresh floating latency: got=%v ok=%t", got, ok)
	}
	if err := node.ingestFloatingLatencyReport(&cdraftv1.FloatingLatencyReport{
		OriginDomain: "a", GlobalTerm: 1,
		Rtts: []*cdraftv1.FloatingDomainRtt{{ToDomain: "b", OneWayMillis: 10}},
	}); err == nil {
		t.Fatal("consensus origin must not be accepted as floating latency")
	}
	if err := node.ingestFloatingLatencyReport(&cdraftv1.FloatingLatencyReport{
		OriginDomain: "edge", GlobalTerm: 0,
		Rtts: []*cdraftv1.FloatingDomainRtt{{ToDomain: "b", OneWayMillis: 10}},
	}); err == nil {
		t.Fatal("stale global term must be rejected")
	}
	time.Sleep(10 * time.Millisecond)
	if _, ok := node.floatingLatency.oneWay("edge", "b", time.Now()); ok {
		t.Fatal("expired floating latency must not remain fresh")
	}
}

func TestFloatingReturnPlannerSelectsConsensusResponder(t *testing.T) {
	node := servingFloatingTestNode(t)
	now := time.Now()
	node.floatingLatency.observe("edge", "a", 100, now)
	node.floatingLatency.observe("edge", "b", 10, now)
	node.floatingLatency.observe("edge", "c", 50, now)
	node.rtt.merge("a", map[string]float64{"b": 20, "c": 80})

	plan := node.planReturnPath("edge", "a", "127.0.0.1:9000")
	if !plan.Floating || plan.Responder.DomainID != "b" {
		t.Fatalf("expected floating responder b, got %+v", plan)
	}
	consensus := node.planReturnPath("b", "a", "127.0.0.1:9000")
	if consensus.Floating || consensus.Responder.DomainID != "b" {
		t.Fatalf("consensus origin should keep origin-domain responder: %+v", consensus)
	}
	noReply := node.planReturnPath("edge", "a", "")
	if noReply.Responder.Valid() {
		t.Fatalf("empty reply route must not select floating responder: %+v", noReply)
	}
}

func TestFloatingDomainNeverCreatesQuorumState(t *testing.T) {
	node := servingFloatingTestNode(t)
	node.stats.record("edge", false, time.Now())
	if _, ok := node.state.DomainQuorumIndex["edge"]; ok {
		t.Fatal("floating domain leaked into DomainQuorumIndex")
	}
	if node.config.IsConsensusDomain("edge") {
		t.Fatal("floating domain classified as consensus")
	}
	if members := node.config.DomainMembers("edge"); len(members) != 0 {
		t.Fatalf("floating domain has members: %#v", members)
	}
}
