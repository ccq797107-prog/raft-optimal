package cdraft

import (
	"testing"

	"github.com/czq/cd-raft/internal/topology"
)

func TestEstimateWritePathCostCoversConsensusAndFloatingOrigins(t *testing.T) {
	cfg := writeCostTestConfig()

	same := EstimateWritePathCost(cfg, WritePathCostInput{
		OriginDomain: "b", GlobalDomain: "b", FastReturn: true,
	})
	if same.FastestSecondQuorumDomain != "a" || same.LowerBoundMillis != 100 || same.CallbackAvailable {
		t.Fatalf("same-domain cost = %+v, want second=a lower=100 no callback", same)
	}

	cross := EstimateWritePathCost(cfg, WritePathCostInput{
		OriginDomain: "a", GlobalDomain: "b", FastReturn: true,
	})
	if cross.FastestSecondQuorumDomain != "a" || cross.ResponderDomain != "a" ||
		cross.GlobalDirectMillis != 200 || cross.CallbackMillis != 100 ||
		cross.LowerBoundMillis != 100 || cross.TheoreticalWinner != FastResponse {
		t.Fatalf("cross-domain cost = %+v, want direct=200 callback=100 lower=100 fast", cross)
	}

	floating := EstimateWritePathCost(cfg, WritePathCostInput{
		OriginDomain: "edge", GlobalDomain: "b", FastReturn: true,
	})
	if floating.FastestSecondQuorumDomain != "a" || floating.ResponderDomain != "c" ||
		floating.GlobalDirectMillis != 150 || floating.CallbackMillis != 175 ||
		floating.LowerBoundMillis != 150 || floating.TheoreticalWinner != GlobalResponse {
		t.Fatalf("floating cost = %+v, want direct=150 callback=175 lower=150 global", floating)
	}
}

func writeCostTestConfig() topology.Config {
	nodes := []topology.Node{
		{ID: "a1", DomainCode: "a", ListenAddress: "a1", InterDomainAddress: "ia1"},
		{ID: "b1", DomainCode: "b", ListenAddress: "b1", InterDomainAddress: "ib1"},
		{ID: "c1", DomainCode: "c", ListenAddress: "c1", InterDomainAddress: "ic1"},
	}
	return topology.Config{
		ApplicationGroup: topology.ApplicationGroup,
		Nodes:            nodes,
		Features: topology.Features{
			FastReturnEnabled: true,
			FloatingDomains:   []string{"edge"},
		},
		NetworkSimulation: topology.NetworkSimulation{
			Enabled: true,
			InterDomainOneWayMillis: map[string]int{
				"a->b": 50, "b->a": 50,
				"a->c": 80, "c->a": 80,
				"b->c": 60, "c->b": 60,
				"edge->a": 120, "a->edge": 120,
				"edge->b": 25, "b->edge": 25,
				"edge->c": 90, "c->edge": 90,
			},
		},
	}
}
