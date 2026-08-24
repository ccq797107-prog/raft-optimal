package cdraft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Task 9: the Metrics gRPC must expose enough real state to verify the core
// mechanisms, and per-node safety invariants must hold after activity.
func TestRealGRPCMetricsExposeCoreObservability(t *testing.T) {
	harness := newRPCHarness(t, true)
	harness.elect(t)
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(latencyProfile())
		node.SetRPCPolicy(RPCPolicy{Deadline: time.Second, MaxRetries: 2})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	target := mustNode(t, harness.config, "a1").ListenAddress

	// Steady cross-domain Fast Return writes plus reads from domain b populate
	// the per-domain request distribution and the measured RTT matrix — the raw
	// signals the external migration controller consumes.
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	go driveLoad(loadCtx, harness, target, "b")

	waitUntil(t, ctx, func() bool {
		metrics, err := harness.client.Metrics(ctx, target)
		return err == nil &&
			len(metrics.GetInterDomainRttMillis()) > 0 && metrics.GetWriteRequestsByDomain()["b"] > 0
	}, "metrics did not expose RTT and request distribution")

	metrics, err := harness.client.Metrics(ctx, target)
	if err != nil {
		t.Fatal(err)
	}

	// Leaders, terms, positions.
	if metrics.GetGlobalLeader().GetNodeId() != "a1" || metrics.GetGlobalTerm() == 0 {
		t.Fatalf("metrics missing global leader/term: %+v", metrics.GetGlobalLeader())
	}
	if len(metrics.GetDomainLeaders()) != 3 {
		t.Fatalf("metrics missing per-domain leaders: %v", metrics.GetDomainLeaders())
	}
	if metrics.GetKnownGlobalCommitIndex() == 0 || metrics.GetAppliedIndex() == 0 {
		t.Fatalf("metrics missing commit/apply positions: %+v", metrics)
	}
	if metrics.GetDomainQuorumIndex()["a"] == 0 {
		t.Fatalf("metrics missing global-domain quorum position: %v", metrics.GetDomainQuorumIndex())
	}
	// Per-domain request distribution (raw controller input).
	if metrics.GetWriteRequestsByDomain()["b"] == 0 {
		t.Fatalf("metrics missing per-domain write distribution: %v", metrics.GetWriteRequestsByDomain())
	}

	// Per-node invariants hold across all nodes after the activity.
	for _, node := range harness.nodes {
		if err := node.AssertInvariants(); err != nil {
			t.Fatalf("invariant violated: %v", err)
		}
	}
}

func requestSeq(prefix string, i int) string {
	return fmt.Sprintf("%s-%d", prefix, i)
}
