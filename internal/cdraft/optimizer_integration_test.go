package cdraft

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"github.com/czq/cd-raft/internal/optimizer"
	"github.com/czq/cd-raft/internal/topology"
)

func latencyProfile() topology.NetworkSimulation {
	return topology.NetworkSimulation{
		Enabled: true, LocalOneWayDelayMillis: 1,
		InterDomainOneWayMillis: map[string]int{
			"a->b": 50, "b->a": 50,
			"a->c": 80, "c->a": 80,
			"b->c": 60, "c->b": 60,
		},
	}
}

// drives steady write+read load from one origin domain to the Global Leader
// until the context is cancelled. Errors are tolerated (re-elections etc.).
func driveLoad(ctx context.Context, harness *rpcHarness, target, origin string) {
	var seq int64
	for ctx.Err() == nil {
		i := atomic.AddInt64(&seq, 1)
		writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, _ = harness.client.Write(writeCtx, target, ClientWriteRequest{
			RequestID: fmt.Sprintf("load-%s-%d", origin, i), OriginDomain: origin,
			Command: Command{Key: fmt.Sprintf("k-%s-%d", origin, i), Value: "v"},
		})
		_, _ = harness.client.ReadFrom(writeCtx, target, origin, fmt.Sprintf("k-%s-%d", origin, i))
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Millisecond):
		}
	}
}

// The core only MEASURES and EXPOSES raw signals; it never decides or migrates
// on its own. This test proves the decoupling end to end: from the raw window
// statistics + RTT matrix the node publishes over Metrics, the EXTERNAL cost
// model (internal/optimizer) recommends the load domain, while the core leaves
// the Global Leader untouched.
func TestRealGRPCMetricsFeedExternalCostModel(t *testing.T) {
	harness := newRPCHarness(t, false)
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

	// Steady load originates entirely in domain b. The paper model then favors
	// co-locating the Global Leader in b (writes pay one nearest-domain RTT and
	// reads become free), while the current Global Leader stays in domain a.
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	go driveLoad(loadCtx, harness, target, "b")

	domains := harness.config.Domains()
	// Wait until the EXTERNAL cost model, fed only by the published metrics,
	// recommends domain b. The recommendation is computed in the test (acting as
	// the mover), never by the node.
	waitUntil(t, ctx, func() bool {
		metrics, err := harness.client.Metrics(ctx, target)
		if err != nil {
			return false
		}
		result := optimizer.ComputeCosts(metricsToOptimizerInput(domains, "a", metrics))
		return result.Recommended == "b" && len(result.Candidates) == 3
	}, "external cost model did not converge to the load domain from published metrics")

	stopLoad()
	metrics, err := harness.client.Metrics(ctx, target)
	if err != nil {
		t.Fatal(err)
	}

	// Read-only guarantee: the core never migrated on its own.
	if metrics.GetMigrations() != 0 {
		t.Fatalf("core migrated without a Move request: %d", metrics.GetMigrations())
	}
	if metrics.GetGlobalLeader().GetNodeId() != "a1" {
		t.Fatalf("core changed the Global Leader on its own: %+v", metrics.GetGlobalLeader())
	}

	// Candidate cost ordering must reflect the paper model: b cheaper than a.
	result := optimizer.ComputeCosts(metricsToOptimizerInput(domains, "a", metrics))
	cost := map[string]float64{}
	for _, candidate := range result.Candidates {
		cost[candidate.Domain] = candidate.Ls
	}
	if cost["b"] == 0 || cost["a"] == 0 || cost["b"] >= cost["a"] {
		t.Fatalf("expected predicted cost b < a from b-origin load, got %+v", cost)
	}

	// Measured RTT must be real and roughly match the injected one-way delays.
	rtt := metrics.GetInterDomainRttMillis()
	if ab := rtt["a->b"]; ab < 80 || ab > 130 {
		t.Fatalf("measured a->b RTT not near 100ms: %dms (matrix=%v)", ab, rtt)
	}

	// Consensus still works during/after the controller observes metrics.
	decision, err := harness.client.Write(ctx, target, ClientWriteRequest{
		RequestID: "after-metrics", OriginDomain: "b",
		Command: Command{Key: "post", Value: "ok"},
	})
	if err != nil || !decision.Result.Committed {
		t.Fatalf("write failed after metrics were observed: decision=%+v err=%v", decision, err)
	}
}

// metricsToOptimizerInput mirrors what cmd/cdraft-mover does: turn the node's
// published raw window statistics into the external cost model's input.
func metricsToOptimizerInput(domains []string, glDomain string, metrics *cdraftv1.MetricsResponse) optimizer.Input {
	writes := make(map[string]float64)
	for d, c := range metrics.GetWriteRequestsByDomain() {
		writes[d] = float64(c)
	}
	reads := make(map[string]float64)
	for d, c := range metrics.GetReadRequestsByDomain() {
		reads[d] = float64(c)
	}
	oneWay := make(map[string]map[string]float64)
	for key, r := range metrics.GetInterDomainRttMillis() {
		parts := strings.SplitN(key, "->", 2)
		if len(parts) != 2 {
			continue
		}
		if oneWay[parts[0]] == nil {
			oneWay[parts[0]] = make(map[string]float64)
		}
		oneWay[parts[0]][parts[1]] = float64(r) / 2
	}
	leaders := metrics.GetDomainLeaders()
	available := make(map[string]bool, len(domains))
	for _, d := range domains {
		available[d] = d == glDomain || leaders[d] != ""
	}
	return optimizer.Input{Domains: domains, Writes: writes, Reads: reads, Available: available, OneWayMillis: oneWay}
}
