package cdraft

import (
	"context"
	"testing"
	"time"

	"github.com/czq/cd-raft/internal/topology"
)

func enableFloatingEdge(h *rpcHarness) {
	for _, node := range h.nodes {
		node.config.Features.FloatingDomains = []string{"edge"}
		node.config.NetworkSimulation = topology.NetworkSimulation{
			Enabled: true,
			InterDomainOneWayMillis: map[string]int{
				"edge->a": 100,
				"edge->b": 5,
				"edge->c": 80,
				"a->b":    20,
				"a->c":    80,
			},
		}
		node.rtt.merge("a", map[string]float64{"b": 20, "c": 80})
	}
}

func TestRealGRPCFloatingClientDiscoveryTelemetryAndResponderRace(t *testing.T) {
	harness := newRPCHarness(t, true)
	enableFloatingEdge(harness)
	harness.elect(t)
	harness.nodes["a1"].SetResponseDelays(150*time.Millisecond, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entry := harness.config.Nodes[3].ListenAddress // b1, deliberately not the GL.
	view, err := harness.client.DiscoverTopology(ctx, entry, "edge")
	if err != nil {
		t.Fatal(err)
	}
	if view.GetGlobalLeader().GetNodeId() != "a1" || view.GetGlobalLeaderAddress() == "" {
		t.Fatalf("unexpected topology view: %+v", view)
	}
	if len(view.GetDomainLeaders()) != 3 {
		t.Fatalf("expected only consensus domain leaders, got %+v", view.GetDomainLeaders())
	}
	if err := harness.client.ReportFloatingLatency(ctx, view.GetGlobalLeaderAddress(), "edge", view.GetGlobalTerm(), map[string]float64{
		"a": 100,
		"b": 5,
		"c": 80,
	}); err != nil {
		t.Fatal(err)
	}

	decision, err := harness.client.Write(ctx, view.GetGlobalLeaderAddress(), ClientWriteRequest{
		RequestID:    "floating-fast-wins",
		OriginDomain: "edge",
		Command:      Command{Key: "floating", Value: "ok"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Winner != FastResponse {
		t.Fatalf("expected responder Fast Return to win, got %s (%+v)", decision.Winner, decision.Result)
	}
	if decision.Result.ResponderDomain != "b" {
		t.Fatalf("expected responder domain b, got %+v", decision.Result)
	}
	metrics, err := harness.client.Metrics(ctx, view.GetGlobalLeaderAddress())
	if err != nil {
		t.Fatal(err)
	}
	if metrics.GetFloatingResponderSelected() == 0 || metrics.GetFloatingResponderByDomain()["b"] == 0 {
		t.Fatalf("GL metrics did not record floating responder selection: %+v", metrics)
	}
}

func TestRealGRPCFloatingClientFallsBackToGLWithoutTelemetry(t *testing.T) {
	harness := newRPCHarness(t, true)
	enableFloatingEdge(harness)
	harness.elect(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	gl := harness.config.Nodes[0].ListenAddress
	decision, err := harness.client.Write(ctx, gl, ClientWriteRequest{
		RequestID:    "floating-gl-fallback",
		OriginDomain: "edge",
		Command:      Command{Key: "floating-fallback", Value: "ok"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if decision.Winner != GlobalResponse {
		t.Fatalf("expected GL normal response without telemetry, got %s", decision.Winner)
	}
}
