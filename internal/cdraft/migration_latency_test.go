package cdraft

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"
)

// TestRealGRPCMigrationLatency measures how long an automatic Global Leader
// handoff ("切主") takes over the PRODUCTION gRPC path. It drives the same Move
// RPC the external controller (cmd/cdraft-mover) uses and times two things per
// handoff:
//
//  1. moveLatency — wall time for the Move RPC to return accepted. handleMove ->
//     initiateMigration is synchronous: it runs the full safe handoff (N-1
//     pre-check -> barrier+drain -> catch-up the target -> higher-term election
//     -> adopt the new GL) before returning, so this single number IS "how long
//     one switch takes" from the controller's point of view.
//  2. serveLatency — wall time from the moment the Move was issued until the
//     whole cluster has reconverged on a UNIQUE serving Global Leader in the
//     target domain. This is the externally observable "time to restored
//     service" and always >= moveLatency.
//
// Background write load keeps arriving throughout, so every handoff has a real,
// non-trivial catch-up tail (not a quiet log), reflecting realistic cost. The
// run rotates the leadership through domains a -> b -> c repeatedly so we sample
// many independent handoffs and report their distribution.
func TestRealGRPCMigrationLatency(t *testing.T) {
	if os.Getenv("CDRAFT_RUN_MIGRATION_LATENCY") != "1" {
		t.Skip("migration latency experiment is explicit; set CDRAFT_RUN_MIGRATION_LATENCY=1 to run")
	}
	harness := newRPCHarness(t, false)
	harness.elect(t)
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(latencyProfile())
		node.SetRPCPolicy(RPCPolicy{Deadline: 2 * time.Second, MaxRetries: 2})
		node.SetCatchUpTimeout(8 * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	// A fixed seed node the client always talks to: the client follows redirects
	// to whoever is currently GL, so a single load driver keeps working across
	// every migration and grows the log the next catch-up has to converge to.
	seed := mustNode(t, harness.config, "a1").ListenAddress
	loadCtx, stopLoad := context.WithCancel(ctx)
	defer stopLoad()
	go driveLoad(loadCtx, harness, seed, "b")

	const iterations = 9
	rotation := []string{"b", "c", "a"}

	var moveLatencies, serveLatencies []time.Duration
	for i := 0; i < iterations; i++ {
		target := rotation[i%len(rotation)]
		var glNode, glDomain, glAddr string

		// A handoff can transiently fail at the election step when a domain
		// leader is momentarily re-stabilizing after the previous back-to-back
		// move (the incumbent relinquishes and the cluster re-campaigns). That is
		// a real recoverable window, not a perf signal, so we retry until one
		// handoff actually completes and only time the successful attempt.
		var (
			start       time.Time
			moveElapsed time.Duration
			newTerm     uint64
		)
		for attempt := 0; ; attempt++ {
			glNode, glDomain, glAddr = currentGlobalLeader(t, harness, ctx)
			if glNode == "" {
				t.Fatalf("iteration %d: no global leader visible to migrate from", i)
			}
			if target == glDomain {
				target = rotation[(i+1)%len(rotation)]
			}
			start = time.Now()
			moveCtx, moveCancel := context.WithTimeout(ctx, 30*time.Second)
			r, err := harness.client.Move(moveCtx, glAddr, target, "latency-probe")
			moveCancel()
			if err == nil && r.GetAccepted() {
				moveElapsed = time.Since(start)
				newTerm = r.GetNewGlobalTerm()
				break
			}
			if attempt >= 8 {
				t.Fatalf("iteration %d: move %s(%s) -> %s never accepted after %d attempts: resp=%+v err=%v",
					i, glNode, glDomain, target, attempt+1, r, err)
			}
			t.Logf("handoff #%d %s(%s) -> %s transient attempt %d failed (resp=%q err=%v); retrying",
				i+1, glNode, glDomain, target, attempt+1, r.GetError(), err)
			time.Sleep(300 * time.Millisecond)
		}

		// Time-to-restored-service: wait until every node reports Serving, agrees
		// on a single Global Leader, and that leader lives in the target domain.
		settleCtx, settleCancel := context.WithTimeout(ctx, 30*time.Second)
		waitUntil(t, settleCtx, func() bool {
			agreed := ""
			leaders := 0
			for _, configured := range harness.config.Nodes {
				status, err := harness.client.Status(settleCtx, configured.ListenAddress)
				if err != nil {
					return false
				}
				leader := status.GetGlobalLeader().GetNodeId()
				if status.GetStage() != string(Serving) || leader == "" ||
					status.GetGlobalLeader().GetDomainId() != target {
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
			return leaders == 1
		}, fmt.Sprintf("iteration %d: cluster did not reconverge on a unique serving GL in domain %s", i, target))
		settleCancel()
		serveElapsed := time.Since(start)

		moveLatencies = append(moveLatencies, moveElapsed)
		serveLatencies = append(serveLatencies, serveElapsed)
		t.Logf("handoff #%d %s(%s) -> %s: move RPC=%v, time-to-serve=%v (term %d)",
			i+1, glNode, glDomain, target, moveElapsed.Round(time.Millisecond),
			serveElapsed.Round(time.Millisecond), newTerm)
	}

	stopLoad()

	reportLatency(t, "Move RPC (handoff completes)", moveLatencies)
	reportLatency(t, "time-to-serve (full reconvergence)", serveLatencies)
}

// currentGlobalLeader polls nodes (mirroring the mover's discovery) until one
// reports the current Global Leader, returning its node id, domain, and the
// client address to send the next Move RPC to.
func currentGlobalLeader(t *testing.T, harness *rpcHarness, ctx context.Context) (node, domain, addr string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, configured := range harness.config.Nodes {
			mctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			metrics, err := harness.client.Metrics(mctx, configured.ListenAddress)
			cancel()
			if err != nil {
				continue
			}
			gl := metrics.GetGlobalLeader()
			if gl.GetNodeId() != "" {
				return gl.GetNodeId(), gl.GetDomainId(), mustNode(t, harness.config, gl.GetNodeId()).ListenAddress
			}
		}
		select {
		case <-ctx.Done():
			return "", "", ""
		case <-time.After(50 * time.Millisecond):
		}
	}
	return "", "", ""
}

// reportLatency logs the min/p50/p90/max/mean of a latency sample set.
func reportLatency(t *testing.T, label string, samples []time.Duration) {
	t.Helper()
	if len(samples) == 0 {
		t.Logf("%s: no samples", label)
		return
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	n := len(sorted)
	p90 := int(float64(n) * 0.9)
	if p90 >= n {
		p90 = n - 1
	}
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	t.Logf("%s over %d handoffs: min=%v p50=%v p90=%v max=%v mean=%v",
		label, n,
		sorted[0].Round(time.Millisecond),
		sorted[n/2].Round(time.Millisecond),
		sorted[p90].Round(time.Millisecond),
		sorted[n-1].Round(time.Millisecond),
		(sum / time.Duration(n)).Round(time.Millisecond))
}
