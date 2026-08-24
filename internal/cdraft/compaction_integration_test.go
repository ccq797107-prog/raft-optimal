package cdraft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestRealGRPCLogCompactionAndSnapshotCatchUp drives the production node
// end-to-end: automatic compaction at a small threshold, continued service
// across the snapshot cut, idempotent retries of compacted requests, and the
// InstallSnapshot path healing a domain that fell below the compaction floor.
func TestRealGRPCLogCompactionAndSnapshotCatchUp(t *testing.T) {
	harness := newRPCHarness(t, false)
	for _, node := range harness.nodes {
		// Aggressive policy so the test compacts within a handful of writes:
		// fold everything but the newest applied entry once >2 applied entries
		// sit above the floor.
		node.SetLogCompaction(2, 1)
		node.SetRPCPolicy(RPCPolicy{Deadline: 300 * time.Millisecond, MaxRetries: 2})
	}
	harness.elect(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	glAddr := mustNode(t, harness.config, "a1").ListenAddress

	// Cut domain c off from the Global Leader so it lags behind everything that
	// is about to be written and compacted.
	harness.nodes["a1"].SetPartitionedFrom("c1", "c2", "c3")

	write := func(id, key, value string) ClientResult {
		t.Helper()
		wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
		defer wcancel()
		decision, err := harness.client.Write(wctx, glAddr, ClientWriteRequest{
			RequestID: id, OriginDomain: "b", Command: Command{Key: key, Value: value},
		})
		if err != nil {
			t.Fatalf("write %s failed: %v", id, err)
		}
		return decision.Result
	}

	for i := 1; i <= 5; i++ {
		write(fmt.Sprintf("req-%d", i), fmt.Sprintf("key-%d", i), fmt.Sprintf("val-%d", i))
	}

	// The Global Leader applies on commit, so automatic compaction must kick in.
	waitUntil(t, ctx, func() bool {
		status, err := harness.client.Status(ctx, glAddr)
		return err == nil && status.GetSnapshotIndex() >= 3 && status.GetAppliedIndex() >= 5
	}, "global leader never compacted its applied log prefix")

	// Retrying a compacted request stays idempotent and returns the settled result.
	retried := write("req-2", "key-2", "val-2")
	if retried.GlobalIndex != 2 || retried.Result != "key-2=val-2" || !retried.Committed {
		t.Fatalf("compacted request retry broke idempotency: %+v", retried)
	}

	// Linearizable reads still see every compacted value.
	for i := 1; i <= 5; i++ {
		value, err := harness.client.Read(ctx, glAddr, fmt.Sprintf("key-%d", i))
		if err != nil || value != fmt.Sprintf("val-%d", i) {
			t.Fatalf("compacted value unreadable: key-%d=%q err=%v", i, value, err)
		}
	}

	// Heal the partition. Domain c sits below the compaction floor, so the next
	// write can only reach it through InstallSnapshot + tail backfill.
	harness.nodes["a1"].HealPartition()
	write("req-after-heal", "key-heal", "val-heal")

	waitUntil(t, ctx, func() bool {
		for _, id := range []string{"c1", "c2", "c3"} {
			status, err := harness.client.Status(ctx, mustNode(t, harness.config, id).ListenAddress)
			if err != nil || status.GetAppliedIndex() < 5 {
				return false
			}
		}
		return true
	}, "lagging domain was not healed through the snapshot path")

	// The healed Domain Leader must have received an actual snapshot (its own
	// floor advanced past entries it never held in its log).
	status, err := harness.client.Status(ctx, mustNode(t, harness.config, "c1").ListenAddress)
	if err != nil {
		t.Fatal(err)
	}
	if status.GetSnapshotIndex() == 0 {
		t.Fatalf("healed domain leader has no snapshot installed: %+v", status)
	}
	for _, node := range harness.nodes {
		if err := node.AssertInvariants(); err != nil {
			t.Fatal(err)
		}
	}
}
