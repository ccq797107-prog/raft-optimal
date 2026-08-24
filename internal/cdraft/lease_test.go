package cdraft

import (
	"context"
	"testing"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestGlobalLeaderReadLeaseGatesStaleReads guards issue #1: an isolated/old
// Global Leader must not keep serving (potentially stale) reads. With the lease
// enforced, a fresh lease serves reads; once the lease lapses (no quorum acks)
// reads are refused with a retryable Unavailable and the leader steps down.
func TestGlobalLeaderReadLeaseGatesStaleReads(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()
	a1 := harness.nodes["a1"]

	a1.SetGlobalLeaseDuration(200 * time.Millisecond)
	a1.SetGlobalLeaseEnabled(true)
	a1.mu.Lock()
	a1.refreshGlobalLeaseLocked(time.Now())
	a1.mu.Unlock()

	if _, err := a1.Read(ctx, &cdraftv1.ClientReadRequest{Key: "k"}); err != nil {
		t.Fatalf("read under a fresh lease should succeed: %v", err)
	}

	// Simulate isolation: the lease can no longer be refreshed and lapses.
	a1.mu.Lock()
	a1.globalLeaseDeadline = time.Now().Add(-time.Millisecond)
	a1.mu.Unlock()

	_, err := a1.Read(ctx, &cdraftv1.ClientReadRequest{Key: "k"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("read after lease expiry must be Unavailable, got %v", err)
	}

	// The failure detector relinquishes global leadership so a new GL elsewhere
	// is unopposed.
	a1.mu.Lock()
	stepped := a1.maybeStepDownExpiredGlobalLeaseLocked(time.Now())
	role, stage := a1.globalRole, a1.stage
	a1.mu.Unlock()
	if !stepped {
		t.Fatal("expected the leader to step down after lease expiry")
	}
	if role == Leader {
		t.Fatalf("global role should not remain Leader after step-down, got %v", role)
	}
	if stage != GlobalElecting {
		t.Fatalf("stage after step-down = %v, want GlobalElecting", stage)
	}
}

// TestGlobalLeaderReadLeaseDisabledByDefault ensures the manual (loop-less) test
// harness is unaffected: without the live runtime the lease is not enforced.
func TestGlobalLeaderReadLeaseDisabledByDefault(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	a1 := harness.nodes["a1"]

	a1.mu.Lock()
	a1.globalLeaseDeadline = time.Time{} // zero / "expired"
	enabled := a1.globalLeaseEnabled
	serves := a1.servesAsGlobalLeaderLocked()
	a1.mu.Unlock()

	if enabled {
		t.Fatal("lease should be disabled when running under the manual harness")
	}
	if !serves {
		t.Fatal("with the lease disabled the GL must still serve regardless of deadline")
	}
}
