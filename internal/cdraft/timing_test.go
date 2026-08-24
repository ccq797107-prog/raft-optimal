package cdraft

import (
	"testing"
	"time"
)

// TestValidateLeaseTimingAcceptsDefaults guards the shipped defaults: the
// production lease durations must satisfy the no-dual-leader timing constraints.
func TestValidateLeaseTimingAcceptsDefaults(t *testing.T) {
	if err := validateLeaseTiming(400*time.Millisecond, 400*time.Millisecond); err != nil {
		t.Fatalf("default lease timing must be safe, got error: %v", err)
	}
}

// TestValidateLeaseTimingRejectsUnsafeConfigs ensures each safety relationship is
// actually enforced, so a config-induced split brain cannot start.
func TestValidateLeaseTimingRejectsUnsafeConfigs(t *testing.T) {
	cases := []struct {
		name        string
		globalLease time.Duration
		domainLease time.Duration
	}{
		{"non-positive global", 0, 400 * time.Millisecond},
		{"non-positive domain", 400 * time.Millisecond, 0},
		{"global lease at election floor", minGlobalElectionTimeout, 400 * time.Millisecond},
		{"global lease past election floor", minGlobalElectionTimeout + time.Millisecond, 400 * time.Millisecond},
		{"domain lease at election floor", 400 * time.Millisecond, minDomainElectionTimeout},
		{"heartbeat not below global lease", heartbeatInterval, 400 * time.Millisecond},
		{"heartbeat not below domain lease", 400 * time.Millisecond, heartbeatInterval},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateLeaseTiming(tc.globalLease, tc.domainLease); err == nil {
				t.Fatalf("expected unsafe timing to be rejected: global=%s domain=%s",
					tc.globalLease, tc.domainLease)
			}
		})
	}
}

// TestStickinessWindowCoversGlobalLease documents the invariant the runtime relies
// on: the leader-stickiness window is at least the global election floor, so an
// incumbent's lease always lapses before any rival could be elected.
func TestStickinessWindowCoversGlobalLease(t *testing.T) {
	if globalStickyWindow != minGlobalElectionTimeout {
		t.Fatalf("stickiness window (%s) must equal min global election timeout (%s)",
			globalStickyWindow, minGlobalElectionTimeout)
	}
}
