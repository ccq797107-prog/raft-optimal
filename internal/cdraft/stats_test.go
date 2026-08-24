package cdraft

import (
	"math"
	"testing"
	"time"
)

func TestStatsWindowSlidesAndCountsPerDomain(t *testing.T) {
	now := time.Unix(0, 0)
	window := newStatsWindow(10 * time.Second)
	window.record("a", false, now)
	window.record("a", false, now.Add(time.Second))
	window.record("b", true, now.Add(2*time.Second))
	writes, reads := window.counts(now.Add(3 * time.Second))
	if writes["a"] != 2 || reads["b"] != 1 {
		t.Fatalf("unexpected counts writes=%v reads=%v", writes, reads)
	}
	// Slide past the window: original events expire.
	writes, reads = window.counts(now.Add(20 * time.Second))
	if len(writes) != 0 || len(reads) != 0 {
		t.Fatalf("window did not slide: writes=%v reads=%v", writes, reads)
	}
}

func TestRTTStoreSmoothsAndHalvesRTT(t *testing.T) {
	store := newRTTStore()
	store.observeRTT("a", "b", 100*time.Millisecond)
	snap := store.localSnapshot("a")
	if snap["b"] != 50 {
		t.Fatalf("one-way should be RTT/2=50, got %v", snap["b"])
	}
	store.observeRTT("a", "b", 200*time.Millisecond) // one-way 100
	snap = store.localSnapshot("a")
	if math.Abs(snap["b"]-(0.7*50+0.3*100)) > 1e-6 {
		t.Fatalf("smoothing not applied: %v", snap["b"])
	}
}
