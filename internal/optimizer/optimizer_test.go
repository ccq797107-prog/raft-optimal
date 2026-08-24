package optimizer

import (
	"math"
	"testing"
	"time"
)

func symMatrix(values map[[2]string]float64) map[string]map[string]float64 {
	out := make(map[string]map[string]float64)
	add := func(from, to string, v float64) {
		if out[from] == nil {
			out[from] = make(map[string]float64)
		}
		out[from][to] = v
	}
	for pair, v := range values {
		add(pair[0], pair[1], v)
		add(pair[1], pair[0], v)
	}
	return out
}

func TestComputeCostsPaperModel(t *testing.T) {
	// One-way latencies (RTT/2): a-b=25, a-c=40, b-c=30.
	latency := symMatrix(map[[2]string]float64{
		{"a", "b"}: 25,
		{"a", "c"}: 40,
		{"b", "c"}: 30,
	})

	tests := []struct {
		name       string
		in         Input
		wantRecomm string
		wantCosts  map[string]float64
		wantCommit map[string]bool
	}{
		{
			name: "all available symmetric load picks lowest total",
			in: Input{
				Domains:      []string{"a", "b", "c"},
				Writes:       map[string]float64{"a": 10, "b": 10, "c": 10},
				Reads:        map[string]float64{"a": 10, "b": 10, "c": 10},
				Available:    map[string]bool{"a": true, "b": true, "c": true},
				OneWayMillis: latency,
			},
			wantRecomm: "b",
			wantCosts:  map[string]float64{"a": 3100, "b": 2700, "c": 3400},
			wantCommit: map[string]bool{"a": true, "b": true, "c": true},
		},
		{
			name: "same-domain client reads are free for candidate",
			in: Input{
				Domains:      []string{"a", "b", "c"},
				Writes:       map[string]float64{"a": 0, "b": 0, "c": 0},
				Reads:        map[string]float64{"a": 100, "b": 0, "c": 0},
				Available:    map[string]bool{"a": true, "b": true, "c": true},
				OneWayMillis: latency,
			},
			wantRecomm: "a",
			wantCosts:  map[string]float64{"a": 0, "b": 2 * 25 * 100, "c": 2 * 40 * 100},
			wantCommit: map[string]bool{"a": true, "b": true, "c": true},
		},
		{
			name: "unavailable source domain uses relay formula",
			in: Input{
				Domains:      []string{"a", "b", "c"},
				Writes:       map[string]float64{"a": 0, "b": 0, "c": 10},
				Reads:        map[string]float64{"a": 0, "b": 0, "c": 5},
				Available:    map[string]bool{"a": true, "b": true, "c": false},
				OneWayMillis: latency,
			},
			wantRecomm: "b",
			wantCosts:  map[string]float64{"a": 1700, "b": 1400},
			wantCommit: map[string]bool{"a": true, "b": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeCosts(tt.in)
			if got.Recommended != tt.wantRecomm {
				t.Fatalf("recommended=%q want %q (costs=%+v)", got.Recommended, tt.wantRecomm, got.Candidates)
			}
			for _, candidate := range got.Candidates {
				if want, ok := tt.wantCosts[candidate.Domain]; ok {
					if math.Abs(candidate.Ls-want) > 1e-6 {
						t.Fatalf("domain %s Ls=%v want %v", candidate.Domain, candidate.Ls, want)
					}
				}
				if want, ok := tt.wantCommit[candidate.Domain]; ok && candidate.CanCommit != want {
					t.Fatalf("domain %s canCommit=%v want %v", candidate.Domain, candidate.CanCommit, want)
				}
			}
		})
	}
}

func TestComputeCostsNoCommitPartner(t *testing.T) {
	in := Input{
		Domains:      []string{"a", "b"},
		Writes:       map[string]float64{"a": 5, "b": 5},
		Reads:        map[string]float64{"a": 5, "b": 5},
		Available:    map[string]bool{"a": true, "b": false},
		OneWayMillis: symMatrix(map[[2]string]float64{{"a", "b"}: 25}),
	}
	got := ComputeCosts(in)
	if got.Recommended != "" {
		t.Fatalf("single available domain must not be recommendable: %q", got.Recommended)
	}
	for _, candidate := range got.Candidates {
		if candidate.Domain == "a" && candidate.CanCommit {
			t.Fatalf("domain a has no commit partner but was marked committable")
		}
	}
}

func TestComputeCostsMissingLatencyExcludesCandidate(t *testing.T) {
	in := Input{
		Domains:   []string{"a", "b", "c"},
		Writes:    map[string]float64{"a": 1, "b": 1, "c": 1},
		Reads:     map[string]float64{},
		Available: map[string]bool{"a": true, "b": true, "c": true},
		OneWayMillis: symMatrix(map[[2]string]float64{
			{"a", "b"}: 25,
			{"a", "c"}: 40,
		}),
	}
	got := ComputeCosts(in)
	for _, candidate := range got.Candidates {
		if candidate.Domain == "c" && candidate.CanCommit {
			t.Fatalf("candidate c with missing b<->c latency should not be committable")
		}
	}
	if got.Recommended == "c" {
		t.Fatalf("candidate with missing latency must not be recommended")
	}
}

func TestComputeCostsDeterministicTieBreak(t *testing.T) {
	latency := symMatrix(map[[2]string]float64{
		{"a", "b"}: 30,
		{"a", "c"}: 30,
		{"b", "c"}: 30,
	})
	in := Input{
		Domains:      []string{"a", "b", "c"},
		Writes:       map[string]float64{"a": 10, "b": 10, "c": 10},
		Reads:        map[string]float64{"a": 10, "b": 10, "c": 10},
		Available:    map[string]bool{"a": true, "b": true, "c": true},
		OneWayMillis: latency,
	}
	first := ComputeCosts(in).Recommended
	for i := 0; i < 20; i++ {
		if got := ComputeCosts(in).Recommended; got != first {
			t.Fatalf("non-deterministic recommendation: %q vs %q", got, first)
		}
	}
	if first != "a" {
		t.Fatalf("symmetric tie should pick lowest domain id, got %q", first)
	}
}

func TestComputeCostsFloatingDemandChangesRecommendation(t *testing.T) {
	latency := symMatrix(map[[2]string]float64{
		{"a", "b"}: 20,
		{"a", "c"}: 80,
		{"b", "c"}: 40,
	})
	latency["edge"] = map[string]float64{"a": 100, "b": 60, "c": 10}
	latency["a"]["edge"] = 100
	latency["b"]["edge"] = 60
	latency["c"]["edge"] = 10

	got := ComputeCosts(Input{
		ConsensusDomains: []string{"a", "b", "c"},
		FloatingDomains:  []string{"edge"},
		Writes:           map[string]float64{"edge": 10},
		Reads:            map[string]float64{},
		Available:        map[string]bool{"a": true, "b": true, "c": true},
		OneWayMillis:     latency,
	})
	if got.Recommended != "c" {
		t.Fatalf("floating demand near c should recommend c, got %q (%+v)", got.Recommended, got.Candidates)
	}
	for _, candidate := range got.Candidates {
		if candidate.Domain == "edge" {
			t.Fatalf("floating domain must not appear as candidate: %+v", got.Candidates)
		}
	}
}

func TestComputeCostsMissingFloatingLatencyDoesNotCreateCandidate(t *testing.T) {
	got := ComputeCosts(Input{
		ConsensusDomains: []string{"a", "b"},
		FloatingDomains:  []string{"edge"},
		Writes:           map[string]float64{"edge": 100},
		Available:        map[string]bool{"a": true, "b": true},
		OneWayMillis:     symMatrix(map[[2]string]float64{{"a", "b"}: 25}),
	})
	if len(got.Candidates) != 2 {
		t.Fatalf("expected only consensus candidates, got %+v", got.Candidates)
	}
	for _, candidate := range got.Candidates {
		if candidate.Domain == "edge" {
			t.Fatalf("floating domain must not become candidate: %+v", got.Candidates)
		}
	}
}

func TestControllerRequiresBenefitConfirmationAndCooldown(t *testing.T) {
	controller := NewController(Policy{MinImprovement: 0.1, Confirmations: 3, Cooldown: time.Minute})
	now := time.Unix(0, 0)

	// Benefit below threshold (only 4% lower) never migrates.
	for i := 0; i < 5; i++ {
		if target := controller.Observe("a", "b", 100, 96, now); target != "" {
			t.Fatalf("migrated despite sub-threshold benefit")
		}
	}
	// Sufficient benefit but not yet confirmed across enough windows.
	if target := controller.Observe("a", "b", 100, 70, now); target != "" {
		t.Fatalf("migrated before confirmation streak")
	}
	if target := controller.Observe("a", "b", 100, 70, now); target != "" {
		t.Fatalf("migrated before confirmation streak")
	}
	// Third confirmation triggers a decision.
	if target := controller.Observe("a", "b", 100, 70, now); target != "b" {
		t.Fatalf("expected migration to b after confirmations, got %q", target)
	}
	// The move succeeded, which arms the cooldown.
	controller.RecordResult(true, now)
	for i := 0; i < 3; i++ {
		if target := controller.Observe("a", "c", 100, 50, now.Add(time.Second)); target != "" {
			t.Fatalf("migrated during cooldown")
		}
	}
	// After cooldown, a confirmed target migrates again.
	later := now.Add(2 * time.Minute)
	controller.Observe("a", "c", 100, 50, later)
	controller.Observe("a", "c", 100, 50, later)
	if target := controller.Observe("a", "c", 100, 50, later); target != "c" {
		t.Fatalf("expected migration to c after cooldown, got %q", target)
	}
}

func TestControllerFailedMigrationDoesNotImposeCooldown(t *testing.T) {
	controller := NewController(Policy{MinImprovement: 0.1, Confirmations: 3, Cooldown: time.Minute})
	now := time.Unix(0, 0)

	controller.Observe("a", "b", 100, 70, now)
	controller.Observe("a", "b", 100, 70, now)
	if target := controller.Observe("a", "b", 100, 70, now); target != "b" {
		t.Fatalf("expected migration decision to b, got %q", target)
	}
	// The migration FAILED: do not arm the cooldown.
	controller.RecordResult(false, now)

	// A freshly confirmed, still-beneficial target must be allowed to retry
	// immediately, not blocked for a full cooldown.
	controller.Observe("a", "b", 100, 70, now)
	controller.Observe("a", "b", 100, 70, now)
	if target := controller.Observe("a", "b", 100, 70, now); target != "b" {
		t.Fatalf("failed migration must not impose cooldown; expected retry to b, got %q", target)
	}
	// This retry SUCCEEDED: the cooldown now stands and blocks the next attempt.
	controller.RecordResult(true, now)
	controller.Observe("a", "c", 100, 50, now)
	controller.Observe("a", "c", 100, 50, now)
	if target := controller.Observe("a", "c", 100, 50, now); target != "" {
		t.Fatalf("successful migration must start cooldown; expected no migration, got %q", target)
	}
}

func TestControllerResetsOnTargetChange(t *testing.T) {
	controller := NewController(Policy{MinImprovement: 0.1, Confirmations: 3, Cooldown: 0})
	now := time.Unix(0, 0)
	controller.Observe("a", "b", 100, 50, now)
	controller.Observe("a", "b", 100, 50, now)
	if target := controller.Observe("a", "c", 100, 50, now); target != "" {
		t.Fatalf("target change should reset streak, got %q", target)
	}
}
