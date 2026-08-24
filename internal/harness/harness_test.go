package harness

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestBuiltinSmokeScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("real gRPC harness smoke is skipped in -short")
	}
	h, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	scenario, ok := BuiltinScenario("smoke")
	if !ok {
		t.Fatal("missing smoke scenario")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	report, err := h.RunScenario(ctx, scenario)
	if err != nil {
		t.Fatalf("smoke scenario failed: %v\n%s", err, report.Markdown())
	}
	if report.FinalLeader.NodeID == "" {
		t.Fatalf("report did not record final leader:\n%s", report.Markdown())
	}
	if len(report.Steps) != len(scenario.Steps) {
		t.Fatalf("steps=%d, want %d", len(report.Steps), len(scenario.Steps))
	}
	if markdown := report.Markdown(); !strings.Contains(markdown, "write request=harness-smoke") {
		t.Fatalf("report missing write details:\n%s", markdown)
	}
}

func TestBuiltinFloatingScenariosDeclareClientOnlyDomains(t *testing.T) {
	for _, name := range []string{"floating-smoke", "floating-fallback"} {
		scenario, ok := BuiltinScenario(name)
		if !ok {
			t.Fatalf("missing %s scenario", name)
		}
		if err := scenario.Validate(); err != nil {
			t.Fatalf("%s did not validate: %v", name, err)
		}
		if len(scenario.FloatingDomains) != 1 || scenario.FloatingDomains[0] != "edge" {
			t.Fatalf("%s missing floating domain declaration: %+v", name, scenario.FloatingDomains)
		}
		if len(scenario.FloatingLatencies["edge"]) == 0 {
			t.Fatalf("%s missing floating latency declaration", name)
		}
	}
	fallback, _ := BuiltinScenario("floating-fallback")
	foundExpectedWinner := false
	for _, step := range fallback.Steps {
		if step.ExpectWinner == "global-leader" {
			foundExpectedWinner = true
		}
	}
	if !foundExpectedWinner {
		t.Fatalf("floating fallback scenario should assert GL fallback winner: %+v", fallback.Steps)
	}
}

func TestWriteOverheadScenarioReportsOriginsStatsAndTrace(t *testing.T) {
	if testing.Short() {
		t.Skip("real gRPC overhead scenario is skipped in -short")
	}
	h, err := New(Options{FastReturnEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report, err := h.RunScenario(ctx, Scenario{
		Name:            "overhead-test",
		FastReturn:      true,
		FloatingDomains: []string{"edge"},
		FloatingLatencies: map[string]map[string]float64{
			"edge": {"a": 100, "b": 5, "c": 80},
		},
		Steps: []Step{
			{Name: "start", Type: StepStart},
			{Name: "wait", Type: StepWaitServing, BudgetMillis: 12000},
			{Name: "latency off", Type: StepSetLatency, LatencyProfile: "off"},
			{Name: "measure", Type: StepWriteOverhead, Count: 1, Warmup: 1, Trace: true, BudgetMillis: 12000},
		},
	})
	if err != nil {
		t.Fatalf("overhead scenario failed: %v\n%s", err, report.Markdown())
	}
	var overheads []WriteOverheadResult
	for _, step := range report.Steps {
		overheads = append(overheads, step.WriteOverheads...)
	}
	if len(overheads) != 3 {
		t.Fatalf("overhead origins=%d, want 3: %+v\n%s", len(overheads), overheads, report.Markdown())
	}
	seenFloating := false
	for _, item := range overheads {
		if item.Count != 1 || item.P50 <= 0 || item.StoreProfile != StoreProfileMemory {
			t.Fatalf("bad overhead item: %+v", item)
		}
		if item.OriginKind == "floating" {
			seenFloating = true
		}
		if len(item.TraceSummary) == 0 {
			t.Fatalf("missing trace summary for %+v", item)
		}
	}
	if !seenFloating {
		t.Fatalf("floating origin missing: %+v", overheads)
	}
	if markdown := report.Markdown(); !strings.Contains(markdown, "## Write Overhead") || !strings.Contains(markdown, "Trace Summary") {
		t.Fatalf("markdown missing overhead sections:\n%s", markdown)
	}
}

func TestFaultControlValidationAndFailureReport(t *testing.T) {
	h, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if err := h.Partition([]string{"missing"}, []string{"a1"}); err == nil {
		t.Fatal("partition with an unknown node should fail")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := h.RunScenario(ctx, Scenario{
		Name:  "bad-step",
		Steps: []Step{{Name: "unknown", Type: "not-a-real-step"}},
	})
	if err == nil {
		t.Fatal("unknown step should fail")
	}
	if report.Error == "" || len(report.Steps) != 1 || report.Steps[0].Error == "" {
		t.Fatalf("failure report lacks diagnostics: %+v", report)
	}
}

func TestMoveScenarioReportsMigration(t *testing.T) {
	if testing.Short() {
		t.Skip("real gRPC migration scenario is skipped in -short")
	}
	h, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	initial, err := h.WaitServing(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target := differentDomain(h.Config.Domains(), initial.DomainID)
	report, err := h.RunScenario(ctx, Scenario{
		Name: "move-smoke",
		Steps: []Step{
			{Name: "move", Type: StepMove, TargetDomain: target, BudgetMillis: 20000},
			{Name: "wait", Type: StepWaitServing, BudgetMillis: 15000},
		},
	})
	if err != nil {
		t.Fatalf("move scenario failed: %v\n%s", err, report.Markdown())
	}
	if report.FinalLeader.DomainID != target {
		t.Fatalf("final leader domain=%s, want %s\n%s", report.FinalLeader.DomainID, target, report.Markdown())
	}
}

func differentDomain(domains []string, current string) string {
	for _, domain := range domains {
		if domain != current {
			return domain
		}
	}
	return current
}
