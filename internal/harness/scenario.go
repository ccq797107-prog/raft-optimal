package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/czq/cd-raft/internal/cdraft"
)

type StepType string

const (
	StepStart         StepType = "start"
	StepWaitServing   StepType = "wait_serving"
	StepWriteRead     StepType = "write_read"
	StepMove          StepType = "move"
	StepPartition     StepType = "partition"
	StepHeal          StepType = "heal"
	StepRestart       StepType = "restart"
	StepSetDropRate   StepType = "set_drop_rate"
	StepSetLatency    StepType = "set_latency"
	StepMetrics       StepType = "metrics"
	StepSleep         StepType = "sleep"
	StepStartLoad     StepType = "start_load"
	StepStopLoad      StepType = "stop_load"
	StepWriteOverhead StepType = "write_overhead"
)

type Scenario struct {
	Name              string                        `json:"name"`
	FastReturn        bool                          `json:"fastReturn,omitempty"`
	FloatingDomains   []string                      `json:"floatingDomains,omitempty"`
	FloatingLatencies map[string]map[string]float64 `json:"floatingLatencies,omitempty"`
	Steps             []Step                        `json:"steps"`
}

type Step struct {
	Name           string   `json:"name,omitempty"`
	Type           StepType `json:"type"`
	Target         string   `json:"target,omitempty"`
	TargetDomain   string   `json:"targetDomain,omitempty"`
	Origin         string   `json:"origin,omitempty"`
	RequestID      string   `json:"requestId,omitempty"`
	Key            string   `json:"key,omitempty"`
	Value          string   `json:"value,omitempty"`
	Reason         string   `json:"reason,omitempty"`
	GroupA         []string `json:"groupA,omitempty"`
	GroupB         []string `json:"groupB,omitempty"`
	Nodes          []string `json:"nodes,omitempty"`
	DropRate       float64  `json:"dropRate,omitempty"`
	LatencyProfile string   `json:"latencyProfile,omitempty"`
	DurationMillis int      `json:"durationMillis,omitempty"`
	BudgetMillis   int      `json:"budgetMillis,omitempty"`
	IntervalMillis int      `json:"intervalMillis,omitempty"`
	ExpectWinner   string   `json:"expectWinner,omitempty"`
	Origins        []string `json:"origins,omitempty"`
	Count          int      `json:"count,omitempty"`
	Warmup         int      `json:"warmup,omitempty"`
	Trace          bool     `json:"trace,omitempty"`
}

type StepResult struct {
	Name           string                `json:"name,omitempty"`
	Type           StepType              `json:"type"`
	StartedAt      time.Time             `json:"startedAt"`
	FinishedAt     time.Time             `json:"finishedAt"`
	Duration       time.Duration         `json:"duration"`
	OK             bool                  `json:"ok"`
	Error          string                `json:"error,omitempty"`
	LeaderBefore   LeaderInfo            `json:"leaderBefore,omitempty"`
	LeaderAfter    LeaderInfo            `json:"leaderAfter,omitempty"`
	Details        []string              `json:"details,omitempty"`
	WriteOverheads []WriteOverheadResult `json:"writeOverheads,omitempty"`
}

type WriteOverheadResult struct {
	OriginDomain       string        `json:"originDomain"`
	OriginKind         string        `json:"originKind"`
	GlobalDomain       string        `json:"globalDomain"`
	ResponderDomain    string        `json:"responderDomain,omitempty"`
	Winner             string        `json:"winner"`
	TheoreticalWinner  string        `json:"theoreticalWinner"`
	SecondQuorumDomain string        `json:"secondQuorumDomain,omitempty"`
	StoreProfile       string        `json:"storeProfile"`
	FastReturn         bool          `json:"fastReturn"`
	Count              int           `json:"count"`
	Warmup             int           `json:"warmup"`
	FirstLatency       time.Duration `json:"firstLatency"`
	Min                time.Duration `json:"min"`
	P50                time.Duration `json:"p50"`
	P95                time.Duration `json:"p95"`
	P99                time.Duration `json:"p99"`
	Max                time.Duration `json:"max"`
	Mean               time.Duration `json:"mean"`
	TheoreticalNetwork float64       `json:"theoreticalNetworkMillis"`
	ObservedOverhead   float64       `json:"observedOverheadMillis"`
	GlobalDirectMillis float64       `json:"globalDirectMillis"`
	CallbackMillis     float64       `json:"callbackMillis,omitempty"`
	CallbackAvailable  bool          `json:"callbackAvailable"`
	TraceSummary       []string      `json:"traceSummary,omitempty"`
}

type Report struct {
	Scenario        string        `json:"scenario"`
	StartedAt       time.Time     `json:"startedAt"`
	FinishedAt      time.Time     `json:"finishedAt"`
	Duration        time.Duration `json:"duration"`
	FastReturn      bool          `json:"fastReturn"`
	Domains         []string      `json:"domains"`
	FloatingDomains []string      `json:"floatingDomains,omitempty"`
	NodeCount       int           `json:"nodeCount"`
	InitialLeader   LeaderInfo    `json:"initialLeader,omitempty"`
	FinalLeader     LeaderInfo    `json:"finalLeader,omitempty"`
	Steps           []StepResult  `json:"steps"`
	Error           string        `json:"error,omitempty"`
}

// BuiltinScenario returns a small named scenario suitable for CLI smoke checks.
func BuiltinScenario(name string) (Scenario, bool) {
	switch name {
	case "", "smoke":
		return Scenario{
			Name: "smoke",
			Steps: []Step{
				{Name: "start cluster", Type: StepStart},
				{Name: "wait for serving", Type: StepWaitServing, BudgetMillis: 12000},
				{Name: "write and read", Type: StepWriteRead, Origin: "b", RequestID: "harness-smoke", Key: "harness-smoke", Value: "ok", BudgetMillis: 8000},
				{Name: "sample metrics", Type: StepMetrics},
			},
		}, true
	case "floating-smoke":
		return Scenario{
			Name:            "floating-smoke",
			FastReturn:      true,
			FloatingDomains: []string{"edge"},
			FloatingLatencies: map[string]map[string]float64{
				"edge": {"a": 100, "b": 5, "c": 80},
			},
			Steps: []Step{
				{Name: "start cluster", Type: StepStart},
				{Name: "wait for serving", Type: StepWaitServing, BudgetMillis: 12000},
				{Name: "floating write and read", Type: StepWriteRead, Origin: "edge", RequestID: "harness-floating", Key: "harness-floating", Value: "ok", BudgetMillis: 8000},
				{Name: "sample metrics", Type: StepMetrics},
			},
		}, true
	case "floating-fallback":
		return Scenario{
			Name:            "floating-fallback",
			FastReturn:      false,
			FloatingDomains: []string{"edge"},
			FloatingLatencies: map[string]map[string]float64{
				"edge": {"a": 100, "b": 5, "c": 80},
			},
			Steps: []Step{
				{Name: "start cluster", Type: StepStart},
				{Name: "wait for serving", Type: StepWaitServing, BudgetMillis: 12000},
				{Name: "floating write and read fallback", Type: StepWriteRead, Origin: "edge", RequestID: "harness-floating-fallback", Key: "harness-floating-fallback", Value: "ok", BudgetMillis: 8000, ExpectWinner: string(cdraft.GlobalResponse)},
				{Name: "sample metrics", Type: StepMetrics},
			},
		}, true
	case "write-overhead":
		return Scenario{
			Name:            "write-overhead",
			FastReturn:      true,
			FloatingDomains: []string{"edge"},
			FloatingLatencies: map[string]map[string]float64{
				"edge": {"a": 100, "b": 5, "c": 80},
			},
			Steps: []Step{
				{Name: "start cluster", Type: StepStart},
				{Name: "wait for serving", Type: StepWaitServing, BudgetMillis: 12000},
				{Name: "disable injected latency", Type: StepSetLatency, LatencyProfile: "off"},
				{Name: "measure write overhead", Type: StepWriteOverhead, Count: 8, Warmup: 3, Trace: true, BudgetMillis: 20000},
				{Name: "sample metrics", Type: StepMetrics},
			},
		}, true
	case "write-overhead-latency":
		return Scenario{
			Name:            "write-overhead-latency",
			FastReturn:      true,
			FloatingDomains: []string{"edge"},
			FloatingLatencies: map[string]map[string]float64{
				"edge": {"a": 100, "b": 5, "c": 80},
			},
			Steps: []Step{
				{Name: "start cluster", Type: StepStart},
				{Name: "wait for serving", Type: StepWaitServing, BudgetMillis: 12000},
				{Name: "apply default latency", Type: StepSetLatency, LatencyProfile: "default"},
				{Name: "measure write overhead with latency", Type: StepWriteOverhead, Count: 8, Warmup: 3, Trace: true, BudgetMillis: 30000},
				{Name: "sample metrics", Type: StepMetrics},
			},
		}, true
	default:
		return Scenario{}, false
	}
}

func LoadScenario(path string) (Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, err
	}
	var scenario Scenario
	if err := json.Unmarshal(data, &scenario); err != nil {
		return Scenario{}, fmt.Errorf("decode scenario: %w", err)
	}
	if scenario.Name == "" {
		scenario.Name = filepath.Base(path)
	}
	return scenario, nil
}

func (s Scenario) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("scenario name is required")
	}
	if len(s.Steps) == 0 {
		return fmt.Errorf("scenario %q has no steps", s.Name)
	}
	for i, step := range s.Steps {
		if step.Type == "" {
			return fmt.Errorf("step %d has no type", i+1)
		}
	}
	return nil
}

func (h *Harness) RunScenario(ctx context.Context, scenario Scenario) (Report, error) {
	if err := scenario.Validate(); err != nil {
		return Report{}, err
	}
	if err := h.applyScenarioConfig(scenario); err != nil {
		return Report{}, err
	}
	report := Report{
		Scenario:        scenario.Name,
		StartedAt:       time.Now(),
		FastReturn:      scenario.FastReturn,
		Domains:         h.Config.Domains(),
		FloatingDomains: h.Config.FloatingDomains(),
		NodeCount:       len(h.Config.Nodes),
	}
	loads := make(map[string]context.CancelFunc)
	defer func() {
		for _, cancel := range loads {
			cancel()
		}
	}()

	for i, step := range scenario.Steps {
		result := StepResult{
			Name:      stepName(step, i),
			Type:      step.Type,
			StartedAt: time.Now(),
		}
		if leader, err := h.CurrentGlobalLeader(ctx); err == nil {
			result.LeaderBefore = leader
			if report.InitialLeader.NodeID == "" {
				report.InitialLeader = leader
			}
		}
		details, overheads, err := h.runStep(ctx, scenario, step, loads)
		result.FinishedAt = time.Now()
		result.Duration = result.FinishedAt.Sub(result.StartedAt)
		result.OK = err == nil
		result.Details = details
		result.WriteOverheads = overheads
		if leader, leaderErr := h.CurrentGlobalLeader(ctx); leaderErr == nil {
			result.LeaderAfter = leader
			report.FinalLeader = leader
			if report.InitialLeader.NodeID == "" {
				report.InitialLeader = leader
			}
		}
		if err != nil {
			result.Error = err.Error()
			report.Steps = append(report.Steps, result)
			report.Error = fmt.Sprintf("%s: %v", result.Name, err)
			report.FinishedAt = time.Now()
			report.Duration = report.FinishedAt.Sub(report.StartedAt)
			return report, err
		}
		report.Steps = append(report.Steps, result)
	}
	report.FinishedAt = time.Now()
	report.Duration = report.FinishedAt.Sub(report.StartedAt)
	return report, nil
}

func (h *Harness) applyScenarioConfig(scenario Scenario) error {
	if len(scenario.FloatingDomains) == 0 {
		return nil
	}
	updated := h.Config
	updated.Features.FloatingDomains = append([]string(nil), scenario.FloatingDomains...)
	if err := updated.Validate(); err != nil {
		return err
	}
	h.Config = updated
	for _, node := range h.Nodes {
		node.SetFloatingDomains(scenario.FloatingDomains, false)
	}
	return nil
}

func stepName(step Step, index int) string {
	if step.Name != "" {
		return step.Name
	}
	return fmt.Sprintf("%02d %s", index+1, step.Type)
}

func (h *Harness) runStep(ctx context.Context, scenario Scenario, step Step, loads map[string]context.CancelFunc) ([]string, []WriteOverheadResult, error) {
	budget := stepBudget(step, 8*time.Second)
	switch step.Type {
	case StepStart:
		return []string{"started node listeners and live runtimes"}, nil, h.Run(ctx)
	case StepWaitServing:
		waitCtx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		leader, err := h.WaitServing(waitCtx)
		if err != nil {
			return nil, nil, err
		}
		return []string{fmt.Sprintf("global leader %s domain %s term %d", leader.NodeID, leader.DomainID, leader.GlobalTerm)}, nil, nil
	case StepWriteRead:
		details, err := h.runWriteRead(ctx, scenario, step, budget)
		return details, nil, err
	case StepWriteOverhead:
		details, overheads, err := h.runWriteOverhead(ctx, scenario, step, budget)
		return details, overheads, err
	case StepMove:
		target := step.Target
		if target == "" {
			leader, err := h.CurrentGlobalLeader(ctx)
			if err != nil {
				return nil, nil, err
			}
			target = leader.Address
		}
		if step.TargetDomain == "" {
			return nil, nil, fmt.Errorf("targetDomain is required for move")
		}
		moveCtx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		resp, err := h.Move(moveCtx, target, step.TargetDomain, nonEmpty(step.Reason, "harness scenario move"))
		if err != nil {
			return nil, nil, err
		}
		if !resp.GetAccepted() {
			return nil, nil, fmt.Errorf("move rejected: %s", resp.GetError())
		}
		return []string{fmt.Sprintf("moved to domain %s term %d", step.TargetDomain, resp.GetNewGlobalTerm())}, nil, nil
	case StepPartition:
		if err := h.Partition(step.GroupA, step.GroupB); err != nil {
			return nil, nil, err
		}
		return []string{fmt.Sprintf("partitioned %v from %v", step.GroupA, step.GroupB)}, nil, nil
	case StepHeal:
		h.Heal()
		return []string{"healed all partitions"}, nil, nil
	case StepRestart:
		nodes := step.Nodes
		if len(nodes) == 0 && step.Target != "" {
			nodes = []string{step.Target}
		}
		if len(nodes) == 0 {
			return nil, nil, fmt.Errorf("restart requires nodes or target")
		}
		for _, id := range nodes {
			if err := h.Restart(id); err != nil {
				return nil, nil, err
			}
		}
		return []string{fmt.Sprintf("restarted %v", nodes)}, nil, nil
	case StepSetDropRate:
		if err := h.SetDropRate(step.DropRate); err != nil {
			return nil, nil, err
		}
		return []string{fmt.Sprintf("drop rate %.3f", step.DropRate)}, nil, nil
	case StepSetLatency:
		switch step.LatencyProfile {
		case "", "default", "latency-cluster":
			h.SetNetworkSimulation(latencyProfileForScenario(scenario))
			return []string{"applied default latency profile"}, nil, nil
		case "off", "disabled":
			disabled := DefaultLatencyProfile()
			disabled.Enabled = false
			h.SetNetworkSimulation(disabled)
			return []string{"disabled latency profile"}, nil, nil
		default:
			return nil, nil, fmt.Errorf("unknown latency profile %q", step.LatencyProfile)
		}
	case StepMetrics:
		target := step.Target
		if target == "" {
			leader, err := h.CurrentGlobalLeader(ctx)
			if err != nil {
				return nil, nil, err
			}
			target = leader.Address
		}
		metricsCtx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		metrics, err := h.Metrics(metricsCtx, target)
		if err != nil {
			return nil, nil, err
		}
		return []string{
			fmt.Sprintf("node=%s stage=%s term=%d migrations=%d", metrics.GetNodeId(), metrics.GetStage(), metrics.GetGlobalTerm(), metrics.GetMigrations()),
		}, nil, nil
	case StepSleep:
		duration := time.Duration(step.DurationMillis) * time.Millisecond
		if duration <= 0 {
			duration = time.Second
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(duration):
		}
		return []string{fmt.Sprintf("slept %s", duration)}, nil, nil
	case StepStartLoad:
		name := nonEmpty(step.Name, "load")
		if _, ok := loads[name]; ok {
			return nil, nil, fmt.Errorf("load %q already running", name)
		}
		loadCtx, cancel := context.WithCancel(ctx)
		loads[name] = cancel
		target, err := h.resolveTarget(step.Target)
		if err != nil {
			return nil, nil, err
		}
		interval := time.Duration(step.IntervalMillis) * time.Millisecond
		if interval <= 0 {
			interval = 15 * time.Millisecond
		}
		origin := nonEmpty(step.Origin, "a")
		prefix := nonEmpty(step.RequestID, name)
		go h.driveLoad(loadCtx, target, origin, prefix, interval)
		return []string{fmt.Sprintf("started load %q origin=%s interval=%s", name, origin, interval)}, nil, nil
	case StepStopLoad:
		name := nonEmpty(step.Name, "load")
		cancel, ok := loads[name]
		if !ok {
			return nil, nil, fmt.Errorf("load %q is not running", name)
		}
		cancel()
		delete(loads, name)
		return []string{fmt.Sprintf("stopped load %q", name)}, nil, nil
	default:
		return nil, nil, fmt.Errorf("unknown step type %q", step.Type)
	}
}

func (h *Harness) runWriteRead(ctx context.Context, scenario Scenario, step Step, budget time.Duration) ([]string, error) {
	key := nonEmpty(step.Key, "harness-key")
	value := nonEmpty(step.Value, "ok")
	origin := nonEmpty(step.Origin, "a")
	requestID := step.RequestID
	if requestID == "" {
		requestID = fmt.Sprintf("%s-%s", scenario.Name, key)
	}
	details := make([]string, 0, 3)
	if report := scenario.FloatingLatencies[origin]; len(report) > 0 {
		entry, err := h.resolveTarget(step.Target)
		if err != nil {
			return nil, err
		}
		reportCtx, cancel := context.WithTimeout(ctx, budget)
		view, err := h.Client.DiscoverTopology(reportCtx, entry, origin)
		if err == nil {
			target := view.GetGlobalLeaderAddress()
			if target == "" {
				target = entry
			}
			err = h.Client.ReportFloatingLatency(reportCtx, target, origin, view.GetGlobalTerm(), report)
			if err == nil {
				details = append(details, fmt.Sprintf("reported floating latency origin=%s targets=%d", origin, len(report)))
			}
		}
		cancel()
		if err != nil {
			return nil, err
		}
	}
	decision, err := h.WriteUntilCommitted(ctx, step.Target, cdraft.ClientWriteRequest{
		RequestID:    requestID,
		OriginDomain: origin,
		Command:      cdraft.Command{Key: key, Value: value},
	}, budget)
	if err != nil {
		return nil, err
	}
	if step.ExpectWinner != "" && !winnerMatches(step.ExpectWinner, decision.Winner) {
		return nil, fmt.Errorf("winner=%s, expected %s", decision.Winner, step.ExpectWinner)
	}
	readValue, err := h.ReadUntil(ctx, step.Target, origin, key, value, budget)
	if err != nil {
		return nil, err
	}
	details = append(details,
		fmt.Sprintf("write request=%s winner=%s index=%d", requestID, decision.Winner, decision.Result.GlobalIndex),
		fmt.Sprintf("read %s=%s", key, readValue),
	)
	return details, nil
}

func winnerMatches(expected string, got cdraft.ResultSource) bool {
	switch expected {
	case string(got):
		return true
	case "fast":
		return got == cdraft.FastResponse
	case "global":
		return got == cdraft.GlobalResponse
	default:
		return false
	}
}

func (h *Harness) driveLoad(ctx context.Context, target, origin, prefix string, interval time.Duration) {
	var seq int64
	for ctx.Err() == nil {
		i := atomic.AddInt64(&seq, 1)
		key := fmt.Sprintf("%s-%s-%d", prefix, origin, i)
		writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, _ = h.Client.Write(writeCtx, target, cdraft.ClientWriteRequest{
			RequestID:    key,
			OriginDomain: origin,
			Command:      cdraft.Command{Key: key, Value: "v"},
		})
		_, _ = h.Client.ReadFrom(writeCtx, target, origin, key)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func stepBudget(step Step, fallback time.Duration) time.Duration {
	if step.BudgetMillis <= 0 {
		return fallback
	}
	return time.Duration(step.BudgetMillis) * time.Millisecond
}

func nonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
