package harness

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/czq/cd-raft/internal/cdraft"
	"github.com/czq/cd-raft/internal/topology"
)

func latencyProfileForScenario(scenario Scenario) topology.NetworkSimulation {
	profile := DefaultLatencyProfile()
	for origin, row := range scenario.FloatingLatencies {
		for domain, oneWay := range row {
			delay := int(oneWay)
			profile.InterDomainOneWayMillis[cdraft.WriteCostRoute(origin, domain)] = delay
			profile.InterDomainOneWayMillis[cdraft.WriteCostRoute(domain, origin)] = delay
		}
	}
	return profile
}

func (h *Harness) runWriteOverhead(ctx context.Context, scenario Scenario, step Step, budget time.Duration) ([]string, []WriteOverheadResult, error) {
	leader, err := h.CurrentGlobalLeader(ctx)
	if err != nil {
		return nil, nil, err
	}
	origins := h.writeOverheadOrigins(step, leader)
	if len(origins) == 0 {
		return nil, nil, fmt.Errorf("write_overhead has no origins to measure")
	}
	count := step.Count
	if count <= 0 {
		count = 10
	}
	warmup := step.Warmup
	if warmup < 0 {
		warmup = 0
	}
	details := []string{
		fmt.Sprintf("store=%s fastReturn=%t count=%d warmup=%d", h.storeProfile, scenario.FastReturn, count, warmup),
	}
	results := make([]WriteOverheadResult, 0, len(origins))
	for _, origin := range origins {
		if report := scenario.FloatingLatencies[origin]; len(report) > 0 {
			if err := h.reportFloatingLatency(ctx, origin, report, budget); err != nil {
				return details, results, err
			}
		}
		result, err := h.measureWriteOrigin(ctx, scenario, step, leader, origin, count, warmup, budget)
		if err != nil {
			return details, results, err
		}
		results = append(results, result)
		details = append(details, fmt.Sprintf(
			"origin=%s kind=%s p50=%s p95=%s p99=%s theory=%s",
			result.OriginDomain, result.OriginKind, result.P50, result.P95, result.P99,
			cdraft.WritePathCostSummary(cdraft.WritePathCost{
				FastestSecondQuorumDomain: result.SecondQuorumDomain,
				ResponderDomain:           result.ResponderDomain,
				GlobalDirectMillis:        result.GlobalDirectMillis,
				CallbackMillis:            result.CallbackMillis,
				CallbackAvailable:         result.CallbackAvailable,
				LowerBoundMillis:          result.TheoreticalNetwork,
				TheoreticalWinner:         cdraft.ResultSource(result.TheoreticalWinner),
			}, float64(result.P50)/float64(time.Millisecond)),
		))
	}
	return details, results, nil
}

func (h *Harness) writeOverheadOrigins(step Step, leader LeaderInfo) []string {
	if len(step.Origins) > 0 {
		return append([]string(nil), step.Origins...)
	}
	out := []string{leader.DomainID}
	for _, domain := range h.Config.ConsensusDomains() {
		if domain != leader.DomainID {
			out = append(out, domain)
			break
		}
	}
	if floating := h.Config.FloatingDomains(); len(floating) > 0 {
		out = append(out, floating[0])
	}
	return out
}

func (h *Harness) reportFloatingLatency(ctx context.Context, origin string, report map[string]float64, budget time.Duration) error {
	entry, err := h.EntryAddress()
	if err != nil {
		return err
	}
	reportCtx, cancel := context.WithTimeout(ctx, minDuration(budget, 5*time.Second))
	defer cancel()
	view, err := h.Client.DiscoverTopology(reportCtx, entry, origin)
	if err != nil {
		return err
	}
	target := view.GetGlobalLeaderAddress()
	if target == "" {
		target = entry
	}
	return h.Client.ReportFloatingLatency(reportCtx, target, origin, view.GetGlobalTerm(), report)
}

func (h *Harness) measureWriteOrigin(
	ctx context.Context,
	scenario Scenario,
	step Step,
	leader LeaderInfo,
	origin string,
	count, warmup int,
	budget time.Duration,
) (WriteOverheadResult, error) {
	total := warmup + count
	if total <= 0 {
		total = 1
	}
	durations := make([]time.Duration, 0, total)
	var winner cdraft.ResultSource
	var traceSummary []string
	for i := 0; i < total; i++ {
		requestID := fmt.Sprintf("%s-%s-%03d", scenario.Name, origin, i+1)
		key := requestID
		recorder := (*cdraft.WriteTraceRecorder)(nil)
		sampleCtx := ctx
		if step.Trace && i == warmup {
			recorder = cdraft.NewWriteTraceRecorder()
			h.setWriteTraceRecorder(recorder)
			sampleCtx = cdraft.WithWriteTrace(ctx, recorder)
		}
		start := time.Now()
		callCtx, cancel := context.WithTimeout(sampleCtx, minDuration(budget, 3*time.Second))
		decision, err := h.Client.Write(callCtx, leader.Address, cdraft.ClientWriteRequest{
			RequestID: requestID, OriginDomain: origin,
			Command: cdraft.Command{Key: key, Value: "v"},
		})
		cancel()
		elapsed := time.Since(start)
		if recorder != nil {
			h.setWriteTraceRecorder(nil)
			traceSummary = summarizeWriteTrace(recorder.Events())
		}
		if err != nil {
			return WriteOverheadResult{}, err
		}
		if !decision.Result.Committed {
			return WriteOverheadResult{}, fmt.Errorf("write %s was not committed", requestID)
		}
		winner = decision.Winner
		durations = append(durations, elapsed)
	}
	steady := durations
	if warmup < len(durations) {
		steady = durations[warmup:]
	}
	stats := durationStats(steady)
	costConfig := h.costConfig()
	cost := cdraft.EstimateWritePathCost(costConfig, cdraft.WritePathCostInput{
		OriginDomain: origin, GlobalDomain: leader.DomainID, FastReturn: scenario.FastReturn,
	})
	observed := float64(stats.P50)/float64(time.Millisecond) - cost.LowerBoundMillis
	if observed < 0 {
		observed = 0
	}
	return WriteOverheadResult{
		OriginDomain:       origin,
		OriginKind:         cost.OriginKind,
		GlobalDomain:       leader.DomainID,
		ResponderDomain:    cost.ResponderDomain,
		Winner:             string(winner),
		TheoreticalWinner:  string(cost.TheoreticalWinner),
		SecondQuorumDomain: cost.FastestSecondQuorumDomain,
		StoreProfile:       h.storeProfile,
		FastReturn:         scenario.FastReturn,
		Count:              count,
		Warmup:             warmup,
		FirstLatency:       durations[0],
		Min:                stats.Min,
		P50:                stats.P50,
		P95:                stats.P95,
		P99:                stats.P99,
		Max:                stats.Max,
		Mean:               stats.Mean,
		TheoreticalNetwork: cost.LowerBoundMillis,
		ObservedOverhead:   observed,
		GlobalDirectMillis: cost.GlobalDirectMillis,
		CallbackMillis:     cost.CallbackMillis,
		CallbackAvailable:  cost.CallbackAvailable,
		TraceSummary:       traceSummary,
	}, nil
}

func (h *Harness) setWriteTraceRecorder(recorder *cdraft.WriteTraceRecorder) {
	for _, node := range h.Nodes {
		node.SetWriteTraceRecorder(recorder)
	}
}

func (h *Harness) costConfig() topology.Config {
	h.mu.Lock()
	simulation := h.networkSimulation
	h.mu.Unlock()
	cfg := h.Config
	if simulation != nil {
		cfg.NetworkSimulation = *simulation
	}
	return cfg
}

type latencyStats struct {
	Min  time.Duration
	P50  time.Duration
	P95  time.Duration
	P99  time.Duration
	Max  time.Duration
	Mean time.Duration
}

func durationStats(values []time.Duration) latencyStats {
	if len(values) == 0 {
		return latencyStats{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, value := range sorted {
		total += value
	}
	return latencyStats{
		Min:  sorted[0],
		P50:  percentileDuration(sorted, 0.50),
		P95:  percentileDuration(sorted, 0.95),
		P99:  percentileDuration(sorted, 0.99),
		Max:  sorted[len(sorted)-1],
		Mean: total / time.Duration(len(sorted)),
	}
}

func percentileDuration(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * p)
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func summarizeWriteTrace(events []cdraft.WriteTraceEvent) []string {
	if len(events) == 0 {
		return nil
	}
	persistCount := 0
	var persistTotal time.Duration
	var persistMax time.Duration
	for _, event := range events {
		if strings.Contains(event.Phase, "persist") {
			persistCount++
			persistTotal += event.Duration
			if event.Duration > persistMax {
				persistMax = event.Duration
			}
		}
	}
	byDuration := append([]cdraft.WriteTraceEvent(nil), events...)
	sort.Slice(byDuration, func(i, j int) bool { return byDuration[i].Duration > byDuration[j].Duration })
	limit := 8
	if len(byDuration) < limit {
		limit = len(byDuration)
	}
	out := []string{fmt.Sprintf("trace events=%d", len(events))}
	if persistCount > 0 {
		out = append(out, fmt.Sprintf("persist phases=%d total=%s max=%s", persistCount, persistTotal, persistMax))
	}
	for i := 0; i < limit; i++ {
		event := byDuration[i]
		parts := []string{event.Phase}
		if event.NodeID != "" {
			parts = append(parts, "node="+event.NodeID)
		}
		if event.DomainID != "" {
			parts = append(parts, "domain="+event.DomainID)
		}
		if event.PeerDomain != "" {
			parts = append(parts, "peer="+event.PeerDomain)
		}
		if event.Detail != "" {
			parts = append(parts, event.Detail)
		}
		out = append(out, fmt.Sprintf("%s %s", event.Duration, strings.Join(parts, " ")))
	}
	return out
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 {
		return b
	}
	if b <= 0 || a < b {
		return a
	}
	return b
}
