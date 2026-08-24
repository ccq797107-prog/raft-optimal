package harness

import (
	"fmt"
	"strings"
	"time"
)

func (r Report) Markdown() string {
	var out strings.Builder
	fmt.Fprintf(&out, "# CD-Raft Harness Report\n\n")
	fmt.Fprintf(&out, "- Scenario: `%s`\n", r.Scenario)
	fmt.Fprintf(&out, "- Started: `%s`\n", r.StartedAt.Format(time.RFC3339))
	if !r.FinishedAt.IsZero() {
		fmt.Fprintf(&out, "- Finished: `%s`\n", r.FinishedAt.Format(time.RFC3339))
	}
	fmt.Fprintf(&out, "- Duration: `%s`\n", r.Duration)
	fmt.Fprintf(&out, "- Domains: `%s`\n", strings.Join(r.Domains, ", "))
	if len(r.FloatingDomains) > 0 {
		fmt.Fprintf(&out, "- Floating Domains: `%s`\n", strings.Join(r.FloatingDomains, ", "))
	}
	fmt.Fprintf(&out, "- Nodes: `%d`\n", r.NodeCount)
	fmt.Fprintf(&out, "- Fast Return: `%t`\n", r.FastReturn)
	if r.InitialLeader.NodeID != "" {
		fmt.Fprintf(&out, "- Initial Global Leader: `%s` domain `%s` term `%d`\n", r.InitialLeader.NodeID, r.InitialLeader.DomainID, r.InitialLeader.GlobalTerm)
	}
	if r.FinalLeader.NodeID != "" {
		fmt.Fprintf(&out, "- Final Global Leader: `%s` domain `%s` term `%d`\n", r.FinalLeader.NodeID, r.FinalLeader.DomainID, r.FinalLeader.GlobalTerm)
	}
	if r.Error != "" {
		fmt.Fprintf(&out, "- Error: `%s`\n", r.Error)
	}
	fmt.Fprintf(&out, "\n## Steps\n\n")
	fmt.Fprintf(&out, "| # | Step | Type | OK | Duration | Leader After | Details |\n")
	fmt.Fprintf(&out, "|---|------|------|----|----------|--------------|---------|\n")
	for i, step := range r.Steps {
		ok := "yes"
		if !step.OK {
			ok = "no"
		}
		leader := ""
		if step.LeaderAfter.NodeID != "" {
			leader = fmt.Sprintf("%s/%s/t%d", step.LeaderAfter.NodeID, step.LeaderAfter.DomainID, step.LeaderAfter.GlobalTerm)
		}
		details := strings.Join(step.Details, "<br>")
		if step.Error != "" {
			if details != "" {
				details += "<br>"
			}
			details += "error: " + step.Error
		}
		fmt.Fprintf(&out, "| %d | %s | `%s` | %s | `%s` | `%s` | %s |\n",
			i+1, escapeTable(step.Name), step.Type, ok, step.Duration, leader, escapeTable(details))
	}
	writeOverheads := collectWriteOverheads(r.Steps)
	if len(writeOverheads) > 0 {
		fmt.Fprintf(&out, "\n## Write Overhead\n\n")
		fmt.Fprintf(&out, "| Origin | Kind | GL | Winner | Theoretical Winner | Store | Samples | First | p50 | p95 | p99 | Network LB | Overhead | Direct | Callback | Second Quorum | Responder |\n")
		fmt.Fprintf(&out, "|--------|------|----|--------|--------------------|-------|---------|-------|-----|-----|-----|------------|----------|--------|----------|---------------|-----------|\n")
		for _, item := range writeOverheads {
			callback := ""
			if item.CallbackAvailable {
				callback = fmt.Sprintf("%.1fms", item.CallbackMillis)
			}
			fmt.Fprintf(&out, "| `%s` | `%s` | `%s` | `%s` | `%s` | `%s` | `%d/%d` | `%s` | `%s` | `%s` | `%s` | `%.1fms` | `%.1fms` | `%.1fms` | `%s` | `%s` | `%s` |\n",
				item.OriginDomain, item.OriginKind, item.GlobalDomain, item.Winner, item.TheoreticalWinner,
				item.StoreProfile, item.Count, item.Warmup, item.FirstLatency, item.P50, item.P95, item.P99,
				item.TheoreticalNetwork, item.ObservedOverhead, item.GlobalDirectMillis, callback,
				item.SecondQuorumDomain, item.ResponderDomain)
		}
		fmt.Fprintf(&out, "\n### Trace Summary\n\n")
		for _, item := range writeOverheads {
			if len(item.TraceSummary) == 0 {
				continue
			}
			fmt.Fprintf(&out, "- `%s`:\n", item.OriginDomain)
			for _, line := range item.TraceSummary {
				fmt.Fprintf(&out, "  - %s\n", line)
			}
		}
	}
	return out.String()
}

func collectWriteOverheads(steps []StepResult) []WriteOverheadResult {
	var out []WriteOverheadResult
	for _, step := range steps {
		out = append(out, step.WriteOverheads...)
	}
	return out
}

func escapeTable(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "|", "\\|")
	return value
}
