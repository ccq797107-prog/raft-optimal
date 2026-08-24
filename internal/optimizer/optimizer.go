// Package optimizer holds the paper cost model (L_s) and the migration decision
// policy. It is deliberately OUTSIDE the consensus runtime: the cd-raft node no
// longer decides when or where to move the Global Leader. Instead an external
// controller (cmd/cdraft-mover) pulls the GL's raw window statistics over the
// Metrics RPC, runs ComputeCosts here, applies a MigrationController policy, and
// triggers the safe handoff via the GL-only Move RPC. Keeping this logic in a
// plain library (no consensus state, no goroutines) is what decouples the
// "where should the leader be" question from the consensus core.
package optimizer

import (
	"sort"
	"time"
)

// Input is an immutable snapshot consumed by the deterministic paper cost model.
// All latencies are one-way estimates in the same unit; if only RTT is measured
// the caller MUST pass RTT/2 here.
type Input struct {
	Domains          []string                      // legacy: all consensus domains
	ConsensusDomains []string                      // configured consensus domains and only GL candidates
	FloatingDomains  []string                      // client-only demand domains
	Writes           map[string]float64            // W_i per source domain
	Reads            map[string]float64            // R_i per source domain
	Available        map[string]bool               // consensus-domain availability + Domain Leader present
	OneWayMillis     map[string]map[string]float64 // l[i][j] one-way latency estimate
}

// CandidateCost is the predicted total cross-domain cost L_s for one candidate
// Global Leader domain.
type CandidateCost struct {
	Domain    string
	Ls        float64
	CanCommit bool
}

// Result ranks candidate domains by predicted total cost. It is pure data.
type Result struct {
	Candidates    []CandidateCost
	Recommended   string
	RecommendedLs float64
}

func oneWay(in Input, from, to string) (float64, bool) {
	if from == to {
		return 0, true
	}
	row, ok := in.OneWayMillis[from]
	if !ok {
		return 0, false
	}
	v, ok := row[to]
	return v, ok
}

// minToOtherAvailable returns min_j(l_zj) over available, reachable domains
// j != z. The second return is false when no such j exists, meaning candidate z
// cannot form a two-domain commit and must not be a migration target.
func minToOtherAvailable(in Input, z string) (float64, bool) {
	return minToOtherAvailableIn(in, consensusDomains(in), z)
}

func minToOtherAvailableIn(in Input, domains []string, z string) (float64, bool) {
	best := 0.0
	found := false
	for _, j := range domains {
		if j == z || !in.Available[j] {
			continue
		}
		l, ok := oneWay(in, z, j)
		if !ok {
			continue
		}
		if !found || l < best {
			best = l
			found = true
		}
	}
	return best, found
}

func consensusDomains(in Input) []string {
	domains := in.ConsensusDomains
	if len(domains) == 0 {
		domains = in.Domains
	}
	out := append([]string(nil), domains...)
	sort.Strings(out)
	return out
}

func floatingDomains(in Input, consensus []string) []string {
	set := make(map[string]struct{}, len(in.FloatingDomains))
	for _, domain := range in.FloatingDomains {
		set[domain] = struct{}{}
	}
	consensusSet := make(map[string]struct{}, len(consensus))
	for _, domain := range consensus {
		consensusSet[domain] = struct{}{}
	}
	for domain, count := range in.Writes {
		if count > 0 {
			if _, ok := consensusSet[domain]; !ok {
				set[domain] = struct{}{}
			}
		}
	}
	for domain, count := range in.Reads {
		if count > 0 {
			if _, ok := consensusSet[domain]; !ok {
				set[domain] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(set))
	for domain := range set {
		out = append(out, domain)
	}
	sort.Strings(out)
	return out
}

// ComputeCosts implements the paper's three L_i forms and the L_s = sum(L_i)
// aggregation, enumerating only available candidate domains, filtering
// candidates with no commit partner, and breaking ties by domain id.
func ComputeCosts(in Input) Result {
	domains := consensusDomains(in)
	floating := floatingDomains(in, domains)

	result := Result{}
	for _, z := range domains {
		if !in.Available[z] {
			continue
		}
		cost, canCommit := candidateCost(in, domains, floating, z)
		result.Candidates = append(result.Candidates, CandidateCost{Domain: z, Ls: cost, CanCommit: canCommit})
	}

	sort.SliceStable(result.Candidates, func(i, j int) bool {
		left, right := result.Candidates[i], result.Candidates[j]
		if left.CanCommit != right.CanCommit {
			return left.CanCommit
		}
		if left.Ls != right.Ls {
			return left.Ls < right.Ls
		}
		return left.Domain < right.Domain
	})
	for _, candidate := range result.Candidates {
		if candidate.CanCommit {
			result.Recommended = candidate.Domain
			result.RecommendedLs = candidate.Ls
			break
		}
	}
	return result
}

func candidateCost(in Input, domains, floating []string, z string) (float64, bool) {
	minZJ, ok := minToOtherAvailableIn(in, domains, z)
	if !ok {
		return 0, false
	}
	total := 0.0
	for _, i := range domains {
		w := in.Writes[i]
		r := in.Reads[i]
		switch {
		case i == z:
			// Same domain as candidate Global Leader: reads cost nothing
			// cross-domain; each write pays one nearest-other-domain RTT.
			total += 2 * minZJ * w
		case in.Available[i]:
			liz, ok := oneWay(in, i, z)
			if !ok {
				return 0, false
			}
			total += 2 * liz * (w + r)
		default:
			// Unavailable source domain: its writes still route through the
			// nearest commit domain, its reads still pay i<->z.
			liz, ok := oneWay(in, i, z)
			if !ok {
				return 0, false
			}
			total += 2*(liz+minZJ)*w + 2*liz*r
		}
	}
	for _, u := range floating {
		total += floatingCost(in, domains, u, z, minZJ)
	}
	return total, true
}

func floatingCost(in Input, domains []string, u, z string, minZJ float64) float64 {
	w := in.Writes[u]
	r := in.Reads[u]
	luz, ok := oneWay(in, u, z)
	if !ok {
		return 0
	}
	readCost := 2 * luz * r
	directWrite := 2 * (luz + minZJ) * w
	bestWrite := directWrite
	for _, responder := range domains {
		if responder == z || !in.Available[responder] {
			continue
		}
		lzr, ok := oneWay(in, z, responder)
		if !ok {
			continue
		}
		lru, ok := oneWay(in, responder, u)
		if !ok {
			lru, ok = oneWay(in, u, responder)
		}
		if !ok {
			continue
		}
		relay := (luz + lzr + lru) * w
		if relay < bestWrite {
			bestWrite = relay
		}
	}
	return readCost + bestWrite
}

// Policy bounds migration decisions to avoid measurement-noise churn.
type Policy struct {
	MinImprovement float64       // required fractional cost reduction, e.g. 0.1
	Confirmations  int           // consecutive decision windows agreeing on target
	Cooldown       time.Duration // minimum spacing between migrations
}

// DefaultPolicy is a conservative production default.
func DefaultPolicy() Policy {
	return Policy{MinImprovement: 0.1, Confirmations: 3, Cooldown: time.Minute}
}

// Controller turns a stream of cost recommendations into at most one migration
// decision per cooldown, requiring a stable target across consecutive windows
// and a minimum predicted benefit. It is used by the external mover, never by
// the consensus core.
type Controller struct {
	policy        Policy
	candidate     string
	streak        int
	lastMigration time.Time
}

func NewController(policy Policy) *Controller {
	return &Controller{policy: policy}
}

// Observe records one decision window. It returns the target domain to migrate
// to, or "" when no migration should happen. currentLs is the predicted cost of
// keeping the current Global Leader domain; recommendedLs is the best candidate.
//
// Unlike the old in-core controller this does NOT optimistically start the
// cooldown: the mover calls RecordResult after the Move RPC returns, so the
// cooldown only starts on a migration that actually succeeded.
func (c *Controller) Observe(currentDomain, recommended string, currentLs, recommendedLs float64, now time.Time) string {
	if recommended == "" || recommended == currentDomain {
		c.candidate = ""
		c.streak = 0
		return ""
	}
	if currentLs <= 0 || recommendedLs >= currentLs*(1-c.policy.MinImprovement) {
		c.candidate = ""
		c.streak = 0
		return ""
	}
	if recommended == c.candidate {
		c.streak++
	} else {
		c.candidate = recommended
		c.streak = 1
	}
	if c.streak < c.policy.Confirmations {
		return ""
	}
	if !c.lastMigration.IsZero() && now.Sub(c.lastMigration) < c.policy.Cooldown {
		c.candidate = ""
		c.streak = 0
		return ""
	}
	c.candidate = ""
	c.streak = 0
	return recommended
}

// RecordResult starts the cooldown when a triggered migration succeeded. A
// failed handoff does not arm the cooldown, so a beneficial retry is not blocked
// by a transient catch-up/quorum/election failure.
func (c *Controller) RecordResult(success bool, now time.Time) {
	if success {
		c.lastMigration = now
	}
}
