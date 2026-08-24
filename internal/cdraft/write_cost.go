package cdraft

import (
	"fmt"
	"math"

	"github.com/czq/cd-raft/internal/topology"
)

type WritePathCostInput struct {
	OriginDomain    string
	GlobalDomain    string
	ResponderDomain string
	FastReturn      bool
	OneWayMillis    map[string]float64
}

type WritePathCost struct {
	OriginKind                string
	FastestSecondQuorumDomain string
	ResponderDomain           string
	GlobalDirectMillis        float64
	CallbackMillis            float64
	CallbackAvailable         bool
	LowerBoundMillis          float64
	TheoreticalWinner         ResultSource
}

func EstimateWritePathCost(config topology.Config, input WritePathCostInput) WritePathCost {
	cost := WritePathCost{OriginKind: config.DomainKind(input.OriginDomain).String()}
	origin := input.OriginDomain
	global := input.GlobalDomain
	if global == "" {
		return cost
	}
	second, secondRTT := fastestSecondQuorumDomain(config, input, global)
	cost.FastestSecondQuorumDomain = second
	originToGlobal := writeCostOneWay(config, input, origin, global)
	globalToOrigin := writeCostOneWay(config, input, global, origin)
	cost.GlobalDirectMillis = originToGlobal + secondRTT + globalToOrigin
	cost.LowerBoundMillis = cost.GlobalDirectMillis
	cost.TheoreticalWinner = GlobalResponse

	responder := input.ResponderDomain
	if responder == "" && input.FastReturn && origin != "" && origin != global {
		if config.DomainKind(origin) == topology.DomainConsensus {
			responder = origin
		} else if config.DomainKind(origin) == topology.DomainFloating {
			responder = fastestFloatingResponder(config, input, global, origin)
		}
	}
	cost.ResponderDomain = responder
	if input.FastReturn && responder != "" && responder != global && origin != "" && origin != global {
		glToResponder := writeCostOneWay(config, input, global, responder)
		responderToOrigin := writeCostOneWay(config, input, responder, origin)
		// The replicated entry and the global-domain-quorum announcement both
		// travel GL->responder, but they are driven in parallel after the GL
		// receives the request. The lower bound therefore includes one such hop.
		cost.CallbackMillis = originToGlobal + glToResponder + responderToOrigin
		cost.CallbackAvailable = true
		if cost.CallbackMillis < cost.LowerBoundMillis {
			cost.LowerBoundMillis = cost.CallbackMillis
			cost.TheoreticalWinner = FastResponse
		}
	}
	return cost
}

func WriteCostRoute(from, to string) string {
	return from + "->" + to
}

func WritePathCostSummary(cost WritePathCost, observedMillis float64) string {
	overhead := observedMillis - cost.LowerBoundMillis
	if overhead < 0 {
		overhead = 0
	}
	if cost.CallbackAvailable {
		return fmt.Sprintf("network=%.1fms overhead=%.1fms direct=%.1fms callback=%.1fms responder=%s theoryWinner=%s secondQuorum=%s",
			cost.LowerBoundMillis, overhead, cost.GlobalDirectMillis, cost.CallbackMillis, cost.ResponderDomain,
			cost.TheoreticalWinner, cost.FastestSecondQuorumDomain)
	}
	return fmt.Sprintf("network=%.1fms overhead=%.1fms direct=%.1fms secondQuorum=%s",
		cost.LowerBoundMillis, overhead, cost.GlobalDirectMillis, cost.FastestSecondQuorumDomain)
}

func fastestSecondQuorumDomain(config topology.Config, input WritePathCostInput, global string) (string, float64) {
	bestDomain := ""
	bestRTT := math.Inf(1)
	for _, domain := range config.ConsensusDomains() {
		if domain == global {
			continue
		}
		rtt := writeCostOneWay(config, input, global, domain) + writeCostOneWay(config, input, domain, global)
		if bestDomain == "" || rtt < bestRTT {
			bestDomain = domain
			bestRTT = rtt
		}
	}
	if bestDomain == "" {
		return "", 0
	}
	return bestDomain, bestRTT
}

func fastestFloatingResponder(config topology.Config, input WritePathCostInput, global, origin string) string {
	bestDomain := ""
	bestCost := math.Inf(1)
	for _, domain := range config.ConsensusDomains() {
		if domain == global {
			continue
		}
		cost := writeCostOneWay(config, input, global, domain) + writeCostOneWay(config, input, domain, origin)
		if bestDomain == "" || cost < bestCost {
			bestDomain = domain
			bestCost = cost
		}
	}
	return bestDomain
}

func writeCostOneWay(config topology.Config, input WritePathCostInput, from, to string) float64 {
	if from == "" || to == "" {
		return 0
	}
	if input.OneWayMillis != nil {
		if delay, ok := input.OneWayMillis[WriteCostRoute(from, to)]; ok {
			return delay
		}
		if delay, ok := input.OneWayMillis[WriteCostRoute(to, from)]; ok {
			return delay
		}
	}
	return float64(config.OneWayDelay(from, to).Milliseconds())
}
