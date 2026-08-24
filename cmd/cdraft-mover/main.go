// Command cdraft-mover is the EXTERNAL Global Leader migration controller.
//
// The cd-raft consensus core no longer decides where the Global Leader should
// live. It only (a) measures per-domain read/write window statistics and the
// inter-domain RTT matrix, exposing them over the Metrics RPC, and (b) executes
// a safe handoff when the current Global Leader receives a Move RPC.
//
// This tool closes that loop from the outside: it discovers the current Global
// Leader, pulls its window statistics, runs the paper cost model
// (internal/optimizer) to pick the lowest-cost domain, applies a hysteresis +
// cooldown policy, and—only when a move is warranted—calls the GL-only Move RPC.
// The single invariant the core enforces is that Move is honored exclusively by
// the current Global Leader, so a stray controller cannot move leadership from a
// non-leader.
package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/czq/cd-raft/internal/cdraft"
	"github.com/czq/cd-raft/internal/optimizer"
	"github.com/czq/cd-raft/internal/topology"
)

func main() {
	configPath := flag.String("config", "config/cluster.json", "static CD-Raft topology (to map node ids -> client addresses)")
	poll := flag.Duration("poll", 2*time.Second, "decision/poll interval")
	minImprovement := flag.Float64("min-improvement", 0.1, "required fractional L_s reduction before moving")
	confirmations := flag.Int("confirmations", 3, "consecutive windows that must agree on the target before moving")
	cooldown := flag.Duration("cooldown", 30*time.Second, "minimum spacing between successful moves")
	rpcTimeout := flag.Duration("rpc-timeout", 3*time.Second, "deadline for Metrics/Status RPCs")
	moveTimeout := flag.Duration("move-timeout", 20*time.Second, "deadline for a Move RPC (covers catch-up + handoff)")
	once := flag.Bool("once", false, "evaluate a single decision and exit")
	flag.Parse()

	cfg, err := topology.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	addrByNode := make(map[string]string, len(cfg.Nodes))
	var seeds []string
	for _, node := range cfg.Nodes {
		addrByNode[node.ID] = node.ClientDialAddress()
		seeds = append(seeds, node.ClientDialAddress())
	}

	client := cdraft.NewRPCClient()
	defer client.Close()

	controller := optimizer.NewController(optimizer.Policy{
		MinImprovement: *minImprovement,
		Confirmations:  *confirmations,
		Cooldown:       *cooldown,
	})

	domains := cfg.Domains()
	log.Printf("cdraft-mover: domains=%v poll=%s minImprovement=%.2f confirmations=%d cooldown=%s",
		domains, *poll, *minImprovement, *confirmations, *cooldown)

	for {
		tick(client, addrByNode, seeds, domains, controller, *rpcTimeout, *moveTimeout)
		if *once {
			return
		}
		time.Sleep(*poll)
	}
}

// tick performs one discover -> measure -> decide -> (maybe) move cycle.
func tick(
	client *cdraft.RPCClient,
	addrByNode map[string]string,
	seeds []string,
	domains []string,
	controller *optimizer.Controller,
	rpcTimeout, moveTimeout time.Duration,
) {
	glNode, glDomain := discoverGlobalLeader(client, seeds, rpcTimeout)
	if glNode == "" {
		log.Printf("cdraft-mover: no global leader visible yet; skipping")
		return
	}
	glAddr, ok := addrByNode[glNode]
	if !ok {
		log.Printf("cdraft-mover: GL node %q not in config; skipping", glNode)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	metrics, err := client.Metrics(ctx, glAddr)
	cancel()
	if err != nil {
		log.Printf("cdraft-mover: metrics from GL %s failed: %v", glNode, err)
		return
	}

	input := buildInput(domains, glDomain, metrics)
	result := optimizer.ComputeCosts(input)

	currentLs := 0.0
	for _, c := range result.Candidates {
		if c.Domain == glDomain {
			currentLs = c.Ls
		}
	}

	if result.Recommended != "" && result.Recommended != glDomain {
		log.Printf("cdraft-mover: GL=%s(%s) currentLs=%.1f best=%s(Ls=%.1f)",
			glNode, glDomain, currentLs, result.Recommended, result.RecommendedLs)
	}

	target := controller.Observe(glDomain, result.Recommended, currentLs, result.RecommendedLs, time.Now())
	if target == "" {
		return
	}

	reason := "cdraft-mover: cost-model decision"
	log.Printf("cdraft-mover: requesting MOVE %s -> %s (Ls %.1f -> %.1f)", glDomain, target, currentLs, result.RecommendedLs)
	mctx, mcancel := context.WithTimeout(context.Background(), moveTimeout)
	resp, err := client.Move(mctx, glAddr, target, reason)
	mcancel()
	if err != nil {
		log.Printf("cdraft-mover: move RPC error: %v", err)
		controller.RecordResult(false, time.Now())
		return
	}
	if resp.GetNotGlobalLeader() {
		log.Printf("cdraft-mover: %s is no longer GL; will rediscover next tick", glNode)
		controller.RecordResult(false, time.Now())
		return
	}
	controller.RecordResult(resp.GetAccepted(), time.Now())
	if resp.GetAccepted() {
		log.Printf("cdraft-mover: MOVE OK -> %s (term %d)", target, resp.GetNewGlobalTerm())
	} else {
		log.Printf("cdraft-mover: MOVE rejected: %s", resp.GetError())
	}
}

// discoverGlobalLeader queries seeds until one reports the current Global Leader.
func discoverGlobalLeader(client *cdraft.RPCClient, seeds []string, timeout time.Duration) (node, domain string) {
	for _, addr := range seeds {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		metrics, err := client.Metrics(ctx, addr)
		cancel()
		if err != nil {
			continue
		}
		gl := metrics.GetGlobalLeader()
		if gl.GetNodeId() != "" {
			return gl.GetNodeId(), gl.GetDomainId()
		}
	}
	return "", ""
}

// buildInput turns the GL's raw window statistics into the cost-model input. The
// node deliberately does NOT compute costs; that is this controller's job.
func buildInput(domains []string, glDomain string, metrics interface {
	GetWriteRequestsByDomain() map[string]uint64
	GetReadRequestsByDomain() map[string]uint64
	GetInterDomainRttMillis() map[string]uint64
	GetDomainLeaders() map[string]string
}) optimizer.Input {
	writes := make(map[string]float64)
	for d, c := range metrics.GetWriteRequestsByDomain() {
		writes[d] = float64(c)
	}
	reads := make(map[string]float64)
	for d, c := range metrics.GetReadRequestsByDomain() {
		reads[d] = float64(c)
	}

	// inter_domain_rtt_millis keys are "from->to" with the measured round trip;
	// the cost model wants one-way estimates, so halve them.
	oneWay := make(map[string]map[string]float64)
	for key, rtt := range metrics.GetInterDomainRttMillis() {
		parts := strings.SplitN(key, "->", 2)
		if len(parts) != 2 {
			continue
		}
		from, to := parts[0], parts[1]
		if oneWay[from] == nil {
			oneWay[from] = make(map[string]float64)
		}
		oneWay[from][to] = float64(rtt) / 2
	}

	leaders := metrics.GetDomainLeaders()
	available := make(map[string]bool, len(domains))
	consensus := make(map[string]struct{}, len(domains))
	for _, d := range domains {
		consensus[d] = struct{}{}
		available[d] = d == glDomain || leaders[d] != ""
	}
	floatingSet := make(map[string]struct{})
	for d, c := range writes {
		if c > 0 {
			if _, ok := consensus[d]; !ok {
				floatingSet[d] = struct{}{}
			}
		}
	}
	for d, c := range reads {
		if c > 0 {
			if _, ok := consensus[d]; !ok {
				floatingSet[d] = struct{}{}
			}
		}
	}
	floating := make([]string, 0, len(floatingSet))
	for d := range floatingSet {
		floating = append(floating, d)
	}

	return optimizer.Input{
		Domains:          domains,
		ConsensusDomains: domains,
		FloatingDomains:  floating,
		Writes:           writes,
		Reads:            reads,
		Available:        available,
		OneWayMillis:     oneWay,
	}
}
