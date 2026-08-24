package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/czq/cd-raft/internal/cdraft"
)

func main() {
	target := flag.String("target", "127.0.0.1:7101", "any node client address")
	origin := flag.String("origin-domain", "domain-b", "client domainCode")
	requestID := flag.String("request-id", "", "request id prefix; a per-op \"-<i>\" suffix is appended for writes")
	key := flag.String("key", "", "key to write or read")
	value := flag.String("value", "", "value to write; omit for read")
	count := flag.Int("count", 1, "number of operations to run sequentially")
	timeout := flag.Duration("timeout", 5*time.Second, "per-operation request deadline")
	callbackListen := flag.String("callback-listen", "127.0.0.1:0", "Fast Return callback bind address")
	replyRoute := flag.String("reply-route", "", "Fast Return address advertised to Domain Leaders")
	status := flag.Bool("status", false, "query the target node's status (domain/global leader, term, indices) and exit")
	metrics := flag.Bool("metrics", false, "query the target node's metrics (inter-domain RTT matrix, request counts) and exit")
	move := flag.String("move", "", "migrate the Global Leader to this domainCode (e.g. domain-c) via the GL-only Move RPC, then exit")
	moveReason := flag.String("move-reason", "manual move via cdraft-client", "reason recorded with -move")
	discover := flag.Bool("discover-topology", false, "discover current GL/DL topology and exit")
	reportFloatingLatency := flag.Bool("report-floating-latency", false, "discover topology, measure endpoint latency, report it as floating-domain telemetry, and exit")
	flag.Parse()

	if *count < 1 {
		*count = 1
	}

	client := cdraft.NewRPCClientWithCallback(*callbackListen, *replyRoute)
	defer client.Close()

	if *status {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		resp, err := client.Status(ctx, *target)
		if err != nil {
			fmt.Printf("status error: %v\n", err)
			os.Exit(1)
		}
		gl := resp.GetGlobalLeader()
		dl := resp.GetDomainLeader()
		fmt.Printf("node=%s domain=%s stage=%s domainRole=%s globalRole=%s domainLeader=%s globalLeader=%s globalLeaderDomain=%s globalTerm=%d domainTerm=%d commit=%d applied=%d\n",
			resp.GetNodeId(), resp.GetDomainCode(), resp.GetStage(),
			resp.GetDomainRole(), resp.GetGlobalRole(),
			orDash(dl.GetNodeId()), orDash(gl.GetNodeId()), orDash(gl.GetDomainId()),
			resp.GetGlobalTerm(), resp.GetDomainTerm(),
			resp.GetCommitIndex(), resp.GetAppliedIndex())
		return
	}
	if *metrics {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		m, err := client.Metrics(ctx, *target)
		if err != nil {
			fmt.Printf("metrics error: %v\n", err)
			os.Exit(1)
		}
		gl := m.GetGlobalLeader()
		fmt.Printf("metrics node=%s domain=%s globalLeader=%s globalLeaderDomain=%s\n",
			m.GetNodeId(), m.GetDomainCode(), orDash(gl.GetNodeId()), orDash(gl.GetDomainId()))
		rtt := m.GetInterDomainRttMillis()
		keys := make([]string, 0, len(rtt))
		for k := range rtt {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("rtt %s=%d\n", k, rtt[k])
		}
		ages := m.GetFloatingLatencyAgeMillis()
		fkeys := make([]string, 0, len(ages))
		for k := range ages {
			fkeys = append(fkeys, k)
		}
		sort.Strings(fkeys)
		for _, k := range fkeys {
			fmt.Printf("floating-latency-age %s=%dms\n", k, ages[k])
		}
		return
	}
	if *move != "" {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		// The Move RPC is GL-only, so first discover the current GL's dialable
		// address from the seed node, then send the move to the GL itself.
		view, err := client.DiscoverTopology(ctx, *target, *origin)
		if err != nil {
			fmt.Printf("discover error: %v\n", err)
			os.Exit(1)
		}
		glAddr := view.GetGlobalLeaderAddress()
		if glAddr == "" {
			glAddr = *target
		}
		if from := view.GetGlobalLeader().GetDomainId(); from == *move {
			fmt.Printf("Global Leader already in %s; nothing to do\n", *move)
			return
		}
		resp, err := client.Move(ctx, glAddr, *move, *moveReason)
		if err != nil {
			fmt.Printf("move RPC error: %v\n", err)
			os.Exit(1)
		}
		if resp.GetNotGlobalLeader() {
			fmt.Printf("move rejected: %s is not the global leader (current GL: %s)\n",
				glAddr, orDash(resp.GetGlobalLeader().GetNodeId()))
			os.Exit(1)
		}
		if !resp.GetAccepted() {
			fmt.Printf("move rejected: %s\n", orDash(resp.GetError()))
			os.Exit(1)
		}
		fmt.Printf("move OK: GL -> %s (term %d -> %d)\n", *move, resp.GetFromGlobalTerm(), resp.GetNewGlobalTerm())
		return
	}
	if *discover || *reportFloatingLatency {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		defer cancel()
		view, err := client.DiscoverTopology(ctx, *target, *origin)
		if err != nil {
			fmt.Printf("discover error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("globalLeader=%s domain=%s term=%d address=%s ttl=%dms\n",
			orDash(view.GetGlobalLeader().GetNodeId()),
			orDash(view.GetGlobalLeader().GetDomainId()),
			view.GetGlobalTerm(),
			orDash(view.GetGlobalLeaderAddress()),
			view.GetTtlMillis())
		for _, endpoint := range view.GetDomainLeaders() {
			fmt.Printf("domainLeader domain=%s node=%s client=%s interDomain=%s global=%t\n",
				endpoint.GetDomainId(), endpoint.GetNodeId(), endpoint.GetClientAddress(),
				endpoint.GetInterDomainAddress(), endpoint.GetGlobalLeader())
		}
		if !*reportFloatingLatency {
			return
		}
		latencies, err := client.MeasureFloatingLatencies(ctx, *origin, view)
		if err != nil {
			fmt.Printf("measure error: %v\n", err)
			os.Exit(1)
		}
		targetAddr := view.GetGlobalLeaderAddress()
		if targetAddr == "" {
			targetAddr = *target
		}
		if err := client.ReportFloatingLatency(ctx, targetAddr, *origin, view.GetGlobalTerm(), latencies); err != nil {
			fmt.Printf("report error: %v\n", err)
			os.Exit(1)
		}
		for domain, oneWay := range latencies {
			fmt.Printf("latency origin=%s target=%s oneWayMillis=%.0f\n", *origin, domain, oneWay)
		}
		return
	}

	isWrite := *value != ""
	base := *requestID
	if base == "" {
		base = fmt.Sprintf("cli-%d", time.Now().UnixNano())
	}

	latencies := make([]time.Duration, 0, *count)
	var total time.Duration
	failures := 0

	for i := 1; i <= *count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		start := time.Now()

		var line string
		var opErr error
		if isWrite {
			// Each op MUST be a distinct request: a shared request id is treated
			// as idempotent and would return the cached result instantly (~0s),
			// which would make latency numbers meaningless.
			decision, err := client.Write(ctx, *target, cdraft.ClientWriteRequest{
				RequestID:    fmt.Sprintf("%s-%d", base, i),
				OriginDomain: *origin,
				Command:      cdraft.Command{Key: *key, Value: *value},
			})
			opErr = err
			if err == nil {
				line = fmt.Sprintf("write winner=%s index=%d result=%s",
					decision.Winner, decision.Result.GlobalIndex, decision.Result.Result)
			}
		} else {
			result, err := client.ReadFrom(ctx, *target, *origin, *key)
			opErr = err
			if err == nil {
				line = fmt.Sprintf("read %s=%s", *key, result)
			}
		}

		elapsed := time.Since(start)
		cancel()

		if opErr != nil {
			failures++
			fmt.Printf("[%d/%d] ERROR after %s: %v\n", i, *count, elapsed.Round(time.Microsecond), opErr)
			continue
		}
		latencies = append(latencies, elapsed)
		total += elapsed
		fmt.Printf("[%d/%d] %s latency=%s\n", i, *count, line, elapsed.Round(time.Microsecond))
	}

	fmt.Println("----")
	success := len(latencies)
	if success == 0 {
		fmt.Printf("ops=%d success=0 failures=%d (all operations failed)\n", *count, failures)
		os.Exit(1)
	}

	min, max := latencies[0], latencies[0]
	for _, d := range latencies {
		if d < min {
			min = d
		}
		if d > max {
			max = d
		}
	}
	avg := total / time.Duration(success)
	fmt.Printf("ops=%d success=%d failures=%d total=%s avg=%s min=%s max=%s\n",
		*count, success, failures,
		total.Round(time.Microsecond), avg.Round(time.Microsecond),
		min.Round(time.Microsecond), max.Round(time.Microsecond))
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
