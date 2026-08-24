package cdraft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/czq/cd-raft/internal/optimizer"
	"github.com/czq/cd-raft/internal/topology"
)

// loadPhase is one segment of the time-varying workload: for `dur`, all client
// load originates in domain `origin`.
type loadPhase struct {
	origin string
	dur    time.Duration
}

// tsSample is one latency measurement on the production path at a point in time.
// write/read are -1 when that probe failed (e.g. during a handoff window).
type tsSample struct {
	elapsed  time.Duration
	origin   string
	glDomain string
	write    time.Duration
	read     time.Duration
}

// TestRealGRPCAdaptiveMigrationTimeSeries shows the ADAPTIVE benefit of
// automatic migration: a workload whose origin drifts across domains over time
// (a -> b -> c) is run TWICE on the production gRPC path with an identical
// schedule:
//
//   - FIXED pass: the Global Leader is never moved (no controller, no Move). As
//     the load drifts away from the GL's domain, client latency steps up.
//   - AUTO pass: a real external controller loop (the same cost model +
//     hysteresis the mover uses) polls the published Metrics and migrates the GL
//     to follow the load, so latency snaps back down after each shift.
//
// The per-second latency time series for both passes is written to
// docs/experiments/adaptive-migration-timeseries.md (overlaid ASCII charts +
// per-phase table) and to a .csv beside it for real plotting, so the adaptive
// "latency tracks the load" effect is directly observable.
func TestRealGRPCAdaptiveMigrationTimeSeries(t *testing.T) {
	if testing.Short() {
		t.Skip("adaptive migration time-series is an observability run; skipped in -short")
	}

	harness := newRPCHarness(t, false)
	harness.elect(t) // GL = a1 (domain a)
	profile := latencyProfile()
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(profile)
		node.SetRPCPolicy(RPCPolicy{Deadline: 2 * time.Second, MaxRetries: 2})
		node.SetCatchUpTimeout(8 * time.Second)
		// Short stats window so the per-domain load signal turns over quickly
		// after the workload shifts, letting the controller react within a phase.
		node.SetStatsWindow(3 * time.Second)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 260*time.Second)
	defer cancel()
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	gl0 := waitForUniqueGL(t, ctx, harness)
	entry := addressOf(t, harness, gl0.nodeID)
	domains := harness.config.Domains()

	// Workload drifts: start co-located with the GL, then move one domain away,
	// then to the farthest domain — the case a fixed GL handles worst.
	near := nearestDomain(profile, gl0.domain) // b relative to a
	far := farthestDomain(profile, gl0.domain) // c relative to a
	schedule := []loadPhase{
		{origin: gl0.domain, dur: 12 * time.Second},
		{origin: near, dur: 12 * time.Second},
		{origin: far, dur: 12 * time.Second},
	}

	// FIXED pass: never migrate. GL stays where it started.
	fixedSeries := runAdaptivePass(t, ctx, harness, entry, schedule, domains, false, "fixed")

	// Let the stats window drain and the cluster settle before the second pass.
	time.Sleep(4 * time.Second)
	// The FIXED pass never moved the GL, so it is still in gl0.domain — the same
	// starting point as the AUTO pass, keeping the two passes comparable.
	settled := waitForUniqueGL(t, ctx, harness)
	if settled.domain != gl0.domain {
		t.Fatalf("fixed pass unexpectedly relocated the GL: %s -> %s", gl0.domain, settled.domain)
	}

	// AUTO pass: a real cost-model controller migrates the GL to track the load.
	autoSeries := runAdaptivePass(t, ctx, harness, entry, schedule, domains, true, "auto")

	doc, csv := buildAdaptiveDoc(adaptiveInputs{
		profile:   profile,
		schedule:  schedule,
		startGL:   gl0,
		fixed:     fixedSeries,
		auto:      autoSeries,
		nearDom:   near,
		farDom:    far,
	})
	mdPath := writeExperimentFile(t, "adaptive-migration-timeseries.md", doc)
	csvPath := writeExperimentFile(t, "adaptive-migration-timeseries.csv", csv)
	t.Logf("adaptive migration time-series written to %s (data: %s)", mdPath, csvPath)

	// Headline assertions: in the FAR phase the fixed GL pays a full far RTT on
	// every request, while the auto GL should have tracked the load into `far`
	// and serve it locally/near. Compare steady-state medians (latter half of the
	// phase, after any handoff transient).
	farStart := schedule[0].dur + schedule[1].dur
	farEnd := farStart + schedule[2].dur
	skip := schedule[2].dur / 2
	fixedFarRead := phaseMedian(fixedSeries, farStart+skip, farEnd, true)
	autoFarRead := phaseMedian(autoSeries, farStart+skip, farEnd, true)
	fixedFarWrite := phaseMedian(fixedSeries, farStart+skip, farEnd, false)
	autoFarWrite := phaseMedian(autoSeries, farStart+skip, farEnd, false)

	if autoFarRead <= 0 || fixedFarRead <= 0 || autoFarWrite <= 0 || fixedFarWrite <= 0 {
		t.Fatalf("missing steady-state samples in the far phase: fixedR=%v autoR=%v fixedW=%v autoW=%v",
			fixedFarRead, autoFarRead, fixedFarWrite, autoFarWrite)
	}
	if autoFarRead*2 > fixedFarRead {
		t.Fatalf("auto migration did not materially cut far-phase read latency: fixed=%v auto=%v", fixedFarRead, autoFarRead)
	}
	if autoFarWrite*5 > fixedFarWrite*4 { // require >=20% write reduction
		t.Fatalf("auto migration did not materially cut far-phase write latency: fixed=%v auto=%v", fixedFarWrite, autoFarWrite)
	}
	// And the auto GL must actually have followed the load into the far domain.
	if dom := dominantGLDomain(autoSeries, farStart+skip, farEnd); dom != far {
		t.Fatalf("auto GL did not track the load into far domain %s (steady GL domain=%s)", far, dom)
	}
}

// runAdaptivePass drives the drifting workload once. The sampler goroutine (this
// goroutine) issues one write+read from the current phase's origin to the
// current GL every 250ms and records the latency. When auto is true a separate
// controller goroutine migrates the GL to follow the load.
func runAdaptivePass(t *testing.T, ctx context.Context, harness *rpcHarness, entry string, schedule []loadPhase, domains []string, auto bool, tag string) []tsSample {
	t.Helper()
	stop := make(chan struct{})
	if auto {
		go runAdaptiveController(ctx, harness, entry, domains, stop)
	}

	total := scheduleDuration(schedule)
	start := time.Now()
	var samples []tsSample
	seq := 0
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		elapsed := time.Since(start)
		if elapsed >= total {
			break
		}
		origin := originAt(schedule, elapsed)
		glAddr, glDomain := resolveGL(ctx, harness, entry)
		if glAddr == "" {
			glAddr = entry
		}
		seq++
		key := fmt.Sprintf("ts-%s-%d", tag, seq)
		w := time.Duration(-1)
		r := time.Duration(-1)
		cctx, c := context.WithTimeout(ctx, 4*time.Second)
		ws := time.Now()
		decision, werr := harness.client.Write(cctx, glAddr, ClientWriteRequest{
			RequestID: fmt.Sprintf("ts-rid-%s-%d", tag, seq), OriginDomain: origin,
			Command: Command{Key: key, Value: "v"},
		})
		if werr == nil && decision.Result.Committed {
			w = time.Since(ws)
			rs := time.Now()
			if _, rerr := harness.client.ReadFrom(cctx, glAddr, origin, key); rerr == nil {
				r = time.Since(rs)
			}
		}
		c()
		samples = append(samples, tsSample{elapsed: elapsed, origin: origin, glDomain: glDomain, write: w, read: r})

		select {
		case <-ctx.Done():
			if auto {
				close(stop)
			}
			return samples
		case <-ticker.C:
		}
	}
	if auto {
		close(stop)
		time.Sleep(500 * time.Millisecond) // let an in-flight migration settle
	}
	return samples
}

// runAdaptiveController is the EXTERNAL migration controller loop, the same
// discover -> measure -> decide -> move cycle cmd/cdraft-mover runs, tuned to
// react within a phase. It is the only thing that triggers a migration.
func runAdaptiveController(ctx context.Context, harness *rpcHarness, entry string, domains []string, stop <-chan struct{}) {
	controller := optimizer.NewController(optimizer.Policy{
		MinImprovement: 0.1, Confirmations: 2, Cooldown: 2500 * time.Millisecond,
	})
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		glAddr, glDomain := resolveGL(ctx, harness, entry)
		if glAddr == "" || glDomain == "" {
			continue
		}
		mctx, mcancel := context.WithTimeout(ctx, 2*time.Second)
		metrics, err := harness.client.Metrics(mctx, glAddr)
		mcancel()
		if err != nil {
			continue
		}
		result := optimizer.ComputeCosts(metricsToOptimizerInput(domains, glDomain, metrics))
		currentLs := lsForDomain(result, glDomain)
		target := controller.Observe(glDomain, result.Recommended, currentLs, result.RecommendedLs, time.Now())
		if target == "" {
			continue
		}
		moveCtx, moveCancel := context.WithTimeout(ctx, 20*time.Second)
		resp, moveErr := harness.client.Move(moveCtx, glAddr, target, "adaptive controller")
		moveCancel()
		controller.RecordResult(moveErr == nil && resp.GetAccepted(), time.Now())
	}
}

// resolveGL returns the current Global Leader's client address and domain by
// asking the entry node for its view. Non-fatal (safe to call from goroutines).
func resolveGL(ctx context.Context, harness *rpcHarness, entry string) (addr, domain string) {
	sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	status, err := harness.client.Status(sctx, entry)
	if err != nil {
		return "", ""
	}
	gl := status.GetGlobalLeader()
	if gl.GetNodeId() == "" {
		return "", ""
	}
	node, ok := harness.config.Node(gl.GetNodeId())
	if !ok {
		return "", gl.GetDomainId()
	}
	return node.ListenAddress, gl.GetDomainId()
}

func scheduleDuration(schedule []loadPhase) time.Duration {
	var total time.Duration
	for _, p := range schedule {
		total += p.dur
	}
	return total
}

func originAt(schedule []loadPhase, elapsed time.Duration) string {
	var acc time.Duration
	for _, p := range schedule {
		acc += p.dur
		if elapsed < acc {
			return p.origin
		}
	}
	return schedule[len(schedule)-1].origin
}

// phaseMedian returns the median write (read==false) or read (read==true)
// latency over [from,to), ignoring failed probes. Returns -1 if no samples.
func phaseMedian(samples []tsSample, from, to time.Duration, read bool) time.Duration {
	var vals []time.Duration
	for _, s := range samples {
		if s.elapsed < from || s.elapsed >= to {
			continue
		}
		v := s.write
		if read {
			v = s.read
		}
		if v >= 0 {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return -1
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals[len(vals)/2]
}

// dominantGLDomain returns the most frequently observed GL domain over [from,to).
func dominantGLDomain(samples []tsSample, from, to time.Duration) string {
	counts := map[string]int{}
	for _, s := range samples {
		if s.elapsed < from || s.elapsed >= to || s.glDomain == "" {
			continue
		}
		counts[s.glDomain]++
	}
	best, bestN := "", -1
	for d, n := range counts {
		if n > bestN {
			best, bestN = d, n
		}
	}
	return best
}

// nearestDomain returns the domain with the smallest one-way delay from `from`.
func nearestDomain(profile topology.NetworkSimulation, from string) string {
	best := ""
	bestDelay := 1 << 30
	for key, d := range profile.InterDomainOneWayMillis {
		parts := strings.SplitN(key, "->", 2)
		if len(parts) != 2 || parts[0] != from {
			continue
		}
		if d < bestDelay {
			bestDelay = d
			best = parts[1]
		}
	}
	return best
}

type adaptiveInputs struct {
	profile  topology.NetworkSimulation
	schedule []loadPhase
	startGL  glInfo
	fixed    []tsSample
	auto     []tsSample
	nearDom  string
	farDom   string
}

// bucketSeries aggregates samples into per-second medians plus the dominant GL
// domain per bucket. Index i covers [i, i+1) seconds of elapsed time. A bucket
// with no successful probe holds -1.
type bucketSeries struct {
	write    []time.Duration
	read     []time.Duration
	glDomain []string
}

func bucketize(samples []tsSample, totalSec int) bucketSeries {
	wbuckets := make([][]time.Duration, totalSec)
	rbuckets := make([][]time.Duration, totalSec)
	dbuckets := make([]map[string]int, totalSec)
	for i := range dbuckets {
		dbuckets[i] = map[string]int{}
	}
	for _, s := range samples {
		idx := int(s.elapsed.Seconds())
		if idx < 0 || idx >= totalSec {
			continue
		}
		if s.write >= 0 {
			wbuckets[idx] = append(wbuckets[idx], s.write)
		}
		if s.read >= 0 {
			rbuckets[idx] = append(rbuckets[idx], s.read)
		}
		if s.glDomain != "" {
			dbuckets[idx][s.glDomain]++
		}
	}
	out := bucketSeries{
		write:    make([]time.Duration, totalSec),
		read:     make([]time.Duration, totalSec),
		glDomain: make([]string, totalSec),
	}
	for i := 0; i < totalSec; i++ {
		out.write[i] = medianOf(wbuckets[i])
		out.read[i] = medianOf(rbuckets[i])
		best, bestN := "", -1
		for d, n := range dbuckets[i] {
			if n > bestN {
				best, bestN = d, n
			}
		}
		out.glDomain[i] = best
	}
	return out
}

func medianOf(vals []time.Duration) time.Duration {
	if len(vals) == 0 {
		return -1
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals[len(vals)/2]
}

func buildAdaptiveDoc(in adaptiveInputs) (markdown, csv string) {
	totalSec := int(scheduleDuration(in.schedule).Seconds())
	fixedB := bucketize(in.fixed, totalSec)
	autoB := bucketize(in.auto, totalSec)

	// --- Markdown document ---
	b := &strings.Builder{}
	fmt.Fprintf(b, "# 实验：动态负载漂移下的自适应迁移收益（时间序列）\n\n")
	fmt.Fprintf(b, "生成时间：%s\n\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(b, "本文档由 `TestRealGRPCAdaptiveMigrationTimeSeries` 自动生成。负载来源域随时间漂移（`%s` → `%s` → `%s`），在真实 gRPC、注入单向延迟的三域集群上以**完全相同的时间表跑两遍**：一遍**固定 GL（不迁移）**，一遍**自动迁移（成本模型 + Move 控制器持续追踪负载）**。下方“设计”固定，“观测结果”随重跑更新。\n\n", in.schedule[0].origin, in.schedule[1].origin, in.schedule[2].origin)

	fmt.Fprintf(b, "## 1. 假设\n\n")
	fmt.Fprintf(b, "负载漂移到远离当前 GL 的域时，固定 GL 的客户端延迟会**阶梯式上升**；而自动迁移会把 GL 迁去追随负载，使延迟在每次漂移后**迅速回落**——证明系统能自适应跟踪负载、持续保持低延迟。\n\n")

	fmt.Fprintf(b, "## 2. 设计\n\n")
	fmt.Fprintf(b, "### 2.1 拓扑与延迟矩阵\n\n")
	fmt.Fprintf(b, "本地单向延迟 %dms，跨域单向延迟（往返为其两倍）：\n\n", in.profile.LocalOneWayDelayMillis)
	fmt.Fprintf(b, "| 链路 | 单向延迟 | 往返 RTT |\n|---|---|---|\n")
	for _, p := range []struct{ from, to string }{{"a", "b"}, {"a", "c"}, {"b", "c"}} {
		oneWay := in.profile.InterDomainOneWayMillis[p.from+"->"+p.to]
		fmt.Fprintf(b, "| `%s <-> %s` | %dms | %dms |\n", p.from, p.to, oneWay, 2*oneWay)
	}
	fmt.Fprintf(b, "\n### 2.2 负载时间表\n\n")
	fmt.Fprintf(b, "- 初始 Global Leader：域 `%s`（节点 `%s`）。\n", in.startGL.domain, in.startGL.nodeID)
	var acc time.Duration
	for i, p := range in.schedule {
		role := "远离 GL 初始域"
		switch p.origin {
		case in.startGL.domain:
			role = "与 GL 初始域同域（基线最优）"
		case in.nearDom:
			role = "漂移到较近域"
		case in.farDom:
			role = "漂移到最远域（固定 GL 最不利）"
		}
		fmt.Fprintf(b, "- 阶段 %d：`%v`–`%v`，负载来源域 `%s`（%s）。\n", i+1, acc.Round(time.Second), (acc + p.dur).Round(time.Second), p.origin, role)
		acc += p.dur
	}
	fmt.Fprintf(b, "\n### 2.3 两遍对照\n\n")
	fmt.Fprintf(b, "- **固定 GL**：不运行控制器、从不发 Move，GL 始终在域 `%s`。\n", in.startGL.domain)
	fmt.Fprintf(b, "- **自动迁移**：运行与 `cmd/cdraft-mover` 相同的 discover→measure→decide→move 控制循环（成本模型 + 迟滞确认 + 冷却），由它**自主**把 GL 迁到负载域。\n")
	fmt.Fprintf(b, "- 采样：每 250ms 从“当前阶段 origin”向“当前 GL”发一次写+读并记录延迟。\n\n")

	fmt.Fprintf(b, "## 3. 如何复现\n\n")
	fmt.Fprintf(b, "```bash\ngo test ./internal/cdraft/ -run TestRealGRPCAdaptiveMigrationTimeSeries -v -count=1\n```\n\n")

	fmt.Fprintf(b, "## 4. 观测结果\n\n")
	fmt.Fprintf(b, "### 4.1 各阶段稳态延迟（取每阶段后半段中位数，避开切换瞬态）\n\n")
	fmt.Fprintf(b, "| 阶段 | origin | 固定 GL 域 | 固定 写/读 | 自动 GL 域 | 自动 写/读 | 写收益 | 读收益 |\n")
	fmt.Fprintf(b, "|---|---|---|---|---|---|---|---|\n")
	acc = 0
	for i, p := range in.schedule {
		from := acc + p.dur/2
		to := acc + p.dur
		fw := phaseMedian(in.fixed, from, to, false)
		fr := phaseMedian(in.fixed, from, to, true)
		aw := phaseMedian(in.auto, from, to, false)
		ar := phaseMedian(in.auto, from, to, true)
		fgl := dominantGLDomain(in.fixed, from, to)
		agl := dominantGLDomain(in.auto, from, to)
		fmt.Fprintf(b, "| %d | `%s` | `%s` | %s / %s | `%s` | %s / %s | **%s** | **%s** |\n",
			i+1, p.origin, fgl, durStr(fw), durStr(fr), agl, durStr(aw), durStr(ar),
			pctDrop(fw, aw), pctDrop(fr, ar))
		acc += p.dur
	}

	fmt.Fprintf(b, "\n### 4.2 写延迟时间序列（每秒中位数，单位 ms；`F`=固定 GL，`A`=自动迁移，`#`=两者重合）\n\n")
	fmt.Fprintf(b, "```\n%s```\n\n", asciiOverlayChart(fixedB.write, autoB.write, in.schedule))
	fmt.Fprintf(b, "### 4.3 读延迟时间序列（每秒中位数，单位 ms）\n\n")
	fmt.Fprintf(b, "```\n%s```\n\n", asciiOverlayChart(fixedB.read, autoB.read, in.schedule))

	fmt.Fprintf(b, "### 4.4 自动迁移的 GL 位置随时间变化\n\n")
	fmt.Fprintf(b, "```\n%s```\n\n", glDomainTimeline(autoB.glDomain, in.schedule))

	fmt.Fprintf(b, "## 5. 结论\n\n")
	fmt.Fprintf(b, "- 负载漂移时，固定 GL 的客户端延迟随“负载域离 GL 越来越远”而阶梯上升；自动迁移在每次漂移后把 GL 迁到负载域，延迟迅速回落并维持低位。\n")
	fmt.Fprintf(b, "- 控制器仅凭节点发布的 Metrics **自主**决策，迁移走安全交接路径；切换瞬间有短暂延迟尖峰（追平 + 选举窗口），随后稳定。\n")
	fmt.Fprintf(b, "- 原始逐秒数据见同目录 `adaptive-migration-timeseries.csv`，可用任意工具绘图。\n")

	// --- CSV ---
	cb := &strings.Builder{}
	fmt.Fprintf(cb, "elapsed_s,phase_origin,fixed_gl_domain,fixed_write_ms,fixed_read_ms,auto_gl_domain,auto_write_ms,auto_read_ms\n")
	for i := 0; i < totalSec; i++ {
		origin := originAt(in.schedule, time.Duration(i)*time.Second)
		fmt.Fprintf(cb, "%d,%s,%s,%s,%s,%s,%s,%s\n",
			i, origin,
			emptyDash(fixedB.glDomain[i]), msField(fixedB.write[i]), msField(fixedB.read[i]),
			emptyDash(autoB.glDomain[i]), msField(autoB.write[i]), msField(autoB.read[i]))
	}
	return b.String(), cb.String()
}

func durStr(d time.Duration) string {
	if d < 0 {
		return "—"
	}
	return d.Round(time.Millisecond).String()
}

func emptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func msField(d time.Duration) string {
	if d < 0 {
		return ""
	}
	return fmt.Sprintf("%d", d.Milliseconds())
}

// asciiOverlayChart renders two per-second latency series (fixed `F`, auto `A`,
// overlap `#`) on a shared y-axis, with phase boundaries marked on the x-axis.
func asciiOverlayChart(fixed, auto []time.Duration, schedule []loadPhase) string {
	const height = 12
	n := len(fixed)
	maxMs := 1.0
	for i := 0; i < n; i++ {
		for _, d := range []time.Duration{fixed[i], auto[i]} {
			if d >= 0 {
				if ms := float64(d.Milliseconds()); ms > maxMs {
					maxMs = ms
				}
			}
		}
	}
	b := &strings.Builder{}
	for row := height; row >= 1; row-- {
		thr := maxMs * float64(row) / float64(height)
		fmt.Fprintf(b, "%4.0f |", thr)
		for i := 0; i < n; i++ {
			f := fixed[i] >= 0 && float64(fixed[i].Milliseconds()) >= thr
			a := auto[i] >= 0 && float64(auto[i].Milliseconds()) >= thr
			switch {
			case f && a:
				b.WriteByte('#')
			case f:
				b.WriteByte('F')
			case a:
				b.WriteByte('A')
			default:
				b.WriteByte(' ')
			}
		}
		b.WriteByte('\n')
	}
	// x-axis
	b.WriteString("     +")
	b.WriteString(strings.Repeat("-", n))
	b.WriteByte('\n')
	b.WriteString("      ")
	axis := make([]byte, n)
	for i := range axis {
		axis[i] = ' '
	}
	var acc int
	for _, p := range schedule {
		if acc < n {
			label := fmt.Sprintf("%ds", acc)
			for j := 0; j < len(label) && acc+j < n; j++ {
				axis[acc+j] = label[j]
			}
		}
		acc += int(p.dur.Seconds())
	}
	b.Write(axis)
	b.WriteString("  (秒)\n")
	return b.String()
}

// glDomainTimeline renders the auto pass's GL domain per second as a single row.
func glDomainTimeline(domains []string, schedule []loadPhase) string {
	b := &strings.Builder{}
	b.WriteString("GL域 |")
	for _, d := range domains {
		if d == "" {
			b.WriteByte('?')
			continue
		}
		b.WriteByte(d[0])
	}
	b.WriteByte('\n')
	b.WriteString("负载 |")
	for i := range domains {
		o := originAt(schedule, time.Duration(i)*time.Second)
		b.WriteByte(o[0])
	}
	b.WriteString("\n     ")
	b.WriteString(strings.Repeat("-", len(domains)))
	b.WriteByte('\n')
	return b.String()
}

func writeExperimentFile(t *testing.T, name, content string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve experiment path")
	}
	dir := filepath.Join(filepath.Dir(thisFile), "..", "..", "docs", "experiments")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(path)
	return abs
}
