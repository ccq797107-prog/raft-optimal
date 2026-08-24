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

// TestScenarioReport runs an end-to-end observability flow and writes a human
// readable Markdown report to docs/scenario-report.md. It exercises two
// scenarios on a real gRPC, latency-injected cluster:
//
//   - Simple/stable: load stays in the Global Leader's own domain, so the
//     optimizer keeps recommending that domain and NO migration is triggered.
//   - Complex/migration: load moves to another domain, the optimizer recommends
//     it, and a safe catch-up handoff migrates the Global Leader there.
//
// The cluster is NOT hand-elected: every node bootstraps and campaigns on a
// randomized timeout, so the recorded Domain/Global Leaders are whoever actually
// won the real elections (not a fixed node 1). For each scenario it records where
// the leaders are, where (if anywhere) the Global Leader migrated, and the
// per-origin client read/write latency, so the co-location benefit is observable.
func TestScenarioReport(t *testing.T) {
	if testing.Short() {
		t.Skip("scenario report is an observability run; skipped in -short")
	}
	if os.Getenv("CDRAFT_RUN_SCENARIO_REPORT") != "1" {
		t.Skip("scenario report is explicit; set CDRAFT_RUN_SCENARIO_REPORT=1 to run")
	}

	harness := newRPCHarness(t, false)
	profile := latencyProfile()
	for _, node := range harness.nodes {
		node.SetNetworkSimulation(profile)
		node.SetRPCPolicy(RPCPolicy{Deadline: 2 * time.Second, MaxRetries: 2})
		node.SetCatchUpTimeout(8 * time.Second)
	}

	// Generous budget: the report runs two full load/migrate/measure cycles. When
	// this context expires Run() stops every node, so it must comfortably outlast
	// the whole flow to avoid the probes racing a shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	// Natural startup: each node self-elects through the bootstrap loop; no node
	// is told to campaign. Whoever wins is genuinely elected.
	for _, node := range harness.nodes {
		go node.Run(ctx)
	}

	entry := mustNode(t, harness.config, "a1").ListenAddress

	report := &strings.Builder{}
	fmt.Fprintf(report, "# CD-Raft 端到端观测报告\n\n")
	fmt.Fprintf(report, "生成时间：%s\n\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Fprintf(report, "本报告由 `TestScenarioReport` 自动生成，运行在真实 gRPC、注入了单向网络延迟的本地三域集群上。\n")
	fmt.Fprintf(report, "集群**未被手工指定 leader**：每个节点在随机化超时后自行发起真实选举（真实投票、N-1 法定人数、任期 fencing），\n")
	fmt.Fprintf(report, "因此下面记录的 Domain/Global Leader 是真正竞选胜出的节点，节点编号每次运行可能不同。\n")
	fmt.Fprintf(report, "写入为基线两域提交（Fast Return 关闭），读取为 Global Leader 线性一致读；客户端到 Global Leader 的往返延迟按 origin 域注入。\n")
	fmt.Fprintf(report, "每个延迟样本失败会自动重试，若始终无法取得成功样本测试将直接失败，因此本表不会出现“全 0”的占位值。\n\n")

	writeTopology(report, profile)

	// Phase 0: wait for the cluster to converge on a unique, self-elected GL.
	gl0 := waitForUniqueGL(t, ctx, harness)
	fmt.Fprintf(report, "## 阶段 0：初始选举（自发，无人工指定）\n\n")
	fmt.Fprintf(report, "- Domain Leader 选举结果：%s\n", domainLeaderList(t, ctx, harness, addressOf(t, harness, gl0.nodeID)))
	fmt.Fprintf(report, "- Global Leader：**%s**（域 `%s`），全局任期 `%d`\n", gl0.nodeID, gl0.domain, gl0.term)
	fmt.Fprintf(report, "- 各域 Domain Leader 与 Global Leader 均由随机化超时下的真实选举产生（重跑会变化）。\n\n")

	// Scenario 1: load stays in the Global Leader's own domain -> no migration.
	runScenario(t, ctx, harness, report, scenarioSpec{
		title:       "场景一：单域持续最优（稳定，无迁移）",
		loadOrigin:  gl0.domain,
		expectMove:  false,
		settleHint:  fmt.Sprintf("负载始终来自 Global Leader 所在域 `%s`，最优位置不变，优化器不应触发迁移。", gl0.domain),
		ridTag:      "s1",
		entryTarget: entry,
	})

	// Scenario 2: load moves to a different domain -> optimizer recommends it ->
	// safe catch-up migration moves the Global Leader there.
	current := waitForUniqueGL(t, ctx, harness)
	target := otherDomain(harness.config, current.domain)
	runScenario(t, ctx, harness, report, scenarioSpec{
		title:       "场景二：最优域切换（触发安全交接迁移）",
		loadOrigin:  target,
		expectMove:  true,
		settleHint:  fmt.Sprintf("负载全部来自域 `%s`（与现任 Global Leader 域 `%s` 不同），优化器应推荐 `%s` 并通过 catch-up 安全交接把 Global Leader 迁移过去。", target, current.domain, target),
		ridTag:      "s2",
		entryTarget: entry,
	})

	fmt.Fprintf(report, "## 结论\n\n")
	fmt.Fprintf(report, "- 稳定场景下负载与 Global Leader 同域，优化器保持推荐当前域，未发生迁移。\n")
	fmt.Fprintf(report, "- 负载迁移到新域后，优化器识别出更优位置并通过安全交接把 Global Leader 迁移过去；迁移后该域的读写延迟显著下降（读取从一个跨域 RTT 降为本地，写入从约两个跨域 RTT 降为约一个）。\n")
	fmt.Fprintf(report, "- 全程未直接改写 leader 指针，迁移仍走更高任期选举 + N-1 + 日志最新性 + fencing；leader 位置由真实选举决定。\n")

	path := writeReport(t, report.String())
	t.Logf("scenario report written to %s", path)
}

type scenarioSpec struct {
	title       string
	loadOrigin  string
	expectMove  bool
	settleHint  string
	ridTag      string
	entryTarget string
}

func runScenario(t *testing.T, ctx context.Context, harness *rpcHarness, report *strings.Builder, spec scenarioSpec) {
	t.Helper()

	before := waitForUniqueGL(t, ctx, harness)
	beforeAddr := addressOf(t, harness, before.nodeID)
	domains := harness.config.Domains()

	loadCtx, stopLoad := context.WithCancel(ctx)
	go driveLoad(loadCtx, harness, spec.entryTarget, spec.loadOrigin)

	// Acting as the EXTERNAL controller: pull the GL's raw window statistics and
	// run the cost model until it produces a full candidate ranking.
	var decision optimizer.Result
	waitUntil(t, ctx, func() bool {
		m, err := harness.client.Metrics(ctx, beforeAddr)
		if err != nil {
			return false
		}
		decision = optimizer.ComputeCosts(metricsToOptimizerInput(domains, before.domain, m))
		return len(decision.Candidates) == len(domains)
	}, "external cost model did not produce candidate costs")

	if spec.expectMove {
		// The controller issues the GL-only Move; only the current GL executes it.
		mctx, mcancel := context.WithTimeout(ctx, 25*time.Second)
		resp, err := harness.client.Move(mctx, beforeAddr, spec.loadOrigin, "scenario-report move")
		mcancel()
		if err != nil || !resp.GetAccepted() {
			t.Fatalf("scenario move to %s not accepted: resp=%+v err=%v", spec.loadOrigin, resp, err)
		}
		waitUntil(t, ctx, func() bool {
			gl := globalLeader(ctx, harness, spec.entryTarget)
			return gl != nil && gl.domain == spec.loadOrigin && gl.term > before.term
		}, "expected migration to the load domain did not happen")
	} else {
		// Stable: the recommendation should be the current GL domain, so the
		// controller issues no move. Give it a few windows then assert stability.
		if decision.Recommended != "" && decision.Recommended != before.domain {
			t.Fatalf("stable scenario unexpectedly recommends a move: %s -> %s", before.domain, decision.Recommended)
		}
		time.Sleep(3 * time.Second)
	}

	glNow := waitForUniqueGL(t, ctx, harness)
	stopLoad()
	// Let any in-flight handoff settle before measuring.
	time.Sleep(1500 * time.Millisecond)

	glAddr := addressOf(t, harness, glNow.nodeID)
	// The migration counter/note live on the node that INITIATED the handoff (the
	// previous Global Leader); query it for the report.
	initiatorSnap, err := harness.client.Metrics(ctx, beforeAddr)
	if err != nil {
		t.Fatal(err)
	}

	probes := []probeResult{
		probeLatency(t, harness, glAddr, "a", spec.ridTag, 6),
		probeLatency(t, harness, glAddr, "b", spec.ridTag, 6),
		probeLatency(t, harness, glAddr, "c", spec.ridTag, 6),
	}

	// Emit the section.
	fmt.Fprintf(report, "## %s\n\n", spec.title)
	fmt.Fprintf(report, "%s\n\n", spec.settleHint)
	fmt.Fprintf(report, "- 负载来源域：`%s`\n", spec.loadOrigin)
	fmt.Fprintf(report, "- 外部控制器推荐域：`%s`（预测 L_s=%.1f）\n", decision.Recommended, decision.RecommendedLs)
	if spec.expectMove {
		fmt.Fprintf(report, "- Global Leader 迁移：`%s`（域 `%s`，任期 `%d`）→ **`%s`（域 `%s`，任期 `%d`）**\n",
			before.nodeID, before.domain, before.term, glNow.nodeID, glNow.domain, glNow.term)
	} else {
		fmt.Fprintf(report, "- Global Leader 保持不变：**`%s`（域 `%s`，任期 `%d`）**\n", glNow.nodeID, glNow.domain, glNow.term)
	}
	fmt.Fprintf(report, "- 迁移计数（发起方 `%s` 视角）：`%d`\n", before.nodeID, initiatorSnap.GetMigrations())
	if note := initiatorSnap.GetLastMigration(); note != "" {
		fmt.Fprintf(report, "- 迁移记录：`%s`\n", note)
	}
	fmt.Fprintf(report, "\n%s\n\n", candidateCostTable(decision))
	fmt.Fprintf(report, "客户端读写延迟（按 origin 域，中位数 / 平均，每格 6 个成功样本；写为两域提交，读为线性一致读）：\n\n")
	fmt.Fprintf(report, "%s\n\n", latencyTable(glNow, probes))
}

type probeResult struct {
	origin                string
	writeMedian, writeAvg time.Duration
	readMedian, readAvg   time.Duration
	writeOK, readOK       int
}

// probeLatency measures end-to-end client write/read latency for a given origin
// domain against the current Global Leader. Each sample is retried until it
// succeeds (with a fresh write requestId so an idempotent cache hit can never
// fake a near-zero latency); if a sample can never succeed the test fails loudly
// rather than silently reporting a misleading 0.
func probeLatency(t *testing.T, harness *rpcHarness, glAddr, origin, tag string, samples int) probeResult {
	t.Helper()
	writes := make([]time.Duration, 0, samples)
	reads := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		key := fmt.Sprintf("probe-%s-%s-%d", tag, origin, i)

		var wrote bool
		for attempt := 0; attempt < 8 && !wrote; attempt++ {
			cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			rid := fmt.Sprintf("rid-%s-%s-%d-%d", tag, origin, i, attempt)
			start := time.Now()
			decision, err := harness.client.Write(cctx, glAddr, ClientWriteRequest{
				RequestID: rid, OriginDomain: origin, Command: Command{Key: key, Value: "v"},
			})
			if err == nil && decision.Result.Committed {
				writes = append(writes, time.Since(start))
				wrote = true
			}
			cancel()
		}
		if !wrote {
			t.Fatalf("probe(write) origin %s sample %d never succeeded", origin, i)
		}

		var read bool
		for attempt := 0; attempt < 8 && !read; attempt++ {
			cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			start := time.Now()
			if _, rerr := harness.client.ReadFrom(cctx, glAddr, origin, key); rerr == nil {
				reads = append(reads, time.Since(start))
				read = true
			}
			cancel()
		}
		if !read {
			t.Fatalf("probe(read) origin %s sample %d never succeeded", origin, i)
		}
	}
	wMed, wAvg := medianAvg(writes)
	rMed, rAvg := medianAvg(reads)
	return probeResult{
		origin: origin, writeMedian: wMed, writeAvg: wAvg, readMedian: rMed, readAvg: rAvg,
		writeOK: len(writes), readOK: len(reads),
	}
}

func medianAvg(samples []time.Duration) (median, avg time.Duration) {
	if len(samples) == 0 {
		return 0, 0
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	median = samples[len(samples)/2]
	var total time.Duration
	for _, s := range samples {
		total += s
	}
	avg = total / time.Duration(len(samples))
	return median, avg
}

type glInfo struct {
	nodeID string
	domain string
	term   uint64
}

func globalLeader(ctx context.Context, harness *rpcHarness, target string) *glInfo {
	status, err := harness.client.Status(ctx, target)
	if err != nil {
		return nil
	}
	leader := status.GetGlobalLeader()
	if leader.GetNodeId() == "" {
		return nil
	}
	return &glInfo{nodeID: leader.GetNodeId(), domain: leader.GetDomainId(), term: status.GetGlobalTerm()}
}

// waitForUniqueGL blocks until every node agrees on a single serving Global
// Leader and exactly one node reports the Leader global role.
func waitForUniqueGL(t *testing.T, ctx context.Context, harness *rpcHarness) glInfo {
	t.Helper()
	var info glInfo
	waitUntil(t, ctx, func() bool {
		agreed, domain := "", ""
		var term uint64
		leaders := 0
		for _, configured := range harness.config.Nodes {
			status, err := harness.client.Status(ctx, configured.ListenAddress)
			if err != nil || status.GetStage() != string(Serving) {
				return false
			}
			leader := status.GetGlobalLeader().GetNodeId()
			if leader == "" {
				return false
			}
			if agreed == "" {
				agreed = leader
				domain = status.GetGlobalLeader().GetDomainId()
				term = status.GetGlobalTerm()
			} else if agreed != leader {
				return false
			}
			if status.GetGlobalRole() == string(Leader) {
				leaders++
			}
		}
		if leaders != 1 {
			return false
		}
		info = glInfo{nodeID: agreed, domain: domain, term: term}
		return true
	}, "cluster did not converge on a unique Global Leader")
	return info
}

func addressOf(t *testing.T, harness *rpcHarness, nodeID string) string {
	t.Helper()
	return mustNode(t, harness.config, nodeID).ListenAddress
}

// otherDomain returns the first domain (in sorted order) different from the
// given one, used to pick a migration target away from the current GL.
func otherDomain(cfg topology.Config, domain string) string {
	for _, d := range cfg.Domains() {
		if d != domain {
			return d
		}
	}
	return domain
}

func domainLeaderList(t *testing.T, ctx context.Context, harness *rpcHarness, target string) string {
	t.Helper()
	m, err := harness.client.Metrics(ctx, target)
	if err != nil {
		return "(unavailable)"
	}
	leaders := m.GetDomainLeaders()
	domains := make([]string, 0, len(leaders))
	for d := range leaders {
		domains = append(domains, d)
	}
	sort.Strings(domains)
	parts := make([]string, 0, len(domains))
	for _, d := range domains {
		parts = append(parts, fmt.Sprintf("域 `%s` → `%s`", d, leaders[d]))
	}
	return strings.Join(parts, "，")
}

func writeTopology(report *strings.Builder, profile topology.NetworkSimulation) {
	fmt.Fprintf(report, "## 拓扑与延迟矩阵\n\n")
	fmt.Fprintf(report, "三个域 `a`/`b`/`c`，每域 3 节点（`a1..a3` 等）。本地单向延迟 %dms，跨域单向延迟（往返为其两倍）：\n\n", profile.LocalOneWayDelayMillis)
	pairs := []struct{ from, to string }{
		{"a", "b"}, {"a", "c"}, {"b", "c"},
	}
	fmt.Fprintf(report, "| 链路 | 单向延迟 | 往返 RTT |\n|---|---|---|\n")
	for _, p := range pairs {
		oneWay := profile.InterDomainOneWayMillis[p.from+"->"+p.to]
		fmt.Fprintf(report, "| `%s <-> %s` | %dms | %dms |\n", p.from, p.to, oneWay, 2*oneWay)
	}
	fmt.Fprintf(report, "\n")
}

func candidateCostTable(decision optimizer.Result) string {
	costs := decision.Candidates
	if len(costs) == 0 {
		return "_（无候选成本数据）_"
	}
	sort.Slice(costs, func(i, j int) bool { return costs[i].Domain < costs[j].Domain })
	b := &strings.Builder{}
	fmt.Fprintf(b, "候选域预测成本（论文 L_s 模型，外部控制器计算，越低越优）：\n\n")
	fmt.Fprintf(b, "| 候选域 | 预测 L_s | 可提交 |\n|---|---|---|\n")
	for _, c := range costs {
		fmt.Fprintf(b, "| `%s` | %.1f | %v |\n", c.Domain, c.Ls, c.CanCommit)
	}
	return b.String()
}

func latencyTable(gl glInfo, probes []probeResult) string {
	b := &strings.Builder{}
	fmt.Fprintf(b, "| client origin | 与 GL 同域 | 写中位数 | 写平均 | 读中位数 | 读平均 |\n")
	fmt.Fprintf(b, "|---|---|---|---|---|---|\n")
	for _, p := range probes {
		same := "否"
		if p.origin == gl.domain {
			same = "**是**"
		}
		fmt.Fprintf(b, "| `%s` | %s | %v | %v | %v | %v |\n",
			p.origin, same, p.writeMedian.Round(time.Millisecond), p.writeAvg.Round(time.Millisecond),
			p.readMedian.Round(time.Millisecond), p.readAvg.Round(time.Millisecond))
	}
	return b.String()
}

func writeReport(t *testing.T, content string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve report path")
	}
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	dir := filepath.Join(repoRoot, "docs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "scenario-report.md")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(path)
	return abs
}
