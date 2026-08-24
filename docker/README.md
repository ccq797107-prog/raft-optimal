# CD-Raft in Docker with real (tc/netem) network latency

> 想要一条龙的「照抄即可」流程(设延迟 → 起集群 → 指定 GL → 跑 client)?
> 见 **[`快速部署测试.md`](./快速部署测试.md)**。本文件是底层细节与原始命令参考。


This setup runs a full 9-node, 3-domain CD-Raft cluster in Docker, where each
container injects **real kernel-level one-way latency** with `tc netem` on the
actual gRPC path. There is no application-level fakery — the
`networkSimulation` knob in the config is disabled, and the delay you observe is
real packets being held in the qdisc.

```
domain-a: a1 a2 a3   10.10.1.11-13
domain-b: b1 b2 b3   10.10.2.11-13
domain-c: c1 c2 c3   10.10.3.11-13
client:   10.10.1.20  (helper container inside domain-a, shaped reply-route)
client-d: 10.10.4.20  (floating-domain client helper; no replicas, no DL/GL)
```

One-way latency matrix (symmetric; round trip = both sides' one-way):

| link        | one-way | RTT    |
|-------------|---------|--------|
| a <-> b     | 50 ms   | 100 ms |
| a <-> c     | 80 ms   | 160 ms |
| b <-> c     | 60 ms   | 120 ms |
| a <-> d     | 120 ms  | 240 ms |
| b <-> d     | 25 ms   | 50 ms  |
| c <-> d     | 90 ms   | 180 ms |
| same domain | 0 ms    | ~0     |

## Interpreting steady-state write latency

For performance checks, ignore the first client operation and compare warmed
samples against the theoretical network lower bound. This applies to consensus
domains and floating domains alike.

If the current Global Leader is in domain `b` under the default matrix:

- origin `b` (same consensus domain): fastest second quorum is normally `a`, so
  the write network lower bound is `b<->a = 100 ms`.
- origin `a` (cross consensus domain): the normal GL response path is
  `a->b + b<->a + b->a = 200 ms`, while Fast Return through the origin DL is
  `a->b + b->a + a->a ~= 100 ms`.
- origin `d` (floating domain): the normal GL response path is
  `d->b + b<->a + b->d = 150 ms`; the responder callback path depends on the
  GL-selected responder and should be compared separately.

The warmed client latency minus that lower bound is the candidate local
processing overhead. To isolate the same question without Docker `tc`, use the
harness:

```bash
go run ./cmd/cdraft-harness -scenario write-overhead -store memory \
  -out /tmp/cd-raft-write-overhead-memory.md
go run ./cmd/cdraft-harness -scenario write-overhead -store leveldb-sync \
  -out /tmp/cd-raft-write-overhead-leveldb-sync.md
```

## Quick start (helper scripts)

Two wrappers hide all the IPs/ports/flags. Run them from the `docker/` dir:

```bash
cd docker

./cluster.sh up        # build + start all 9 nodes + client
./cluster.sh staged    # OR: bring domains up one at a time (a, then b, then c)
./cluster.sh gl        # who is the Global Leader right now?
./cluster.sh status    # every node's domain leader / GL / term / commit
./cluster.sh down      # stop + wipe state
./cluster.sh logs c2   # follow a node's logs

# client: ./client.sh <a|b|c|d> <read|write> [count] [key] [value]
./client.sh c write 10        # 10 writes originating in domain-c (key=foo value=bar)
./client.sh c read 10         # 10 reads from domain-c
./client.sh d write 10        # 10 writes from floating domain-d
./client.sh d read 10         # 10 reads from floating domain-d
./client.sh a write 10 k v    # 10 writes of k=v from domain-a
./client.sh b read 5 k        # 5 reads of key k from domain-b

# latency: change inter-domain one-way delay live (no restart, no rebuild)
./latency.sh show             # current egress shaping per domain
./latency.sh set 50 80 60     # abc matrix plus default d links
./latency.sh set 50 80 60 120 25 90  # full matrix: ab ac bc ad bd cd
./latency.sh pair a c 100     # just the a<->c link -> 100ms one-way (200ms RTT)
./latency.sh pair b d 40      # just the b<->d floating-client link
./latency.sh reset            # back to defaults (50/80/60/120/25/90)

# global leader: inspect or pin which domain holds the GL
./gl.sh show                  # current Global Leader node + domain
./gl.sh move a                # force the GL into domain-a (re-elects until it lands)
```

> **Note:** `gl.sh move` stops/starts the evicted domain's containers, which
> re-runs their entrypoint and **re-applies the default tc matrix** to that
> domain. If you set a custom matrix with `latency.sh`, run it again *after*
> `gl.sh move`. `latency.sh` itself is live (`tc qdisc`) and never restarts
> containers.

`client.sh` execs into that domain's first node (`a1`/`b1`/`c1`). For floating
domain `d`, it execs into `client-d`, which has no CD-Raft node and only provides
a client origin plus Fast Return reply-route. The request originates inside the
selected domain and pays the real tc-shaped client↔GL RTT. For `d`, the wrapper
starts floating latency discovery/probe/report in the background for writes, so
the foreground request can immediately use the normal GL RPC path while
responder selection warms up. Reads do not wait for floating telemetry. It fills in `-target`,
`-origin-domain`, `-reply-route` and `-callback-listen` for you, prints each
op's latency, and a `total / avg / min / max` summary. The first op pays a
one-time redirect + cold connection; the client caches the Global Leader, so op
2 onward is steady-state.

The raw `docker compose` commands these scripts wrap are documented below.

## Files

- `Dockerfile` — multi-stage build; ships `cdraft`, `cdraft-client`, and `iproute2`.
- `entrypoint.sh` — applies the per-domain `tc netem` egress matrix, then runs the node.
- `cluster.docker.json` — topology + feature flags (mounted read-only, editable without rebuild).
- `docker-compose.yml` — 9 nodes + consensus/floating client helpers on a `10.10.0.0/16` bridge with static IPs.
- `cluster.sh` — convenience wrapper for up / staged / down / status / gl / logs.
- `client.sh` — convenience wrapper: `./client.sh <a|b|c|d> <read|write> [count] [key] [value]`.
- `latency.sh` — change the inter-domain one-way latency live via `tc` (set / pair / show / reset).
- `gl.sh` — inspect or pin which domain holds the Global Leader (show / move).

## Prerequisites

- Docker Engine / Desktop with Compose v2 (`docker compose version`).
- Containers get `NET_ADMIN` (declared in compose) so `tc` can run inside them.

## Build & start

```bash
cd docker
docker compose build      # builds the cd-raft:local image once
docker compose up -d       # starts all 9 nodes + client helpers
docker compose ps          # all should be "running"
```

Give it ~8s to elect Domain Leaders and a Global Leader.

### Start the domains one at a time (staged)

You can bring the cluster up domain-by-domain to watch election unfold — only
the listed services start, the rest stay down:

```bash
docker compose down -v                 # clean slate

docker compose up -d a1 a2 a3          # Phase 1: only domain-a
docker compose up -d b1 b2 b3          # Phase 2: add domain-b
docker compose up -d c1 c2 c3 client client-d   # Phase 3: add domain-c + clients
```

What you should observe at each phase (query each running node over gRPC; a node
whose domain isn't up yet simply reports `(down)`):

```bash
./cluster.sh status
```

- **Phase 1 (one domain up):** domain-a elects a Domain Leader, but there is
  **no Global Leader** yet — a Global Leader needs `N-1 = 2` domains. The global
  term stays put (`gTerm=0`); the lone leader does **not** campaign into the void.
- **Phase 2 (two domains up):** the `N-1` threshold is met and a Global Leader is
  elected across domain-a and domain-b.
- **Phase 3 (all three up):** domain-c joins and follows the existing Global
  Leader; all nine nodes converge on the same `GL` and `gTerm`.

> This relies on two robustness behaviors (see the core spec): a Global-election
> **pre-vote guard** (don't bump the global term unless ≥ `N-1` domain leaders are
> known) and **periodic Domain-Leader re-announcement** (the one-shot announce at
> election time would be lost to domains that boot later, so leaders keep
> re-announcing). Without them, a staggered start would inflate the global term
> forever or split into rival Global Leaders.

### Confirm the latency is really applied

Inspect the qdisc:

```bash
docker compose exec a1 tc qdisc show dev eth0
# qdisc netem 10: parent 1:10 ... delay 50ms     (egress to domain-b)
# qdisc netem 20: parent 1:20 ... delay 80ms     (egress to domain-c)
```

Or just `ping` across domains — this is real kernel-level delay on the actual
packet path (ICMP, not even the app), and the RTT is the sum of both sides'
egress one-way delays:

```bash
docker compose exec a1 ping -c 3 10.10.1.12   # same domain  -> ~0 ms
docker compose exec a1 ping -c 3 10.10.2.11   # a <-> b       -> ~100 ms
docker compose exec a1 ping -c 3 10.10.3.11   # a <-> c       -> ~160 ms
docker compose exec b1 ping -c 3 10.10.3.11   # b <-> c       -> ~120 ms
docker compose exec client-d ping -c 3 10.10.2.11 # d <-> b    -> ~50 ms
```

## Send writes / reads

Exec into the `client` helper (fixed IP `10.10.1.20`, **inside domain-a** so its
hops to the GL are tc-shaped). It uses `10.10.1.20:9100` as the Fast Return
reply-route. You can target **any** node — the client follows the Global Leader
redirect automatically.

```bash
# write (Fast Return reply-route set so cross-domain writes can be answered fast)
docker compose exec client cdraft-client \
  -target 10.10.1.11:7101 -origin-domain domain-a \
  -key foo -value bar -timeout 12s \
  -callback-listen 0.0.0.0:9100 -reply-route 10.10.1.20:9100

# linearizable read (served by the Global Leader)
docker compose exec client cdraft-client \
  -target 10.10.1.11:7101 -origin-domain domain-a -key foo -timeout 12s
```

Expected output:

```
[1/1] write winner=domain-leader-fast-return index=1 result=foo=bar latency=89ms
----
ops=1 success=1 failures=0 total=89ms avg=89ms min=89ms max=89ms
```

### Floating-domain client d

Domain `d` is declared in `cluster.docker.json` as a floating domain. It has a
client helper (`client-d`, `10.10.4.20`) but no members, Domain Leader, quorum
state, or Global Leader eligibility. The default matrix makes `d` closest to
domain-b, so the wrapper bootstraps through b1 before following any GL redirect:

```bash
./client.sh d write 10
./client.sh d read 10
```

For writes, the wrapper starts `d`'s fresh latency telemetry report in the
background and immediately issues the request. Cold writes can complete through
the always-present GL normal RPC path; after telemetry arrives, if the current
GL is in domain-a or domain-c, the responder selected for `d` should normally be
the domain-b DL and you should see `winner=domain-leader-fast-return` once the
path is warm. If the current GL is already in domain-b, the normal GL response
may win, which is also expected
because `d` is closest to b.

`winner` tells you which path answered:
- `domain-leader-fast-return` — origin domain ≠ Global Leader domain, answered after ~1 RTT to the GL.
- `global-leader` — origin domain = Global Leader domain (full two-domain commit, no Fast Return).

### Benchmark: run N ops and print latencies

`-count N` runs the operation N times sequentially and prints each op's latency
plus a summary (`total / avg / min / max`). Write vs read is still chosen by
whether `-value` is set. The client reuses one persistent Fast Return callback
server, so a fixed `-reply-route` port is safe across all N writes.

```bash
# 10 cross-domain writes from domain-a
docker compose exec client cdraft-client \
  -target 10.10.1.11:7101 -origin-domain domain-a \
  -key bench -value v -count 10 -timeout 12s \
  -callback-listen 0.0.0.0:9100 -reply-route 10.10.1.20:9100

# 10 linearizable reads
docker compose exec client cdraft-client \
  -target 10.10.1.11:7101 -origin-domain domain-a -key bench -count 10 -timeout 12s
```

```
[1/10] write winner=domain-leader-fast-return index=2 result=bench=v latency=91ms
[2/10] write winner=global-leader index=3 result=bench=v latency=131ms
...
----
ops=10 success=10 failures=0 total=1.1s avg=110ms min=88ms max=134ms
```

For writes each op gets a unique request id (`<request-id>-<i>`); a shared id
would be treated as idempotent and return the cached result with ~0 latency.
Failed ops are reported per-line and counted in `failures`, and the run exits
non-zero only if every op failed.

## Observe cluster state

Durable state now lives in an **embedded LevelDB** per node (`/var/lib/cd-raft/<id>/`)
instead of a JSON file — Raft metadata, the log (`log:<index>` keys) and the
applied key/value state machine (`sm:<key>` keys) all share one DB, written in
atomic batches. Because the running node holds an exclusive single-process lock
on its LevelDB, you can **no longer `cat` a state file**; query the live node
over gRPC instead.

The simplest way is `cluster.sh status` (every node) or `cluster.sh gl` (just the
Global Leader). Under the hood those call `cdraft-client -status`, which you can
also run directly against any node (from inside that node, using its own IP since
the client listener binds to the domain IP, not localhost):

```bash
./cluster.sh status
./cluster.sh gl

# or talk to one node directly:
docker compose exec -T b1 cdraft-client -status -target 10.10.2.11:7101
# node=b1 domain=domain-b stage=Serving domainRole=... globalRole=...
# domainLeader=b1 globalLeader=b1 globalLeaderDomain=domain-b globalTerm=3
# domainTerm=2 commit=20 applied=20
```

Logs (including the `tc:` lines printed at startup):

```bash
docker compose logs -f a1
```

## Latency you should see

The client helper sits **inside domain-a**, so its hops to the Global Leader are
real tc-shaped links. The per-op latency therefore reflects the actual protocol
cost (the per-op `latency=` line and the `avg/min/max` summary already exclude
`docker exec` startup; only the **first** op in a `-count` run also pays the
gRPC cold-connection handshake **and** the one-time redirect that discovers the
Global Leader, both of which cross the shaped link a few extra times — so
steady-state = 2nd op onward).

The client **caches the Global Leader address**: after the first redirect it
talks to the GL directly, so steady-state requests pay exactly one client↔GL
round trip with no extra local hop and no redirect backoff. In practice a
same-domain read drops to **~0.1 ms** (sub-millisecond, pure local gRPC) and a
cross-domain read converges to **one clean RTT** (e.g. ~121 ms over a 120 ms
link). If the cached leader later moves, the next request fails over to the
original target node, re-learns the new GL, and re-caches it.

**Reads** are served by the Global Leader after the read barrier — the only
cross-domain cost is the single client↔GL round trip, so read latency tracks
domain-a↔(GL domain):

| GL domain | client↔GL RTT | steady read latency |
|-----------|---------------|---------------------|
| domain-a  | same domain   | ~0–1 ms             |
| domain-b  | 100 ms        | ~100 ms             |
| domain-c  | 160 ms        | ~160 ms             |

**Writes** add the GL→second-domain replication on top; when the origin domain
differs from the GL domain, Fast Return can answer once the origin domain holds
the entry (≈ one client↔GL RTT) instead of waiting for the full two-domain
commit. That is why in a mixed run you see writes alternate between the
`domain-leader-fast-return` (~1 RTT) and `global-leader` (full commit) winners.

For clean, in-process latency numbers and migration scenarios, use the Go
harness: `go test ./internal/cdraft/ -run TestScenarioReport`.

## Feature flags

Edit `cluster.docker.json` (mounted read-only, no rebuild needed) and restart:

```bash
docker compose restart
```

```json
"features": {
  "fastReturnEnabled": true,
  "floatingDomains": ["domain-d"],
  "floatingTelemetryTtlMillis": 5000,
  "allowUnknownFloatingDomains": false
}
```

No automatic Global Leader migration controller is started by `cluster.sh up`,
`cluster.sh staged`, or `docker compose up`. Migration stays disabled unless you
explicitly run `auto-migration-demo.sh` or `cdraft-mover`.

## Global Leader migration (decoupled)

Migration is **decoupled** from the consensus core. The node only:

- measures per-domain W/R window statistics and the inter-domain RTT matrix and
  exposes them over the Metrics RPC, and
- executes a safe two-phase handoff when the **current GL** receives a GL-only
  `Move` RPC (catch-up to a barrier, fence, higher-term election; aborts without
  disturbing the incumbent if the target cannot catch up).

The **decision** of where the leader should be lives entirely in an external
controller, `cmd/cdraft-mover`, which polls the GL's statistics, runs the paper
cost model, applies a hysteresis + cooldown policy, and issues `Move`. Only the
GL honors `Move`, so a stray controller can never move leadership from a
non-leader.

Run the end-to-end demo (drives asymmetric load and runs the controller):

```bash
./auto-migration-demo.sh 60 c        # load domain c for 60s; watch GL move to c
```

Or run the controller directly inside the cluster network:

```bash
docker compose exec a1 cdraft-mover -config /etc/cd-raft/cluster.json \
  -poll 2s -confirmations 2 -cooldown 8s -min-improvement 0.1
```

Handoff timings remain Go-API tunable on the node (`SetCatchUpTimeout`,
`SetRPCPolicy`); the decision policy (poll interval, confirmations, cooldown,
min improvement) is configured via `cdraft-mover` flags.

## Tuning the latency matrix

Edit the `case "$CD_DOMAIN"` block in `entrypoint.sh`, then rebuild + recreate:

```bash
docker compose build && docker compose up -d --force-recreate
```

## Teardown

```bash
docker compose down -v     # -v also removes the state volume for a clean slate
```
