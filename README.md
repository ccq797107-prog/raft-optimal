# CD-Raft Core

This repository implements the OpenSpec `implement-cd-raft-core` tasks through
Gate E, including real protobuf/gRPC node-to-node communication, a runnable
client, the read-only optimal Global Leader decision model, and active Global
Leader migration through real higher-term elections. It intentionally does not
implement multiple application-level groups, sharding, cross-group
transactions, or runtime membership changes.

For a guided Chinese code-reading path through the project, see
[`docs/project-learning/README.md`](docs/project-learning/README.md).

## Topology

Each machine has one static `domainCode` in `config/cluster.json`. Nodes with
the same code are members of the same domain and can discover their local
domain peers from the validated topology.

```bash
./scripts/start-local-cluster.sh

go run ./cmd/cdraft-client \
  -target 127.0.0.1:7202 \
  -origin-domain domain-b \
  -key example \
  -value value

go run ./cmd/cdraft-client \
  -target 127.0.0.1:7302 \
  -origin-domain domain-c \
  -key example
```

For a client on another machine, bind the callback listener and advertise a
Domain Leader-reachable address:

```bash
go run ./cmd/cdraft-client \
  -target NODE_ADDRESS \
  -origin-domain domain-b \
  -callback-listen 0.0.0.0:9000 \
  -reply-route CLIENT_REACHABLE_ADDRESS:9000 \
  -key example -value value
```

## Floating Domain Clients

A floating domain is a client-only origin domain: it can produce reads/writes
and latency telemetry, but it has no replicas, no Domain Leader, no votes, no
quorum state, and can never host the Global Leader. Existing consensus-domain
clients keep the same behavior.

Floating clients bootstrap through the current topology view:

```bash
go run ./cmd/cdraft-client \
  -target NODE_ADDRESS \
  -origin-domain edge \
  -discover-topology
```

They can also measure the returned consensus endpoints and report bounded-TTL
latency telemetry to the Global Leader:

```bash
go run ./cmd/cdraft-client \
  -target NODE_ADDRESS \
  -origin-domain edge \
  -report-floating-latency
```

For floating writes, the Global Leader always keeps the normal unary RPC
response. When Fast Return is enabled, a valid `replyRoute` exists, and fresh
floating latency telemetry is available, the Global Leader additionally chooses
one consensus-domain Domain Leader as a responder. The client races the GL
normal response against that responder's Fast Return and accepts the first valid
success; the late result is still checked and deduplicated. If responder
selection or callback delivery fails, the request falls back to the normal GL
response without changing the two-domain commit rule.

Feature gates default to:

```text
fastReturnEnabled=false
optimizerEnabled=false
activeMigrationEnabled=false
```

## Implemented Core

- Deterministic startup stages:
  `Booting -> DomainElecting -> DomainReady -> GlobalElecting -> Serving`
- Persisted, independent Domain and Global terms, votes, leader identities,
  logs, commit/apply positions, state machine data, and idempotent results
- Domain-majority Domain Leader elections and `N-1` Domain Leader Global
  Leader elections, including failure re-election and stale identity fencing
- Global Leader-only ordering, two-domain commit evidence, and linearizable
  Global Leader reads
- Self-healing cross-domain replication: when a single-entry replication hits a
  contiguity gap (a domain left trailing by an election/migration, or
  uncommitted tail entries on the leader), the Global Leader probes that domain
  and backfills its missing contiguous tail over the catch-up channel, so a
  lagging domain can never permanently wedge new two-domain commits
- Fast Return qualification and a dual-response race where the first valid
  success completes the foreground request while the late response is still
  consumed and checked
- Manual clock and controllable in-memory network for deterministic partition,
  message-loss, stop, recovery, and restart tests
- Generated protobuf messages and gRPC client/server stubs
- Independent node TCP listeners, gRPC connection pooling, per-attempt
  deadlines, bounded retries, randomized election retries, heartbeat failure
  detection, and automatic Domain/Global Leader re-election
- Runnable `cmd/cdraft-client` with Global Leader redirect handling, an
  independent Fast Return callback server, and a real dual-response race

`proto/cdraft.proto` defines election, replication, normal client result, and
Fast Return callback services. Generated code is committed under
`gen/cdraft/v1`; regenerate it with:

```bash
make proto
```

Deterministic state-machine tests and real localhost TCP/gRPC integration tests
are both retained. The latter start independent node states and exercise
generated stubs rather than calling consensus methods directly.

## Latency Simulation

`networkSimulation` injects directed one-way delay around every real gRPC
request and response. Domain-local RPCs use `localOneWayDelayMillis`; cross
domain RPCs use `interDomainOneWayMillis`.

The profile in `config/latency-cluster.json` models:

```text
domain-local one-way: 1 ms
domain-a <-> domain-b: 50 ms one-way, 100 ms RTT
domain-a <-> domain-c: 80 ms one-way, 160 ms RTT
domain-b <-> domain-c: 60 ms one-way, 120 ms RTT
```

Run the repeatable real-gRPC latency experiment:

```bash
go test ./internal/cdraft -run '^TestRealGRPCLatencyExperiment$' -count=1 -v
```

Representative median results, with Global Leader in domain A:

```text
client in A:                    112.9 ms = 1.13 A-B RTT, Global response
client in B with Fast Return:   108.2 ms = 1.08 A-B RTT, Fast Return
client in B without Fast Return:216.2 ms = 2.16 A-B RTT, Global response
client in C with Fast Return:   168.3 ms = 1.05 A-C RTT, Fast Return
client in C without Fast Return:276.3 ms = 1.73 A-C RTT, Global response
```

The same-domain write still needs one nearest remote-domain RTT to form the
two-domain commit certificate. The cross-domain Fast Return path overlaps that
replication with the client-to-Global-Leader request and returns locally from
the client-domain Domain Leader, so its foreground path is also approximately
one RTT. Without Fast Return, the cross-domain client additionally waits for
the Global Leader response to travel back. For domain B, which is also the
nearest commit domain, this produces approximately two A-B RTTs. For the more
distant domain C, normal response is about 1.73 A-C RTT because the Global
Leader can form its two-domain certificate through nearer domain B before
returning to C. Fast Return still tracks one client-to-Global-Leader RTT.

This is a fixed-delay application-level network simulation. It validates the
protocol critical path, but it does not yet model bandwidth limits, queueing,
jitter, kernel packet loss, or TLS overhead.

## Write-Path Overhead Measurement

To separate network delay from local processing time, run the real-gRPC
write-overhead harness:

```bash
go run ./cmd/cdraft-harness -scenario write-overhead -store memory \
  -out /tmp/cd-raft-write-overhead-memory.md

go run ./cmd/cdraft-harness -scenario write-overhead -store leveldb-sync \
  -out /tmp/cd-raft-write-overhead-leveldb-sync.md

go run ./cmd/cdraft-harness -scenario write-overhead-latency -store memory \
  -out /tmp/cd-raft-write-overhead-latency.md
```

The report covers same-GL-domain consensus clients, cross-domain consensus
clients, and floating-domain clients. It reports first-sample latency separately
from warmed steady-state min/p50/p95/p99/max/mean, computes the theoretical
network lower bound for each origin, and shows `observed - theoretical` as the
candidate local overhead. `leveldb-nosync` exists only as an explicit
experimental comparison; the default durable path is `leveldb-sync`.

## Optimal Global Leader Decision (Gate D)

The optimizer runs on the current Global Leader and is read-only: it never edits
the Global Leader pointer or term. All of its inputs are real measurements over
the live gRPC mesh:

- `W_i` / `R_i`: per-origin-domain write and read counts, recorded by the Global
  Leader's real `Write`/`Read` handlers into a sliding stats window.
- Inter-domain RTT: every Domain Leader periodically `Ping`s the other domains'
  Domain Leaders over real gRPC. The measured round-trip wall-clock time is
  halved to a one-way estimate and exponentially smoothed. Each Domain Leader
  reports its row to the Global Leader via `ReportTelemetry`, which aggregates a
  full directed `l[i][j]` matrix.
- Availability: a domain is available when it has a current Domain Leader and a
  fresh telemetry observation.

It then evaluates the paper cost model for every available candidate domain `z`
(`L_i = 2*min(l_zj)*W_i` for `i=z`; `2*l_iz*(W_i+R_i)` for available `i!=z`;
`2*(l_iz+min(l_zj))*W_i + 2*l_iz*R_i` for unavailable `i`), filters candidates
with no commit partner, and picks the minimum `L_s` with a domain-id tie-break.

Floating domains are demand domains only. They contribute `R[u]`, `W[u]`, and
fresh client-reported `l[u][z]` latency, but they are never candidates. Floating
read cost is `2*l[u][z]*R[u]`; floating write cost is estimated as the lower
client-visible race cost between the always-present GL response and the optional
responder Fast Return path.

`DecisionPeriod()` returns at least 100x the largest measured cross-domain RTT,
never below the observation window.

## Active Global Leader Migration (Gate E)

When `activeMigrationEnabled` is set, the optimizer's recommendation is gated by
a migration controller (minimum benefit threshold, consecutive-window
confirmation, and a cooldown). A confirmed target is handed off with a **safe,
two-phase catch-up handoff** rather than the naive "bump the target's term and
let it campaign" trigger that would create an unnecessary leaderless window:

1. **Pre-check** — abort (and leave the incumbent untouched) unless at least
   `N-1` Domain Leaders are reachable, since a higher-term election could not
   otherwise succeed.
2. **Barrier + drain** — freeze a migration barrier at the current log head and
   enter `draining`, soft-pausing the ordering of *new* writes so the target can
   converge to a fixed point. New `requestId`s get a retryable `Unavailable`;
   already-ordered/committed `requestId`s are still served idempotently.
3. **Catch-up** — stream the missing log tail to the target's Domain Leader
   (`Telemetry/CatchUp`) until its in-domain quorum reaches the barrier, bounded
   by `catchUpTimeout`.
4. **Handoff** — only now trigger `BeginMigration`, which runs the **same**
   Global Leader election at a higher `globalTerm` (with the `N-1` vote
   threshold, log-recency check and fencing). Because the target is already
   caught up, the election succeeds deterministically and the old Global Leader
   is fenced exactly as in failure re-election; there is no direct leader-pointer
   edit.

If the pre-check fails or catch-up times out, the migration aborts **without
fencing** the incumbent: `draining` is lifted and it keeps serving. During the
handoff window the only client-visible effect is a retryable error; the stable
`requestId` lets the idempotent client retry onto the new Global Leader without
double-committing, preserving two-domain commit and linearizability. A strictly
zero-downtime window is unreachable in a distributed handoff (etcd has a minimal
window too); the goal is to minimize it while never disrupting the incumbent on a
failed attempt.

## Observability

Every node serves a `Metrics/Snapshot` gRPC exposing stage, leaders and terms,
per-domain quorum positions, commit/apply indices, Fast Return success and
degraded counts, rejection reasons, per-domain request distribution, the
measured inter-domain RTT matrix, candidate `L_s` costs, the current
recommendation, and migration history. `AssertInvariants` checks the per-node
safety invariant `appliedIndex <= knownGlobalCommitIndex <= domainQuorumIndex`
on the Global Leader. Dual-response race outcomes (winning path, late, and
inconsistent results) are observed client-side via `RaceDecision`.

## Verification

```bash
go test ./...
go test -race ./...
openspec validate implement-cd-raft-core
```

## Harness Scaffold

Reusable real-gRPC experiment scaffolding lives in `internal/harness`, with a
thin CLI at `cmd/cdraft-harness`. The default smoke scenario starts a local
three-domain, nine-node cluster on free loopback ports, waits for a unique
Global Leader, performs a write/read probe, samples metrics, and writes a
Markdown report.

```bash
go run ./cmd/cdraft-harness \
  -scenario smoke \
  -out /tmp/cd-raft-harness-smoke.md
```

JSON scenarios can be run with `-scenario-file`. Long-running reports and
experiments should be invoked explicitly through the harness CLI or a targeted
test command rather than folded into the fast feedback path.
