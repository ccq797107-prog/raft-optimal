# CD-Raft Harness

`cmd/cdraft-harness` runs reusable real-gRPC scenarios on top of
`internal/harness`.

Smoke run:

```bash
go run ./cmd/cdraft-harness -scenario smoke -out /tmp/cd-raft-harness-smoke.md
```

Floating-domain smoke runs:

```bash
go run ./cmd/cdraft-harness -scenario floating-smoke -out /tmp/cd-raft-harness-floating.md
go run ./cmd/cdraft-harness -scenario floating-fallback -out /tmp/cd-raft-harness-floating-fallback.md
```

Write-path overhead runs:

```bash
# Zero injected latency: protocol/runtime overhead only.
go run ./cmd/cdraft-harness \
  -scenario write-overhead \
  -store memory \
  -out /tmp/cd-raft-write-overhead-memory.md

# Default durable LevelDB path. This keeps per-batch fsync enabled.
go run ./cmd/cdraft-harness \
  -scenario write-overhead \
  -store leveldb-sync \
  -out /tmp/cd-raft-write-overhead-leveldb-sync.md

# Experimental comparison only: LevelDB without per-batch fsync.
go run ./cmd/cdraft-harness \
  -scenario write-overhead \
  -store leveldb-nosync \
  -out /tmp/cd-raft-write-overhead-leveldb-nosync.md

# With the built-in a/b/c plus floating edge latency profile.
go run ./cmd/cdraft-harness \
  -scenario write-overhead-latency \
  -store memory \
  -out /tmp/cd-raft-write-overhead-latency.md
```

The write-overhead scenarios measure warmed steady-state writes from three
origins: one consensus domain colocated with the current Global Leader, one
different consensus domain, and one floating domain. The report separates the
first sample from warmup-adjusted min/p50/p95/p99/max/mean, lists the store
profile, computes the theoretical network lower bound, and reports
`observed - theoretical` as the candidate local processing overhead. Trace
summaries are collected for the first warmed sample so persistence, quorum,
commit/apply, client race, and callback phases can be inspected without adding
diagnostic overhead to every sample.

After the `cut-gl-write-path-fsync-to-two` change, the Global Leader's
critical-path fsync count is 2: one merged `server.domain_quorum_persist`
(covers the new entry and the advanced `DomainQuorumIndex` in one batch) and
one `server.commit_persist` (commit index + apply). The pre-quorum
`server.replicate_persist` on the GL is now deferred into the post-quorum
fsync and marked `server.replicate_persist_deferred` in the trace; the
`server.persist_before_commit` fsync from the previous round has been removed
— durability before a Fast Return announce is supplied by the followers'
own `server.replicate_persist` on two domain majorities, not by an
announce-time fsync on the GL itself. Two safety drains remain: a
`server.quorum_progress_persist detail=drain` fires if the background commit
goroutine exits without two-domain quorum, and a
`server.replicate_persist detail=drain` fires if a leader-path Replicate
handler exits without in-domain quorum. Both keep the on-disk view consistent
with any in-memory state that survived the goroutine. Follower-side
`server.replicate_persist` (FanoutToDomain=false) and `server.commit_persist`
on every applying node still fire as before.

`leveldb-nosync` is intentionally marked experimental. It is useful for
separating protocol/gRPC/goroutine overhead from fsync cost, but the default
safe profile remains `leveldb-sync`.

JSON scenarios use the same step names as the Go `harness.StepType` constants,
for example:

```json
{
  "name": "json-smoke",
  "floatingDomains": ["edge"],
  "floatingLatencies": {
    "edge": {"a": 100, "b": 5, "c": 80}
  },
  "steps": [
    {"name": "start", "type": "start"},
    {"name": "wait", "type": "wait_serving", "budgetMillis": 12000},
    {
      "name": "probe",
      "type": "write_read",
      "origin": "edge",
      "requestId": "json-smoke",
      "key": "json-smoke",
      "value": "ok",
      "budgetMillis": 8000,
      "expectWinner": "fast"
    },
    {"name": "metrics", "type": "metrics"}
  ]
}
```

`floatingDomains` declares client-only origins. `floatingLatencies` is reported
to the current Global Leader before a matching `write_read` step, allowing the
runtime to choose a responder Domain Leader while still keeping the normal GL
response path. `expectWinner` accepts `fast`, `global`, or the full result
source string.

Long-running migration, latency, or fault-injection experiments should be run
explicitly and should write their reports to this directory or `/tmp`.
