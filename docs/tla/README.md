# CD-Raft TLA+ Models

This directory contains the current TLA+ models used to check the key safety
properties of CD-Raft.

The models are split into three small groups:

- `CDRaft.tla`: Fast Return, commit/apply ordering, and dual-index read safety.
- `CDRaftElection.tla`: two-level election safety, especially "at most one Global Leader per global term".
- `CDRaftQuorum.tla`: inter-domain quorum boundary cases for `N=2` and `N>2`.

## Files

```text
CDRaft.tla
CDRaft.cfg
CDRaftLive.cfg
CDRaftLarge.cfg
CDRaftPrefix.cfg
CDRaftPrefix3.cfg
CDRaftForking.tla
CDRaftForking.cfg
CDRaft_buggy.tla
CDRaft_buggy.cfg

CDRaftElection.tla
CDRaftElection.cfg
CDRaftElection2.cfg
CDRaftElection_buggy.cfg

CDRaftQuorum.tla
CDRaftQuorum2.cfg
CDRaftQuorum3.cfg
CDRaftQuorum4.cfg
CDRaftQuorum5.cfg
CDRaftQuorum_NMinusOneBuggy.cfg
```

## Model Scope

`CDRaft.tla` abstracts each domain as a logical quorum holder. It checks that a
Fast Return result cannot be overwritten after a term change, and that reads
respect the dual-index barrier. The Fast Return model uses two-domain prefix
evidence: if the Global Domain and one remote domain both quorum-hold the
current prefix through index `i`, the model may advance commit knowledge through
`i` in one step. This captures the production behavior where a later Fast Return
certificate, such as `k3`, proves the earlier ordered entries `k1..k2` are also
committed, while still forbidding real prefix gaps.

`CDRaftElection.tla` models domain-level and global-level elections. The main
property is:

```text
For each global term, at most one Global Leader can be elected.
```

The key guard is `PerDomainOnce`: each domain may cast at most one global vote
per global term. Election configs use three replicas per participating domain:
`CDRaftElection.cfg` checks the three-domain case, and `CDRaftElection2.cfg`
checks the two-domain `max(2, N-1) = 2-of-2` boundary. The default
three-domain config uses one bounded domain term for a fast routine run; the
two-domain and buggy configs use two domain terms to exercise old/new Domain
Leader overlap.

`CDRaftQuorum.tla` isolates the quorum arithmetic. The safe Global Leader
election quorum is:

```text
q = max(N - 1, 2), where N > 1
```

So:

```text
N = 2  => q = 2
N > 2  => q = N - 1
```

This matters because `N-1` is enough to intersect a two-domain Fast Return
holder set, but at `N=2`, `N-1 = 1` does not force two election quorums to
intersect. The buggy quorum config demonstrates the counterexample:

```text
q1 = {d1}
q2 = {d2}
```

## Run

Set `JAVA` and `JAR` for your local machine:

```bash
JAVA=java
JAR="/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar"
```

Fast Return safety:

```bash
$JAVA -cp "$JAR" tlc2.TLC -workers auto -deadlock CDRaft
```

Fast Return liveness:

```bash
$JAVA -cp "$JAR" tlc2.TLC -config CDRaftLive.cfg -workers auto CDRaft
```

Fast Return prefix advance:

```bash
$JAVA -cp "$JAR" tlc2.TLC -config CDRaftPrefix.cfg -workers auto CDRaft
$JAVA -cp "$JAR" tlc2.TLC -config CDRaftPrefix3.cfg -workers auto CDRaft
```

Fork-recovery safety:

```bash
$JAVA -cp "$JAR" tlc2.TLC -workers auto -deadlock CDRaftForking
```

Election safety:

```bash
$JAVA -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -workers auto -deadlock CDRaftElection
$JAVA -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -config CDRaftElection2.cfg \
  -workers auto -deadlock CDRaftElection
```

Quorum boundary checks:

```bash
$JAVA -cp "$JAR" tlc2.TLC -config CDRaftQuorum2.cfg -workers auto -deadlock CDRaftQuorum
$JAVA -cp "$JAR" tlc2.TLC -config CDRaftQuorum3.cfg -workers auto -deadlock CDRaftQuorum
$JAVA -cp "$JAR" tlc2.TLC -config CDRaftQuorum4.cfg -workers auto -deadlock CDRaftQuorum
$JAVA -cp "$JAR" tlc2.TLC -config CDRaftQuorum5.cfg -workers auto -deadlock CDRaftQuorum
```

Negative checks:

```bash
$JAVA -cp "$JAR" tlc2.TLC -workers auto -deadlock CDRaft_buggy

$JAVA -cp "$JAR" tlc2.TLC -config CDRaftElection_buggy.cfg \
  -workers auto -deadlock CDRaftElection

$JAVA -cp "$JAR" tlc2.TLC -config CDRaftQuorum_NMinusOneBuggy.cfg \
  -workers auto -deadlock CDRaftQuorum
```

The positive runs should finish with:

```text
Model checking completed. No error has been found.
```

The negative runs are expected to fail:

- `CDRaft_buggy`: violates `FastReturnSafety`.
- `CDRaftElection_buggy`: violates `OneGlobalVotePerDomainPerTerm`.
- `CDRaftQuorum_NMinusOneBuggy`: violates `NMinusOneElectionQuorumsIntersect`.

## Latest Checked Results

```text
CDRaft.cfg                       9,842 distinct states
CDRaftLive.cfg                   9,842 distinct states + temporal checks
CDRaftPrefix.cfg             6,454,082 distinct states
CDRaftPrefix3.cfg            7,035,230 distinct states
CDRaftForking.cfg               12,594 distinct states
CDRaftElection.cfg             312,227 distinct states
CDRaftElection2.cfg          3,558,816 distinct states

CDRaftQuorum2.cfg                   64 distinct states
CDRaftQuorum3.cfg                  512 distinct states
CDRaftQuorum4.cfg                4,096 distinct states
CDRaftQuorum5.cfg               32,768 distinct states
```

## Limits

These are bounded TLC model checks, not a full symbolic proof for all possible
cluster sizes and executions.

The models intentionally do not cover:

- real network behavior,
- crash recovery and persistence,
- lease timing,
- overlapping read-serving windows during migration,
- full implementation refinement from Go code to TLA+.

The quorum model checks the important boundary instances and records the
general rule used by the protocol:

```text
Global election quorum = max(N - 1, 2)
```
