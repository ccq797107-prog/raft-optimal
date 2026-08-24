package cdraft

import (
	"testing"
)

// compactionTestState builds a state with n committed+applied entries so the
// compaction primitives have a legal prefix to fold away.
func compactionTestState(n int) PersistentState {
	state := PersistentState{}
	state.initMaps()
	for i := 1; i <= n; i++ {
		entry := LogEntry{
			GlobalTerm: 1, GlobalIndex: uint64(i),
			RequestID: reqID(i), OriginDomain: "a",
			Command: Command{Key: key(i), Value: "v"},
			Result:  "ok",
		}
		state.Log = append(state.Log, entry)
		applyEntry(&state, entry)
	}
	return state
}

func reqID(i int) string { return "r" + string(rune('0'+i)) }
func key(i int) string   { return "k" + string(rune('0'+i)) }

func TestCompactLogFoldsAppliedPrefixIntoSnapshot(t *testing.T) {
	state := compactionTestState(5)
	if !compactLog(&state, 3) {
		t.Fatal("compaction of an applied prefix was refused")
	}
	if state.SnapshotLastIndex != 3 || state.SnapshotLastTerm != 1 || state.SnapshotLastRequestID != reqID(3) {
		t.Fatalf("snapshot boundary identity wrong: %+v", state)
	}
	if len(state.Log) != 2 || state.Log[0].GlobalIndex != 4 {
		t.Fatalf("retained tail wrong: %+v", state.Log)
	}
	if state.LastLogIndex() != 5 || state.Summary().LastGlobalIndex != 5 {
		t.Fatalf("last index broke across the cut: lastLog=%d summary=%+v", state.LastLogIndex(), state.Summary())
	}
	// Compacted entries are gone; retained entries stay addressable by index.
	if _, ok := findEntryByIndex(state.Log, 2); ok {
		t.Fatal("compacted entry still resolvable")
	}
	if e, ok := findEntryByIndex(state.Log, 4); !ok || e.RequestID != reqID(4) {
		t.Fatalf("retained entry not resolvable by global index: %+v ok=%v", e, ok)
	}
	// State machine and results survive untouched.
	if state.StateMachine[key(2)] != "v" || !state.Results[reqID(2)].Committed {
		t.Fatalf("compaction lost applied effects: %+v", state)
	}
}

func TestCompactLogRefusesUnappliedOrStalePoints(t *testing.T) {
	state := compactionTestState(4)
	// Append an ordered-but-unapplied head entry.
	state.Log = append(state.Log, LogEntry{GlobalTerm: 1, GlobalIndex: 5, RequestID: "r-head"})
	if compactLog(&state, 5) {
		t.Fatal("compacted past the applied index")
	}
	if !compactLog(&state, 2) {
		t.Fatal("legal compaction refused")
	}
	if compactLog(&state, 2) || compactLog(&state, 1) {
		t.Fatal("stale compaction point accepted")
	}
}

func TestSummaryFallsBackToSnapshotWhenLogIsEmpty(t *testing.T) {
	state := compactionTestState(3)
	if !compactLog(&state, 3) {
		t.Fatal("compaction refused")
	}
	summary := state.Summary()
	if summary.LastGlobalIndex != 3 || summary.LastGlobalTerm != 1 {
		t.Fatalf("summary does not reflect the snapshot boundary: %+v", summary)
	}
	// Vote freshness comparisons must treat the compacted node as up to date.
	fresh := LogSummary{LastGlobalTerm: 1, LastGlobalIndex: 3}
	if !fresh.AtLeast(summary) || !summary.AtLeast(fresh) {
		t.Fatal("snapshot-backed summary lost the election comparison")
	}
}

func TestAppendAcrossSnapshotBoundary(t *testing.T) {
	state := compactionTestState(3)
	if !compactLog(&state, 3) {
		t.Fatal("compaction refused")
	}
	// prev == snapshot boundary: identity is verified against snapshot metadata.
	next := LogEntry{GlobalTerm: 2, GlobalIndex: 4, RequestID: "r-next"}
	if outcome := appendEntryChecked(&state, next, 3, 1); outcome != appendAccepted {
		t.Fatalf("append across boundary refused: %v", outcome)
	}
	if outcome := appendEntryChecked(&state, LogEntry{GlobalTerm: 2, GlobalIndex: 5, RequestID: "r5"}, 4, 99); outcome != appendPrefixMismatch {
		t.Fatalf("wrong prev term accepted: %v", outcome)
	}
	// An entry at or below the boundary is already covered by the snapshot.
	if outcome := appendEntryChecked(&state, LogEntry{GlobalTerm: 1, GlobalIndex: 2, RequestID: reqID(2)}, 1, 1); outcome != appendAccepted {
		t.Fatalf("compacted-prefix entry not acked: %v", outcome)
	}
	if !appendEntry(&state, LogEntry{GlobalTerm: 1, GlobalIndex: 1, RequestID: reqID(1)}) {
		t.Fatal("appendEntry did not ack a compacted entry")
	}
	// Wrong prev term AT the boundary must be rejected.
	state2 := compactionTestState(3)
	compactLog(&state2, 3)
	if outcome := appendEntryChecked(&state2, next, 3, 42); outcome != appendPrefixMismatch {
		t.Fatalf("boundary log-matching check skipped: %v", outcome)
	}
}

func TestConflictTruncationUsesGlobalIndexOffsets(t *testing.T) {
	state := compactionTestState(3)
	if !compactLog(&state, 2) {
		t.Fatal("compaction refused")
	}
	// Two uncommitted entries above the applied head.
	state.Log = append(state.Log,
		LogEntry{GlobalTerm: 1, GlobalIndex: 4, RequestID: "old4"},
		LogEntry{GlobalTerm: 1, GlobalIndex: 5, RequestID: "old5"},
	)
	// A new leader overwrites index 4: the divergent uncommitted tail truncates.
	replacement := LogEntry{GlobalTerm: 2, GlobalIndex: 4, RequestID: "new4"}
	if outcome := appendEntryChecked(&state, replacement, 3, 1); outcome != appendAccepted {
		t.Fatalf("conflict overwrite refused: %v", outcome)
	}
	if state.LastLogIndex() != 4 {
		t.Fatalf("tail not truncated: %+v", state.Log)
	}
	if e, ok := findEntryByIndex(state.Log, 4); !ok || e.RequestID != "new4" {
		t.Fatalf("replacement not installed: %+v", e)
	}
	// Overwriting a committed (snapshot-covered or not) index must still fail.
	committed := LogEntry{GlobalTerm: 9, GlobalIndex: 3, RequestID: "evil"}
	if outcome := appendEntryChecked(&state, committed, 2, 1); outcome != appendCommittedConflict {
		t.Fatalf("committed conflict not surfaced: %v", outcome)
	}
}

func TestInstallSnapshotStateOnLaggingNode(t *testing.T) {
	leader := compactionTestState(5)
	if !compactLog(&leader, 4) {
		t.Fatal("compaction refused")
	}

	follower := compactionTestState(2) // applied through 2, missing 3..5
	installed := installSnapshotState(&follower, leader.AppliedIndex, 1, reqID(5),
		leader.StateMachine, leader.Results)
	if !installed {
		t.Fatal("snapshot install refused")
	}
	if follower.SnapshotLastIndex != 5 || follower.AppliedIndex != 5 || follower.KnownGlobalCommitIndex != 5 {
		t.Fatalf("follower positions wrong: %+v", follower)
	}
	if len(follower.Log) != 0 {
		t.Fatalf("stale log retained: %+v", follower.Log)
	}
	for i := 1; i <= 5; i++ {
		if follower.StateMachine[key(i)] != "v" {
			t.Fatalf("state machine incomplete at %s: %+v", key(i), follower.StateMachine)
		}
		if !follower.Results[reqID(i)].Committed {
			t.Fatalf("result cache incomplete at %s", reqID(i))
		}
	}
	// Replication continues from the boundary.
	next := LogEntry{GlobalTerm: 2, GlobalIndex: 6, RequestID: "r-six"}
	if outcome := appendEntryChecked(&follower, next, 5, 1); outcome != appendAccepted {
		t.Fatalf("post-install append refused: %v", outcome)
	}
}

func TestInstallSnapshotRetainsMatchingTailAndIgnoresStale(t *testing.T) {
	follower := compactionTestState(3)
	follower.Log = append(follower.Log, LogEntry{GlobalTerm: 1, GlobalIndex: 4, RequestID: "tail4"})

	leaderAt3 := compactionTestState(3)
	// Snapshot at index 3 with matching term: tail entry 4 must survive.
	if !installSnapshotState(&follower, 3, 1, reqID(3), leaderAt3.StateMachine, leaderAt3.Results) {
		t.Fatal("covered snapshot reported failure")
	}
	if follower.LastLogIndex() != 4 {
		t.Fatalf("matching tail dropped: %+v", follower.Log)
	}

	// A snapshot that does not advance past the applied prefix is a no-op ack.
	clone := follower.Clone()
	if !installSnapshotState(&follower, 2, 1, reqID(2), leaderAt3.StateMachine, leaderAt3.Results) {
		t.Fatal("stale snapshot not acked as covered")
	}
	if follower.SnapshotLastIndex != clone.SnapshotLastIndex || follower.LastLogIndex() != clone.LastLogIndex() {
		t.Fatalf("stale snapshot mutated state: %+v", follower)
	}

	// A divergent-term boundary discards the whole log and rolls quorum marks back.
	diverged := compactionTestState(2)
	diverged.Log = append(diverged.Log, LogEntry{GlobalTerm: 1, GlobalIndex: 3, RequestID: "old3"})
	diverged.DomainQuorumIndex["a"] = 3
	leader5 := compactionTestState(5)
	if !installSnapshotState(&diverged, 3, 2, "new3", leader5.StateMachine, leader5.Results) {
		t.Fatal("snapshot install refused")
	}
	if len(diverged.Log) != 0 || diverged.DomainQuorumIndex["a"] != 3 {
		t.Fatalf("divergent tail handling wrong: log=%+v quorum=%+v", diverged.Log, diverged.DomainQuorumIndex)
	}
	if diverged.SnapshotLastTerm != 2 || diverged.SnapshotLastRequestID != "new3" {
		t.Fatalf("boundary identity wrong: %+v", diverged)
	}
}

func TestKVStoreDeletesCompactedLogPrefix(t *testing.T) {
	engine := newMemKVEngine()
	store := NewKVStore(engine)

	state := compactionTestState(5)
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	if !compactLog(&state, 3) {
		t.Fatal("compaction refused")
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}

	recovered, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if recovered.SnapshotLastIndex != 3 || recovered.SnapshotLastTerm != 1 || recovered.SnapshotLastRequestID != reqID(3) {
		t.Fatalf("snapshot metadata did not survive: %+v", recovered)
	}
	if len(recovered.Log) != 2 || recovered.Log[0].GlobalIndex != 4 || recovered.LastLogIndex() != 5 {
		t.Fatalf("compacted log did not reload correctly: %+v", recovered.Log)
	}
	// The compacted keys must be physically gone from the engine.
	for i := uint64(1); i <= 3; i++ {
		if _, found, _ := engine.Get(logKey(i)); found {
			t.Fatalf("compacted log key %d still durable", i)
		}
	}
	if recovered.StateMachine[key(2)] != "v" || recovered.AppliedIndex != 5 {
		t.Fatalf("state machine / applied index lost: %+v", recovered)
	}

	// A reloaded store must keep diffing correctly (appends land above the cut).
	next := LogEntry{GlobalTerm: 2, GlobalIndex: 6, RequestID: "r-six"}
	recovered.Log = append(recovered.Log, next)
	if err := store.Save(recovered); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := engine.Get(logKey(6)); !found {
		t.Fatal("post-compaction append not durable")
	}
}

func TestFileStoreRoundTripsSnapshotMetadata(t *testing.T) {
	dir := t.TempDir()
	store := NewFileStore(dir + "/state.json")
	state := compactionTestState(4)
	if !compactLog(&state, 4) {
		t.Fatal("compaction refused")
	}
	if err := store.Save(state); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if recovered.SnapshotLastIndex != 4 || recovered.SnapshotLastTerm != 1 ||
		recovered.SnapshotLastRequestID != reqID(4) || recovered.Summary().LastGlobalIndex != 4 {
		t.Fatalf("snapshot metadata lost on file round trip: %+v", recovered)
	}
}
