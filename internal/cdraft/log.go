package cdraft

// log.go holds the engine-agnostic log and quorum primitives that operate
// purely on a PersistentState value. They carry NO consensus orchestration and
// NO networking, so they are shared verbatim by BOTH execution engines:
//
//   - the production gRPC node (rpc_node.go / telemetry.go), and
//   - the deterministic in-memory test model (simmodel.go / simmodel_data.go).
//
// Keeping them here is the single source of truth for log-matching, commit
// application, and quorum-rollback rules, so the two engines can never disagree
// on how an individual entry is appended, overwritten, or applied.

func appendEntry(state *PersistentState, entry LogEntry) bool {
	// Append-only convenience used by the in-memory simulation and by the simple
	// "the prefix is already known good" callers. It never overwrites; for the
	// conflict-overwrite semantics use appendEntryChecked.
	if entry.GlobalIndex <= state.SnapshotLastIndex {
		// The index was compacted into the snapshot, which only ever covers
		// committed (immutable) entries, so the entry is already durably held.
		return true
	}
	if existing, ok := findEntryByIndex(state.Log, entry.GlobalIndex); ok {
		return existing.GlobalTerm == entry.GlobalTerm && existing.RequestID == entry.RequestID
	}
	expected := state.LastLogIndex() + 1
	if entry.GlobalIndex != expected {
		return false
	}
	state.Log = append(state.Log, entry)
	return true
}

// appendOutcome is the result of a Raft-style consistency-checked append.
type appendOutcome int

const (
	// appendAccepted means the entry is now present at its index (either it was
	// appended/overwritten, or it was already there identically).
	appendAccepted appendOutcome = iota
	// appendPrefixMismatch means the prevLogIndex/prevLogTerm consistency check
	// failed (a missing or divergent prefix). The caller should back up / run a
	// CatchUp backfill so the leader can re-converge the follower's log.
	appendPrefixMismatch
	// appendCommittedConflict means accepting the entry would overwrite an entry
	// the receiver has already committed. This must never happen under a correct
	// single-leader-per-term protocol; it is surfaced as a hard safety error.
	appendCommittedConflict
)

// appendEntryChecked enforces the Raft log-matching property for a single
// incoming entry, given the leader's prevLogIndex/prevLogTerm:
//
//   - The receiver must already hold an entry at prevLogIndex whose term equals
//     prevLogTerm (trivially true for prevLogIndex == 0).
//   - If a different entry already occupies entry.GlobalIndex, the conflicting
//     *uncommitted* tail [GlobalIndex .. end] is truncated and replaced. A
//     conflict at or below the commit index is refused (appendCommittedConflict)
//     because committed entries are immutable.
//   - Otherwise the entry must extend the log contiguously.
func appendEntryChecked(state *PersistentState, entry LogEntry, prevIndex, prevTerm uint64) appendOutcome {
	if entry.GlobalIndex <= state.SnapshotLastIndex {
		// Already compacted into the snapshot; the snapshot only covers committed
		// entries, so the incoming (committed-prefix) entry is already held.
		return appendAccepted
	}
	if prevIndex > 0 {
		if prevIndex == state.SnapshotLastIndex {
			// The previous entry was compacted; its identity survives in the
			// snapshot metadata, so the log-matching check still applies.
			if prevTerm != state.SnapshotLastTerm {
				return appendPrefixMismatch
			}
		} else {
			prev, ok := findEntryByIndex(state.Log, prevIndex)
			if !ok || prev.GlobalTerm != prevTerm {
				return appendPrefixMismatch
			}
		}
	}
	if existing, ok := findEntryByIndex(state.Log, entry.GlobalIndex); ok {
		if existing.GlobalTerm == entry.GlobalTerm && existing.RequestID == entry.RequestID {
			return appendAccepted
		}
		if entry.GlobalIndex <= state.KnownGlobalCommitIndex {
			return appendCommittedConflict
		}
		// Truncate the divergent uncommitted tail, then append the leader's entry.
		keep := entry.GlobalIndex - state.SnapshotLastIndex - 1
		state.Log = append(state.Log[:keep:keep], entry)
		// The content at >= GlobalIndex just changed, so any per-domain quorum
		// high-water mark recorded at or above this index now refers to a stale
		// entry and must be rolled back. Otherwise a leftover "domain reached
		// quorum at i" (earned by the overwritten entry) would let the leader
		// commit or Fast-Return the *replacement* entry that has not actually
		// been quorum-replicated yet — a false two-domain commit.
		rollbackQuorumAbove(state, entry.GlobalIndex-1)
		return appendAccepted
	}
	if entry.GlobalIndex != state.LastLogIndex()+1 {
		return appendPrefixMismatch
	}
	state.Log = append(state.Log, entry)
	return appendAccepted
}

// rollbackQuorumAbove lowers every per-domain quorum high-water mark that sits
// above ceiling back down to ceiling. It is invoked after an uncommitted tail is
// overwritten so the leader never treats a replaced index as still
// quorum-backed.
func rollbackQuorumAbove(state *PersistentState, ceiling uint64) {
	for domain, idx := range state.DomainQuorumIndex {
		if idx > ceiling {
			state.DomainQuorumIndex[domain] = ceiling
		}
	}
}

func applyEntry(state *PersistentState, entry LogEntry) {
	if entry.GlobalIndex != state.KnownGlobalCommitIndex+1 && entry.GlobalIndex > state.KnownGlobalCommitIndex {
		return
	}
	state.KnownGlobalCommitIndex = max(state.KnownGlobalCommitIndex, entry.GlobalIndex)
	if entry.GlobalIndex == state.AppliedIndex+1 {
		state.StateMachine[entry.Command.Key] = entry.Command.Value
		state.AppliedIndex = entry.GlobalIndex
	}
	state.Results[entry.RequestID] = ClientResult{
		RequestID: entry.RequestID, GlobalTerm: entry.GlobalTerm, GlobalIndex: entry.GlobalIndex,
		Result: entry.Result, Source: GlobalResponse, Committed: true,
	}
}

func findEntryByRequest(log []LogEntry, requestID string) (LogEntry, bool) {
	for _, entry := range log {
		if entry.RequestID == requestID {
			return entry, true
		}
	}
	return LogEntry{}, false
}

func findEntryByIndex(log []LogEntry, index uint64) (LogEntry, bool) {
	// The log slice may start past index 1 once a prefix has been compacted into
	// a snapshot; entries carry their own GlobalIndex, so the offset is derived
	// from the first retained entry rather than assumed to be zero.
	if index == 0 || len(log) == 0 {
		return LogEntry{}, false
	}
	first := log[0].GlobalIndex
	if index < first || index >= first+uint64(len(log)) {
		return LogEntry{}, false
	}
	entry := log[index-first]
	return entry, entry.GlobalIndex == index
}

func hasEntry(log []LogEntry, index uint64, requestID string) bool {
	entry, ok := findEntryByIndex(log, index)
	return ok && entry.RequestID == requestID
}

// compactLog discards every log entry at or below compactIndex, folding that
// prefix into the snapshot metadata. Only the APPLIED prefix may be compacted:
// applied implies committed, committed entries are immutable, and their effects
// are fully captured by StateMachine + Results — so nothing is lost. It returns
// false (and changes nothing) when the requested point is stale, unapplied, or
// not held in the log.
func compactLog(state *PersistentState, compactIndex uint64) bool {
	if compactIndex <= state.SnapshotLastIndex || compactIndex > state.AppliedIndex {
		return false
	}
	boundary, ok := findEntryByIndex(state.Log, compactIndex)
	if !ok {
		return false
	}
	keep := state.Log[compactIndex-state.SnapshotLastIndex:]
	state.Log = append([]LogEntry(nil), keep...)
	state.SnapshotLastIndex = compactIndex
	state.SnapshotLastTerm = boundary.GlobalTerm
	state.SnapshotLastRequestID = boundary.RequestID
	return true
}

// installSnapshotState applies a leader-provided snapshot (state machine +
// idempotency results at lastIndex/lastTerm) to a lagging node, mirroring Raft's
// InstallSnapshot receiver rules:
//
//   - A snapshot that does not advance past our applied prefix is a no-op: we
//     already cover it (return true so the sender treats us as caught up).
//   - If we still hold the entry at lastIndex with a matching term, the tail
//     after it is retained (it is consistent by Log Matching); otherwise the
//     entire log is replaced by the snapshot.
//   - StateMachine is replaced wholesale; Results are merged so locally settled
//     requests are never forgotten.
func installSnapshotState(state *PersistentState, lastIndex, lastTerm uint64, lastRequestID string, sm map[string]string, results map[string]ClientResult) bool {
	if lastIndex <= state.AppliedIndex || lastIndex <= state.SnapshotLastIndex {
		return true
	}
	if existing, ok := findEntryByIndex(state.Log, lastIndex); ok && existing.GlobalTerm == lastTerm {
		keep := state.Log[lastIndex-state.Log[0].GlobalIndex+1:]
		state.Log = append([]LogEntry(nil), keep...)
	} else {
		state.Log = nil
		// Any quorum high-water mark above the snapshot boundary referred to
		// entries we just dropped; reconcile it down to what we provably hold.
		rollbackQuorumAbove(state, lastIndex)
	}
	state.SnapshotLastIndex = lastIndex
	state.SnapshotLastTerm = lastTerm
	state.SnapshotLastRequestID = lastRequestID
	state.StateMachine = make(map[string]string, len(sm))
	for k, v := range sm {
		state.StateMachine[k] = v
	}
	if state.Results == nil {
		state.Results = make(map[string]ClientResult, len(results))
	}
	for k, v := range results {
		state.Results[k] = v
	}
	state.AppliedIndex = lastIndex
	state.KnownGlobalCommitIndex = max(state.KnownGlobalCommitIndex, lastIndex)
	return true
}

// distinctDomains counts the unique non-empty domain codes in the slice. Used
// to verify a CommitNotice carries genuine two-domain quorum evidence.
func distinctDomains(domains []string) int {
	seen := make(map[string]struct{}, len(domains))
	for _, d := range domains {
		if d != "" {
			seen[d] = struct{}{}
		}
	}
	return len(seen)
}
