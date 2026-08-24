package cdraft

import (
	"context"
	"testing"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func logState(entries ...LogEntry) *PersistentState {
	s := &PersistentState{}
	s.initMaps()
	s.Log = append(s.Log, entries...)
	return s
}

func logEntry(term, idx uint64, req string) LogEntry {
	return LogEntry{GlobalTerm: term, GlobalIndex: idx, RequestID: req, Command: Command{Key: "k", Value: req}}
}

// TestAppendEntryCheckedTruncatesUncommittedConflict is the core regression for
// issue #2: a higher-term leader's entry must overwrite a divergent UNCOMMITTED
// tail rather than being silently rejected (which previously let a stale local
// entry survive and later be applied by CommitNotice).
func TestAppendEntryCheckedTruncatesUncommittedConflict(t *testing.T) {
	// Local log: [a@t1#1, orphanX@t1#2]; index 2 is uncommitted (commit=0).
	s := logState(logEntry(1, 1, "a"), logEntry(1, 2, "orphanX"))

	// New Global Leader (term 2) replicates Y at index 2; prev=(1, term 1) matches.
	out := appendEntryChecked(s, logEntry(2, 2, "Y"), 1, 1)
	if out != appendAccepted {
		t.Fatalf("outcome = %v, want appendAccepted", out)
	}
	if len(s.Log) != 2 {
		t.Fatalf("log length = %d, want 2 (tail truncated then replaced)", len(s.Log))
	}
	got, ok := findEntryByIndex(s.Log, 2)
	if !ok || got.RequestID != "Y" || got.GlobalTerm != 2 {
		t.Fatalf("index 2 = %+v, want Y@term2", got)
	}
}

// TestAppendEntryCheckedOverwriteRollsBackDomainQuorum is the regression for the
// "conflict overwrite leaves a stale quorum certificate" bug: after the
// uncommitted entry that earned quorum at an index is replaced, the per-domain
// quorum high-water mark for that index must be rolled back, or the leader would
// falsely treat the *replacement* as already quorum-replicated and commit / Fast
// Return it.
func TestAppendEntryCheckedOverwriteRollsBackDomainQuorum(t *testing.T) {
	s := logState(logEntry(1, 1, "a"), logEntry(1, 2, "orphanX"))
	// Domain b reached an in-domain majority for the OLD entry at index 2.
	s.DomainQuorumIndex["b"] = 2
	s.DomainQuorumIndex["a"] = 2

	if out := appendEntryChecked(s, logEntry(2, 2, "Y"), 1, 1); out != appendAccepted {
		t.Fatalf("outcome = %v, want appendAccepted", out)
	}
	if s.DomainQuorumIndex["b"] != 1 {
		t.Fatalf("DomainQuorumIndex[b] = %d after overwrite, want 1 (rolled back below replaced index)", s.DomainQuorumIndex["b"])
	}
	if s.DomainQuorumIndex["a"] != 1 {
		t.Fatalf("DomainQuorumIndex[a] = %d after overwrite, want 1", s.DomainQuorumIndex["a"])
	}
}

// TestKVStorePersistsInPlaceOverwrite is the regression for the durability bug:
// an in-place tail overwrite does NOT change the log length, so an append-only
// Save would skip it and a reload would resurrect the old entry. Save must
// detect the changed slot and flush it.
func TestKVStorePersistsInPlaceOverwrite(t *testing.T) {
	store := NewKVStore(newMemKVEngine())
	if err := store.Save(PersistentState{
		Log: []LogEntry{
			{GlobalTerm: 1, GlobalIndex: 1, RequestID: "a"},
			{GlobalTerm: 1, GlobalIndex: 2, RequestID: "orphanX"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	// Overwrite index 2 in place (same length, new term + requestID).
	if err := store.Save(PersistentState{
		Log: []LogEntry{
			{GlobalTerm: 1, GlobalIndex: 1, RequestID: "a"},
			{GlobalTerm: 2, GlobalIndex: 2, RequestID: "Y"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Log) != 2 {
		t.Fatalf("log length = %d, want 2", len(got.Log))
	}
	if e := got.Log[1]; e.RequestID != "Y" || e.GlobalTerm != 2 {
		t.Fatalf("index 2 reloaded as %+v, want Y@term2 (old entry resurrected)", e)
	}
}

// TestAppendEntryCheckedRefusesCommittedConflict: a conflict at or below the
// commit index is a safety violation and must never overwrite.
func TestAppendEntryCheckedRefusesCommittedConflict(t *testing.T) {
	s := logState(logEntry(1, 1, "a"), logEntry(1, 2, "committed"))
	s.KnownGlobalCommitIndex = 2

	out := appendEntryChecked(s, logEntry(2, 2, "Y"), 1, 1)
	if out != appendCommittedConflict {
		t.Fatalf("outcome = %v, want appendCommittedConflict", out)
	}
	if got, _ := findEntryByIndex(s.Log, 2); got.RequestID != "committed" {
		t.Fatalf("committed entry was overwritten: %+v", got)
	}
}

// TestAppendEntryCheckedPrefixMismatch: missing or divergent prev entry is
// rejected so the leader backs up and CatchUp-backfills.
func TestAppendEntryCheckedPrefixMismatch(t *testing.T) {
	// Missing prefix: only index 1 present, asked to append index 3.
	s := logState(logEntry(1, 1, "a"))
	if out := appendEntryChecked(s, logEntry(1, 3, "c"), 2, 1); out != appendPrefixMismatch {
		t.Fatalf("missing-prefix outcome = %v, want appendPrefixMismatch", out)
	}
	// Divergent prev term: index 2 has term 1, but leader claims prevTerm 9.
	s = logState(logEntry(1, 1, "a"), logEntry(1, 2, "b"))
	if out := appendEntryChecked(s, logEntry(2, 3, "c"), 2, 9); out != appendPrefixMismatch {
		t.Fatalf("wrong-prev-term outcome = %v, want appendPrefixMismatch", out)
	}
}

// TestAppendEntryCheckedIdempotentAndContiguous: re-appending an identical entry
// and appending the next contiguous entry both succeed without corrupting state.
func TestAppendEntryCheckedIdempotentAndContiguous(t *testing.T) {
	s := logState(logEntry(1, 1, "a"))
	if out := appendEntryChecked(s, logEntry(1, 1, "a"), 0, 0); out != appendAccepted || len(s.Log) != 1 {
		t.Fatalf("idempotent re-append: out=%v len=%d", out, len(s.Log))
	}
	if out := appendEntryChecked(s, logEntry(1, 2, "b"), 1, 1); out != appendAccepted || len(s.Log) != 2 {
		t.Fatalf("contiguous append: out=%v len=%d", out, len(s.Log))
	}
}

func commitIndexOf(n *RPCNode) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.KnownGlobalCommitIndex
}

// TestReplicateTruncatesUncommittedConflictTail exercises the full RPC path:
// a domain leader holding a stale uncommitted entry accepts the current Global
// Leader's conflicting entry by truncating its divergent tail.
func TestReplicateTruncatesUncommittedConflictTail(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()
	b1 := harness.nodes["b1"]

	b1.mu.Lock()
	term := b1.state.GlobalTerm
	gl := b1.globalLeader.DomainLeader.NodeID
	b1.state.Log = []LogEntry{{GlobalTerm: term, GlobalIndex: 1, RequestID: "orphanX", Command: Command{Key: "k", Value: "X"}}}
	b1.mu.Unlock()

	if _, err := b1.Replicate(ctx, &cdraftv1.ReplicateEntry{
		GlobalTerm: term, GlobalIndex: 1, RequestId: "Y", Key: "k", Value: "Y",
		SenderNodeId: gl, PrevLogIndex: 0, PrevLogTerm: 0,
	}); err != nil {
		t.Fatalf("replicate Y: %v", err)
	}

	b1.mu.Lock()
	got, ok := findEntryByIndex(b1.state.Log, 1)
	b1.mu.Unlock()
	if !ok || got.RequestID != "Y" {
		t.Fatalf("index 1 not overwritten with Y: %+v", got)
	}
}

// TestReplicateRejectsUntrustedSender guards issue #5: a same-term entry from a
// node that is neither the Global Leader nor our Domain Leader is refused.
func TestReplicateRejectsUntrustedSender(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()
	b1 := harness.nodes["b1"]

	b1.mu.Lock()
	term := b1.state.GlobalTerm
	b1.mu.Unlock()

	_, err := b1.Replicate(ctx, &cdraftv1.ReplicateEntry{
		GlobalTerm: term, GlobalIndex: 1, RequestId: "Z", Key: "k", Value: "Z",
		SenderNodeId: "rogue",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for untrusted sender, got %v", err)
	}
	b1.mu.Lock()
	logLen := len(b1.state.Log)
	b1.mu.Unlock()
	if logLen != 0 {
		t.Fatalf("untrusted replicate mutated the log (len=%d)", logLen)
	}
}

// TestCommitNoticeProvenanceAndEvidence guards issue #5 on the commit path:
// stale-term, untrusted-sender, and under-evidenced commit notices must not
// advance the commit index; a well-formed one does.
func TestCommitNoticeProvenanceAndEvidence(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()
	b1 := harness.nodes["b1"]

	b1.mu.Lock()
	term := b1.state.GlobalTerm
	gl := b1.globalLeader.DomainLeader.NodeID
	b1.state.Log = []LogEntry{{GlobalTerm: term, GlobalIndex: 1, RequestID: "r1", OriginDomain: "a", Command: Command{Key: "k", Value: "v"}}}
	b1.mu.Unlock()

	valid := func() *cdraftv1.CommitNotice {
		return &cdraftv1.CommitNotice{
			GlobalTerm: term, CommitIndex: 1, EvidenceDomains: []string{"a", "b"}, SenderNodeId: gl,
			CommitTerm: term, CommitRequestId: "r1",
		}
	}

	cases := []struct {
		name   string
		mutate func(*cdraftv1.CommitNotice)
	}{
		{"insufficient evidence", func(n *cdraftv1.CommitNotice) { n.EvidenceDomains = []string{"a"} }},
		{"untrusted sender", func(n *cdraftv1.CommitNotice) { n.SenderNodeId = "rogue" }},
		{"stale term", func(n *cdraftv1.CommitNotice) { n.GlobalTerm = term - 1 }},
	}
	for _, tc := range cases {
		notice := valid()
		tc.mutate(notice)
		if _, err := b1.Commit(ctx, notice); err != nil {
			t.Fatalf("%s: unexpected error %v", tc.name, err)
		}
		if got := commitIndexOf(b1); got != 0 {
			t.Fatalf("%s: commit advanced to %d, want 0", tc.name, got)
		}
	}

	if _, err := b1.Commit(ctx, valid()); err != nil {
		t.Fatalf("valid commit: %v", err)
	}
	if got := commitIndexOf(b1); got != 1 {
		t.Fatalf("valid commit did not advance, got %d", got)
	}
}

// TestCommitNoticeRejectsMismatchedEntryIdentity is the core regression for the
// "commit the wrong log entry" bug: a node holding a stale entry X@1 must NOT
// commit it just because a CommitNotice names index 1 — the leader committed a
// DIFFERENT entry (Y) there. Without the identity check the node would commit X
// and then (since X is "committed") permanently refuse Y, diverging the state
// machine. The same identity must also gate the heartbeat commit-advance path.
func TestCommitNoticeRejectsMismatchedEntryIdentity(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()
	b1 := harness.nodes["b1"]

	b1.mu.Lock()
	term := b1.state.GlobalTerm
	gl := b1.globalLeader.DomainLeader.NodeID
	// We hold a stale, uncommitted X at index 1 (e.g. from a previous leader).
	b1.state.Log = []LogEntry{logEntry(term, 1, "X")}
	b1.mu.Unlock()

	// The Global Leader actually committed Y@1 (different requestId) and fans out
	// a CommitNotice for index 1 that overtook Y's replication to us.
	mismatched := &cdraftv1.CommitNotice{
		GlobalTerm: term, CommitIndex: 1, EvidenceDomains: []string{"a", "b"}, SenderNodeId: gl,
		CommitTerm: term, CommitRequestId: "Y",
	}
	if _, err := b1.Commit(ctx, mismatched); err != nil {
		t.Fatalf("commit rpc error: %v", err)
	}
	if got := commitIndexOf(b1); got != 0 {
		t.Fatalf("committed a stale entry under a mismatched identity: commit=%d", got)
	}

	// The heartbeat commit-advance path must refuse the same mismatch.
	if _, err := b1.HeartbeatGlobalInternal(ctx, &cdraftv1.GlobalHeartbeat{
		Leader:      toPBDomainIdentity(b1.globalLeader.DomainLeader),
		GlobalTerm:  term,
		CommitIndex: 1, CommitTerm: term, CommitRequestId: "Y",
	}); err != nil {
		t.Fatalf("heartbeat rpc error: %v", err)
	}
	if got := commitIndexOf(b1); got != 0 {
		t.Fatalf("heartbeat committed a stale entry under a mismatched identity: commit=%d", got)
	}

	// Once we actually hold Y at index 1, the matching notice commits it.
	b1.mu.Lock()
	b1.state.Log = []LogEntry{logEntry(term, 1, "Y")}
	b1.mu.Unlock()
	if _, err := b1.Commit(ctx, mismatched); err != nil {
		t.Fatalf("commit rpc error: %v", err)
	}
	if got := commitIndexOf(b1); got != 1 {
		t.Fatalf("matching commit did not advance, got %d", got)
	}
}

// TestAnnounceFastReturnRejectsMismatchedCertificate is the regression for the
// "Fast Return reuses another entry's quorum certificate" bug: a domain whose
// majority holds old-X@1 must NOT vouch (Quorum=true) for a new-Y@1 announce that
// merely lands on the same index.
func TestAnnounceFastReturnRejectsMismatchedCertificate(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()
	b1 := harness.nodes["b1"]

	b1.mu.Lock()
	term := b1.state.GlobalTerm
	glDomain := b1.globalLeader.DomainLeader.DomainID
	// Our domain reached an in-domain majority for X at index 1.
	b1.state.Log = []LogEntry{logEntry(term, 1, "X")}
	b1.state.DomainQuorumIndex[b1.local.DomainCode] = 1
	b1.mu.Unlock()

	// An announce for a DIFFERENT entry (Y) at the same index must not be vouched.
	resp, err := b1.AnnounceGlobalDomainQuorum(ctx, &cdraftv1.GlobalDomainQuorumAck{
		GlobalTerm: term, GlobalIndex: 1, GlobalLeaderDomain: glDomain,
		RequestId: "Y", ReplyRoute: "",
	})
	if err != nil {
		t.Fatalf("announce rpc error: %v", err)
	}
	if resp.GetQuorum() {
		t.Fatal("domain falsely vouched a Fast Return for an entry its majority never held")
	}

	// The announce that matches the entry we actually hold IS vouched.
	resp, err = b1.AnnounceGlobalDomainQuorum(ctx, &cdraftv1.GlobalDomainQuorumAck{
		GlobalTerm: term, GlobalIndex: 1, GlobalLeaderDomain: glDomain,
		RequestId: "X", ReplyRoute: "",
	})
	if err != nil {
		t.Fatalf("announce rpc error: %v", err)
	}
	if !resp.GetQuorum() {
		t.Fatal("domain failed to vouch a Fast Return for the entry it actually holds")
	}
}

// TestDomainLeaseGatesGlobalVote is the regression for "one domain casts two
// global votes": a Domain Leader that can no longer confirm an in-domain
// majority (expired domain lease) must neither cast nor grant a global vote,
// even though it remains the domain's leader.
func TestDomainLeaseGatesGlobalVote(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()
	a1 := harness.nodes["a1"] // domain leader of a (and current GL)

	a1.SetDomainLeaseDuration(200 * time.Millisecond)
	a1.SetDomainLeaseEnabled(true)
	a1.mu.Lock()
	a1.domainLeaseDeadline = time.Now().Add(-time.Millisecond) // lapsed: no majority
	candidate := toPBDomainIdentity(a1.domainLeaders[a1.local.DomainCode])
	gterm := a1.state.GlobalTerm
	a1.mu.Unlock()

	// Casting: a majority-less Domain Leader may not campaign globally.
	if err := a1.CampaignGlobal(ctx); err != ErrNotDomainLeader {
		t.Fatalf("majority-less leader campaigned globally: err=%v", err)
	}

	// Granting: it must abstain from another domain's candidacy.
	resp, err := a1.RequestVoteGlobalInternal(ctx, &cdraftv1.GlobalVoteRequest{
		Candidate: candidate, GlobalTerm: gterm + 1, LeadershipTransfer: true,
	})
	if err != nil {
		t.Fatalf("vote rpc error: %v", err)
	}
	if resp.GetGranted() {
		t.Fatal("majority-less Domain Leader granted a global vote")
	}

	// A fresh lease restores eligibility to grant.
	a1.mu.Lock()
	a1.refreshDomainLeaseLocked(time.Now())
	a1.mu.Unlock()
	resp, err = a1.RequestVoteGlobalInternal(ctx, &cdraftv1.GlobalVoteRequest{
		Candidate: candidate, GlobalTerm: gterm + 1, LeadershipTransfer: true,
		Log: toPBLogSummary(LogSummary{}),
	})
	if err != nil {
		t.Fatalf("vote rpc error: %v", err)
	}
	if !resp.GetGranted() {
		t.Fatalf("domain leader with a fresh lease failed to grant: %+v", resp)
	}
}

// TestGlobalHeartbeatLeaseConfirmRequiresDomainLeader is the regression for
// "ordinary followers count as global-lease confirmations": only a current
// Domain Leader (with a fresh domain lease) may answer Granted=true to a global
// heartbeat, and it must stamp its own identity so the GL can verify the source.
func TestGlobalHeartbeatLeaseConfirmRequiresDomainLeader(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()

	a1 := harness.nodes["a1"]
	a1.mu.Lock()
	gl := a1.globalLeader.DomainLeader
	gterm := a1.state.GlobalTerm
	a1.mu.Unlock()
	hb := &cdraftv1.GlobalHeartbeat{Leader: toPBDomainIdentity(gl), GlobalTerm: gterm}

	// A domain follower must NOT confirm the Global Leader's read lease.
	resp, err := harness.nodes["a2"].HeartbeatGlobalInternal(ctx, hb)
	if err != nil {
		t.Fatalf("heartbeat rpc error: %v", err)
	}
	if resp.GetGranted() {
		t.Fatal("a domain follower falsely confirmed the Global Leader's read lease")
	}

	// A current Domain Leader (another domain) confirms and stamps its identity.
	resp, err = harness.nodes["b1"].HeartbeatGlobalInternal(ctx, hb)
	if err != nil {
		t.Fatalf("heartbeat rpc error: %v", err)
	}
	if !resp.GetGranted() {
		t.Fatal("a current Domain Leader failed to confirm the lease")
	}
	if got := fromPBDomainIdentity(resp.GetVoter()); got.DomainID != "b" || got.NodeID != "b1" {
		t.Fatalf("confirmation carried wrong/absent voter identity: %+v", got)
	}
}

// TestHandoffRevokesIncumbentReadLease is the regression for the migration
// dual-leader window: the outgoing Global Leader must stop serving the instant it
// relinquishes for a handoff (before the target can begin serving), since the
// leadership-transfer election bypasses voter stickiness.
func TestHandoffRevokesIncumbentReadLease(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	a1 := harness.nodes["a1"]

	a1.SetGlobalLeaseEnabled(true)
	a1.mu.Lock()
	a1.refreshGlobalLeaseLocked(time.Now())
	servingBefore := a1.servesAsGlobalLeaderLocked()
	a1.mu.Unlock()
	if !servingBefore {
		t.Fatal("incumbent should be serving before the handoff")
	}

	a1.mu.Lock()
	a1.revokeGlobalLeadershipForHandoffLocked()
	servingAfter := a1.servesAsGlobalLeaderLocked()
	role := a1.globalRole
	a1.mu.Unlock()
	if servingAfter {
		t.Fatal("incumbent kept serving after relinquishing for the handoff")
	}
	if role == Leader {
		t.Fatalf("incumbent remained Global Leader after relinquishing, role=%v", role)
	}
}
