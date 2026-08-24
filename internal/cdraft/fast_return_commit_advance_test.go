package cdraft

import (
	"context"
	"testing"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
)

// seedResponderDomainQuorum makes the responder's own domain hold a contiguous
// prefix 1..upTo WITHOUT advancing its commit index, simulating the window where
// the asynchronous CommitNotice has not yet arrived.
func seedResponderDomainQuorum(n *RPCNode, term, upTo uint64) {
	log := make([]LogEntry, 0, upTo)
	for i := uint64(1); i <= upTo; i++ {
		command := Command{Key: keyAt(i), Value: valAt(i)}
		log = append(log, LogEntry{
			GlobalTerm: term, GlobalIndex: i, RequestID: reqAt(i), OriginDomain: "b",
			Command: command, Result: commandResult(command),
		})
	}
	n.state.initMaps()
	n.state.Log = log
	n.state.DomainQuorumIndex[n.local.DomainCode] = upTo
	n.state.KnownGlobalCommitIndex = 0
	n.state.AppliedIndex = 0
}

func reqAt(i uint64) string { return "r" + itoa(i) }
func keyAt(i uint64) string { return "k" + itoa(i) }
func valAt(i uint64) string { return "v" + itoa(i) }
func itoa(i uint64) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

func announce(t *testing.T, n *RPCNode, term, index uint64, glDomain, requestID string) {
	t.Helper()
	if _, err := n.AnnounceGlobalDomainQuorum(context.Background(), &cdraftv1.GlobalDomainQuorumAck{
		GlobalTerm: term, GlobalIndex: index, GlobalLeaderDomain: glDomain,
		RequestId: requestID, ResponderDomain: n.local.DomainCode,
	}); err != nil {
		t.Fatalf("announce(index=%d): %v", index, err)
	}
}

// Task 2.1: a valid announce, with the responder's own domain already holding the
// entry, advances the responder's knownGlobalCommitIndex on its own — without any
// CommitNotice — so the contiguity gate that guards Fast Return is satisfied.
func TestAnnounceAdvancesResponderCommitIndex(t *testing.T) {
	h := newRPCHarness(t, true)
	h.elect(t)
	b1 := h.nodes["b1"]
	term := b1.state.GlobalTerm

	seedResponderDomainQuorum(b1, term, 2)
	announce(t, b1, term, 2, "a", reqAt(2))

	if got := b1.state.KnownGlobalCommitIndex; got != 2 {
		t.Fatalf("knownGlobalCommitIndex = %d, want 2 (announce should advance commit without a CommitNotice)", got)
	}
}

// Task 2.2: consecutive cross-domain writes keep satisfying the contiguity gate
// purely from announces, even though no CommitNotice ever arrives. Before the
// fix the commit index stayed at 0 and every entry after the first was gated out
// (falling back to global-leader).
func TestConsecutiveAnnouncesAdvanceWithoutCommitNotice(t *testing.T) {
	h := newRPCHarness(t, true)
	h.elect(t)
	b1 := h.nodes["b1"]
	term := b1.state.GlobalTerm

	seedResponderDomainQuorum(b1, term, 3)
	for idx := uint64(1); idx <= 3; idx++ {
		announce(t, b1, term, idx, "a", reqAt(idx))
		if got := b1.state.KnownGlobalCommitIndex; got != idx {
			t.Fatalf("after announce index=%d, knownGlobalCommitIndex = %d, want %d", idx, got, idx)
		}
		// Contiguity gate for the NEXT write must hold off only async notices.
		if next := idx + 1; next <= 3 {
			if b1.state.KnownGlobalCommitIndex+1 < next {
				t.Fatalf("contiguity gate would drop Fast Return for index=%d after announcing %d", next, idx)
			}
		}
	}
}

// A later Fast Return certificate carries evidence for the whole contiguous
// prefix, not just the target entry. If k3 reaches the responder's quorum and
// receives the Global Domain quorum certificate, k1 and k2 must become settled
// too; clients that did not win Fast Return for those earlier requests can then
// complete through the Global Leader's normal response path.
func TestLaterAnnounceCommitsEarlierPrefixResults(t *testing.T) {
	h := newRPCHarness(t, true)
	h.elect(t)
	b1 := h.nodes["b1"]
	term := b1.state.GlobalTerm

	seedResponderDomainQuorum(b1, term, 3)
	announce(t, b1, term, 3, "a", reqAt(3))

	if got := b1.state.KnownGlobalCommitIndex; got != 3 {
		t.Fatalf("knownGlobalCommitIndex = %d, want 3", got)
	}
	if got := b1.state.AppliedIndex; got != 3 {
		t.Fatalf("appliedIndex = %d, want 3", got)
	}
	for idx := uint64(1); idx <= 3; idx++ {
		requestID := reqAt(idx)
		result, ok := b1.state.Results[requestID]
		if !ok {
			t.Fatalf("missing committed result for %s after k3 prefix advance", requestID)
		}
		if !result.Committed || result.Source != GlobalResponse || result.GlobalIndex != idx ||
			result.GlobalTerm != term || result.Result != commandResult(Command{Key: keyAt(idx), Value: valAt(idx)}) {
			t.Fatalf("result for %s = %+v, want committed GlobalResponse at index %d term %d", requestID, result, idx, term)
		}
	}
}

func TestPrefixAdvanceSignalsEarlierGlobalWaiters(t *testing.T) {
	h := newRPCHarness(t, true)
	h.elect(t)
	gl := h.nodes["a1"]
	term := gl.state.GlobalTerm

	seedResponderDomainQuorum(gl, term, 3)
	done1 := make(chan struct{})
	done2 := make(chan struct{})
	done3 := make(chan struct{})
	gl.committing[reqAt(1)] = done1
	gl.committing[reqAt(2)] = done2
	gl.committing[reqAt(3)] = done3

	if !gl.advanceCommitLocked(3, term, reqAt(3)) {
		t.Fatal("prefix commit to k3 was refused")
	}
	for idx, done := range map[uint64]chan struct{}{1: done1, 2: done2} {
		select {
		case <-done:
		default:
			t.Fatalf("waiter for r%d was not signaled by prefix commit", idx)
		}
		if _, ok := gl.committing[reqAt(idx)]; ok {
			t.Fatalf("committing waiter for r%d was not removed", idx)
		}
	}
	select {
	case <-done3:
		t.Fatal("target waiter r3 was signaled before its own normal response path")
	default:
	}
	if _, ok := gl.committing[reqAt(3)]; !ok {
		t.Fatal("target waiter r3 was removed before its own normal response path")
	}
}

// Task 2.3 (safety): an announce must NOT advance commit across a prefix gap the
// responder's domain has not actually quorum-held.
func TestAnnounceDoesNotAdvanceAcrossGap(t *testing.T) {
	h := newRPCHarness(t, true)
	h.elect(t)
	b1 := h.nodes["b1"]
	term := b1.state.GlobalTerm

	// Domain holds only entry 1; entries 2..3 are missing. Announce targets 3.
	b1.state.Log = []LogEntry{{GlobalTerm: term, GlobalIndex: 1, RequestID: reqAt(1), OriginDomain: "b"}}
	b1.state.DomainQuorumIndex[b1.local.DomainCode] = 1
	b1.state.KnownGlobalCommitIndex = 0
	b1.state.AppliedIndex = 0

	announce(t, b1, term, 3, "a", reqAt(3))

	if got := b1.state.KnownGlobalCommitIndex; got != 0 {
		t.Fatalf("knownGlobalCommitIndex = %d, want 0 (must not advance across an unheld prefix gap)", got)
	}
}

func TestAdvanceCommitLockedRejectsInteriorGapWithoutPartialApply(t *testing.T) {
	h := newRPCHarness(t, true)
	h.elect(t)
	gl := h.nodes["a1"]
	term := gl.state.GlobalTerm
	entry1 := LogEntry{
		GlobalTerm: term, GlobalIndex: 1, RequestID: reqAt(1), OriginDomain: "b",
		Command: Command{Key: keyAt(1), Value: valAt(1)},
	}
	entry1.Result = commandResult(entry1.Command)
	entry3 := LogEntry{
		GlobalTerm: term, GlobalIndex: 3, RequestID: reqAt(3), OriginDomain: "b",
		Command: Command{Key: keyAt(3), Value: valAt(3)},
	}
	entry3.Result = commandResult(entry3.Command)
	gl.state.initMaps()
	gl.state.Log = []LogEntry{entry1, entry3}
	gl.state.KnownGlobalCommitIndex = 0
	gl.state.AppliedIndex = 0

	if gl.advanceCommitLocked(3, term, reqAt(3)) {
		t.Fatal("advanceCommitLocked accepted a prefix with missing index 2")
	}
	if gl.state.KnownGlobalCommitIndex != 0 || gl.state.AppliedIndex != 0 {
		t.Fatalf("prefix gap partially advanced commit/apply to %d/%d", gl.state.KnownGlobalCommitIndex, gl.state.AppliedIndex)
	}
	if _, ok := gl.state.Results[reqAt(1)]; ok {
		t.Fatalf("prefix gap partially cached result for %s", reqAt(1))
	}
}

// Task 2.3 (safety): stale-term and foreign-Global-Leader-domain announces are
// rejected and never advance commit.
func TestStaleOrForeignAnnounceDoesNotAdvance(t *testing.T) {
	h := newRPCHarness(t, true)
	h.elect(t)
	b1 := h.nodes["b1"]
	term := b1.state.GlobalTerm

	// Stale term: announce term older than the responder's current global term.
	seedResponderDomainQuorum(b1, term, 2)
	b1.state.GlobalTerm = term + 1
	announce(t, b1, term, 2, "a", reqAt(2))
	if got := b1.state.KnownGlobalCommitIndex; got != 0 {
		t.Fatalf("stale-term announce advanced commit to %d, want 0", got)
	}

	// Foreign Global Leader domain at the current term: provenance rejects it.
	h2 := newRPCHarness(t, true)
	h2.elect(t)
	b1b := h2.nodes["b1"]
	term2 := b1b.state.GlobalTerm
	seedResponderDomainQuorum(b1b, term2, 2)
	announce(t, b1b, term2, 2, "c", reqAt(2)) // claims GL domain c, but b follows a
	if got := b1b.state.KnownGlobalCommitIndex; got != 0 {
		t.Fatalf("foreign-GL-domain announce advanced commit to %d, want 0", got)
	}
}
