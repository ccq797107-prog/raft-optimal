package cdraft

import (
	"context"
	"testing"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
)

func globalTermOf(n *RPCNode) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.GlobalTerm
}

func cIdentity(t *testing.T, h *rpcHarness) DomainLeaderIdentity {
	t.Helper()
	c1 := h.nodes["c1"]
	c1.mu.Lock()
	defer c1.mu.Unlock()
	return c1.state.DomainLeader
}

// TestGlobalVoteLeaderStickiness guards the residual split-window of issue #1:
// a Domain Leader that recently heard from a valid Global Leader refuses a
// competing higher-term global vote (without advancing its term), but honours a
// deliberate leadership-transfer vote.
func TestGlobalVoteLeaderStickiness(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()
	b1 := harness.nodes["b1"]
	cand := cIdentity(t, harness)

	b1.mu.Lock()
	b1.domainLeaders["c"] = cand
	b1.lastGlobalContact = time.Now() // just heard from GL a1
	term := b1.state.GlobalTerm
	b1.mu.Unlock()

	req := &cdraftv1.GlobalVoteRequest{
		Candidate: toPBDomainIdentity(cand), GlobalTerm: term + 1, Log: toPBLogSummary(LogSummary{}),
	}

	// Sticky: a competing vote is refused and our term is NOT advanced.
	resp, err := b1.RequestVoteGlobalInternal(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetGranted() {
		t.Fatal("vote granted despite leader stickiness")
	}
	if got := globalTermOf(b1); got != term {
		t.Fatalf("term advanced to %d under stickiness (should stay %d)", got, term)
	}

	// Leadership transfer bypasses stickiness.
	transferReq := &cdraftv1.GlobalVoteRequest{
		Candidate: toPBDomainIdentity(cand), GlobalTerm: term + 1, Log: toPBLogSummary(LogSummary{}),
		LeadershipTransfer: true,
	}
	resp, err = b1.RequestVoteGlobalInternal(ctx, transferReq)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetGranted() {
		t.Fatal("leadership-transfer vote was refused")
	}
}

// TestGlobalVoteStickinessLiftsAfterTimeout: once the incumbent has gone silent
// past the sticky window, a competing vote is granted so a genuinely dead leader
// can be replaced.
func TestGlobalVoteStickinessLiftsAfterTimeout(t *testing.T) {
	harness := newRPCHarness(t, false)
	harness.elect(t)
	ctx := context.Background()
	b1 := harness.nodes["b1"]
	cand := cIdentity(t, harness)

	b1.mu.Lock()
	b1.domainLeaders["c"] = cand
	b1.lastGlobalContact = time.Now().Add(-2 * globalStickyWindow) // GL long silent
	term := b1.state.GlobalTerm
	b1.mu.Unlock()

	resp, err := b1.RequestVoteGlobalInternal(ctx, &cdraftv1.GlobalVoteRequest{
		Candidate: toPBDomainIdentity(cand), GlobalTerm: term + 1, Log: toPBLogSummary(LogSummary{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetGranted() {
		t.Fatal("vote refused even though the leader was silent past the sticky window")
	}
}
