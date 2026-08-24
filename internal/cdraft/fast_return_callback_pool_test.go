package cdraft

import (
	"context"
	"net"
	"sync"
	"testing"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"google.golang.org/grpc"
)

type recordingCallback struct {
	cdraftv1.UnimplementedClientCallbackServer
	mu    sync.Mutex
	count int
}

func (r *recordingCallback) FastReturn(_ context.Context, _ *cdraftv1.ClientResult) (*cdraftv1.Empty, error) {
	r.mu.Lock()
	r.count++
	r.mu.Unlock()
	return &cdraftv1.Empty{}, nil
}

func (r *recordingCallback) received() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// Task 2.1: consecutive Fast Return callbacks to the same ReplyRoute reuse a
// single pooled gRPC connection instead of dialing (and handshaking) a fresh one
// each time — the jitter that otherwise flips thin-margin races to global-leader.
func TestFastReturnCallbackConnectionReused(t *testing.T) {
	h := newRPCHarness(t, true)
	h.elect(t)
	b1 := h.nodes["b1"]
	term := b1.state.GlobalTerm

	// b's domain holds 1..2 and knows them committed, so both deliveries pass
	// every Fast Return safety gate.
	seedResponderDomainQuorum(b1, term, 2)
	b1.state.KnownGlobalCommitIndex = 2
	b1.state.AppliedIndex = 2

	addr := freeAddress(t)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen callback route: %v", err)
	}
	srv := grpc.NewServer()
	cb := &recordingCallback{}
	cdraftv1.RegisterClientCallbackServer(srv, cb)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	deliver := func(idx uint64) {
		b1.deliverFastReturn(context.Background(), pendingFastReturn{
			RequestID: reqAt(idx), Result: "ok", ReplyRoute: addr,
			OriginDomain: "b", ResponderDomain: "b",
			GlobalTerm: term, GlobalIndex: idx,
		})
	}

	deliver(1)
	b1.mu.Lock()
	connAfterFirst := b1.conns[addr]
	b1.mu.Unlock()
	if connAfterFirst == nil {
		t.Fatal("expected a pooled callback connection after first delivery")
	}

	deliver(2)
	b1.mu.Lock()
	connAfterSecond := b1.conns[addr]
	b1.mu.Unlock()
	if connAfterSecond != connAfterFirst {
		t.Fatalf("callback connection not reused: first=%p second=%p", connAfterFirst, connAfterSecond)
	}

	if got := cb.received(); got != 2 {
		t.Fatalf("client received %d Fast Return callbacks, want 2", got)
	}
}
