package cdraft

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"github.com/czq/cd-raft/internal/topology"
)

// faultyStore wraps a MemoryStore and can be toggled to fail every Save, to
// exercise the fail-stop persistence path.
type faultyStore struct {
	inner *MemoryStore
	fail  atomic.Bool
}

func newFaultyStore() *faultyStore { return &faultyStore{inner: NewMemoryStore()} }

func (s *faultyStore) Load() (PersistentState, error) { return s.inner.Load() }

func (s *faultyStore) Save(state PersistentState) error {
	if s.fail.Load() {
		return errors.New("injected persistence failure")
	}
	return s.inner.Save(state)
}

func newSingleFaultyNode(t *testing.T) (*RPCNode, *faultyStore) {
	t.Helper()
	var configured []topology.Node
	for _, domain := range []string{"a", "b", "c"} {
		for i := 1; i <= 3; i++ {
			configured = append(configured, topology.Node{
				ID:            domain + string(rune('0'+i)),
				DomainCode:    domain,
				ListenAddress: freeAddress(t), InterDomainAddress: freeAddress(t),
			})
		}
	}
	cfg := topology.Config{ApplicationGroup: topology.ApplicationGroup, Nodes: configured}
	store := newFaultyStore()
	node, err := NewRPCNode(cfg, "a1", store)
	if err != nil {
		t.Fatal(err)
	}
	return node, store
}

// TestPersistenceFailureFailStopsNode verifies that a failed durable write marks
// the node failed and strips it of its serving capability (fail-stop), rather
// than silently continuing with un-persisted state.
func TestPersistenceFailureFailStopsNode(t *testing.T) {
	node, store := newSingleFaultyNode(t)
	store.fail.Store(true)

	node.mu.Lock()
	err := node.persistLocked()
	failed := node.failed
	serves := node.servesAsGlobalLeaderLocked()
	node.mu.Unlock()

	if err == nil {
		t.Fatal("persistLocked returned nil on a failing store")
	}
	if !failed {
		t.Fatal("node was not marked failed after a persistence failure")
	}
	if serves {
		t.Fatal("a failed node must not report itself as a serving Global Leader")
	}
}

// TestPersistenceFailureRefusesDomainVote verifies an ack-critical handler does
// not acknowledge un-persisted state: if votedFor cannot be persisted, the vote
// must NOT be granted (a granted-but-lost vote could split an election).
func TestPersistenceFailureRefusesDomainVote(t *testing.T) {
	node, store := newSingleFaultyNode(t)
	node.mu.Lock()
	node.stage = DomainElecting // leave Booting so RequestVote is considered
	node.mu.Unlock()

	store.fail.Store(true)
	resp, err := node.RequestVote(context.Background(), &cdraftv1.DomainVoteRequest{
		DomainId: "a", CandidateId: "a2", DomainTerm: 5,
		Log: toPBLogSummary(LogSummary{}),
	})
	if err != nil {
		t.Fatalf("RequestVote rpc error: %v", err)
	}
	if resp.GetGranted() {
		t.Fatal("a vote was granted despite a persistence failure")
	}

	node.mu.Lock()
	failed := node.failed
	node.mu.Unlock()
	if !failed {
		t.Fatal("node should have fail-stopped after the persistence failure")
	}
}

// TestPersistenceSucceedsWhenStoreHealthy is the positive control: with a healthy
// store the same vote path grants normally and the node is not marked failed.
func TestPersistenceSucceedsWhenStoreHealthy(t *testing.T) {
	node, _ := newSingleFaultyNode(t)
	node.mu.Lock()
	node.stage = DomainElecting
	node.mu.Unlock()

	resp, err := node.RequestVote(context.Background(), &cdraftv1.DomainVoteRequest{
		DomainId: "a", CandidateId: "a2", DomainTerm: 5,
		Log: toPBLogSummary(LogSummary{}),
	})
	if err != nil {
		t.Fatalf("RequestVote rpc error: %v", err)
	}
	if !resp.GetGranted() {
		t.Fatalf("healthy store should grant the vote: %+v", resp)
	}
	node.mu.Lock()
	failed := node.failed
	node.mu.Unlock()
	if failed {
		t.Fatal("node was wrongly marked failed with a healthy store")
	}
}
