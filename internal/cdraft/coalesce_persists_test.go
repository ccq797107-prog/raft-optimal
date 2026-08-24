package cdraft

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/czq/cd-raft/internal/topology"
)

// coalesceCountingStore wraps a MemoryStore and counts Save() calls so a test
// can assert how many fsyncs the GL write path actually issues.
type coalesceCountingStore struct {
	inner *MemoryStore
	saves atomic.Int64
}

func newCoalesceCountingStore() *coalesceCountingStore {
	return &coalesceCountingStore{inner: NewMemoryStore()}
}

func (s *coalesceCountingStore) Load() (PersistentState, error) { return s.inner.Load() }

func (s *coalesceCountingStore) Save(state PersistentState) error {
	s.saves.Add(1)
	return s.inner.Save(state)
}

// coalesceGatedStore wraps a MemoryStore and can be armed to fail every Save()
// after a given GlobalIndex appears in the log. Used to prove "no fsync, no
// quorum" — when persistence fails on every consensus node, no domain can
// form quorum and the client times out.
type coalesceGatedStore struct {
	inner       *MemoryStore
	targetIndex uint64
	armed       atomic.Bool
	tripped     atomic.Bool
}

func newCoalesceGatedStore(targetIndex uint64) *coalesceGatedStore {
	s := &coalesceGatedStore{inner: NewMemoryStore(), targetIndex: targetIndex}
	s.armed.Store(true)
	return s
}

func (s *coalesceGatedStore) Load() (PersistentState, error) { return s.inner.Load() }

func (s *coalesceGatedStore) Save(state PersistentState) error {
	if s.armed.Load() && s.matchesTargetEntry(state) {
		s.tripped.Store(true)
		return errors.New("injected post-target persist failure")
	}
	return s.inner.Save(state)
}

func (s *coalesceGatedStore) matchesTargetEntry(state PersistentState) bool {
	for i := range state.Log {
		if state.Log[i].GlobalIndex == s.targetIndex {
			return true
		}
	}
	return false
}

func newCoalesceClusterWithStores(t *testing.T, fastReturn bool, makeStore func(id string) Store) *rpcHarness {
	t.Helper()
	var configured []topology.Node
	for _, domain := range []string{"a", "b", "c"} {
		for i := 1; i <= 3; i++ {
			id := fmt.Sprintf("%s%d", domain, i)
			configured = append(configured, topology.Node{
				ID: id, DomainCode: domain,
				ListenAddress: freeAddress(t), InterDomainAddress: freeAddress(t),
			})
		}
	}
	cfg := topology.Config{
		ApplicationGroup: topology.ApplicationGroup,
		Nodes:            configured,
		Features:         topology.Features{FastReturnEnabled: fastReturn},
	}
	harness := &rpcHarness{config: cfg, nodes: make(map[string]*RPCNode), stores: make(map[string]Store), client: NewRPCClient()}
	for _, configuredNode := range cfg.Nodes {
		store := makeStore(configuredNode.ID)
		node, err := NewRPCNode(cfg, configuredNode.ID, store)
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		harness.nodes[configuredNode.ID] = node
		harness.stores[configuredNode.ID] = store
	}
	t.Cleanup(func() {
		harness.client.Close()
		for _, node := range harness.nodes {
			node.Stop()
		}
	})
	return harness
}

func (h *rpcHarness) setAllTrace(recorder *WriteTraceRecorder) {
	for _, node := range h.nodes {
		node.SetWriteTraceRecorder(recorder)
	}
}

// TestCoalescedGLWritePathPersistShape exercises a normal cross-domain write
// with Fast Return enabled and asserts that the critical-path fsync count on
// the GL has been reduced to 2:
//
//  1. The deferred replicate_persist mark fires (leader-path fsync coalesce).
//  2. The pre-quorum replicate_persist fsync on GL is gone (the post-quorum
//     domain_quorum_persist now covers entry + DomainQuorumIndex in one batch).
//  3. The persist_before_commit fsync is gone — durability before announce is
//     supplied by follower-side replicate_persist on two domain majorities.
//  4. The drain fsync does NOT fire on the success path.
//  5. The follower replicate_persist still runs unchanged; follower commit
//     notices are propagated asynchronously after the Domain Leader has durably
//     committed, so follower apply-side commit_persist is not part of the client
//     response path.
//  6. The GL node issues no more than 3 Save() calls for the whole write
//     (one merged replicate/quorum fsync + commit_persist + at most one stray
//     bookkeeping persist for retry-protection state). This is a strict upper
//     bound that would fail if any removed fsync creeps back in.
func TestCoalescedGLWritePathPersistShape(t *testing.T) {
	stores := make(map[string]*coalesceCountingStore)
	harness := newCoalesceClusterWithStores(t, true, func(id string) Store {
		s := newCoalesceCountingStore()
		stores[id] = s
		return s
	})
	harness.elect(t)

	baseline := make(map[string]int64)
	for id, store := range stores {
		baseline[id] = store.saves.Load()
	}

	recorder := NewWriteTraceRecorder()
	harness.setAllTrace(recorder)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target := mustNode(t, harness.config, "a1")
	decision, err := harness.client.Write(ctx, target.ListenAddress, ClientWriteRequest{
		RequestID: "coalesce-shape", OriginDomain: "b",
		Command: Command{Key: "k", Value: "v"},
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if !decision.Result.Committed {
		t.Fatalf("write not committed: %+v", decision)
	}

	var persistBeforeCommitCount, deferredReplicateCount, drainCount int
	var glReplicatePersistPreQuorum, glDomainQuorumPersist, glCommitPersist int
	var followerReplicatePersist int
	for _, event := range recorder.Events() {
		if event.RequestID != "coalesce-shape" {
			continue
		}
		switch {
		case event.Phase == "server.persist_before_commit":
			persistBeforeCommitCount++
		case event.Phase == "server.replicate_persist_deferred" && event.NodeID == "a1":
			deferredReplicateCount++
		case event.Phase == "server.quorum_progress_persist" && event.Detail == "drain":
			drainCount++
		case event.Phase == "server.replicate_persist" && event.NodeID == "a1" && event.Detail != "drain":
			glReplicatePersistPreQuorum++
		case event.Phase == "server.domain_quorum_persist" && event.NodeID == "a1":
			glDomainQuorumPersist++
		case event.Phase == "server.commit_persist" && event.NodeID == "a1":
			glCommitPersist++
		case event.Phase == "server.replicate_persist" && event.NodeID != "a1":
			followerReplicatePersist++
		}
	}

	if persistBeforeCommitCount != 0 {
		t.Fatalf("persist_before_commit must be gone after the 4→2 coalesce, got %d", persistBeforeCommitCount)
	}
	if deferredReplicateCount == 0 {
		t.Fatal("expected at least one replicate_persist_deferred mark on GL")
	}
	if drainCount != 0 {
		t.Fatalf("drain fsync must not fire on success path, got %d", drainCount)
	}
	if glReplicatePersistPreQuorum != 0 {
		t.Fatalf("GL's pre-quorum replicate_persist must be deferred into the post-quorum fsync, got %d", glReplicatePersistPreQuorum)
	}
	if glDomainQuorumPersist == 0 || glCommitPersist == 0 {
		t.Fatalf("GL must still fsync once via domain_quorum_persist (merged with entry) and once via commit_persist, got domain=%d commit=%d",
			glDomainQuorumPersist, glCommitPersist)
	}
	if followerReplicatePersist == 0 {
		t.Fatal("follower replicate_persist did not fire — coalesce must not affect follower fsync timing")
	}

	glSavesDuringWrite := stores["a1"].saves.Load() - baseline["a1"]
	if glSavesDuringWrite > 3 {
		t.Fatalf("GL node fsynced %d times for a single coalesced write; expected <= 3", glSavesDuringWrite)
	}
}

// TestCoalescedClusterWidePersistFailureBlocksCommit verifies the durability
// invariant: when every consensus node fails to persist the new entry, no
// domain can form quorum and the client must not receive a successful response.
// After the 4→2 coalesce the GL itself fsyncs only once before client-visible
// ack (the merged domain_quorum_persist), but durability is still anchored by
// followers — this test proves the cluster wedges rather than acking when those
// follower fsyncs fail too.
func TestCoalescedClusterWidePersistFailureBlocksCommit(t *testing.T) {
	const targetIndex = uint64(1)
	gatedStores := make(map[string]*coalesceGatedStore)
	harness := newCoalesceClusterWithStores(t, true, func(id string) Store {
		s := newCoalesceGatedStore(targetIndex)
		s.armed.Store(false) // disarmed during election; armed just before the write
		gatedStores[id] = s
		return s
	})
	harness.elect(t)

	// Arm every store so the very next persist that includes the new entry
	// fails on every node — no follower can ack, no domain can form quorum.
	for _, store := range gatedStores {
		store.armed.Store(true)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	target := mustNode(t, harness.config, "a1")
	_, err := harness.client.Write(ctx, target.ListenAddress, ClientWriteRequest{
		RequestID: "coalesce-cluster-fail", OriginDomain: "b",
		Command: Command{Key: "k", Value: "v"},
	})
	if err == nil {
		t.Fatal("expected client write to fail when every node refuses to persist the new entry")
	}
}

// TestCoalescedDrainOnNoQuorumExit forces replicateAndCommit to exit without
// reaching two-domain quorum (by stopping all peers in the other consensus
// domains) and uses a same-domain origin so the announce branch — which would
// otherwise drive persistBeforeAnnounce — never fires. With both the announce
// and the two-domain commit paths bypassed, the deferred drain fsync is the
// only barrier that can persist the in-memory state, so its absence would
// strand DomainQuorumIndex updates non-durable.
func TestCoalescedDrainOnNoQuorumExit(t *testing.T) {
	stores := make(map[string]*coalesceCountingStore)
	harness := newCoalesceClusterWithStores(t, true, func(id string) Store {
		s := newCoalesceCountingStore()
		stores[id] = s
		return s
	})
	harness.elect(t)

	harness.nodes["a1"].SetRPCPolicy(RPCPolicy{Deadline: 100 * time.Millisecond, MaxRetries: 1})

	// Stop both other consensus domains so replicateAndCommit cannot observe a
	// second-domain quorum at all.
	for _, id := range []string{"b1", "b2", "b3", "c1", "c2", "c3"} {
		harness.nodes[id].Stop()
	}

	recorder := NewWriteTraceRecorder()
	harness.setAllTrace(recorder)

	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	target := mustNode(t, harness.config, "a1")
	// Same-domain origin: announce path is gated on origin != globalDomain, so
	// using OriginDomain=a guarantees the announce branch never runs and the
	// only persistence opportunity is the deferred drain.
	_, _ = harness.client.Write(ctx, target.ListenAddress, ClientWriteRequest{
		RequestID: "coalesce-drain", OriginDomain: "a",
		Command: Command{Key: "drain", Value: "v"},
	})
	// commitBudget floor is 5s; wait long enough for the goroutine to run its
	// deferred drain after exhausting cross-domain retries.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		drainSeen := false
		for _, event := range recorder.Events() {
			if event.RequestID != "coalesce-drain" {
				continue
			}
			if event.Phase == "server.quorum_progress_persist" && event.Detail == "drain" && event.NodeID == "a1" {
				drainSeen = true
				break
			}
		}
		if drainSeen {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("drain fsync did not fire on GL after replicateAndCommit gave up without two-domain quorum")
}
