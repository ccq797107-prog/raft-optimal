package sim

// network.go provides the deterministic test infrastructure for the in-memory
// model: a ManualClock and a TestNetwork with per-message-type directed drops.
// Both are test-only and used exclusively by the sim Cluster (production uses
// real timers and gRPC).

import (
	"sync"
	"time"

	. "github.com/czq/cd-raft/internal/cdraft"
)

type ManualClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewManualClock(start time.Time) *ManualClock {
	return &ManualClock{now: start}
}

func (c *ManualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *ManualClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

type link struct {
	from string
	to   string
	kind MessageKind
}

type TestNetwork struct {
	mu      sync.RWMutex
	stopped map[string]bool
	dropped map[link]bool
}

func NewTestNetwork() *TestNetwork {
	return &TestNetwork{
		stopped: make(map[string]bool),
		dropped: make(map[link]bool),
	}
}

func (n *TestNetwork) CanSend(from, to string, kind MessageKind) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return !n.stopped[from] && !n.stopped[to] && !n.dropped[link{from: from, to: to, kind: kind}]
}

func (n *TestNetwork) Stop(node string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopped[node] = true
}

func (n *TestNetwork) Recover(node string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.stopped, node)
}

func (n *TestNetwork) IsStopped(node string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.stopped[node]
}

func (n *TestNetwork) Drop(from, to string, kind MessageKind) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropped[link{from: from, to: to, kind: kind}] = true
}

func (n *TestNetwork) Allow(from, to string, kind MessageKind) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.dropped, link{from: from, to: to, kind: kind})
}

func (n *TestNetwork) Partition(left, right []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	kinds := []MessageKind{
		DomainVoteMessage, DomainHeartbeatMessage, DomainReadyMessage,
		GlobalVoteMessage, GlobalHeartbeatMessage, ReplicateEntryMessage,
		DomainQuorumAckMessage, GlobalDomainQuorumAckMessage, CommitNoticeMessage,
	}
	for _, a := range left {
		for _, b := range right {
			for _, kind := range kinds {
				n.dropped[link{from: a, to: b, kind: kind}] = true
				n.dropped[link{from: b, to: a, kind: kind}] = true
			}
		}
	}
}

func (n *TestNetwork) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropped = make(map[link]bool)
}
