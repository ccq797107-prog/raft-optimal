package sim

import (
	"testing"
	"time"

	. "github.com/czq/cd-raft/internal/cdraft"
)

func TestManualClockAndNetworkControls(t *testing.T) {
	start := time.Unix(100, 0)
	clock := NewManualClock(start)
	if got := clock.Advance(5 * time.Second); !got.Equal(start.Add(5 * time.Second)) {
		t.Fatalf("unexpected clock: %v", got)
	}

	network := NewTestNetwork()
	if !network.CanSend("a1", "a2", DomainVoteMessage) {
		t.Fatal("healthy link should deliver")
	}
	network.Drop("a1", "a2", DomainVoteMessage)
	if network.CanSend("a1", "a2", DomainVoteMessage) {
		t.Fatal("dropped message was delivered")
	}
	network.Allow("a1", "a2", DomainVoteMessage)
	network.Stop("a2")
	if network.CanSend("a1", "a2", DomainVoteMessage) {
		t.Fatal("stopped node received a message")
	}
	network.Recover("a2")
	if !network.CanSend("a1", "a2", DomainVoteMessage) {
		t.Fatal("recovered node remained unavailable")
	}
}
