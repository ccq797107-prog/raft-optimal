package sim

import (
	"fmt"
	"testing"
)

// TestCompactionPreservesServiceAndIdempotency drives the deterministic model
// through writes, compaction at the Global Leader, and continued service:
// committed data stays readable, retries of compacted requests stay idempotent,
// and new writes keep committing across the snapshot cut.
func TestCompactionPreservesServiceAndIdempotency(t *testing.T) {
	cluster := startTestCluster(t)
	global := currentGlobalNode(t, cluster)

	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("r%d", i)
		outcome := cluster.Write(global, writeRequest(id, "b", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
		if outcome.NormalErr != nil || outcome.Normal == nil || !outcome.Normal.Committed {
			t.Fatalf("write %d failed: %+v", i, outcome)
		}
	}

	compacted, err := cluster.Compact(global, 1) // keep one applied entry of headroom
	if err != nil || !compacted {
		t.Fatalf("compaction failed: compacted=%v err=%v", compacted, err)
	}
	snap, _ := cluster.Snapshot(global)
	if snap.State.SnapshotLastIndex != 4 || len(snap.State.Log) != 1 {
		t.Fatalf("unexpected compaction point: snapshot=%d log=%+v", snap.State.SnapshotLastIndex, snap.State.Log)
	}

	// Committed values stay readable behind the read barrier.
	for i := 1; i <= 5; i++ {
		value, err := cluster.Read(global, fmt.Sprintf("k%d", i))
		if err != nil || value != fmt.Sprintf("v%d", i) {
			t.Fatalf("compacted value unreadable: k%d=%q err=%v", i, value, err)
		}
	}

	// Retrying a request whose entry was compacted must return the settled
	// result, not a "requestId reused" error.
	retry := cluster.Write(global, writeRequest("r2", "b", "k2", "v2"))
	if retry.NormalErr != nil || retry.Normal == nil || retry.Normal.Result != "k2=v2" {
		t.Fatalf("compacted request retry broke idempotency: %+v", retry)
	}
	// A reused requestId with a different command must still be rejected.
	if abuse := cluster.Write(global, writeRequest("r2", "b", "k2", "DIFFERENT")); abuse.NormalErr == nil {
		t.Fatalf("requestId reuse with different command accepted after compaction: %+v", abuse)
	}

	// New writes keep committing across the snapshot cut.
	outcome := cluster.Write(global, writeRequest("r6", "c", "k6", "v6"))
	if outcome.NormalErr != nil || outcome.Normal == nil || outcome.Normal.GlobalIndex != 6 {
		t.Fatalf("post-compaction write failed: %+v", outcome)
	}
	if err := cluster.AssertCommitInvariants(); err != nil {
		t.Fatal(err)
	}
	if err := cluster.AssertLeaderUniqueness(); err != nil {
		t.Fatal(err)
	}
}

// TestInstallSnapshotHealsDomainBehindCompactionFloor stops a whole domain,
// advances and compacts the log past everything that domain holds, then heals
// it with the snapshot path: after InstallSnapshotFromGlobal the domain's
// quorum mark covers the boundary and subsequent replication appends cleanly.
func TestInstallSnapshotHealsDomainBehindCompactionFloor(t *testing.T) {
	cluster := startTestCluster(t)
	global := currentGlobalNode(t, cluster)
	globalIdentity, _ := cluster.GlobalLeader()
	globalDomain := globalIdentity.DomainLeader.DomainID

	// Pick a non-global domain to lag behind.
	var lagging string
	for _, domain := range cluster.Config().Domains() {
		if domain != globalDomain {
			lagging = domain
			break
		}
	}
	for _, member := range cluster.Config().DomainMembers(lagging) {
		cluster.StopNode(member.ID)
	}

	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("w%d", i)
		outcome := cluster.Write(global, writeRequest(id, globalDomain, fmt.Sprintf("x%d", i), "v"))
		if outcome.NormalErr != nil {
			t.Fatalf("write with one lagging domain failed: %+v", outcome)
		}
	}

	// Compact everything applied away: the lagging domain now sits below the floor.
	if compacted, err := cluster.Compact(global, 0); err != nil || !compacted {
		t.Fatalf("compaction failed: %v", err)
	}
	floor, _ := cluster.Snapshot(global)
	if floor.State.SnapshotLastIndex == 0 {
		t.Fatal("compaction did not move the floor")
	}

	// Heal the network, then ship the snapshot (entries are gone from the log).
	for _, member := range cluster.Config().DomainMembers(lagging) {
		cluster.Network().Recover(member.ID)
	}
	healed, err := cluster.InstallSnapshotFromGlobal(lagging)
	if err != nil || !healed {
		t.Fatalf("snapshot heal failed: healed=%v err=%v", healed, err)
	}

	for _, member := range cluster.Config().DomainMembers(lagging) {
		snap, _ := cluster.Snapshot(member.ID)
		if snap.State.AppliedIndex < floor.State.SnapshotLastIndex {
			t.Fatalf("node %s not healed by snapshot: %+v", member.ID, snap.State)
		}
		for i := 1; i <= 4; i++ {
			if snap.State.StateMachine[fmt.Sprintf("x%d", i)] != "v" {
				t.Fatalf("node %s state machine incomplete: %+v", member.ID, snap.State.StateMachine)
			}
		}
	}

	// The healed domain participates in replication again.
	outcome := cluster.Write(global, writeRequest("after-heal", lagging, "y", "z"))
	if outcome.NormalErr != nil || outcome.Normal == nil {
		t.Fatalf("post-heal write failed: %+v", outcome)
	}
	healedLeader, _ := cluster.DomainLeader(lagging)
	leaderSnap, _ := cluster.Snapshot(healedLeader.NodeID)
	if leaderSnap.State.DomainQuorumIndex[lagging] < outcome.Normal.GlobalIndex {
		t.Fatalf("healed domain did not reach quorum at the new entry: %+v", leaderSnap.State.DomainQuorumIndex)
	}
	if err := cluster.AssertCommitInvariants(); err != nil {
		t.Fatal(err)
	}
}
