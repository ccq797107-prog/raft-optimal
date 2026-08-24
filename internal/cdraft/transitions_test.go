package cdraft

import "testing"

func TestTransitionDomainVoteObservesHigherTerm(t *testing.T) {
	oldLeader := DomainLeaderIdentity{DomainID: "a", NodeID: "a1", DomainTerm: 3}
	state := PersistentState{
		DomainTerm:     3,
		DomainVotedFor: "a1",
		DomainLeader:   oldLeader,
		Log:            []LogEntry{{GlobalTerm: 4, GlobalIndex: 8}},
	}
	role := Leader

	response, resetDeadline := transitionDomainVote(&state, &role, DomainVoteRequest{
		DomainID: "a", Candidate: "a2", DomainTerm: 4,
		Log: LogSummary{LastGlobalTerm: 4, LastGlobalIndex: 8},
	})

	if !response.Granted || !resetDeadline || response.DomainTerm != 4 {
		t.Fatalf("higher-term eligible vote was not granted: response=%+v reset=%t", response, resetDeadline)
	}
	if state.DomainVotedFor != "a2" || state.DomainLeader.Valid() || role != Follower {
		t.Fatalf("higher domain term did not fence prior state: state=%+v role=%s", state, role)
	}
}

func TestTransitionGlobalVoteObservesHigherTerm(t *testing.T) {
	oldCandidate := DomainLeaderIdentity{DomainID: "a", NodeID: "a1", DomainTerm: 2}
	newCandidate := DomainLeaderIdentity{DomainID: "b", NodeID: "b1", DomainTerm: 3}
	state := PersistentState{
		GlobalTerm:     5,
		GlobalVotedFor: oldCandidate,
		GlobalLeader:   GlobalLeaderIdentity{DomainLeader: oldCandidate, GlobalTerm: 5},
	}
	role := Leader
	stage := Serving

	response, resetDeadline, higherTerm := transitionGlobalVote(&state, &role, &stage, GlobalVoteRequest{
		Candidate: newCandidate, GlobalTerm: 6,
	})

	if !response.Granted || !resetDeadline || !higherTerm || response.GlobalTerm != 6 {
		t.Fatalf("higher-term eligible vote was not granted: response=%+v reset=%t higher=%t", response, resetDeadline, higherTerm)
	}
	if state.GlobalVotedFor != newCandidate || state.GlobalLeader.Valid() || role != Follower || stage != GlobalElecting {
		t.Fatalf("higher global term did not fence prior state: state=%+v role=%s stage=%s", state, role, stage)
	}
}

func TestTransitionLeaderAcceptanceRejectsStaleAndClearsHigherTermVote(t *testing.T) {
	t.Run("domain", func(t *testing.T) {
		state := PersistentState{DomainTerm: 4, DomainVotedFor: "a2"}
		role := Candidate
		stale := DomainLeaderIdentity{DomainID: "a", NodeID: "a1", DomainTerm: 3}
		if transitionAcceptDomainLeader(&state, &role, "a2", stale) {
			t.Fatal("accepted stale domain leader")
		}
		current := DomainLeaderIdentity{DomainID: "a", NodeID: "a1", DomainTerm: 5}
		if !transitionAcceptDomainLeader(&state, &role, "a2", current) {
			t.Fatal("rejected higher-term domain leader")
		}
		if state.DomainVotedFor != "" || state.DomainLeader != current || role != Follower {
			t.Fatalf("higher-term domain leader left stale state: state=%+v role=%s", state, role)
		}
	})

	t.Run("global", func(t *testing.T) {
		oldCandidate := DomainLeaderIdentity{DomainID: "a", NodeID: "a1", DomainTerm: 2}
		state := PersistentState{GlobalTerm: 4, GlobalVotedFor: oldCandidate}
		role := Candidate
		stage := GlobalElecting
		stale := GlobalLeaderIdentity{DomainLeader: oldCandidate, GlobalTerm: 3}
		if transitionAcceptGlobalLeader(&state, &role, &stage, "b1", stale) {
			t.Fatal("accepted stale global leader")
		}
		current := GlobalLeaderIdentity{
			DomainLeader: DomainLeaderIdentity{DomainID: "c", NodeID: "c1", DomainTerm: 3},
			GlobalTerm:   5,
		}
		if !transitionAcceptGlobalLeader(&state, &role, &stage, "b1", current) {
			t.Fatal("rejected higher-term global leader")
		}
		if state.GlobalVotedFor.Valid() || state.GlobalLeader != current || role != Follower || stage != Serving {
			t.Fatalf("higher-term global leader left stale state: state=%+v role=%s stage=%s", state, role, stage)
		}
	})
}

func TestTransitionRevokeGlobalLeadership(t *testing.T) {
	leader := GlobalLeaderIdentity{
		DomainLeader: DomainLeaderIdentity{DomainID: "a", NodeID: "a1", DomainTerm: 2},
		GlobalTerm:   4,
	}
	state := PersistentState{GlobalTerm: 4, GlobalLeader: leader}
	role := Leader
	stage := Serving

	transitionRevokeGlobalLeadership(&state, &role, &stage)

	if state.GlobalLeader.Valid() || state.GlobalTerm != 4 || role != Follower || stage != GlobalElecting {
		t.Fatalf("global leadership was not revoked without changing term: state=%+v role=%s stage=%s", state, role, stage)
	}
}
