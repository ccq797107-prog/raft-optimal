package cdraft

// This file holds deterministic consensus state transitions shared by the
// in-process Cluster harness and the real RPCNode runtime. Callers still own
// transport, timing, reachability, persistence, and lease bookkeeping.

func transitionStartDomainCampaign(state *PersistentState, domainRole *Role, candidateID string) (uint64, LogSummary) {
	*domainRole = Candidate
	state.DomainTerm++
	state.DomainVotedFor = candidateID
	state.DomainLeader = DomainLeaderIdentity{}
	return state.DomainTerm, state.Summary()
}

func transitionObserveHigherDomainTerm(state *PersistentState, domainRole *Role, term uint64) bool {
	if term <= state.DomainTerm {
		return false
	}
	state.DomainTerm = term
	state.DomainVotedFor = ""
	state.DomainLeader = DomainLeaderIdentity{}
	*domainRole = Follower
	return true
}

func transitionDomainVote(state *PersistentState, domainRole *Role, request DomainVoteRequest) (DomainVoteResponse, bool) {
	if request.DomainTerm < state.DomainTerm {
		return DomainVoteResponse{DomainTerm: state.DomainTerm}, false
	}
	if request.DomainTerm > state.DomainTerm {
		transitionObserveHigherDomainTerm(state, domainRole, request.DomainTerm)
	}
	granted := (state.DomainVotedFor == "" || state.DomainVotedFor == request.Candidate) &&
		request.Log.AtLeast(state.Summary())
	if granted {
		state.DomainVotedFor = request.Candidate
	}
	return DomainVoteResponse{DomainTerm: state.DomainTerm, Granted: granted}, granted
}

func transitionAcceptDomainLeader(state *PersistentState, domainRole *Role, localNodeID string, identity DomainLeaderIdentity) bool {
	if identity.DomainTerm < state.DomainTerm {
		return false
	}
	if identity.DomainTerm > state.DomainTerm {
		transitionObserveHigherDomainTerm(state, domainRole, identity.DomainTerm)
	}
	state.DomainLeader = identity
	if identity.NodeID != localNodeID {
		*domainRole = Follower
	}
	return true
}

func transitionBecomeDomainLeader(state *PersistentState, domainRole *Role, identity DomainLeaderIdentity) {
	*domainRole = Leader
	state.DomainTerm = max(state.DomainTerm, identity.DomainTerm)
	state.DomainLeader = identity
}

func transitionStartGlobalCampaign(state *PersistentState, globalRole *Role, candidate DomainLeaderIdentity) (uint64, LogSummary) {
	*globalRole = Candidate
	state.GlobalTerm++
	state.GlobalVotedFor = candidate
	state.GlobalLeader = GlobalLeaderIdentity{}
	return state.GlobalTerm, state.Summary()
}

func transitionObserveHigherGlobalTerm(state *PersistentState, globalRole *Role, stage *Stage, term uint64) bool {
	if term <= state.GlobalTerm {
		return false
	}
	state.GlobalTerm = term
	state.GlobalVotedFor = DomainLeaderIdentity{}
	state.GlobalLeader = GlobalLeaderIdentity{}
	*globalRole = Follower
	if *stage == Serving {
		*stage = GlobalElecting
	}
	return true
}

func transitionGlobalVote(state *PersistentState, globalRole *Role, stage *Stage, request GlobalVoteRequest) (GlobalVoteResponse, bool, bool) {
	if request.GlobalTerm < state.GlobalTerm {
		return GlobalVoteResponse{GlobalTerm: state.GlobalTerm}, false, false
	}
	higherTerm := false
	if request.GlobalTerm > state.GlobalTerm {
		transitionObserveHigherGlobalTerm(state, globalRole, stage, request.GlobalTerm)
		higherTerm = true
	}
	granted := (!state.GlobalVotedFor.Valid() || state.GlobalVotedFor == request.Candidate) &&
		request.Log.AtLeast(state.Summary())
	if granted {
		state.GlobalVotedFor = request.Candidate
	}
	return GlobalVoteResponse{GlobalTerm: state.GlobalTerm, Granted: granted}, granted, higherTerm
}

func transitionAcceptGlobalLeader(state *PersistentState, globalRole *Role, stage *Stage, localNodeID string, identity GlobalLeaderIdentity) bool {
	if identity.GlobalTerm < state.GlobalTerm {
		return false
	}
	if identity.GlobalTerm > state.GlobalTerm {
		transitionObserveHigherGlobalTerm(state, globalRole, stage, identity.GlobalTerm)
	}
	state.GlobalLeader = identity
	if identity.DomainLeader.NodeID != localNodeID {
		*globalRole = Follower
	}
	*stage = Serving
	return true
}

func transitionBecomeGlobalLeader(state *PersistentState, globalRole *Role, stage *Stage, identity GlobalLeaderIdentity) {
	state.GlobalTerm = max(state.GlobalTerm, identity.GlobalTerm)
	state.GlobalLeader = identity
	*globalRole = Leader
	*stage = Serving
}

func transitionRevokeGlobalLeadership(state *PersistentState, globalRole *Role, stage *Stage) {
	state.GlobalLeader = GlobalLeaderIdentity{}
	*globalRole = Follower
	if *stage == Serving {
		*stage = GlobalElecting
	}
}
