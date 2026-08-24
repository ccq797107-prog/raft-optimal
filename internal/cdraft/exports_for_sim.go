package cdraft

// exports_for_sim.go re-exports the internal consensus primitives that the
// sibling deterministic test-model package (internal/cdraft/sim) needs to reuse
// instead of duplicating. PRODUCTION code (rpc_node.go, telemetry.go, ...) keeps
// calling the unexported originals directly and is unaffected by this file.
//
// These are deliberately thin aliases: there is exactly one implementation of
// each protocol primitive (in transitions.go / log.go / types.go / store.go),
// shared verbatim by the production RPCNode and the in-memory Cluster model, so
// the two engines can never diverge on the per-entry / per-vote rules.
//
// Do NOT use these names from production code; use the unexported originals.

// State-machine transitions (see transitions.go).
var (
	TransitionStartDomainCampaign     = transitionStartDomainCampaign
	TransitionObserveHigherDomainTerm = transitionObserveHigherDomainTerm
	TransitionDomainVote              = transitionDomainVote
	TransitionAcceptDomainLeader      = transitionAcceptDomainLeader
	TransitionBecomeDomainLeader      = transitionBecomeDomainLeader
	TransitionStartGlobalCampaign     = transitionStartGlobalCampaign
	TransitionObserveHigherGlobalTerm = transitionObserveHigherGlobalTerm
	TransitionGlobalVote              = transitionGlobalVote
	TransitionAcceptGlobalLeader      = transitionAcceptGlobalLeader
	TransitionBecomeGlobalLeader      = transitionBecomeGlobalLeader
	TransitionRevokeGlobalLeadership  = transitionRevokeGlobalLeadership
)

// Quorum thresholds (see types.go).
var (
	Majority                = majority
	GlobalElectionThreshold = globalElectionThreshold
)

// Log / state primitives (see log.go).
var (
	AppendEntry          = appendEntry
	FindEntryByIndex     = findEntryByIndex
	FindEntryByRequest   = findEntryByRequest
	HasEntry             = hasEntry
	ApplyEntry           = applyEntry
	CompactLog           = compactLog
	InstallSnapshotState = installSnapshotState
)

// CommandResult computes the deterministic per-command result string (see
// types.go).
var CommandResult = commandResult

// InitMaps initializes the nil maps of a freshly loaded PersistentState. It
// wraps the unexported PersistentState.initMaps method so the sim package can
// call it across the package boundary.
func InitMaps(state *PersistentState) { state.initMaps() }
