// Package sim is the deterministic, in-memory TEST MODEL of CD-Raft. Its
// single-goroutine Cluster drives two-tier election and replication over a
// synchronous TestNetwork (per-message-type directed drops) and a ManualClock.
// It is a SECOND implementation of the protocol used ONLY by unit tests;
// production traffic never touches it — that path is cdraft.RPCNode.
//
// Why it exists alongside RPCNode: being synchronous, clock-driven, and free of
// real gRPC lets the safety properties (one leader per term, N-1 quorum,
// fencing, commit/Fast-Return rules) be asserted deterministically with
// fine-grained, reproducible fault injection that the real async node cannot
// offer. The model REUSES the exact same protocol primitives as production via
// the parent cdraft package (state-machine transitions, log/quorum rules), so
// the two engines can never diverge on the per-entry / per-vote rules; only the
// orchestration differs.
//
// model.go holds the engine (election + identity/quorum checks); the data path
// (Write/Read/replicate/commit) lives in data.go.
package sim

// The parent package is dot-imported so this model can reference the shared
// protocol types and primitives (PersistentState, transitions, log rules)
// without a cdraft. prefix on every identifier; it is a tightly-coupled,
// test-only sibling of cdraft.
import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/czq/cd-raft/internal/topology"

	. "github.com/czq/cd-raft/internal/cdraft"
)

type Node struct {
	ID               string
	DomainCode       string
	Stage            Stage
	DomainRole       Role
	GlobalRole       Role
	ElectionDeadline time.Time
	State            PersistentState
	store            Store
}

type NodeSnapshot struct {
	ID               string
	DomainCode       string
	Stage            Stage
	DomainRole       Role
	GlobalRole       Role
	ElectionDeadline time.Time
	State            PersistentState
}

type Cluster struct {
	mu            sync.Mutex
	config        topology.Config
	nodes         map[string]*Node
	network       *TestNetwork
	clock         *ManualClock
	rng           *rand.Rand
	rpcPolicy     RPCPolicy
	domainLeaders map[string]DomainLeaderIdentity
	globalLeader  GlobalLeaderIdentity
}

func NewCluster(config topology.Config, stores map[string]Store, network *TestNetwork, clock *ManualClock) (*Cluster, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if network == nil {
		network = NewTestNetwork()
	}
	if clock == nil {
		clock = NewManualClock(time.Unix(0, 0))
	}
	cluster := &Cluster{
		config:        config,
		nodes:         make(map[string]*Node, len(config.Nodes)),
		network:       network,
		clock:         clock,
		rng:           rand.New(rand.NewSource(1)),
		rpcPolicy:     DefaultRPCPolicy(),
		domainLeaders: make(map[string]DomainLeaderIdentity),
	}
	for _, configured := range config.Nodes {
		store := stores[configured.ID]
		if store == nil {
			store = NewMemoryStore()
		}
		state, err := store.Load()
		if err != nil {
			return nil, fmt.Errorf("load node %s: %w", configured.ID, err)
		}
		InitMaps(&state)
		cluster.nodes[configured.ID] = &Node{
			ID:         configured.ID,
			DomainCode: configured.DomainCode,
			Stage:      Booting,
			DomainRole: Follower,
			GlobalRole: Follower,
			State:      state,
			store:      store,
		}
	}
	return cluster, nil
}

func (c *Cluster) RPCPolicy() RPCPolicy {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rpcPolicy
}

func (c *Cluster) Config() topology.Config {
	return c.config
}

func (c *Cluster) Network() *TestNetwork {
	return c.network
}

func (c *Cluster) Clock() *ManualClock {
	return c.clock
}

func (c *Cluster) SetFastReturnEnabled(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.Features.FastReturnEnabled = enabled
}

func (c *Cluster) Snapshot(nodeID string) (NodeSnapshot, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.nodes[nodeID]
	if !ok {
		return NodeSnapshot{}, false
	}
	return snapshot(node), true
}

func snapshot(node *Node) NodeSnapshot {
	return NodeSnapshot{
		ID:               node.ID,
		DomainCode:       node.DomainCode,
		Stage:            node.Stage,
		DomainRole:       node.DomainRole,
		GlobalRole:       node.GlobalRole,
		ElectionDeadline: node.ElectionDeadline,
		State:            node.State.Clone(),
	}
}

func (c *Cluster) DomainLeader(domain string) (DomainLeaderIdentity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	identity, ok := c.domainLeaders[domain]
	return identity, ok && c.domainIdentityCurrentLocked(identity)
}

func (c *Cluster) GlobalLeader() (GlobalLeaderIdentity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.globalLeader, c.globalIdentityCurrentLocked(c.globalLeader)
}

func (c *Cluster) StartAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, node := range c.nodes {
		node.Stage = DomainElecting
		c.resetElectionDeadlineLocked(node)
	}
	for _, domain := range c.config.Domains() {
		candidate, ok := c.bestAvailableDomainCandidateLocked(domain)
		if !ok {
			return fmt.Errorf("%w: %s", ErrNoDomainQuorum, domain)
		}
		if _, err := c.electDomainLeaderLocked(domain, candidate); err != nil {
			return err
		}
	}
	candidate, ok := c.bestAvailableGlobalCandidateLocked()
	if !ok {
		return ErrNoGlobalQuorum
	}
	_, err := c.electGlobalLeaderLocked(candidate)
	return err
}

func (c *Cluster) StartNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %q", nodeID)
	}
	if c.network.IsStopped(nodeID) {
		return ErrNodeStopped
	}
	node.Stage = DomainElecting
	node.DomainRole = Follower
	node.GlobalRole = Follower
	c.resetElectionDeadlineLocked(node)
	return c.persistLocked(node)
}

func (c *Cluster) RestartNode(nodeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %q", nodeID)
	}
	state, err := node.store.Load()
	if err != nil {
		return err
	}
	InitMaps(&state)
	node.State = state
	node.Stage = Booting
	node.DomainRole = Follower
	node.GlobalRole = Follower
	c.network.Recover(nodeID)
	node.Stage = DomainElecting
	c.resetElectionDeadlineLocked(node)
	return nil
}

func (c *Cluster) StopNode(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.network.Stop(nodeID)
}

func (c *Cluster) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock.Advance(d)
}

func (c *Cluster) ElectionDue(nodeID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.nodes[nodeID]
	return ok && !c.clock.Now().Before(node.ElectionDeadline)
}

func (c *Cluster) resetElectionDeadlineLocked(node *Node) {
	// Randomized but deterministic under the manual test clock.
	jitter := time.Duration(150+c.rng.Intn(151)) * time.Millisecond
	node.ElectionDeadline = c.clock.Now().Add(jitter)
}

func (c *Cluster) ElectDomainLeader(domain, candidateID string) (DomainLeaderIdentity, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.electDomainLeaderLocked(domain, candidateID)
}

func (c *Cluster) electDomainLeaderLocked(domain, candidateID string) (DomainLeaderIdentity, error) {
	candidate, ok := c.nodes[candidateID]
	if !ok || candidate.DomainCode != domain {
		return DomainLeaderIdentity{}, fmt.Errorf("candidate %q is not in domain %q", candidateID, domain)
	}
	if c.network.IsStopped(candidateID) {
		return DomainLeaderIdentity{}, ErrNodeStopped
	}
	if candidate.Stage == Booting {
		return DomainLeaderIdentity{}, ErrNotServing
	}
	candidate.Stage = DomainElecting
	term, summary := TransitionStartDomainCampaign(&candidate.State, &candidate.DomainRole, candidate.ID)
	c.resetElectionDeadlineLocked(candidate)
	if err := c.persistLocked(candidate); err != nil {
		return DomainLeaderIdentity{}, err
	}

	request := DomainVoteRequest{
		DomainID:   domain,
		Candidate:  candidate.ID,
		DomainTerm: term,
		Log:        summary,
	}
	votes := 1
	for _, member := range c.config.DomainMembers(domain) {
		if member.ID == candidateID || !c.network.CanSend(candidateID, member.ID, DomainVoteMessage) {
			continue
		}
		response := c.handleDomainVoteLocked(member.ID, request)
		if response.DomainTerm > candidate.State.DomainTerm {
			TransitionObserveHigherDomainTerm(&candidate.State, &candidate.DomainRole, response.DomainTerm)
			_ = c.persistLocked(candidate)
			return DomainLeaderIdentity{}, ErrNoDomainQuorum
		}
		if response.Granted {
			votes++
		}
	}
	if votes < Majority(len(c.config.DomainMembers(domain))) {
		return DomainLeaderIdentity{}, ErrNoDomainQuorum
	}

	identity := DomainLeaderIdentity{DomainID: domain, NodeID: candidateID, DomainTerm: candidate.State.DomainTerm}
	previous := c.domainLeaders[domain]
	c.domainLeaders[domain] = identity
	for _, member := range c.config.DomainMembers(domain) {
		node := c.nodes[member.ID]
		if !c.network.CanSend(candidateID, node.ID, DomainReadyMessage) {
			continue
		}
		if !TransitionAcceptDomainLeader(&node.State, &node.DomainRole, node.ID, identity) {
			continue
		}
		node.Stage = GlobalElecting
		c.resetElectionDeadlineLocked(node)
		_ = c.persistLocked(node)
	}
	TransitionBecomeDomainLeader(&candidate.State, &candidate.DomainRole, identity)
	candidate.Stage = GlobalElecting
	_ = c.persistLocked(candidate)

	if previous.Valid() && previous != identity {
		c.invalidateGlobalLeaderLocked()
	}
	return identity, nil
}

func (c *Cluster) HandleDomainVote(voterID string, request DomainVoteRequest) DomainVoteResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.handleDomainVoteLocked(voterID, request)
}

func (c *Cluster) handleDomainVoteLocked(voterID string, request DomainVoteRequest) DomainVoteResponse {
	voter, ok := c.nodes[voterID]
	if !ok || c.network.IsStopped(voterID) || voter.Stage == Booting || voter.DomainCode != request.DomainID {
		return DomainVoteResponse{}
	}
	if request.DomainTerm < voter.State.DomainTerm {
		return DomainVoteResponse{DomainTerm: voter.State.DomainTerm}
	}
	response, resetDeadline := TransitionDomainVote(&voter.State, &voter.DomainRole, request)
	if resetDeadline {
		c.resetElectionDeadlineLocked(voter)
	}
	_ = c.persistLocked(voter)
	return response
}

func (c *Cluster) SendDomainHeartbeat(identity DomainLeaderIdentity) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.domainIdentityCurrentLocked(identity) {
		return false
	}
	delivered := 1
	for _, member := range c.config.DomainMembers(identity.DomainID) {
		if member.ID == identity.NodeID {
			continue
		}
		if c.network.CanSend(identity.NodeID, member.ID, DomainHeartbeatMessage) {
			node := c.nodes[member.ID]
			if TransitionAcceptDomainLeader(&node.State, &node.DomainRole, node.ID, identity) {
				c.resetElectionDeadlineLocked(node)
				_ = c.persistLocked(node)
				delivered++
			}
		}
	}
	return delivered >= Majority(len(c.config.DomainMembers(identity.DomainID)))
}

func (c *Cluster) ElectGlobalLeader(candidateID string) (GlobalLeaderIdentity, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.electGlobalLeaderLocked(candidateID)
}

func (c *Cluster) electGlobalLeaderLocked(candidateID string) (GlobalLeaderIdentity, error) {
	candidate, ok := c.nodes[candidateID]
	if !ok || c.network.IsStopped(candidateID) {
		return GlobalLeaderIdentity{}, ErrNodeStopped
	}
	if candidate.Stage == Booting {
		return GlobalLeaderIdentity{}, ErrNotServing
	}
	candidateIdentity, ok := c.domainLeaders[candidate.DomainCode]
	if !ok || candidateIdentity.NodeID != candidateID || !c.domainIdentityCurrentLocked(candidateIdentity) {
		return GlobalLeaderIdentity{}, ErrNotDomainLeader
	}
	candidate.Stage = GlobalElecting
	term, summary := TransitionStartGlobalCampaign(&candidate.State, &candidate.GlobalRole, candidateIdentity)
	if c.globalLeader.Valid() && term > c.globalLeader.GlobalTerm {
		c.invalidateGlobalLeaderLocked()
	}
	if err := c.persistLocked(candidate); err != nil {
		return GlobalLeaderIdentity{}, err
	}

	request := GlobalVoteRequest{
		Candidate:  candidateIdentity,
		GlobalTerm: term,
		Log:        summary,
	}
	votes := 1
	for _, domain := range c.config.Domains() {
		voterIdentity, ok := c.domainLeaders[domain]
		if !ok || voterIdentity.NodeID == candidateID || !c.domainIdentityCurrentLocked(voterIdentity) {
			continue
		}
		if !c.network.CanSend(candidateID, voterIdentity.NodeID, GlobalVoteMessage) {
			continue
		}
		response := c.handleGlobalVoteLocked(voterIdentity.NodeID, request)
		if response.GlobalTerm > candidate.State.GlobalTerm {
			TransitionObserveHigherGlobalTerm(&candidate.State, &candidate.GlobalRole, &candidate.Stage, response.GlobalTerm)
			_ = c.persistLocked(candidate)
			return GlobalLeaderIdentity{}, ErrNoGlobalQuorum
		}
		if response.Granted {
			votes++
		}
	}
	if votes < GlobalElectionThreshold(len(c.config.Domains())) {
		return GlobalLeaderIdentity{}, ErrNoGlobalQuorum
	}

	identity := GlobalLeaderIdentity{DomainLeader: candidateIdentity, GlobalTerm: candidate.State.GlobalTerm}
	c.globalLeader = identity
	for _, node := range c.nodes {
		if node.Stage == Booting {
			continue
		}
		if !c.network.CanSend(candidateID, node.ID, GlobalHeartbeatMessage) {
			continue
		}
		TransitionAcceptGlobalLeader(&node.State, &node.GlobalRole, &node.Stage, node.ID, identity)
		_ = c.persistLocked(node)
	}
	TransitionBecomeGlobalLeader(&candidate.State, &candidate.GlobalRole, &candidate.Stage, identity)
	_ = c.persistLocked(candidate)
	return identity, nil
}

func (c *Cluster) HandleGlobalVote(voterID string, request GlobalVoteRequest) GlobalVoteResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.handleGlobalVoteLocked(voterID, request)
}

func (c *Cluster) handleGlobalVoteLocked(voterID string, request GlobalVoteRequest) GlobalVoteResponse {
	voter, ok := c.nodes[voterID]
	if !ok || c.network.IsStopped(voterID) || voter.Stage == Booting {
		return GlobalVoteResponse{}
	}
	voterIdentity, ok := c.domainLeaders[voter.DomainCode]
	if !ok || voterIdentity.NodeID != voterID || !c.domainIdentityCurrentLocked(voterIdentity) {
		return GlobalVoteResponse{GlobalTerm: voter.State.GlobalTerm}
	}
	if !c.domainIdentityCurrentLocked(request.Candidate) {
		return GlobalVoteResponse{Voter: voterIdentity, GlobalTerm: voter.State.GlobalTerm}
	}
	if request.GlobalTerm < voter.State.GlobalTerm {
		return GlobalVoteResponse{Voter: voterIdentity, GlobalTerm: voter.State.GlobalTerm}
	}
	response, _, _ := TransitionGlobalVote(&voter.State, &voter.GlobalRole, &voter.Stage, request)
	response.Voter = voterIdentity
	_ = c.persistLocked(voter)
	return response
}

func (c *Cluster) SendGlobalHeartbeat(identity GlobalLeaderIdentity) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.globalIdentityCurrentLocked(identity) {
		return false
	}
	domainLeaderAcks := 0
	for _, domain := range c.config.Domains() {
		domainLeader, ok := c.domainLeaders[domain]
		if !ok || !c.domainIdentityCurrentLocked(domainLeader) {
			continue
		}
		if c.network.CanSend(identity.DomainLeader.NodeID, domainLeader.NodeID, GlobalHeartbeatMessage) {
			domainLeaderAcks++
		}
	}
	if domainLeaderAcks < GlobalElectionThreshold(len(c.config.Domains())) {
		return false
	}
	for _, node := range c.nodes {
		if node.Stage == Booting {
			continue
		}
		if c.network.CanSend(identity.DomainLeader.NodeID, node.ID, GlobalHeartbeatMessage) {
			TransitionAcceptGlobalLeader(&node.State, &node.GlobalRole, &node.Stage, node.ID, identity)
			_ = c.persistLocked(node)
		}
	}
	return true
}

func (c *Cluster) DetectAndReelect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, domain := range c.config.Domains() {
		identity, ok := c.domainLeaders[domain]
		if ok && c.domainIdentityHasQuorumLocked(identity) {
			continue
		}
		candidate, found := c.bestAvailableDomainCandidateLocked(domain)
		if !found {
			delete(c.domainLeaders, domain)
			continue
		}
		if _, err := c.electDomainLeaderLocked(domain, candidate); err != nil {
			delete(c.domainLeaders, domain)
		}
	}
	if c.globalIdentityHasElectionQuorumLocked(c.globalLeader) {
		return nil
	}
	c.invalidateGlobalLeaderLocked()
	candidate, ok := c.bestAvailableGlobalCandidateLocked()
	if !ok {
		return ErrNoGlobalQuorum
	}
	_, err := c.electGlobalLeaderLocked(candidate)
	return err
}

func (c *Cluster) domainIdentityCurrentLocked(identity DomainLeaderIdentity) bool {
	if !identity.Valid() {
		return false
	}
	current, ok := c.domainLeaders[identity.DomainID]
	if !ok || current != identity || c.network.IsStopped(identity.NodeID) {
		return false
	}
	node := c.nodes[identity.NodeID]
	return node != nil && node.State.DomainTerm == identity.DomainTerm && node.DomainRole == Leader
}

func (c *Cluster) globalIdentityCurrentLocked(identity GlobalLeaderIdentity) bool {
	if !identity.Valid() || c.globalLeader != identity ||
		!c.domainIdentityCurrentLocked(identity.DomainLeader) ||
		c.network.IsStopped(identity.DomainLeader.NodeID) {
		return false
	}
	node := c.nodes[identity.DomainLeader.NodeID]
	return node != nil && node.GlobalRole == Leader &&
		node.State.GlobalTerm == identity.GlobalTerm && node.State.GlobalLeader == identity
}

func (c *Cluster) domainIdentityHasQuorumLocked(identity DomainLeaderIdentity) bool {
	if !c.domainIdentityCurrentLocked(identity) {
		return false
	}
	acks := 1
	for _, member := range c.config.DomainMembers(identity.DomainID) {
		if member.ID != identity.NodeID && c.network.CanSend(identity.NodeID, member.ID, DomainHeartbeatMessage) {
			acks++
		}
	}
	return acks >= Majority(len(c.config.DomainMembers(identity.DomainID)))
}

func (c *Cluster) globalIdentityHasElectionQuorumLocked(identity GlobalLeaderIdentity) bool {
	if !c.globalIdentityCurrentLocked(identity) {
		return false
	}
	acks := 0
	for _, domain := range c.config.Domains() {
		domainLeader, ok := c.domainLeaders[domain]
		if ok && c.domainIdentityCurrentLocked(domainLeader) &&
			c.network.CanSend(identity.DomainLeader.NodeID, domainLeader.NodeID, GlobalHeartbeatMessage) {
			acks++
		}
	}
	return acks >= GlobalElectionThreshold(len(c.config.Domains()))
}

func (c *Cluster) bestAvailableDomainCandidateLocked(domain string) (string, bool) {
	var candidates []*Node
	for _, configured := range c.config.DomainMembers(domain) {
		node := c.nodes[configured.ID]
		if !c.network.IsStopped(node.ID) {
			candidates = append(candidates, node)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i].State.Summary(), candidates[j].State.Summary()
		if left != right {
			return left.AtLeast(right)
		}
		return candidates[i].ID < candidates[j].ID
	})
	for _, candidate := range candidates {
		reachable := 1
		for _, member := range c.config.DomainMembers(domain) {
			if member.ID != candidate.ID && c.network.CanSend(candidate.ID, member.ID, DomainVoteMessage) {
				reachable++
			}
		}
		if reachable >= Majority(len(c.config.DomainMembers(domain))) {
			return candidate.ID, true
		}
	}
	return "", false
}

func (c *Cluster) bestAvailableGlobalCandidateLocked() (string, bool) {
	var identities []DomainLeaderIdentity
	for _, identity := range c.domainLeaders {
		if c.domainIdentityCurrentLocked(identity) {
			identities = append(identities, identity)
		}
	}
	sort.Slice(identities, func(i, j int) bool {
		left := c.nodes[identities[i].NodeID].State.Summary()
		right := c.nodes[identities[j].NodeID].State.Summary()
		if left != right {
			return left.AtLeast(right)
		}
		return identities[i].NodeID < identities[j].NodeID
	})
	for _, candidate := range identities {
		reachable := 1
		for _, voter := range identities {
			if voter.NodeID != candidate.NodeID &&
				c.network.CanSend(candidate.NodeID, voter.NodeID, GlobalVoteMessage) {
				reachable++
			}
		}
		if reachable >= GlobalElectionThreshold(len(c.config.Domains())) {
			return candidate.NodeID, true
		}
	}
	return "", false
}

func (c *Cluster) invalidateGlobalLeaderLocked() {
	c.globalLeader = GlobalLeaderIdentity{}
	for _, node := range c.nodes {
		TransitionRevokeGlobalLeadership(&node.State, &node.GlobalRole, &node.Stage)
		_ = c.persistLocked(node)
	}
}

func (c *Cluster) persistLocked(node *Node) error {
	return node.store.Save(node.State)
}

func (c *Cluster) AssertLeaderUniqueness() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	perDomainTerm := make(map[string]string)
	globalTerms := make(map[uint64]string)
	for _, node := range c.nodes {
		if node.DomainRole == Leader && !c.network.IsStopped(node.ID) {
			key := fmt.Sprintf("%s/%d", node.DomainCode, node.State.DomainTerm)
			if previous, exists := perDomainTerm[key]; exists && previous != node.ID {
				return fmt.Errorf("two domain leaders for %s: %s and %s", key, previous, node.ID)
			}
			perDomainTerm[key] = node.ID
		}
		if node.GlobalRole == Leader && !c.network.IsStopped(node.ID) {
			if previous, exists := globalTerms[node.State.GlobalTerm]; exists && previous != node.ID {
				return fmt.Errorf("two global leaders for term %d: %s and %s", node.State.GlobalTerm, previous, node.ID)
			}
			globalTerms[node.State.GlobalTerm] = node.ID
		}
	}
	return nil
}
