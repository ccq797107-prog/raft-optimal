package sim

// data.go is the DATA PATH (Write / Read / replication / commit) of the
// deterministic in-memory test model (see model.go for the engine). It is a
// SECOND implementation of the consensus data path used ONLY by unit tests;
// production traffic goes through cdraft.RPCNode.
//
// It exists because it is synchronous and single-threaded over a TestNetwork
// with per-message-type directed drops, which lets the protocol's commit and
// Fast Return rules be asserted deterministically (no real timers, no gRPC, no
// races). The shared log primitives it calls are the same ones production uses,
// reused via the parent cdraft package.

import (
	"fmt"

	. "github.com/czq/cd-raft/internal/cdraft"
)

func (c *Cluster) Write(nodeID string, request ClientWriteRequest) WriteOutcome {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[nodeID]
	if !ok || c.network.IsStopped(nodeID) {
		return WriteOutcome{NormalErr: ErrNodeStopped, FastErr: ErrNodeStopped}
	}
	if node.Stage != Serving {
		return WriteOutcome{NormalErr: ErrNotServing, FastErr: ErrNotServing}
	}
	if !c.globalIdentityCurrentLocked(c.globalLeader) || c.globalLeader.DomainLeader.NodeID != nodeID {
		return WriteOutcome{NormalErr: ErrNotGlobalLeader, FastErr: ErrNotGlobalLeader}
	}
	if request.RequestID == "" || request.Command.Key == "" {
		return WriteOutcome{NormalErr: fmt.Errorf("invalid write request"), FastErr: fmt.Errorf("invalid write request")}
	}

	entry, exists := FindEntryByRequest(node.State.Log, request.RequestID)
	if final, completed := node.State.Results[request.RequestID]; completed {
		if exists {
			if entry.Command != request.Command || entry.OriginDomain != request.OriginDomain {
				return WriteOutcome{NormalErr: fmt.Errorf("requestId reused with different command"), FastErr: ErrInvalidResult}
			}
		} else if final.GlobalIndex > node.State.SnapshotLastIndex ||
			final.Result != CommandResult(request.Command) {
			// Either settled-but-absent from the un-compacted log, or the
			// compacted entry's deterministic result does not match the retried
			// command: the requestId was reused.
			return WriteOutcome{NormalErr: fmt.Errorf("requestId reused with different command"), FastErr: ErrInvalidResult}
		}
		// A result whose entry was compacted into the snapshot stays
		// authoritative for idempotent retries.
		final.Source = GlobalResponse
		return WriteOutcome{Normal: &final, FastErr: ErrInvalidResult}
	}
	if !exists {
		entry = LogEntry{
			GlobalTerm:   c.globalLeader.GlobalTerm,
			GlobalIndex:  node.State.Summary().LastGlobalIndex + 1,
			RequestID:    request.RequestID,
			OriginDomain: request.OriginDomain,
			Command:      request.Command,
			Result:       CommandResult(request.Command),
		}
		node.State.Log = append(node.State.Log, entry)
		_ = c.persistLocked(node)
	} else if entry.Command != request.Command || entry.OriginDomain != request.OriginDomain {
		return WriteOutcome{NormalErr: fmt.Errorf("requestId reused with different command"), FastErr: ErrInvalidResult}
	}

	quorums := c.replicateEntryLocked(entry)
	globalDomain := c.globalLeader.DomainLeader.DomainID
	globalDomainReady := quorums[globalDomain]
	originDomainReady := quorums[request.OriginDomain]

	if globalDomainReady {
		node.State.DomainQuorumIndex[globalDomain] = max(node.State.DomainQuorumIndex[globalDomain], entry.GlobalIndex)
		_ = c.persistLocked(node)
	}
	for domain, ready := range quorums {
		if !ready || domain == globalDomain {
			continue
		}
		domainLeader := c.domainLeaders[domain]
		if c.network.CanSend(domainLeader.NodeID, nodeID, DomainQuorumAckMessage) {
			node.State.DomainQuorumIndex[domain] = max(node.State.DomainQuorumIndex[domain], entry.GlobalIndex)
			_ = c.persistLocked(node)
		}
	}

	var fast *ClientResult
	fastErr := ErrInvalidResult
	if c.config.Features.FastReturnEnabled && request.ReplyRoute != "" && globalDomainReady && originDomainReady {
		originLeader, ok := c.domainLeaders[request.OriginDomain]
		if ok && c.domainIdentityCurrentLocked(originLeader) &&
			c.network.CanSend(nodeID, originLeader.NodeID, GlobalDomainQuorumAckMessage) &&
			c.fastReturnContiguousLocked(originLeader.NodeID, entry.GlobalIndex) {
			result := ClientResult{
				RequestID: request.RequestID, GlobalTerm: entry.GlobalTerm, GlobalIndex: entry.GlobalIndex,
				Result: entry.Result, Source: FastResponse, Committed: true,
			}
			fast = &result
			fastErr = nil
			c.applyCommitToDomainLocked(request.OriginDomain, entry.GlobalIndex)
		}
	}

	c.advanceGlobalCommitLocked()
	var normal *ClientResult
	normalErr := ErrNoCommit
	if final, exists := node.State.Results[request.RequestID]; exists {
		final.Source = GlobalResponse
		normal = &final
		normalErr = nil
	}
	return WriteOutcome{Normal: normal, NormalErr: normalErr, Fast: fast, FastErr: fastErr}
}

func (c *Cluster) Read(nodeID, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.nodes[nodeID]
	if !ok || c.network.IsStopped(nodeID) {
		return "", ErrNodeStopped
	}
	if node.Stage != Serving {
		return "", ErrNotServing
	}
	if !c.globalIdentityCurrentLocked(c.globalLeader) || c.globalLeader.DomainLeader.NodeID != nodeID {
		return "", ErrNotGlobalLeader
	}
	c.reconcileAcksLocked()
	c.advanceGlobalCommitLocked()
	globalDomain := c.globalLeader.DomainLeader.DomainID
	barrier := node.State.DomainQuorumIndex[globalDomain]
	if node.State.KnownGlobalCommitIndex < barrier || node.State.AppliedIndex < barrier {
		return "", ErrReadBarrier
	}
	return node.State.StateMachine[key], nil
}

func (c *Cluster) Reconcile() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reconcileAcksLocked()
	c.advanceGlobalCommitLocked()
}

func (c *Cluster) FastReturnEligible(requestID, originDomain string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.config.Features.FastReturnEnabled || !c.globalIdentityCurrentLocked(c.globalLeader) {
		return false
	}
	global := c.nodes[c.globalLeader.DomainLeader.NodeID]
	entry, ok := FindEntryByRequest(global.State.Log, requestID)
	if !ok {
		return false
	}
	originLeader, ok := c.domainLeaders[originDomain]
	if !ok || !c.domainIdentityCurrentLocked(originLeader) {
		return false
	}
	globalDomain := c.globalLeader.DomainLeader.DomainID
	return global.State.DomainQuorumIndex[globalDomain] >= entry.GlobalIndex &&
		c.nodes[originLeader.NodeID].State.DomainQuorumIndex[originDomain] >= entry.GlobalIndex &&
		c.fastReturnContiguousLocked(originLeader.NodeID, entry.GlobalIndex) &&
		c.network.CanSend(global.ID, originLeader.NodeID, GlobalDomainQuorumAckMessage)
}

func (c *Cluster) replicateEntryLocked(entry LogEntry) map[string]bool {
	quorums := make(map[string]bool)
	globalNode := c.nodes[c.globalLeader.DomainLeader.NodeID]
	for _, domain := range c.config.Domains() {
		domainLeader, ok := c.domainLeaders[domain]
		if !ok || !c.domainIdentityCurrentLocked(domainLeader) {
			continue
		}
		if domainLeader.NodeID != globalNode.ID &&
			!c.network.CanSend(globalNode.ID, domainLeader.NodeID, ReplicateEntryMessage) {
			continue
		}
		leaderNode := c.nodes[domainLeader.NodeID]
		if !AppendEntry(&leaderNode.State, entry) {
			continue
		}
		_ = c.persistLocked(leaderNode)
		acks := 1
		for _, member := range c.config.DomainMembers(domain) {
			if member.ID == domainLeader.NodeID ||
				!c.network.CanSend(domainLeader.NodeID, member.ID, ReplicateEntryMessage) {
				continue
			}
			follower := c.nodes[member.ID]
			if AppendEntry(&follower.State, entry) {
				acks++
				_ = c.persistLocked(follower)
			}
		}
		if acks >= Majority(len(c.config.DomainMembers(domain))) &&
			leaderNode.State.DomainQuorumIndex[domain]+1 >= entry.GlobalIndex {
			quorums[domain] = true
			for _, member := range c.config.DomainMembers(domain) {
				node := c.nodes[member.ID]
				if HasEntry(node.State.Log, entry.GlobalIndex, entry.RequestID) {
					node.State.DomainQuorumIndex[domain] = max(node.State.DomainQuorumIndex[domain], entry.GlobalIndex)
					_ = c.persistLocked(node)
				}
			}
		}
	}
	return quorums
}

func (c *Cluster) reconcileAcksLocked() {
	if !c.globalIdentityCurrentLocked(c.globalLeader) {
		return
	}
	global := c.nodes[c.globalLeader.DomainLeader.NodeID]
	for domain, identity := range c.domainLeaders {
		if !c.domainIdentityCurrentLocked(identity) {
			continue
		}
		index := c.nodes[identity.NodeID].State.DomainQuorumIndex[domain]
		if domain == c.globalLeader.DomainLeader.DomainID ||
			c.network.CanSend(identity.NodeID, global.ID, DomainQuorumAckMessage) {
			global.State.DomainQuorumIndex[domain] = max(global.State.DomainQuorumIndex[domain], index)
		}
	}
	_ = c.persistLocked(global)
}

func (c *Cluster) advanceGlobalCommitLocked() {
	if !c.globalIdentityCurrentLocked(c.globalLeader) {
		return
	}
	global := c.nodes[c.globalLeader.DomainLeader.NodeID]
	globalDomain := c.globalLeader.DomainLeader.DomainID
	globalQuorum := global.State.DomainQuorumIndex[globalDomain]
	var otherQuorum uint64
	for domain, index := range global.State.DomainQuorumIndex {
		if domain != globalDomain {
			otherQuorum = max(otherQuorum, index)
		}
	}
	target := min(globalQuorum, otherQuorum)
	for next := global.State.KnownGlobalCommitIndex + 1; next <= target; next++ {
		entry, ok := FindEntryByIndex(global.State.Log, next)
		if !ok {
			break
		}
		for _, node := range c.nodes {
			if c.network.CanSend(global.ID, node.ID, CommitNoticeMessage) && HasEntry(node.State.Log, next, entry.RequestID) {
				ApplyEntry(&node.State, entry)
				_ = c.persistLocked(node)
			}
		}
		ApplyEntry(&global.State, entry)
		_ = c.persistLocked(global)
	}
}

func (c *Cluster) applyCommitToDomainLocked(domain string, target uint64) {
	for _, configured := range c.config.DomainMembers(domain) {
		node := c.nodes[configured.ID]
		for next := node.State.KnownGlobalCommitIndex + 1; next <= target; next++ {
			entry, ok := FindEntryByIndex(node.State.Log, next)
			if !ok {
				break
			}
			ApplyEntry(&node.State, entry)
		}
		_ = c.persistLocked(node)
	}
}

// Compact folds nodeID's applied log prefix into its snapshot, keeping the
// newest `retain` applied entries in the log. It mirrors the production
// compaction policy deterministically so tests can place the compaction point
// exactly.
func (c *Cluster) Compact(nodeID string, retain uint64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node, ok := c.nodes[nodeID]
	if !ok {
		return false, fmt.Errorf("unknown node %q", nodeID)
	}
	if node.State.AppliedIndex <= retain {
		return false, nil
	}
	if !CompactLog(&node.State, node.State.AppliedIndex-retain) {
		return false, nil
	}
	return true, c.persistLocked(node)
}

// InstallSnapshotFromGlobal ships the Global Leader's applied prefix (state
// machine + result cache) to targetDomain's Domain Leader, which fans it out to
// its in-domain followers — the deterministic mirror of the production
// InstallSnapshot RPC (with fanout) used when a lagging domain sits below the
// leader's compaction floor. On an in-domain majority install, the domain's
// quorum high-water mark advances to the snapshot boundary, exactly like a
// quorum-replicated entry.
func (c *Cluster) InstallSnapshotFromGlobal(targetDomain string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.globalIdentityCurrentLocked(c.globalLeader) {
		return false, ErrNotGlobalLeader
	}
	global := c.nodes[c.globalLeader.DomainLeader.NodeID]
	leaderIdentity, ok := c.domainLeaders[targetDomain]
	if !ok || !c.domainIdentityCurrentLocked(leaderIdentity) {
		return false, ErrNotDomainLeader
	}
	if !c.network.CanSend(global.ID, leaderIdentity.NodeID, ReplicateEntryMessage) {
		return false, ErrNodeStopped
	}
	lastIndex := global.State.AppliedIndex
	if lastIndex == 0 {
		return false, nil
	}
	var lastTerm uint64
	var lastRequestID string
	if entry, found := FindEntryByIndex(global.State.Log, lastIndex); found {
		lastTerm, lastRequestID = entry.GlobalTerm, entry.RequestID
	} else if lastIndex == global.State.SnapshotLastIndex {
		lastTerm, lastRequestID = global.State.SnapshotLastTerm, global.State.SnapshotLastRequestID
	} else {
		return false, fmt.Errorf("global leader cannot identify applied boundary %d", lastIndex)
	}

	leaderNode := c.nodes[leaderIdentity.NodeID]
	if !InstallSnapshotState(&leaderNode.State, lastIndex, lastTerm, lastRequestID,
		global.State.StateMachine, global.State.Results) {
		return false, nil
	}
	_ = c.persistLocked(leaderNode)
	acks := 1
	for _, member := range c.config.DomainMembers(targetDomain) {
		if member.ID == leaderIdentity.NodeID ||
			!c.network.CanSend(leaderIdentity.NodeID, member.ID, ReplicateEntryMessage) {
			continue
		}
		follower := c.nodes[member.ID]
		if InstallSnapshotState(&follower.State, lastIndex, lastTerm, lastRequestID,
			global.State.StateMachine, global.State.Results) {
			acks++
			_ = c.persistLocked(follower)
		}
	}
	if acks < Majority(len(c.config.DomainMembers(targetDomain))) {
		return false, nil
	}
	for _, member := range c.config.DomainMembers(targetDomain) {
		node := c.nodes[member.ID]
		if node.State.AppliedIndex >= lastIndex {
			node.State.DomainQuorumIndex[targetDomain] = max(node.State.DomainQuorumIndex[targetDomain], lastIndex)
			_ = c.persistLocked(node)
		}
	}
	return true, nil
}

func (c *Cluster) fastReturnContiguousLocked(originLeaderID string, index uint64) bool {
	leader := c.nodes[originLeaderID]
	return leader != nil && leader.State.KnownGlobalCommitIndex+1 >= index
}

func (c *Cluster) AssertCommitInvariants() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.globalIdentityCurrentLocked(c.globalLeader) {
		return nil
	}
	globalDomain := c.globalLeader.DomainLeader.DomainID
	for _, node := range c.nodes {
		if node.State.AppliedIndex > node.State.KnownGlobalCommitIndex {
			return fmt.Errorf("%s applied index exceeds known commit", node.ID)
		}
		if node.State.KnownGlobalCommitIndex > node.State.DomainQuorumIndex[globalDomain] {
			// A Fast Return origin domain can know a certificate before receiving
			// the global domain's full quorum progress map locally.
			if node.DomainCode != globalDomain {
				continue
			}
			return fmt.Errorf("%s known commit exceeds global-domain quorum", node.ID)
		}
	}
	return nil
}
