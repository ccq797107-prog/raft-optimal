package cdraft

import (
	"context"
	"fmt"
	"log"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"github.com/czq/cd-raft/internal/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// telemetryService carries real cross-domain RTT probes, per-domain telemetry
// reports aggregated at the Global Leader, and the safe migration trigger.
type telemetryService struct {
	cdraftv1.UnimplementedTelemetryServer
	node *RPCNode
}

func (s *telemetryService) Ping(_ context.Context, request *cdraftv1.PingRequest) (*cdraftv1.PingResponse, error) {
	return &cdraftv1.PingResponse{
		ResponderNodeId: s.node.local.ID,
		ResponderDomain: s.node.local.DomainCode,
	}, nil
}

func (s *telemetryService) ReportTelemetry(_ context.Context, report *cdraftv1.TelemetryReport) (*cdraftv1.Empty, error) {
	s.node.ingestTelemetryReport(report)
	return &cdraftv1.Empty{}, nil
}

func (s *telemetryService) ReportFloatingLatency(_ context.Context, report *cdraftv1.FloatingLatencyReport) (*cdraftv1.Empty, error) {
	if err := s.node.ingestFloatingLatencyReport(report); err != nil {
		return nil, err
	}
	return &cdraftv1.Empty{}, nil
}

func (s *telemetryService) BeginMigration(ctx context.Context, request *cdraftv1.BeginMigrationRequest) (*cdraftv1.BeginMigrationResponse, error) {
	return s.node.beginMigration(ctx, request)
}

func (s *telemetryService) CatchUp(ctx context.Context, request *cdraftv1.CatchUpRequest) (*cdraftv1.CatchUpResponse, error) {
	return s.node.catchUp(ctx, request)
}

func (s *telemetryService) Move(ctx context.Context, request *cdraftv1.MoveRequest) (*cdraftv1.MoveResponse, error) {
	return s.node.handleMove(ctx, request)
}

// catchUp runs on the migration target's Domain Leader during the pre-handoff
// phase. The current Global Leader streams the global-log tail the target is
// missing (up to the migration barrier); the target appends each entry in order
// and replicates it inside its own domain by reusing the standard Replicate
// fan-out, then reports how far it has durably reached. An empty entries list is
// a pure progress probe so the caller can learn the target's current position.
func (n *RPCNode) catchUp(ctx context.Context, request *cdraftv1.CatchUpRequest) (*cdraftv1.CatchUpResponse, error) {
	n.mu.Lock()
	isDomainLeader := n.domainRole == Leader && n.state.DomainLeader.NodeID == n.local.ID
	localDomain := n.local.DomainCode
	followsGL := n.globalLeader.Valid()
	glTerm := n.state.GlobalTerm
	n.mu.Unlock()
	if !isDomainLeader {
		return nil, status.Error(codes.FailedPrecondition, ErrNotDomainLeader.Error())
	}
	// Provenance: catch-up streams the *current* Global Leader's committed tail.
	// Refuse to backfill if we do not yet follow a GL, and never accept an entry
	// from a term newer than the GL we follow — that could only be a forged
	// future-term entry an unauthenticated caller is trying to inject.
	if !followsGL {
		return nil, status.Error(codes.FailedPrecondition, "no global leader to catch up from")
	}
	for _, entry := range request.GetEntries() {
		if entry.GetGlobalTerm() > glTerm {
			return nil, status.Errorf(codes.FailedPrecondition, "catch-up entry from unknown future global term %d", entry.GetGlobalTerm())
		}
	}
	for _, entry := range request.GetEntries() {
		replicate := &cdraftv1.ReplicateEntry{
			GlobalTerm: entry.GetGlobalTerm(), GlobalIndex: entry.GetGlobalIndex(),
			RequestId: entry.GetRequestId(), OriginDomain: entry.GetOriginDomain(),
			Key: entry.GetKey(), Value: entry.GetValue(),
			SenderNodeId: n.local.ID, FanoutToDomain: true,
			PrevLogIndex: entry.GetPrevLogIndex(), PrevLogTerm: entry.GetPrevLogTerm(),
		}
		if _, err := n.Replicate(ctx, replicate); err != nil {
			// A stale-term entry means a newer Global Leader already exists; stop
			// and report progress so the caller can re-evaluate.
			break
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return &cdraftv1.CatchUpResponse{
		DomainQuorumIndex: n.state.DomainQuorumIndex[localDomain],
		LastIndex:         n.state.Summary().LastGlobalIndex,
	}, nil
}

// telemetryLoop runs on every node; only the current Domain Leader actively
// probes the other domains' Domain Leaders over real gRPC and reports its
// measurements to the Global Leader.
func (n *RPCNode) telemetryLoop(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.probeAndReport(ctx)
		}
	}
}

func (n *RPCNode) probeAndReport(ctx context.Context) {
	n.mu.Lock()
	stopped := n.stopped
	isDomainLeader := n.domainRole == Leader && n.state.DomainLeader.NodeID == n.local.ID
	globalLeaderNode := n.globalLeader.DomainLeader.NodeID
	isGlobalLeader := n.globalRole == Leader && globalLeaderNode == n.local.ID
	n.mu.Unlock()
	if stopped || !isDomainLeader {
		return
	}

	localDomain := n.local.DomainCode
	for _, identity := range n.domainLeaderSnapshot() {
		if identity.DomainID == localDomain || identity.NodeID == n.local.ID {
			continue
		}
		start := time.Now()
		err := n.callNode(ctx, identity.NodeID, func(callCtx context.Context, conn *grpc.ClientConn) error {
			_, pingErr := cdraftv1.NewTelemetryClient(conn).Ping(callCtx, &cdraftv1.PingRequest{
				FromDomain: localDomain, FromNodeId: n.local.ID,
			})
			return pingErr
		})
		if err != nil {
			continue
		}
		// callNode already injects the directed one-way delays around the call,
		// so the elapsed wall-clock time is the real measured round trip.
		n.rtt.observeRTT(localDomain, identity.DomainID, time.Since(start))
		n.markDomainSeen(identity.DomainID)
	}
	n.markDomainSeen(localDomain)

	// Report the local view to the Global Leader (skip if we are the GL; it
	// already holds its own measurements).
	if isGlobalLeader || globalLeaderNode == "" {
		return
	}
	report := &cdraftv1.TelemetryReport{
		Reporter:        toPBDomainIdentity(n.state.DomainLeader),
		DomainAvailable: true,
		GlobalTerm:      n.state.GlobalTerm,
	}
	for to, oneWay := range n.rtt.localSnapshot(localDomain) {
		report.Rtts = append(report.Rtts, &cdraftv1.DomainRtt{ToDomain: to, OneWayMillis: uint64(oneWay)})
	}
	_ = n.callNode(ctx, globalLeaderNode, func(callCtx context.Context, conn *grpc.ClientConn) error {
		_, reportErr := cdraftv1.NewTelemetryClient(conn).ReportTelemetry(callCtx, report)
		return reportErr
	})
}

func (n *RPCNode) ingestTelemetryReport(report *cdraftv1.TelemetryReport) {
	reporter := fromPBDomainIdentity(report.GetReporter())
	if reporter.DomainID == "" {
		return
	}
	perDomain := make(map[string]float64, len(report.GetRtts()))
	for _, entry := range report.GetRtts() {
		perDomain[entry.GetToDomain()] = float64(entry.GetOneWayMillis())
	}
	n.rtt.merge(reporter.DomainID, perDomain)
	if report.GetDomainAvailable() {
		n.markDomainSeen(reporter.DomainID)
	}
}

const maxFloatingLatency = 10 * time.Minute

func floatingTelemetryTTL(config topology.Config) time.Duration {
	if config.Features.FloatingTelemetryTTLMillis == 0 {
		return 5 * time.Second
	}
	return time.Duration(config.Features.FloatingTelemetryTTLMillis) * time.Millisecond
}

func (n *RPCNode) ingestFloatingLatencyReport(report *cdraftv1.FloatingLatencyReport) error {
	origin := report.GetOriginDomain()
	if origin == "" {
		return status.Error(codes.InvalidArgument, "floating origin domain is empty")
	}
	if n.config.DomainKind(origin) != topology.DomainFloating {
		return status.Errorf(codes.InvalidArgument, "origin domain %q is not floating", origin)
	}
	n.mu.Lock()
	isGlobalLeader := n.stage == Serving && n.servesAsGlobalLeaderLocked()
	currentTerm := n.state.GlobalTerm
	n.mu.Unlock()
	if !isGlobalLeader {
		return status.Error(codes.Unavailable, ErrNotGlobalLeader.Error())
	}
	if report.GetGlobalTerm() != currentTerm {
		return status.Errorf(codes.FailedPrecondition, "stale floating latency term %d, current %d", report.GetGlobalTerm(), currentTerm)
	}
	now := time.Now()
	for _, rtt := range report.GetRtts() {
		to := rtt.GetToDomain()
		if !n.config.IsConsensusDomain(to) {
			return status.Errorf(codes.InvalidArgument, "latency target %q is not a consensus domain", to)
		}
		oneWay := float64(rtt.GetOneWayMillis())
		if oneWay == 0 && rtt.GetRttMillis() > 0 {
			oneWay = float64(rtt.GetRttMillis()) / 2
		}
		if time.Duration(oneWay)*time.Millisecond > maxFloatingLatency {
			return status.Errorf(codes.InvalidArgument, "floating latency for %s->%s is out of range", origin, to)
		}
		n.floatingLatency.observe(origin, to, oneWay, now)
	}
	return nil
}

func (n *RPCNode) markDomainSeen(domain string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.domainSeen == nil {
		n.domainSeen = make(map[string]time.Time)
	}
	n.domainSeen[domain] = time.Now()
}

// handleMove is the server side of the GL-only Move RPC: the administrative
// trigger an external controller (cmd/cdraft-mover) uses to request a Global
// Leader handoff. The node enforces the single hard constraint of the decoupled
// design: ONLY the current Global Leader may execute a move. A request that
// lands on any other node is rejected with not_global_leader plus the current GL
// identity so the caller can retarget. The decision of WHERE to move is made
// entirely by the caller; the node only runs the safe handoff.
func (n *RPCNode) handleMove(ctx context.Context, request *cdraftv1.MoveRequest) (*cdraftv1.MoveResponse, error) {
	n.mu.Lock()
	isGlobalLeader := !n.stopped && n.globalRole == Leader &&
		n.globalLeader.DomainLeader.NodeID == n.local.ID
	currentDomain := n.local.DomainCode
	glIdentity := n.globalLeader.DomainLeader
	n.mu.Unlock()

	if !isGlobalLeader {
		return &cdraftv1.MoveResponse{
			Accepted:        false,
			NotGlobalLeader: true,
			Error:           "node is not the current global leader",
			GlobalLeader:    toPBDomainIdentity(glIdentity),
		}, nil
	}
	target := request.GetTargetDomain()
	if target == "" {
		return &cdraftv1.MoveResponse{Accepted: false, Error: "empty target domain"}, nil
	}
	if target == currentDomain {
		return &cdraftv1.MoveResponse{Accepted: false, Error: "target domain already hosts the global leader"}, nil
	}
	reason := request.GetReason()
	if reason == "" {
		reason = "manual move"
	}
	outcome := n.initiateMigration(ctx, target, reason)
	return &cdraftv1.MoveResponse{
		Accepted:       outcome.success,
		Error:          outcome.errNote,
		FromGlobalTerm: outcome.fromTerm,
		NewGlobalTerm:  outcome.newTerm,
	}, nil
}

// migrationOutcome is the result of a single safe handoff attempt.
type migrationOutcome struct {
	success  bool
	errNote  string
	fromTerm uint64
	newTerm  uint64
}

// initiateMigration performs a safe, two-phase Global Leader handoff (catch-up
// handoff). Unlike a naive "bump the target's term and let it campaign" trigger,
// it never fences the incumbent up front:
//
//  1. Pre-check: at least N-1 Domain Leaders must be reachable, otherwise a
//     higher-term election could not succeed and we abort without disruption.
//  2. Barrier + drain: freeze a migration barrier at the current log head and
//     enter draining so no NEW writes are ordered past it; the target can then
//     converge to a fixed point instead of chasing a moving log.
//  3. Catch-up: stream the missing tail to the target until its in-domain quorum
//     reaches the barrier (bounded by catchUpTimeout).
//  4. Handoff: only after the target is caught up do we trigger the real
//     higher-term election (BeginMigration), which fences the old GL. Because
//     the target's log is now at least as new as every voter, the election
//     succeeds deterministically.
//
// On any failure before step 4 (insufficient quorum, catch-up timeout) the
// incumbent Global Leader is left completely untouched: draining is lifted and
// it keeps serving. Migration never edits the leader pointer directly; it always
// flows through the same election + N-1 + log-freshness + fencing path.
func (n *RPCNode) initiateMigration(ctx context.Context, targetDomain, reason string) migrationOutcome {
	n.mu.Lock()
	if n.migrating {
		n.mu.Unlock()
		return migrationOutcome{success: false, errNote: "a migration is already in progress"}
	}
	target, ok := n.domainLeaders[targetDomain]
	fromTerm := n.state.GlobalTerm
	currentDomain := n.local.DomainCode
	if !ok || !target.Valid() || target.NodeID == n.local.ID {
		n.mu.Unlock()
		return migrationOutcome{success: false, fromTerm: fromTerm, errNote: fmt.Sprintf("no known valid domain leader for target %s", targetDomain)}
	}
	barrier := n.state.Summary().LastGlobalIndex
	timeout := n.catchUpTimeout
	n.migrating = true
	n.draining = true
	n.migrationBarrier = barrier
	n.lastMigrationNote = fmt.Sprintf("handoff to %s: draining at barrier %d (%s)", targetDomain, barrier, reason)
	n.mu.Unlock()

	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	log.Printf("cd-raft %s: migration START %s -> %s at barrier %d (%s)",
		n.local.ID, currentDomain, targetDomain, barrier, reason)

	finish := func(success bool, newTerm uint64, note string) migrationOutcome {
		n.mu.Lock()
		n.migrating = false
		n.draining = false
		n.migrationBarrier = 0
		n.lastMigrationNote = note
		n.mu.Unlock()
		if success {
			log.Printf("cd-raft %s: migration OK: %s", n.local.ID, note)
		} else {
			log.Printf("cd-raft %s: migration ABORT/FAIL: %s", n.local.ID, note)
		}
		return migrationOutcome{success: success, errNote: note, fromTerm: fromTerm, newTerm: newTerm}
	}

	// Step 1: a higher-term Global Leader election needs N-1 voting Domain
	// Leaders. If they are not reachable, do NOT fence the incumbent.
	threshold := globalElectionThreshold(len(n.config.Domains()))
	if n.reachableDomainLeaders() < threshold {
		return finish(false, 0, fmt.Sprintf("handoff to %s aborted: fewer than %d domain leaders reachable; GL unchanged (%s)", targetDomain, threshold, reason))
	}

	// Steps 2-3: bring the target up to the frozen barrier.
	if !n.runCatchUp(ctx, target, barrier, timeout) {
		return finish(false, 0, fmt.Sprintf("handoff to %s aborted: target did not reach barrier %d within %s; GL unchanged (%s)", targetDomain, barrier, timeout, reason))
	}

	// Step 4: revoke our OWN read lease and step down BEFORE triggering the
	// handoff election. The transfer election bypasses voter stickiness, so this
	// explicit, up-front relinquishment is the only thing that guarantees the
	// outgoing and incoming serving leases never overlap (no stale reads from the
	// old GL in parallel with the new one). The target's log is already caught up
	// to the barrier, so the election still succeeds deterministically.
	n.mu.Lock()
	n.revokeGlobalLeadershipForHandoffLocked()
	n.mu.Unlock()

	var response *cdraftv1.BeginMigrationResponse
	err := n.callNode(ctx, target.NodeID, func(callCtx context.Context, conn *grpc.ClientConn) error {
		var callErr error
		response, callErr = cdraftv1.NewTelemetryClient(conn).BeginMigration(callCtx, &cdraftv1.BeginMigrationRequest{
			TargetDomain: targetDomain, FromGlobalTerm: fromTerm, Reason: reason,
		})
		return callErr
	})
	if err == nil && response.GetAccepted() {
		n.mu.Lock()
		n.migrationsCount++
		n.adoptHandedOffLeaderLocked(target, response.GetNewGlobalTerm())
		_ = n.persistLocked()
		n.mu.Unlock()
		return finish(true, response.GetNewGlobalTerm(), fmt.Sprintf("migrated to %s term %d after catch-up to barrier %d (%s)", targetDomain, response.GetNewGlobalTerm(), barrier, reason))
	}
	// The handoff election did not complete, but we already relinquished our
	// lease. We are no longer serving, so there is no dual-leader risk; bring the
	// global deadline forward so our failure detector promptly re-campaigns and
	// restores a leader (we still hold the most up-to-date log).
	n.mu.Lock()
	n.globalDeadline = time.Now()
	n.mu.Unlock()
	var detail string
	if err != nil {
		detail = "rpc:" + err.Error()
	} else {
		detail = "rejected:" + response.GetError()
	}
	return finish(false, 0, fmt.Sprintf("handoff to %s failed at election [%s]; incumbent relinquished and will re-campaign (%s)", targetDomain, detail, reason))
}

// reachableDomainLeaders counts the currently known, valid Domain Leaders. It
// is the pre-check proxy for "can a higher-term Global Leader election gather
// N-1 votes" before we disturb anything.
func (n *RPCNode) reachableDomainLeaders() int {
	return len(n.domainLeaderSnapshot())
}

// runCatchUp streams the global-log tail [target+1 .. barrier] to the target's
// Domain Leader until its in-domain quorum reaches the barrier, or the budget
// expires. It returns true only when the target is fully caught up.
func (n *RPCNode) runCatchUp(ctx context.Context, target DomainLeaderIdentity, barrier uint64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	targetIndex := uint64(0)
	// Initial probe: key the re-stream floor off the target's in-domain quorum
	// high-water mark (DomainQuorumIndex), NOT its log head (LastIndex). The loop
	// below only succeeds once DomainQuorumIndex reaches the barrier, so that is
	// the index we must drive. A Domain Leader can hold entries in its log
	// WITHOUT having (re)formed an in-domain quorum for them: only the
	// fanning-out leader advances DomainQuorumIndex, so a freshly elected leader
	// inherits a full log but a lagging quorum mark (followers never set it).
	// Streaming off LastIndex would compute an EMPTY tail in that case, leaving
	// the quorum mark stuck below the barrier forever — the handoff would
	// "did-not-reach-barrier" abort until some unrelated write happened to bump
	// it. Re-streaming the tail above DomainQuorumIndex forces the target to
	// re-drive its in-domain quorum (Replicate is idempotent) and advance.
	if resp, err := n.sendCatchUp(ctx, target.NodeID, barrier, nil); err == nil {
		if resp.GetDomainQuorumIndex() >= barrier {
			return true
		}
		targetIndex = resp.GetDomainQuorumIndex()
	}
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return false
		}
		// A target below our compaction floor cannot be healed by streaming
		// entries (they were compacted away); install the snapshot first and
		// continue with the tail above it.
		if targetIndex < n.snapshotFloor() {
			if snap, serr := n.sendInstallSnapshot(ctx, target.NodeID); serr == nil && snap.GetInstalled() {
				targetIndex = snap.GetLastIndex()
			}
		}
		entries := n.collectTailEntries(targetIndex, barrier)
		resp, err := n.sendCatchUp(ctx, target.NodeID, barrier, entries)
		if err == nil {
			if resp.GetDomainQuorumIndex() >= barrier {
				return true
			}
			targetIndex = resp.GetDomainQuorumIndex()
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
	return false
}

// collectTailEntries snapshots the contiguous log entries in (after, barrier].
func (n *RPCNode) collectTailEntries(after, barrier uint64) []*cdraftv1.ReplicateEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	entries := make([]*cdraftv1.ReplicateEntry, 0, barrier-after)
	for idx := after + 1; idx <= barrier; idx++ {
		entry, ok := findEntryByIndex(n.state.Log, idx)
		if !ok {
			break
		}
		var prevTerm uint64
		if idx > 1 {
			if prev, ok := findEntryByIndex(n.state.Log, idx-1); ok {
				prevTerm = prev.GlobalTerm
			} else if idx-1 == n.state.SnapshotLastIndex {
				prevTerm = n.state.SnapshotLastTerm
			}
		}
		entries = append(entries, &cdraftv1.ReplicateEntry{
			GlobalTerm: entry.GlobalTerm, GlobalIndex: entry.GlobalIndex,
			RequestId: entry.RequestID, OriginDomain: entry.OriginDomain,
			Key: entry.Command.Key, Value: entry.Command.Value,
			PrevLogIndex: idx - 1, PrevLogTerm: prevTerm,
		})
	}
	return entries
}

func (n *RPCNode) sendCatchUp(ctx context.Context, nodeID string, barrier uint64, entries []*cdraftv1.ReplicateEntry) (*cdraftv1.CatchUpResponse, error) {
	var resp *cdraftv1.CatchUpResponse
	err := n.callNode(ctx, nodeID, func(callCtx context.Context, conn *grpc.ClientConn) error {
		var callErr error
		resp, callErr = cdraftv1.NewTelemetryClient(conn).CatchUp(callCtx, &cdraftv1.CatchUpRequest{
			BarrierIndex: barrier, Entries: entries,
		})
		return callErr
	})
	return resp, err
}

// beginMigration handles a migration request on the target domain's Domain
// Leader by running the same Global Leader election state machine at a higher
// global term. There is no shortcut around quorum or fencing.
func (n *RPCNode) beginMigration(ctx context.Context, request *cdraftv1.BeginMigrationRequest) (*cdraftv1.BeginMigrationResponse, error) {
	n.mu.Lock()
	isDomainLeader := n.domainRole == Leader && n.state.DomainLeader.NodeID == n.local.ID
	wrongDomain := request.GetTargetDomain() != n.local.DomainCode
	alreadyGlobal := n.globalRole == Leader && n.globalLeader.DomainLeader.NodeID == n.local.ID
	// Provenance/fencing: a migration is a handoff issued by the *incumbent* GL.
	// Only honor it if we currently follow a valid Global Leader and the request
	// names that very term. This rejects a stale or forged BeginMigration (old
	// from_global_term) that would otherwise launch a leadership-transfer
	// campaign and illegitimately bypass voter stickiness.
	freshIncumbent := request.GetFromGlobalTerm() != 0 &&
		request.GetFromGlobalTerm() == n.state.GlobalTerm &&
		n.globalLeader.Valid() &&
		n.globalLeader.GlobalTerm == request.GetFromGlobalTerm()
	n.mu.Unlock()
	if wrongDomain || !isDomainLeader {
		return &cdraftv1.BeginMigrationResponse{Accepted: false, Error: ErrNotDomainLeader.Error()}, nil
	}
	if alreadyGlobal {
		return &cdraftv1.BeginMigrationResponse{Accepted: false, Error: "target is already global leader"}, nil
	}
	if !freshIncumbent {
		return &cdraftv1.BeginMigrationResponse{Accepted: false, Error: "stale or unauthenticated migration request"}, nil
	}
	// A migration is a deliberate handoff initiated by the incumbent, so this
	// election is allowed to disrupt the (healthy) incumbent: mark it as a
	// leadership transfer so voters bypass stickiness.
	if err := n.campaignGlobal(ctx, true); err != nil {
		return &cdraftv1.BeginMigrationResponse{Accepted: false, Error: err.Error()}, nil
	}
	n.mu.Lock()
	term := n.state.GlobalTerm
	n.mu.Unlock()
	return &cdraftv1.BeginMigrationResponse{Accepted: true, NewGlobalTerm: term}, nil
}
