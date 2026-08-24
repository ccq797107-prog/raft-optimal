package cdraft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"github.com/czq/cd-raft/internal/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type pendingFastReturn struct {
	RequestID       string
	Result          string
	ReplyRoute      string
	OriginDomain    string
	ResponderDomain string
	GlobalTerm      uint64
	GlobalIndex     uint64
}

type returnPlan struct {
	OriginKind topology.DomainKind
	Responder  DomainLeaderIdentity
	Floating   bool
}

// RPCNode is one independently persisted CD-Raft node. All communication with
// other RPCNode instances crosses generated gRPC client/server boundaries.
type RPCNode struct {
	cdraftv1.UnimplementedReplicationServer
	cdraftv1.UnimplementedClientServer

	mu                sync.Mutex
	config            topology.Config
	local             topology.Node
	store             Store
	state             PersistentState
	stage             Stage
	domainRole        Role
	globalRole        Role
	domainLeaders     map[string]DomainLeaderIdentity
	globalLeader      GlobalLeaderIdentity
	pendingFast       map[uint64]pendingFastReturn
	fastSent          map[string]bool
	committing        map[string]chan struct{}
	rpcPolicy         RPCPolicy
	networkSimulation topology.NetworkSimulation
	fastReturnEnabled bool
	conns             map[string]*grpc.ClientConn
	server            *grpc.Server
	listeners         []net.Listener
	stopped           bool
	// failed is set when a durable persistence write fails. A consensus node
	// that cannot persist the state it is about to act on must fail-stop rather
	// than risk acknowledging operations it cannot recover after a crash.
	failed bool

	// Fault injection for tests/chaos: a deterministic outbound partition set
	// (peer node IDs this node cannot reach) and a probabilistic per-attempt
	// packet-loss rate. These model link cuts and lossy links over the REAL gRPC
	// transport (the only inter-node chokepoint is callNode), so partition /
	// packet-loss / restart behaviour can be exercised against rpc_node.go
	// itself rather than only the in-process simulator. Guarded by faultMu so the
	// hot path never contends on the main consensus mutex.
	faultMu           sync.Mutex
	blockedPeers      map[string]bool
	dropRate          float64
	traceMu           sync.RWMutex
	writeTrace        *WriteTraceRecorder
	normalDelay       time.Duration
	fastDelay         time.Duration
	domainDeadline    time.Time
	globalDeadline    time.Time
	domainCampaigning bool
	globalCampaigning bool

	// Global Leader read lease. A Global Leader may only serve client reads and
	// order new writes while it holds a fresh lease, i.e. it has heard back from
	// a quorum (globalElectionThreshold) of Domain Leaders within
	// globalLeaseDuration. This stops a partitioned/old Global Leader from
	// returning stale data after a new Global Leader has been elected elsewhere.
	// The lease is only enforced under the live runtime (Run), where the
	// heartbeat loop continuously refreshes it; the manual test harness that
	// drives elections without the loops keeps it disabled.
	globalLeaseEnabled  bool
	globalLeaseDuration time.Duration
	globalLeaseDeadline time.Time
	// lastGlobalContact records when we last accepted a heartbeat from the
	// Global Leader we follow. A voter uses it for leader-stickiness: it refuses
	// to grant a competing global vote within globalStickyWindow of hearing from
	// a valid leader, which (together with the leader's read lease) guarantees
	// two Global Leaders can never overlap.
	lastGlobalContact time.Time

	// Domain Leader lease. A Domain Leader keeps its role only while it has heard
	// back from an in-domain majority within domainLeaseDuration. Without this, a
	// Domain Leader that lost its in-domain majority (partition / minority) would
	// never step down, so an old and a new Domain Leader could coexist and each
	// cast a global vote — two votes from one domain, breaking "one vote per
	// domain" and potentially electing two Global Leaders in the same term.
	// Enforced only under the live runtime (Run), like the global lease.
	domainLeaseEnabled  bool
	domainLeaseDuration time.Duration
	domainLeaseDeadline time.Time

	// The node only MEASURES telemetry (per-domain W/R window + RTT matrix) and
	// EXECUTES handoffs requested via the GL-only Move RPC. The decision of where
	// the Global Leader should live lives in the external controller
	// (cmd/cdraft-mover), not here.
	stats             *statsWindow
	rtt               *rttStore
	floatingLatency   *floatingLatencyStore
	migrationsCount   uint64
	lastMigrationNote string
	migrating         bool
	draining          bool
	migrationBarrier  uint64
	catchUpTimeout    time.Duration
	domainSeen        map[string]time.Time
	metrics           nodeMetrics

	// A freshly (re)elected Domain Leader inherits a DomainQuorumIndex that lags
	// its already-replicated log (only the fanning-out leader advances the mark;
	// followers never do). reconfirmLoop re-drives the in-domain quorum to heal
	// that lag proactively, so Fast Return / migration catch-up for the domain do
	// not stay degraded until the next client write. reconfirmDisabled lets tests
	// that specifically exercise a wedged mark opt out; reconfirmInFlight prevents
	// overlapping heal rounds.
	reconfirmDisabled atomic.Bool
	reconfirmInFlight atomic.Bool
	recommitInFlight  atomic.Bool

	// Log compaction policy. When compactionThreshold > 0 the node folds its
	// applied log prefix into the snapshot once more than compactionThreshold
	// applied entries sit above the last snapshot point, keeping the newest
	// compactionRetain applied entries in the log so ordinary stragglers can
	// still be healed by a CatchUp backfill instead of a full InstallSnapshot.
	compactionThreshold uint64
	compactionRetain    uint64
}

// nodeMetrics holds node-local observability counters guarded by RPCNode.mu.
type nodeMetrics struct {
	fastReturnSuccess         uint64
	fastReturnDegraded        uint64
	floatingResponderChosen   uint64
	floatingResponderFailed   uint64
	floatingRaceGlobalWins    uint64
	floatingRaceFastWins      uint64
	floatingResponderByDomain map[string]uint64
	rejections                map[string]uint64
}

func NewRPCNode(config topology.Config, nodeID string, store Store) (*RPCNode, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	local, ok := config.Node(nodeID)
	if !ok {
		return nil, fmt.Errorf("unknown node %q", nodeID)
	}
	if store == nil {
		store = NewMemoryStore()
	}
	state, err := store.Load()
	if err != nil {
		return nil, err
	}
	state.initMaps()
	return &RPCNode{
		config:              config,
		local:               local,
		store:               store,
		state:               state,
		stage:               Booting,
		domainRole:          Follower,
		globalRole:          Follower,
		domainLeaders:       make(map[string]DomainLeaderIdentity),
		pendingFast:         make(map[uint64]pendingFastReturn),
		fastSent:            make(map[string]bool),
		committing:          make(map[string]chan struct{}),
		rpcPolicy:           DefaultRPCPolicy(),
		networkSimulation:   config.NetworkSimulation,
		fastReturnEnabled:   config.Features.FastReturnEnabled,
		stats:               newStatsWindow(5 * time.Second),
		rtt:                 newRTTStore(),
		floatingLatency:     newFloatingLatencyStore(floatingTelemetryTTL(config)),
		globalLeaseDuration: 400 * time.Millisecond,
		domainLeaseDuration: 400 * time.Millisecond,
		catchUpTimeout:      5 * time.Second,
		conns:               make(map[string]*grpc.ClientConn),
		metrics:             nodeMetrics{rejections: make(map[string]uint64), floatingResponderByDomain: make(map[string]uint64)},
		blockedPeers:        make(map[string]bool),
		compactionThreshold: config.Features.LogCompactionThreshold,
		compactionRetain:    config.Features.LogCompactionRetain,
	}, nil
}

// SetLogCompaction overrides the automatic log compaction policy (see the
// field docs). threshold 0 disables compaction.
func (n *RPCNode) SetLogCompaction(threshold, retain uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.compactionThreshold = threshold
	n.compactionRetain = retain
}

// maybeCompactLocked runs the automatic compaction policy. It only ever
// compacts the APPLIED prefix (applied implies committed on every domain the
// commit evidence covers), keeps compactionRetain entries of headroom for cheap
// backfills, and stays quiet during a migration so the frozen barrier's tail
// remains directly streamable.
func (n *RPCNode) maybeCompactLocked() {
	if n.compactionThreshold == 0 || n.migrating || n.draining {
		return
	}
	if n.state.AppliedIndex-n.state.SnapshotLastIndex <= n.compactionThreshold {
		return
	}
	if n.state.AppliedIndex <= n.compactionRetain {
		return
	}
	target := n.state.AppliedIndex - n.compactionRetain
	if target <= n.state.SnapshotLastIndex {
		return
	}
	if compactLog(&n.state, target) {
		log.Printf("cd-raft %s: compacted log through index %d (applied %d, %d entries retained)",
			n.local.ID, target, n.state.AppliedIndex, len(n.state.Log))
	}
}

func (n *RPCNode) ID() string {
	return n.local.ID
}

// AssertInvariants checks the core per-node safety invariant
// appliedIndex <= knownGlobalCommitIndex <= domainQuorumIndex[globalLeaderDomain]
// on the Global Leader, plus contiguous apply. A Fast Return origin domain may
// legitimately know a certificate before holding the global domain's full
// quorum map, so that relaxation is only allowed off the global domain.
func (n *RPCNode) AssertInvariants() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state.AppliedIndex > n.state.KnownGlobalCommitIndex {
		return fmt.Errorf("%s applied %d exceeds known commit %d", n.local.ID, n.state.AppliedIndex, n.state.KnownGlobalCommitIndex)
	}
	// Only the Global Leader maintains the per-domain quorum map, so the
	// commit-vs-quorum relation is asserted there. Followers learn the commit
	// position from notices without tracking domainQuorumIndex.
	globalDomain := n.globalLeader.DomainLeader.DomainID
	isGlobalLeader := n.globalRole == Leader && n.globalLeader.DomainLeader.NodeID == n.local.ID
	if isGlobalLeader && n.state.KnownGlobalCommitIndex > n.state.DomainQuorumIndex[globalDomain] {
		return fmt.Errorf("%s known commit %d exceeds global-domain quorum %d",
			n.local.ID, n.state.KnownGlobalCommitIndex, n.state.DomainQuorumIndex[globalDomain])
	}
	return nil
}

func (n *RPCNode) SetResponseDelays(normal, fast time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.normalDelay = normal
	n.fastDelay = fast
}

func (n *RPCNode) SetRPCPolicy(policy RPCPolicy) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rpcPolicy = policy
}

func (n *RPCNode) SetNetworkSimulation(simulation topology.NetworkSimulation) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.networkSimulation = simulation
}

func (n *RPCNode) SetFloatingDomains(domains []string, allowUnknown bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.config.Features.FloatingDomains = append([]string(nil), domains...)
	n.config.Features.AllowUnknownFloatingDomains = allowUnknown
}

func (n *RPCNode) SetWriteTraceRecorder(recorder *WriteTraceRecorder) {
	n.traceMu.Lock()
	n.writeTrace = recorder
	n.traceMu.Unlock()
}

func (n *RPCNode) traceRecorder() *WriteTraceRecorder {
	n.traceMu.RLock()
	recorder := n.writeTrace
	n.traceMu.RUnlock()
	return recorder
}

func (n *RPCNode) traceStart(requestID, phase string, peerDomain string) func(...string) {
	return n.traceRecorder().Start(WriteTraceEvent{
		RequestID: requestID, Component: "server", Phase: phase,
		NodeID: n.local.ID, DomainID: n.local.DomainCode, PeerDomain: peerDomain,
	})
}

func (n *RPCNode) traceMark(requestID, phase, peerDomain, detail string) {
	n.traceRecorder().Mark(WriteTraceEvent{
		RequestID: requestID, Component: "server", Phase: phase,
		NodeID: n.local.ID, DomainID: n.local.DomainCode, PeerDomain: peerDomain, Detail: detail,
	})
}

// SetPartitionedFrom replaces this node's outbound partition set: it can no
// longer reach any peer whose node ID is listed (callNode returns Unavailable,
// as if the link were cut). It only affects inter-node RPCs, not client-facing
// calls, so a test client can still inspect any node's Status. A partition is
// inherently directional; for a full split, set the complementary block on the
// peers too.
func (n *RPCNode) SetPartitionedFrom(peerIDs ...string) {
	n.faultMu.Lock()
	defer n.faultMu.Unlock()
	n.blockedPeers = make(map[string]bool, len(peerIDs))
	for _, id := range peerIDs {
		n.blockedPeers[id] = true
	}
}

// HealPartition clears all injected outbound partitions on this node.
func (n *RPCNode) HealPartition() {
	n.faultMu.Lock()
	defer n.faultMu.Unlock()
	n.blockedPeers = make(map[string]bool)
}

// SetDropRate sets the probability [0,1] that any single outbound RPC attempt is
// dropped (returns Unavailable). It models a lossy link; because it is applied
// per attempt, RPC/client retries can still get through, so the cluster should
// remain live (just slower) under a moderate rate.
func (n *RPCNode) SetDropRate(rate float64) {
	n.faultMu.Lock()
	defer n.faultMu.Unlock()
	n.dropRate = rate
}

// linkBlockedLocked reports whether peerID is currently partitioned from us.
func (n *RPCNode) linkBlocked(peerID string) bool {
	n.faultMu.Lock()
	defer n.faultMu.Unlock()
	return n.blockedPeers[peerID]
}

// dropAttempt rolls the per-attempt packet-loss dice.
func (n *RPCNode) dropAttempt() bool {
	n.faultMu.Lock()
	rate := n.dropRate
	n.faultMu.Unlock()
	if rate <= 0 {
		return false
	}
	return rand.Float64() < rate
}

func (n *RPCNode) SetFastReturnEnabled(enabled bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.fastReturnEnabled = enabled
}

// SetStatsWindow tunes the per-domain write/read smoothing window exposed via
// the Metrics RPC, which smooths W_i/R_i over a longer horizon so the external
// controller's recommendation does not flap with bursty load.
func (n *RPCNode) SetStatsWindow(window time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stats.window = window
}

// SetCatchUpTimeout bounds the pre-handoff catch-up phase of a safe Global
// Leader migration. If the target cannot reach the migration barrier within
// this budget, the migration aborts WITHOUT fencing the incumbent.
func (n *RPCNode) SetCatchUpTimeout(timeout time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.catchUpTimeout = timeout
}

func (n *RPCNode) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.server != nil {
		return nil
	}
	clientBind := n.local.ClientBindAddress()
	clientListener, err := net.Listen("tcp", clientBind)
	if err != nil {
		return fmt.Errorf("listen client address %s: %w", clientBind, err)
	}
	interDomainBind := n.local.InterDomainBindAddress()
	interDomainListener, err := net.Listen("tcp", interDomainBind)
	if err != nil {
		_ = clientListener.Close()
		return fmt.Errorf("listen inter-domain address %s: %w", interDomainBind, err)
	}
	n.server = grpc.NewServer()
	cdraftv1.RegisterDomainElectionServer(n.server, &domainElectionService{node: n})
	cdraftv1.RegisterGlobalElectionServer(n.server, &globalElectionService{node: n})
	cdraftv1.RegisterReplicationServer(n.server, n)
	cdraftv1.RegisterClientServer(n.server, n)
	cdraftv1.RegisterTelemetryServer(n.server, &telemetryService{node: n})
	cdraftv1.RegisterMetricsServer(n.server, &metricsService{node: n})
	n.listeners = []net.Listener{clientListener, interDomainListener}
	n.stage = DomainElecting
	n.resetDomainDeadlineLocked()
	n.resetGlobalDeadlineLocked()
	for _, listener := range n.listeners {
		go func(listener net.Listener) {
			_ = n.server.Serve(listener)
		}(listener)
	}
	return nil
}

func (n *RPCNode) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	server := n.server
	listeners := append([]net.Listener(nil), n.listeners...)
	conns := make([]*grpc.ClientConn, 0, len(n.conns))
	for _, conn := range n.conns {
		conns = append(conns, conn)
	}
	n.mu.Unlock()
	if server != nil {
		server.GracefulStop()
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (n *RPCNode) Run(ctx context.Context) {
	// The live runtime maintains heartbeats, so it can (and must) enforce the
	// Global Leader read lease.
	n.SetGlobalLeaseEnabled(true)
	n.SetDomainLeaseEnabled(true)
	// Startup assertion: refuse to run with lease/election timing that could
	// allow two leaders to serve at once. This is a deploy-time misconfiguration,
	// so fail fast rather than silently risk a split brain.
	n.mu.Lock()
	globalLease, domainLease := n.globalLeaseDuration, n.domainLeaseDuration
	n.mu.Unlock()
	if err := validateLeaseTiming(globalLease, domainLease); err != nil {
		panic(fmt.Sprintf("cd-raft node %s: unsafe lease/election timing: %v", n.local.ID, err))
	}
	go n.bootstrapLoop(ctx)
	go n.heartbeatLoop(ctx)
	go n.failureDetectorLoop(ctx)
	go n.discoveryLoop(ctx)
	go n.telemetryLoop(ctx)
	go n.reconfirmLoop(ctx)
	go n.recommitLoop(ctx)
	<-ctx.Done()
	n.Stop()
}

func (n *RPCNode) bootstrapLoop(ctx context.Context) {
	delay := randomizedElectionDelay()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	for ctx.Err() == nil {
		n.mu.Lock()
		domainLeader := n.domainLeaders[n.local.DomainCode]
		isDomainLeader := domainLeader.NodeID == n.local.ID && n.domainRole == Leader
		hasGlobal := n.globalLeader.Valid()
		n.mu.Unlock()
		if !domainLeader.Valid() {
			_ = n.CampaignDomain(ctx)
		} else if isDomainLeader && !hasGlobal {
			select {
			case <-ctx.Done():
				return
			case <-time.After(randomizedElectionDelay()):
			}
			n.mu.Lock()
			stillNeeded := !n.globalLeader.Valid() && n.domainRole == Leader
			n.mu.Unlock()
			if stillNeeded {
				_ = n.CampaignGlobal(ctx)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(randomizedElectionDelay()):
		}
	}
}

func (n *RPCNode) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.sendHeartbeats(ctx)
		}
	}
}

// reconfirmLoop periodically heals a Domain Leader's in-domain quorum mark when
// it lags its own log. This is the common state right after a domain re-election
// (followers never advance DomainQuorumIndex, so a new leader inherits a mark
// behind its already-replicated log). Left unhealed, the lag silently degrades
// Fast Return for the domain (the responder gate `DomainQuorumIndex >= index`
// fails) and breaks GL migration catch-up, until the next client write happens
// to drive a fresh in-domain quorum. The heal only ever advances the mark to an
// index a live in-domain majority demonstrably holds, so it cannot violate any
// safety invariant.
func (n *RPCNode) reconfirmLoop(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.reconfirmDomainQuorum(ctx)
		}
	}
}

// recommitLoop lets a restarted/re-elected Global Leader reconstruct a committed
// prefix from the durable evidence Fast Return relies on: the current GL domain
// has reached in-domain quorum for an index, and at least one other consensus
// domain has durably recorded the same prefix in its DomainQuorumIndex.
func (n *RPCNode) recommitLoop(ctx context.Context) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.recoverCommittedPrefix(ctx)
		}
	}
}

// reconfirmDomainQuorum re-drives the in-domain quorum for this Domain Leader's
// highest log entry and, on majority, advances DomainQuorumIndex up to it. The
// log is strictly append-only (appendEntry enforces GlobalIndex == len+1), so a
// majority holding index N provably holds the whole prefix 1..N — advancing the
// high-water mark to N is therefore safe. It is a no-op (cheap, no RPC) whenever
// the mark already covers the log, which is the steady state under normal writes.
func (n *RPCNode) reconfirmDomainQuorum(ctx context.Context) {
	if n.reconfirmDisabled.Load() {
		return
	}
	// Single heal round at a time: a slow follower must not let rounds pile up.
	if !n.reconfirmInFlight.CompareAndSwap(false, true) {
		return
	}
	defer n.reconfirmInFlight.Store(false)

	n.mu.Lock()
	if n.stopped || n.domainRole != Leader || n.state.DomainLeader.NodeID != n.local.ID {
		n.mu.Unlock()
		return
	}
	domain := n.local.DomainCode
	last := n.state.Summary().LastGlobalIndex
	if last <= n.state.DomainQuorumIndex[domain] {
		n.mu.Unlock()
		return
	}
	entry, ok := findEntryByIndex(n.state.Log, last)
	if !ok {
		n.mu.Unlock()
		return
	}
	var prevTerm uint64
	if last > 1 {
		if prev, found := findEntryByIndex(n.state.Log, last-1); found {
			prevTerm = prev.GlobalTerm
		} else if last-1 == n.state.SnapshotLastIndex {
			prevTerm = n.state.SnapshotLastTerm
		}
	}
	members := n.config.DomainMembers(domain)
	n.mu.Unlock()

	followerReq := &cdraftv1.ReplicateEntry{
		GlobalTerm: entry.GlobalTerm, GlobalIndex: entry.GlobalIndex,
		RequestId: entry.RequestID, OriginDomain: entry.OriginDomain,
		Key: entry.Command.Key, Value: entry.Command.Value,
		SenderNodeId: n.local.ID, FanoutToDomain: false,
		PrevLogIndex: last - 1, PrevLogTerm: prevTerm,
	}
	acks := 1
	var ackMu sync.Mutex
	var wait sync.WaitGroup
	for _, member := range members {
		if member.ID == n.local.ID {
			continue
		}
		wait.Add(1)
		go func(peerID string) {
			defer wait.Done()
			_ = n.callNode(ctx, peerID, func(callCtx context.Context, conn *grpc.ClientConn) error {
				resp, err := cdraftv1.NewReplicationClient(conn).Replicate(callCtx, followerReq)
				if err == nil && resp.GetQuorum() {
					ackMu.Lock()
					acks++
					ackMu.Unlock()
				}
				return err
			})
		}(member.ID)
	}
	wait.Wait()
	if acks < majority(len(members)) {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	// Re-check under lock: we must still be the leader and the entry at `last`
	// must still be the one we confirmed (an overwrite would invalidate the ack).
	if n.domainRole != Leader || n.state.DomainLeader.NodeID != n.local.ID {
		return
	}
	if cur, found := findEntryByIndex(n.state.Log, last); !found ||
		cur.GlobalTerm != entry.GlobalTerm || cur.RequestID != entry.RequestID {
		return
	}
	if last <= n.state.DomainQuorumIndex[domain] {
		return
	}
	n.state.DomainQuorumIndex[domain] = last
	_ = n.persistLocked()
}

func (n *RPCNode) recoverCommittedPrefix(ctx context.Context) {
	if !n.recommitInFlight.CompareAndSwap(false, true) {
		return
	}
	defer n.recommitInFlight.Store(false)

	n.mu.Lock()
	if n.stopped || !n.servesAsGlobalLeaderLocked() {
		n.mu.Unlock()
		return
	}
	globalDomain := n.local.DomainCode
	last := n.state.Summary().LastGlobalIndex
	if last == 0 || n.state.DomainQuorumIndex[globalDomain] <= n.state.KnownGlobalCommitIndex {
		n.mu.Unlock()
		return
	}
	known := make(map[string]uint64, len(n.state.DomainQuorumIndex))
	for domain, index := range n.state.DomainQuorumIndex {
		known[domain] = index
	}
	leaders := make([]DomainLeaderIdentity, 0, len(n.domainLeaders))
	for _, leader := range n.domainLeaders {
		if leader.Valid() && n.config.IsConsensusDomain(leader.DomainID) {
			leaders = append(leaders, leader)
		}
	}
	n.mu.Unlock()

	for _, leader := range leaders {
		if leader.NodeID == n.local.ID {
			continue
		}
		resp, err := n.sendCatchUp(ctx, leader.NodeID, last, nil)
		if err != nil {
			continue
		}
		if idx := resp.GetDomainQuorumIndex(); idx > known[leader.DomainID] {
			known[leader.DomainID] = idx
		}
	}

	n.mu.Lock()
	if n.stopped || !n.servesAsGlobalLeaderLocked() {
		n.mu.Unlock()
		return
	}
	changed := false
	for domain, index := range known {
		if !n.config.IsConsensusDomain(domain) {
			continue
		}
		if index > n.state.DomainQuorumIndex[domain] {
			n.state.DomainQuorumIndex[domain] = index
			changed = true
		}
	}
	globalHeld := n.state.DomainQuorumIndex[globalDomain]
	var otherHeld uint64
	for domain, index := range n.state.DomainQuorumIndex {
		if domain == globalDomain || !n.config.IsConsensusDomain(domain) {
			continue
		}
		if index > otherHeld {
			otherHeld = index
		}
	}
	target := min(globalHeld, otherHeld)
	for target > n.state.KnownGlobalCommitIndex {
		if _, ok := findEntryByIndex(n.state.Log, target); ok {
			break
		}
		target--
	}
	if target <= n.state.KnownGlobalCommitIndex {
		if changed {
			_ = n.persistLocked()
		}
		n.mu.Unlock()
		return
	}
	entry, _ := findEntryByIndex(n.state.Log, target)
	if !n.advanceCommitLocked(target, entry.GlobalTerm, entry.RequestID) {
		if changed {
			_ = n.persistLocked()
		}
		n.mu.Unlock()
		return
	}
	evidence := make(map[string]bool)
	for domain, index := range n.state.DomainQuorumIndex {
		if n.config.IsConsensusDomain(domain) && index >= target {
			evidence[domain] = true
		}
	}
	if err := n.persistLocked(); err != nil {
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	n.commitAcrossDomains(context.Background(), entry, evidence)
}

// SetDomainQuorumReconfirmEnabled toggles the periodic in-domain quorum self-heal
// (enabled by default). Tests that deliberately exercise a wedged quorum mark
// disable it so the heal does not race with the behavior under test.
func (n *RPCNode) SetDomainQuorumReconfirmEnabled(enabled bool) {
	n.reconfirmDisabled.Store(!enabled)
}

func (n *RPCNode) failureDetectorLoop(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			n.mu.Lock()
			if n.stopped {
				n.mu.Unlock()
				continue
			}
			n.maybeStepDownGlobalOnExpiredDomainLeaseLocked(now)
			n.maybeStepDownExpiredGlobalLeaseLocked(now)
			needDomainElection := n.domainRole != Leader && !n.domainCampaigning && now.After(n.domainDeadline)
			needGlobalElection := n.needsGlobalElectionLocked(now)
			if needGlobalElection {
				log.Printf("cd-raft %s: global deadline expired (term %d, no GL); re-campaigning", n.local.ID, n.state.GlobalTerm)
			}
			if needDomainElection {
				delete(n.domainLeaders, n.local.DomainCode)
				n.state.DomainLeader = DomainLeaderIdentity{}
				n.stage = DomainElecting
				n.resetDomainDeadlineLocked()
			}
			if needGlobalElection {
				n.globalLeader = GlobalLeaderIdentity{}
				n.state.GlobalLeader = GlobalLeaderIdentity{}
				n.stage = GlobalElecting
				n.resetGlobalDeadlineLocked()
			}
			_ = n.persistLocked()
			n.mu.Unlock()
			if needDomainElection {
				go func() { _ = n.CampaignDomain(ctx) }()
			}
			if needGlobalElection {
				go func() { _ = n.CampaignGlobal(ctx) }()
			}
		}
	}
}

func (n *RPCNode) sendHeartbeats(ctx context.Context) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	domainIdentity := n.state.DomainLeader
	globalIdentity := n.globalLeader
	isDomainLeader := n.domainRole == Leader && domainIdentity.NodeID == n.local.ID
	isGlobalLeader := n.globalRole == Leader && globalIdentity.DomainLeader.NodeID == n.local.ID
	summary := n.state.Summary()
	commit := n.state.KnownGlobalCommitIndex
	var commitTerm uint64
	var commitReqID string
	if e, ok := findEntryByIndex(n.state.Log, commit); ok {
		commitTerm = e.GlobalTerm
		commitReqID = e.RequestID
	} else if commit > 0 && commit == n.state.SnapshotLastIndex {
		// The committed boundary entry was compacted; its identity survives in
		// the snapshot metadata so the piggybacked commit stays verifiable.
		commitTerm = n.state.SnapshotLastTerm
		commitReqID = n.state.SnapshotLastRequestID
	}
	n.mu.Unlock()
	if isDomainLeader {
		sentAt := time.Now()
		members := n.config.DomainMembers(n.local.DomainCode)
		// Collect in-domain acks off the ticker: a majority (counting ourselves)
		// refreshes the Domain Leader lease; any response carrying a higher
		// DomainTerm means a newer Domain Leader exists, so we step down.
		go func() {
			acks := int32(1) // self
			var wg sync.WaitGroup
			for _, member := range members {
				if member.ID == n.local.ID {
					continue
				}
				member := member
				wg.Add(1)
				go func() {
					defer wg.Done()
					var resp *cdraftv1.DomainVoteResponse
					err := n.callNode(ctx, member.ID, func(callCtx context.Context, conn *grpc.ClientConn) error {
						var callErr error
						resp, callErr = cdraftv1.NewDomainElectionClient(conn).Heartbeat(callCtx, &cdraftv1.DomainHeartbeat{
							Leader: toPBDomainIdentity(domainIdentity),
							Log:    toPBLogSummary(summary),
						})
						return callErr
					})
					if err == nil && resp.GetGranted() {
						atomic.AddInt32(&acks, 1)
					}
					if resp != nil && resp.GetDomainTerm() > domainIdentity.DomainTerm {
						n.observeHigherDomainTerm(resp.GetDomainTerm())
					}
				}()
			}
			wg.Wait()
			if int(atomic.LoadInt32(&acks)) >= majority(len(members)) {
				n.mu.Lock()
				if n.domainRole == Leader && n.state.DomainLeader.NodeID == n.local.ID {
					n.refreshDomainLeaseLocked(sentAt)
				}
				n.mu.Unlock()
			}
		}()
	}
	if isGlobalLeader {
		sentAt := time.Now()
		leaders := n.domainLeaderSnapshot()
		threshold := globalElectionThreshold(len(n.config.Domains()))
		// Collect acks off the heartbeat ticker so a slow/cross-domain round does
		// not stall the next tick. A quorum of grants refreshes the read lease;
		// any response carrying a higher global term steps us down.
		go func() {
			// NOTE: we deliberately also heartbeat ourselves. The Global Leader's
			// own domain followers learn the leader (and cross the Serving gate)
			// because HeartbeatGlobalInternal fans the heartbeat out to domain
			// members. The self-call grants, so acks starts at 0 here.
			var acks int32
			var wg sync.WaitGroup
			for _, identity := range leaders {
				identity := identity
				wg.Add(1)
				go func() {
					defer wg.Done()
					var resp *cdraftv1.GlobalVoteResponse
					err := n.callNode(ctx, identity.NodeID, func(callCtx context.Context, conn *grpc.ClientConn) error {
						var callErr error
						resp, callErr = cdraftv1.NewGlobalElectionClient(conn).Heartbeat(callCtx, &cdraftv1.GlobalHeartbeat{
							Leader:          toPBDomainIdentity(globalIdentity.DomainLeader),
							GlobalTerm:      globalIdentity.GlobalTerm,
							CommitIndex:     commit,
							CommitTerm:      commitTerm,
							CommitRequestId: commitReqID,
						})
						return callErr
					})
					// Count the confirmation only if it carries the identity of the
					// exact Domain Leader we addressed; a response from a node that
					// has since lost leadership (different domain term) must not
					// refresh our lease.
					if err == nil && resp.GetGranted() && fromPBDomainIdentity(resp.GetVoter()) == identity {
						atomic.AddInt32(&acks, 1)
					}
					if resp != nil && resp.GetGlobalTerm() > globalIdentity.GlobalTerm {
						n.observeHigherGlobalTerm(resp.GetGlobalTerm())
					}
				}()
			}
			wg.Wait()
			if int(atomic.LoadInt32(&acks)) >= threshold {
				n.mu.Lock()
				if n.globalRole == Leader &&
					n.globalLeader.DomainLeader.NodeID == n.local.ID &&
					n.globalLeader.GlobalTerm == globalIdentity.GlobalTerm {
					n.refreshGlobalLeaseLocked(sentAt)
				}
				n.mu.Unlock()
			}
		}()
	}
}

func (n *RPCNode) CampaignDomain(ctx context.Context) error {
	n.mu.Lock()
	if n.stage == Booting || n.stopped {
		n.mu.Unlock()
		return ErrNotServing
	}
	if n.domainCampaigning {
		n.mu.Unlock()
		return ErrNoDomainQuorum
	}
	n.domainCampaigning = true
	defer func() {
		n.mu.Lock()
		n.domainCampaigning = false
		n.resetDomainDeadlineLocked()
		n.mu.Unlock()
	}()
	term, summary := transitionStartDomainCampaign(&n.state, &n.domainRole, n.local.ID)
	if err := n.persistLocked(); err != nil {
		// The bumped domain term is not durable; abort rather than solicit votes
		// for a term we might lose on crash.
		n.mu.Unlock()
		return ErrNoDomainQuorum
	}
	n.mu.Unlock()

	votes := 1
	var votesMu sync.Mutex
	var wait sync.WaitGroup
	for _, member := range n.config.DomainMembers(n.local.DomainCode) {
		if member.ID == n.local.ID {
			continue
		}
		wait.Add(1)
		go func(peerID string) {
			defer wait.Done()
			var response *cdraftv1.DomainVoteResponse
			err := n.callNode(ctx, peerID, func(callCtx context.Context, conn *grpc.ClientConn) error {
				var err error
				response, err = cdraftv1.NewDomainElectionClient(conn).RequestVote(callCtx, &cdraftv1.DomainVoteRequest{
					DomainId:    n.local.DomainCode,
					CandidateId: n.local.ID,
					DomainTerm:  term,
					Log:         toPBLogSummary(summary),
				})
				return err
			})
			if err == nil && response.GetGranted() {
				votesMu.Lock()
				votes++
				votesMu.Unlock()
			}
			if response != nil && response.GetDomainTerm() > term {
				n.observeHigherDomainTerm(response.GetDomainTerm())
			}
		}(member.ID)
	}
	wait.Wait()
	if votes < majority(len(n.config.DomainMembers(n.local.DomainCode))) {
		return ErrNoDomainQuorum
	}

	identity := DomainLeaderIdentity{DomainID: n.local.DomainCode, NodeID: n.local.ID, DomainTerm: term}
	n.mu.Lock()
	if n.state.DomainTerm != term {
		n.mu.Unlock()
		return ErrNoDomainQuorum
	}
	transitionBecomeDomainLeader(&n.state, &n.domainRole, identity)
	n.stage = DomainReady
	n.domainLeaders[identity.DomainID] = identity
	n.resetDomainDeadlineLocked()
	// A freshly elected Domain Leader just confirmed an in-domain majority, so
	// seed its lease; the heartbeat loop renews it thereafter.
	n.refreshDomainLeaseLocked(time.Now())
	_ = n.persistLocked()
	n.mu.Unlock()
	n.publishDomainReady(ctx, identity, summary)
	return nil
}

func (n *RPCNode) publishDomainReady(ctx context.Context, identity DomainLeaderIdentity, summary LogSummary) {
	n.broadcastDomainReady(ctx, identity, summary)
	n.mu.Lock()
	n.stage = GlobalElecting
	n.mu.Unlock()
}

// broadcastDomainReady announces this Domain Leader's identity to every other
// node (across all domains) so cross-domain peers can register it. It does NOT
// touch local stage, so it is safe to call repeatedly from the discovery loop.
func (n *RPCNode) broadcastDomainReady(ctx context.Context, identity DomainLeaderIdentity, summary LogSummary) {
	for _, peer := range n.config.Nodes {
		if peer.ID == n.local.ID {
			continue
		}
		_ = n.callNode(ctx, peer.ID, func(callCtx context.Context, conn *grpc.ClientConn) error {
			_, err := cdraftv1.NewDomainElectionClient(conn).PublishReady(callCtx, &cdraftv1.DomainLeaderReady{
				Leader: toPBDomainIdentity(identity),
				Log:    toPBLogSummary(summary),
			})
			return err
		})
	}
}

// discoveryLoop periodically re-announces this node's Domain Leadership to every
// other node. The original one-shot announce at election time races with peer
// boot: a domain that starts before its peers can never be discovered by them
// (and the cross-domain vote handler rejects votes for unknown candidates, while
// the global heartbeat handler rejects heartbeats from an unknown Global Leader).
// That deadlocks Global Leader convergence — or worse, lets two halves that never
// learned each other elect rival Global Leaders. The announce keeps running for
// the whole leadership term, not just until *we* know everyone: a leader knowing
// all peers does NOT imply all peers know it, so it must keep teaching them until
// it is no longer the leader. The handler is idempotent (guarded by DomainTerm),
// so re-announcing is safe, and the messages are tiny.
func (n *RPCNode) discoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			identity := n.state.DomainLeader
			isDomainLeader := !n.stopped && n.domainRole == Leader && identity.NodeID == n.local.ID
			summary := n.state.Summary()
			n.mu.Unlock()
			if isDomainLeader {
				n.broadcastDomainReady(ctx, identity, summary)
			}
		}
	}
}

func (n *RPCNode) CampaignGlobal(ctx context.Context) error {
	return n.campaignGlobal(ctx, false)
}

// campaignGlobal runs a global election. When transfer is true the vote requests
// carry the leadership-transfer flag, which lets voters bypass leader-stickiness
// (used only by a deliberate, incumbent-initiated migration handoff).
func (n *RPCNode) campaignGlobal(ctx context.Context, transfer bool) error {
	n.mu.Lock()
	candidate := n.domainLeaders[n.local.DomainCode]
	if candidate.NodeID != n.local.ID || candidate.DomainTerm != n.state.DomainTerm || n.domainRole != Leader {
		n.mu.Unlock()
		return ErrNotDomainLeader
	}
	// A domain's single global vote may only be cast while we can confirm an
	// in-domain majority. Otherwise an old, majority-less Domain Leader could
	// campaign in parallel with a freshly elected one — two global votes from one
	// domain. (Always true under the manual harness, where the lease is off.)
	if !n.domainLeaseFreshLocked() {
		n.mu.Unlock()
		return ErrNotDomainLeader
	}
	if n.globalCampaigning {
		n.mu.Unlock()
		return ErrNoGlobalQuorum
	}
	// Pre-vote guard: only start a campaign (and bump the global term) once we
	// already know enough distinct Domain Leaders to possibly gather the N-1
	// votes. A domain that boots before its peers would otherwise re-campaign
	// forever while alone, inflating globalTerm unboundedly and poisoning later
	// convergence once the other domains arrive. We back off the deadline so the
	// bootstrap loop re-checks at the normal cadence as discovery fills in.
	knownLeaders := 0
	for _, id := range n.domainLeaders {
		if id.Valid() {
			knownLeaders++
		}
	}
	if knownLeaders < globalElectionThreshold(len(n.config.Domains())) {
		n.resetGlobalDeadlineLocked()
		n.mu.Unlock()
		return ErrNoGlobalQuorum
	}
	n.globalCampaigning = true
	defer func() {
		n.mu.Lock()
		n.globalCampaigning = false
		n.resetGlobalDeadlineLocked()
		n.mu.Unlock()
	}()
	term, summary := transitionStartGlobalCampaign(&n.state, &n.globalRole, candidate)
	n.globalLeader = GlobalLeaderIdentity{}
	if err := n.persistLocked(); err != nil {
		// The bumped global term is not durable; abort the campaign.
		n.mu.Unlock()
		return ErrNoGlobalQuorum
	}
	n.mu.Unlock()

	identities := n.domainLeaderSnapshot()
	votes := 1
	var votesMu sync.Mutex
	var wait sync.WaitGroup
	for _, identity := range identities {
		if identity.NodeID == n.local.ID {
			continue
		}
		wait.Add(1)
		go func(voter DomainLeaderIdentity) {
			defer wait.Done()
			var response *cdraftv1.GlobalVoteResponse
			err := n.callNode(ctx, voter.NodeID, func(callCtx context.Context, conn *grpc.ClientConn) error {
				var err error
				response, err = cdraftv1.NewGlobalElectionClient(conn).RequestVote(callCtx, &cdraftv1.GlobalVoteRequest{
					Candidate:          toPBDomainIdentity(candidate),
					GlobalTerm:         term,
					Log:                toPBLogSummary(summary),
					LeadershipTransfer: transfer,
				})
				return err
			})
			if err == nil && response.GetGranted() {
				votesMu.Lock()
				votes++
				votesMu.Unlock()
			}
			if response != nil && response.GetGlobalTerm() > term {
				n.observeHigherGlobalTerm(response.GetGlobalTerm())
			}
		}(identity)
	}
	wait.Wait()
	if votes < globalElectionThreshold(len(n.config.Domains())) {
		return ErrNoGlobalQuorum
	}

	leader := GlobalLeaderIdentity{DomainLeader: candidate, GlobalTerm: term}
	n.mu.Lock()
	if n.state.GlobalTerm != term || n.domainRole != Leader {
		n.mu.Unlock()
		return ErrNoGlobalQuorum
	}
	transitionBecomeGlobalLeader(&n.state, &n.globalRole, &n.stage, leader)
	n.globalLeader = leader
	n.resetGlobalDeadlineLocked()
	// Winning required a quorum of Domain Leader votes just now, which is the
	// same evidence a heartbeat round provides, so grant the initial lease.
	n.refreshGlobalLeaseLocked(time.Now())
	_ = n.persistLocked()
	n.mu.Unlock()
	n.sendHeartbeats(ctx)
	return nil
}

func (n *RPCNode) RequestVote(_ context.Context, request *cdraftv1.DomainVoteRequest) (*cdraftv1.DomainVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if request.GetDomainId() != n.local.DomainCode || n.stage == Booting {
		return &cdraftv1.DomainVoteResponse{DomainTerm: n.state.DomainTerm}, nil
	}
	response, resetDeadline := transitionDomainVote(&n.state, &n.domainRole, DomainVoteRequest{
		DomainID: request.GetDomainId(), Candidate: request.GetCandidateId(),
		DomainTerm: request.GetDomainTerm(), Log: fromPBLogSummary(request.GetLog()),
	})
	if resetDeadline {
		n.resetDomainDeadlineLocked()
	}
	// votedFor must be durable before we grant: a vote acknowledged but lost on
	// crash could let the same term elect two leaders.
	if err := n.persistLocked(); err != nil {
		return &cdraftv1.DomainVoteResponse{DomainTerm: n.state.DomainTerm}, nil
	}
	return &cdraftv1.DomainVoteResponse{DomainTerm: response.DomainTerm, Granted: response.Granted}, nil
}

func (n *RPCNode) Heartbeat(_ context.Context, request *cdraftv1.DomainHeartbeat) (*cdraftv1.DomainVoteResponse, error) {
	identity := fromPBDomainIdentity(request.GetLeader())
	n.mu.Lock()
	defer n.mu.Unlock()
	if identity.DomainID != n.local.DomainCode ||
		!transitionAcceptDomainLeader(&n.state, &n.domainRole, n.local.ID, identity) {
		return &cdraftv1.DomainVoteResponse{DomainTerm: n.state.DomainTerm}, nil
	}
	n.domainLeaders[identity.DomainID] = identity
	n.resetDomainDeadlineLocked()
	if n.stage != Serving {
		n.stage = GlobalElecting
	}
	if err := n.persistLocked(); err != nil {
		return &cdraftv1.DomainVoteResponse{DomainTerm: n.state.DomainTerm}, nil
	}
	return &cdraftv1.DomainVoteResponse{DomainTerm: n.state.DomainTerm, Granted: true}, nil
}

func (n *RPCNode) PublishReady(_ context.Context, request *cdraftv1.DomainLeaderReady) (*cdraftv1.DomainVoteResponse, error) {
	identity := fromPBDomainIdentity(request.GetLeader())
	if !identity.Valid() {
		return nil, status.Error(codes.InvalidArgument, "invalid domain leader identity")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	current := n.domainLeaders[identity.DomainID]
	if current.Valid() && current.DomainTerm > identity.DomainTerm {
		return &cdraftv1.DomainVoteResponse{DomainTerm: current.DomainTerm}, nil
	}
	if identity.DomainID == n.local.DomainCode {
		if !transitionAcceptDomainLeader(&n.state, &n.domainRole, n.local.ID, identity) {
			return &cdraftv1.DomainVoteResponse{DomainTerm: n.state.DomainTerm}, nil
		}
		n.resetDomainDeadlineLocked()
	}
	n.domainLeaders[identity.DomainID] = identity
	if n.stage != Serving {
		n.stage = GlobalElecting
	}
	if err := n.persistLocked(); err != nil {
		return &cdraftv1.DomainVoteResponse{DomainTerm: identity.DomainTerm}, nil
	}
	return &cdraftv1.DomainVoteResponse{DomainTerm: identity.DomainTerm, Granted: true}, nil
}

func (n *RPCNode) RequestVoteGlobal(ctx context.Context, request *cdraftv1.GlobalVoteRequest) (*cdraftv1.GlobalVoteResponse, error) {
	return n.RequestVoteGlobalInternal(ctx, request)
}

// RequestVote implements GlobalElectionServer.RequestVote. It is named by the
// generated interface exactly like the domain method, so Go cannot implement
// both services directly on one type. rpcService below provides the adapters.
func (n *RPCNode) RequestVoteGlobalInternal(_ context.Context, request *cdraftv1.GlobalVoteRequest) (*cdraftv1.GlobalVoteResponse, error) {
	candidate := fromPBDomainIdentity(request.GetCandidate())
	n.mu.Lock()
	defer n.mu.Unlock()
	selfIdentity := n.domainLeaders[n.local.DomainCode]
	if n.domainRole != Leader || selfIdentity.NodeID != n.local.ID || n.domainLeaders[candidate.DomainID] != candidate {
		return &cdraftv1.GlobalVoteResponse{GlobalTerm: n.state.GlobalTerm, Voter: toPBDomainIdentity(selfIdentity)}, nil
	}
	// Only a Domain Leader that can still confirm its in-domain majority may
	// contribute its domain's global vote (see campaignGlobal). A majority-less
	// (e.g. partitioned) leader must abstain so it cannot double-vote alongside a
	// newly elected leader of the same domain.
	if !n.domainLeaseFreshLocked() {
		n.recordRejectionLocked("global_vote_no_domain_majority")
		return &cdraftv1.GlobalVoteResponse{GlobalTerm: n.state.GlobalTerm, Voter: toPBDomainIdentity(selfIdentity)}, nil
	}
	if request.GetGlobalTerm() < n.state.GlobalTerm {
		return &cdraftv1.GlobalVoteResponse{GlobalTerm: n.state.GlobalTerm, Voter: toPBDomainIdentity(selfIdentity)}, nil
	}
	// Leader stickiness (Raft thesis §9.6): if we still recognize a valid Global
	// Leader and have heard from it within the election timeout, refuse to grant
	// a competing vote — even at a higher term — WITHOUT advancing our term. This
	// protects the incumbent's read lease so two leaders can never overlap. A
	// deliberate leadership transfer (issued by the incumbent during migration)
	// is exempt, as is a re-request from the leader we already follow.
	if !request.GetLeadershipTransfer() &&
		n.globalLeader.Valid() &&
		n.globalLeader.DomainLeader != candidate &&
		time.Since(n.lastGlobalContact) < globalStickyWindow {
		n.recordRejectionLocked("global_vote_sticky")
		return &cdraftv1.GlobalVoteResponse{GlobalTerm: n.state.GlobalTerm, Voter: toPBDomainIdentity(selfIdentity)}, nil
	}
	response, resetDeadline, higherTerm := transitionGlobalVote(&n.state, &n.globalRole, &n.stage, GlobalVoteRequest{
		Candidate: candidate, GlobalTerm: request.GetGlobalTerm(), Log: fromPBLogSummary(request.GetLog()),
	})
	if higherTerm {
		n.globalLeader = GlobalLeaderIdentity{}
	}
	if resetDeadline {
		// Granting a vote means an election is in progress; hold off our own
		// campaign so concurrent same-window candidacies don't churn the term.
		n.resetGlobalDeadlineLocked()
	}
	// The global vote (votedFor + term) must be durable before we grant it.
	if err := n.persistLocked(); err != nil {
		return &cdraftv1.GlobalVoteResponse{GlobalTerm: n.state.GlobalTerm, Voter: toPBDomainIdentity(selfIdentity)}, nil
	}
	return &cdraftv1.GlobalVoteResponse{
		GlobalTerm: response.GlobalTerm,
		Granted:    response.Granted,
		Voter:      toPBDomainIdentity(selfIdentity),
	}, nil
}

func (n *RPCNode) HeartbeatGlobalInternal(ctx context.Context, request *cdraftv1.GlobalHeartbeat) (*cdraftv1.GlobalVoteResponse, error) {
	leaderIdentity := fromPBDomainIdentity(request.GetLeader())
	n.mu.Lock()
	known := n.domainLeaders[leaderIdentity.DomainID]
	if known != leaderIdentity || request.GetGlobalTerm() < n.state.GlobalTerm {
		term := n.state.GlobalTerm
		n.mu.Unlock()
		return &cdraftv1.GlobalVoteResponse{GlobalTerm: term}, nil
	}
	leader := GlobalLeaderIdentity{DomainLeader: leaderIdentity, GlobalTerm: request.GetGlobalTerm()}
	transitionAcceptGlobalLeader(&n.state, &n.globalRole, &n.stage, n.local.ID, leader)
	n.globalLeader = leader
	n.resetGlobalDeadlineLocked()
	n.lastGlobalContact = time.Now()
	// Honor the commit index the Global Leader piggybacks on its heartbeat. A
	// node that missed the final CommitNotice (lost packet) would otherwise stall
	// its apply/commit progress until the next write; the heartbeat lets it catch
	// up purely from liveness. As with CommitNotice, only advance if our local
	// entry at commit_index matches the leader's committed identity — never
	// commit a stale entry that merely occupies that index.
	n.advanceCommitLocked(request.GetCommitIndex(), request.GetCommitTerm(), request.GetCommitRequestId())
	isDomainLeader := n.domainRole == Leader && n.state.DomainLeader.NodeID == n.local.ID
	// Only a CURRENT Domain Leader that still holds a fresh in-domain lease may
	// confirm the Global Leader's read lease. A plain follower (or a deposed /
	// lease-less leader) must not count toward the quorum, otherwise an old GL
	// could keep renewing its lease off stale nodes while a new GL is elected in
	// another domain — two overlapping serving leases.
	leaseConfirm := isDomainLeader && n.domainLeaseFreshLocked()
	selfIdentity := n.domainLeaders[n.local.DomainCode]
	if err := n.persistLocked(); err != nil {
		n.mu.Unlock()
		return &cdraftv1.GlobalVoteResponse{GlobalTerm: request.GetGlobalTerm(), Voter: toPBDomainIdentity(selfIdentity)}, nil
	}
	n.mu.Unlock()

	if isDomainLeader {
		for _, member := range n.config.DomainMembers(n.local.DomainCode) {
			if member.ID == n.local.ID {
				continue
			}
			_ = n.callNode(ctx, member.ID, func(callCtx context.Context, conn *grpc.ClientConn) error {
				_, err := cdraftv1.NewGlobalElectionClient(conn).Heartbeat(callCtx, request)
				return err
			})
		}
	}
	// Voter carries our domain-leader identity so the Global Leader can verify the
	// confirmation came from the exact leader it addressed (not a stale node that
	// merely still answers on that address).
	return &cdraftv1.GlobalVoteResponse{GlobalTerm: request.GetGlobalTerm(), Granted: leaseConfirm, Voter: toPBDomainIdentity(selfIdentity)}, nil
}

// replicationSenderTrustedLocked reports whether senderID is an authority this
// node accepts replication/commit from at the current global term: the Global
// Leader it currently follows, or its own Domain Leader (which fans entries out
// within the domain, including during a CatchUp backfill). When the Global
// Leader is not yet known (startup), it stays permissive and relies on the
// global-term check as the backstop.
func (n *RPCNode) replicationSenderTrustedLocked(senderID string) bool {
	if senderID == "" {
		return true
	}
	if n.state.DomainLeader.Valid() && senderID == n.state.DomainLeader.NodeID {
		return true
	}
	if !n.globalLeader.Valid() {
		return true
	}
	return senderID == n.globalLeader.DomainLeader.NodeID
}

func (n *RPCNode) Replicate(ctx context.Context, request *cdraftv1.ReplicateEntry) (*cdraftv1.DomainQuorumAck, error) {
	finishReplicate := n.traceStart(request.GetRequestId(), "server.replicate.total", request.GetOriginDomain())
	defer finishReplicate()
	entry := LogEntry{
		GlobalTerm: request.GetGlobalTerm(), GlobalIndex: request.GetGlobalIndex(),
		RequestID: request.GetRequestId(), OriginDomain: request.GetOriginDomain(),
		Command: Command{Key: request.GetKey(), Value: request.GetValue()},
		Result:  commandResult(Command{Key: request.GetKey(), Value: request.GetValue()}),
	}
	n.mu.Lock()
	senderID := request.GetSenderNodeId()
	senderIsCurrentDomainLeader := senderID != "" &&
		n.state.DomainLeader.Valid() && senderID == n.state.DomainLeader.NodeID
	if request.GetGlobalTerm() < n.state.GlobalTerm && !senderIsCurrentDomainLeader {
		term := n.state.GlobalTerm
		n.recordRejectionLocked("replicate_stale_term")
		n.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "stale global term: %d", term)
	}
	// Provenance: ReplicateEntry.GlobalTerm is the log entry's term. During
	// CatchUp, the current Domain Leader may replay historical entries whose
	// entry term is below the receiver's current global term; that is not a stale
	// RPC, it is a valid log repair. Other lower-term senders are rejected above.
	// At our current global term, replication may only come from the Global
	// Leader we follow (or our Domain Leader fanning it out). A higher entry term
	// is a newer GL we have not learned about yet, so let it through.
	if request.GetGlobalTerm() == n.state.GlobalTerm && !n.replicationSenderTrustedLocked(request.GetSenderNodeId()) {
		n.recordRejectionLocked("replicate_bad_sender")
		n.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "replicate from untrusted sender")
	}
	if request.GetFanoutToDomain() && (n.domainRole != Leader || n.state.DomainLeader.NodeID != n.local.ID) {
		n.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "target is not current domain leader")
	}
	existing, had := findEntryByIndex(n.state.Log, entry.GlobalIndex)
	overwrote := had && (existing.GlobalTerm != entry.GlobalTerm || existing.RequestID != entry.RequestID)
	outcome := appendEntryChecked(&n.state, entry, request.GetPrevLogIndex(), request.GetPrevLogTerm())
	if outcome == appendCommittedConflict {
		// A committed entry would be overwritten: impossible under one leader per
		// term. Surface it loudly rather than corrupting the log.
		n.recordRejectionLocked("replicate_committed_conflict")
		n.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "replicate conflicts with committed index %d", entry.GlobalIndex)
	}
	if outcome == appendAccepted && overwrote {
		// The uncommitted tail at >= GlobalIndex was just replaced, so any pending
		// Fast Return certificate for those indices refers to a now-stale entry
		// and must be dropped (the identity checks in the deliver path would also
		// reject it, but clearing avoids leaking stale waiters).
		for idx := range n.pendingFast {
			if idx >= entry.GlobalIndex {
				delete(n.pendingFast, idx)
			}
		}
	}
	ok := outcome == appendAccepted
	// On the domain-leader fan-out path we ALSO fsync again after collecting
	// in-domain quorum (server.domain_quorum_persist). That post-quorum fsync
	// already captures both the appended entry and the advanced
	// DomainQuorumIndex in one batch, so the pre-quorum fsync here is pure
	// duplication on the critical path. Defer it to the post-quorum point and
	// trace the deferral so benchmarks can see the merge.
	//
	// Safety: leader is not externally acknowledging anything until the
	// post-quorum ack returns; the followers' own replicate_persist (fired when
	// they receive the fanned-out Replicate) provides cluster-side durability,
	// and the leader's deferred fsync still completes before the leader's ack
	// leaves this function. If quorum is not reached we drain the deferred
	// fsync at the end so a no-quorum exit still leaves a consistent disk.
	deferLeaderFsync := request.GetFanoutToDomain() && ok
	if !deferLeaderFsync {
		finishPersist := n.traceStart(entry.RequestID, "server.replicate_persist", n.local.DomainCode)
		if err := n.persistLocked(); err != nil {
			finishPersist("error")
			// The entry is not durable, so we must not acknowledge holding it.
			n.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "replicate persistence failed")
		}
		finishPersist()
	} else {
		n.traceMark(entry.RequestID, "server.replicate_persist_deferred", n.local.DomainCode, "")
	}
	n.mu.Unlock()
	if !ok {
		// Prefix mismatch: the leader should back up and CatchUp-backfill us.
		return &cdraftv1.DomainQuorumAck{GlobalTerm: entry.GlobalTerm, GlobalIndex: entry.GlobalIndex, DomainId: n.local.DomainCode}, nil
	}
	if !request.GetFanoutToDomain() {
		return &cdraftv1.DomainQuorumAck{
			GlobalTerm: entry.GlobalTerm, GlobalIndex: entry.GlobalIndex, DomainId: n.local.DomainCode, Quorum: true,
		}, nil
	}

	acks := 1
	var ackMu sync.Mutex
	var wait sync.WaitGroup
	finishQuorum := n.traceStart(entry.RequestID, "server.in_domain_quorum", n.local.DomainCode)
	for _, member := range n.config.DomainMembers(n.local.DomainCode) {
		if member.ID == n.local.ID {
			continue
		}
		wait.Add(1)
		go func(peerID string) {
			defer wait.Done()
			followerRequest := proto.Clone(request).(*cdraftv1.ReplicateEntry)
			followerRequest.FanoutToDomain = false
			err := n.callNode(ctx, peerID, func(callCtx context.Context, conn *grpc.ClientConn) error {
				response, err := cdraftv1.NewReplicationClient(conn).Replicate(callCtx, followerRequest)
				if err == nil && response.GetQuorum() {
					ackMu.Lock()
					acks++
					ackMu.Unlock()
				}
				return err
			})
			_ = err
		}(member.ID)
	}
	wait.Wait()
	quorum := acks >= majority(len(n.config.DomainMembers(n.local.DomainCode)))
	if quorum {
		finishQuorum("quorum")
	} else {
		finishQuorum("no-quorum")
	}
	if quorum {
		n.mu.Lock()
		// The log is strictly append-only (appendEntry enforces
		// GlobalIndex == len(Log)+1), so a domain that reaches quorum at this
		// index provably holds every entry 1..GlobalIndex in its majority — no
		// gaps are possible. Advance the per-domain quorum high-water mark
		// unconditionally (matching the Global-Leader-side bookkeeping). A prior
		// strict "+1 only" gate here would permanently wedge the index the
		// moment two concurrent acks landed out of order, silently disabling
		// Fast Return for every future write originating in this domain.
		n.state.DomainQuorumIndex[n.local.DomainCode] = max(n.state.DomainQuorumIndex[n.local.DomainCode], entry.GlobalIndex)
		pending, hasPending := n.pendingFast[entry.GlobalIndex]
		// Only the certificate that matches the entry we actually quorum-held may
		// fire (a stale pending for a since-overwritten entry must not).
		matchesPending := hasPending && pending.GlobalTerm == entry.GlobalTerm && pending.RequestID == entry.RequestID
		// Symmetric to the announce path: if the announce already arrived
		// (matchesPending), we now hold both domains' quorum for the contiguous
		// prefix, so advance commit knowledge here too instead of waiting for the
		// asynchronous CommitNotice. Done before persistLocked so the advance is
		// made durable on the fsync this path already pays.
		if matchesPending {
			n.advanceCommitLocked(entry.GlobalIndex, entry.GlobalTerm, entry.RequestID)
		}
		contiguous := n.state.KnownGlobalCommitIndex+1 >= entry.GlobalIndex
		finishDomainPersist := n.traceStart(entry.RequestID, "server.domain_quorum_persist", n.local.DomainCode)
		persistErr := n.persistLocked()
		n.mu.Unlock()
		if persistErr != nil {
			finishDomainPersist("error")
		} else {
			finishDomainPersist()
		}
		if persistErr != nil {
			// The advanced quorum high-water mark is not durable; do not vouch.
			return nil, status.Error(codes.Unavailable, "replicate persistence failed")
		}
		if matchesPending && contiguous {
			go n.deliverFastReturn(context.Background(), pending)
		}
	} else if deferLeaderFsync {
		// Quorum was not reached: drain the deferred replicate fsync so the
		// leader's in-memory log does not stay ahead of disk after we return.
		n.mu.Lock()
		finishDrain := n.traceStart(entry.RequestID, "server.replicate_persist", n.local.DomainCode)
		err := n.persistLocked()
		n.mu.Unlock()
		if err != nil {
			finishDrain("drain-error")
			return nil, status.Error(codes.Unavailable, "replicate persistence failed")
		}
		finishDrain("drain")
	}
	return &cdraftv1.DomainQuorumAck{
		GlobalTerm: entry.GlobalTerm, GlobalIndex: entry.GlobalIndex, DomainId: n.local.DomainCode, Quorum: quorum,
	}, nil
}

func (n *RPCNode) AnnounceGlobalDomainQuorum(_ context.Context, request *cdraftv1.GlobalDomainQuorumAck) (*cdraftv1.DomainQuorumAck, error) {
	finishAnnounce := n.traceStart(request.GetRequestId(), "server.fast_return.announce_received", request.GetResponderDomain())
	defer finishAnnounce()
	responderDomain := request.GetResponderDomain()
	if responderDomain == "" {
		responderDomain = n.local.DomainCode
	}
	pending := pendingFastReturn{
		RequestID: request.GetRequestId(), Result: request.GetResult(), ReplyRoute: request.GetReplyRoute(),
		OriginDomain: request.GetOriginDomain(), ResponderDomain: responderDomain,
		GlobalTerm: request.GetGlobalTerm(), GlobalIndex: request.GetGlobalIndex(),
	}
	n.mu.Lock()
	if responderDomain != n.local.DomainCode {
		n.recordRejectionLocked("announce_wrong_responder")
		n.mu.Unlock()
		return &cdraftv1.DomainQuorumAck{GlobalTerm: request.GetGlobalTerm(), GlobalIndex: pending.GlobalIndex, DomainId: n.local.DomainCode}, nil
	}
	// Stale announcement from an older global term: ignore.
	if request.GetGlobalTerm() < n.state.GlobalTerm {
		term := n.state.GlobalTerm
		n.recordRejectionLocked("announce_stale_term")
		n.mu.Unlock()
		return &cdraftv1.DomainQuorumAck{GlobalTerm: term, GlobalIndex: pending.GlobalIndex, DomainId: n.local.DomainCode}, nil
	}
	// Provenance: at our current term, a Fast Return certificate may only come
	// from the Global Leader's domain that we currently follow.
	if request.GetGlobalTerm() == n.state.GlobalTerm && n.globalLeader.Valid() &&
		request.GetGlobalLeaderDomain() != "" &&
		request.GetGlobalLeaderDomain() != n.globalLeader.DomainLeader.DomainID {
		n.recordRejectionLocked("announce_bad_sender")
		n.mu.Unlock()
		return &cdraftv1.DomainQuorumAck{GlobalTerm: request.GetGlobalTerm(), GlobalIndex: pending.GlobalIndex, DomainId: n.local.DomainCode}, nil
	}
	n.pendingFast[pending.GlobalIndex] = pending
	localQuorum := n.domainHoldsEntryLocked(pending.GlobalIndex, pending.GlobalTerm, pending.RequestID)
	// Two-domain commit evidence is already in hand: this announce proves the
	// Global Leader domain holds 1..GlobalIndex (append-only), and localQuorum
	// proves our own domain holds 1..GlobalIndex. The contiguous prefix is thus
	// globally committed, so advance our commit knowledge now rather than waiting
	// for the asynchronous CommitNotice — whose cross-domain lag would otherwise
	// trip the contiguity gate below and drop the Fast Return for back-to-back
	// writes. advanceCommitLocked stays strictly contiguous and identity-checked,
	// so a real prefix gap is never crossed (and localQuorum already guarantees a
	// gap-free 1..GlobalIndex on this domain).
	if localQuorum {
		n.advanceCommitLocked(pending.GlobalIndex, pending.GlobalTerm, pending.RequestID)
	}
	contiguous := n.state.KnownGlobalCommitIndex+1 >= pending.GlobalIndex
	n.mu.Unlock()
	if localQuorum && contiguous {
		go n.deliverFastReturn(context.Background(), pending)
	}
	return &cdraftv1.DomainQuorumAck{
		GlobalTerm: pending.GlobalTerm, GlobalIndex: pending.GlobalIndex, DomainId: n.local.DomainCode, Quorum: localQuorum,
	}, nil
}

// advanceCommitLocked advances the commit/apply position up to commitIndex, but
// ONLY if the local entry at commitIndex matches the leader's committed identity
// (commitTerm + commitRequestID). By the Raft Log Matching Property a match at
// commitIndex proves the entire prefix is identical, so the contiguous prefix is
// safe to apply. A mismatch or missing entry means a commit signal overtook the
// replication that would install the right entry; we refuse and wait. It returns
// false only when it refused (caller decides how to surface that).
func (n *RPCNode) advanceCommitLocked(commitIndex, commitTerm uint64, commitRequestID string) bool {
	if commitIndex <= n.state.KnownGlobalCommitIndex {
		return true
	}
	from := n.state.KnownGlobalCommitIndex + 1
	entries := make([]LogEntry, 0, commitIndex-from+1)
	for next := from; next <= commitIndex; next++ {
		entry, ok := findEntryByIndex(n.state.Log, next)
		if !ok {
			return false
		}
		entries = append(entries, entry)
	}
	top := entries[len(entries)-1]
	if top.GlobalTerm != commitTerm || top.RequestID != commitRequestID {
		return false
	}
	for _, entry := range entries {
		applyEntry(&n.state, entry)
		if entry.GlobalIndex < commitIndex {
			n.signalCommittedWaiterLocked(entry.RequestID)
		}
	}
	n.maybeCompactLocked()
	return true
}

func (n *RPCNode) signalCommittedWaiterLocked(requestID string) {
	done, ok := n.committing[requestID]
	if !ok {
		return
	}
	close(done)
	delete(n.committing, requestID)
}

func (n *RPCNode) Commit(ctx context.Context, request *cdraftv1.CommitNotice) (*cdraftv1.DomainQuorumAck, error) {
	finishCommit := n.traceStart(request.GetCommitRequestId(), "server.commit_notice.total", n.local.DomainCode)
	defer finishCommit()
	n.mu.Lock()
	ackStale := func(reason string) (*cdraftv1.DomainQuorumAck, error) {
		commitIndex := n.state.KnownGlobalCommitIndex
		n.recordRejectionLocked(reason)
		n.mu.Unlock()
		return &cdraftv1.DomainQuorumAck{
			GlobalTerm: request.GetGlobalTerm(), GlobalIndex: commitIndex, DomainId: n.local.DomainCode,
		}, nil
	}
	// Stale leader: ignore commit notices from an older global term.
	if request.GetGlobalTerm() < n.state.GlobalTerm {
		return ackStale("commit_stale_term")
	}
	// Provenance: only the Global Leader we follow (or our Domain Leader fanning
	// it out within the domain) may advance our commit index.
	if request.GetGlobalTerm() == n.state.GlobalTerm && !n.replicationSenderTrustedLocked(request.GetSenderNodeId()) {
		return ackStale("commit_bad_sender")
	}
	// Evidence: a legitimate global commit is backed by a two-domain quorum.
	if distinctDomains(request.GetEvidenceDomains()) < 2 {
		return ackStale("commit_insufficient_evidence")
	}
	// Identity: only advance commit if the entry we hold at commit_index is the
	// exact entry the leader committed there. Otherwise a CommitNotice that
	// overtook its replication would make us commit a stale entry that merely
	// occupies that index (and, once "committed", appendEntryChecked would then
	// refuse to overwrite it — permanently committing the wrong value).
	finishApply := n.traceStart(request.GetCommitRequestId(), "server.commit_apply", n.local.DomainCode)
	advanced := n.advanceCommitLocked(request.GetCommitIndex(), request.GetCommitTerm(), request.GetCommitRequestId())
	if !advanced {
		finishApply("identity-mismatch")
		return ackStale("commit_identity_mismatch")
	}
	finishApply()
	commitIndex := n.state.KnownGlobalCommitIndex
	isDomainLeader := n.domainRole == Leader && n.state.DomainLeader.NodeID == n.local.ID
	finishPersist := n.traceStart(request.GetCommitRequestId(), "server.commit_persist", n.local.DomainCode)
	if err := n.persistLocked(); err != nil {
		finishPersist("error")
		// The advanced commit index is not durable; do not acknowledge the commit.
		n.recordRejectionLocked("commit_persist_failed")
		n.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "commit persistence failed")
	}
	finishPersist()
	n.mu.Unlock()

	if request.GetFanoutToDomain() && isDomainLeader {
		for _, member := range n.config.DomainMembers(n.local.DomainCode) {
			if member.ID == n.local.ID {
				continue
			}
			go func(peerID string) {
				followerNotice := proto.Clone(request).(*cdraftv1.CommitNotice)
				followerNotice.FanoutToDomain = false
				_ = n.callNode(context.Background(), peerID, func(callCtx context.Context, conn *grpc.ClientConn) error {
					_, err := cdraftv1.NewReplicationClient(conn).Commit(callCtx, followerNotice)
					return err
				})
			}(member.ID)
		}
	}
	return &cdraftv1.DomainQuorumAck{
		GlobalTerm: request.GetGlobalTerm(), GlobalIndex: commitIndex, DomainId: n.local.DomainCode, Quorum: true,
	}, nil
}

// InstallSnapshot is the receiver side of snapshot-based catch-up: when a node
// is so far behind that the Global Leader has already compacted the entries a
// CatchUp backfill would need, the leader ships its applied prefix (state
// machine + result cache) instead. With fanout_to_domain the receiving Domain
// Leader installs locally and then fans the snapshot out to its domain
// followers, reporting whether an in-domain quorum durably covers it — the
// exact shape of the Replicate fan-out, so the leader's quorum bookkeeping
// works unchanged.
func (n *RPCNode) InstallSnapshot(ctx context.Context, request *cdraftv1.InstallSnapshotRequest) (*cdraftv1.InstallSnapshotResponse, error) {
	var sm map[string]string
	if err := json.Unmarshal(request.GetStateMachine(), &sm); err != nil {
		return nil, status.Error(codes.InvalidArgument, "undecodable snapshot state machine")
	}
	var results map[string]ClientResult
	if len(request.GetResults()) > 0 {
		if err := json.Unmarshal(request.GetResults(), &results); err != nil {
			return nil, status.Error(codes.InvalidArgument, "undecodable snapshot results")
		}
	}

	n.mu.Lock()
	if request.GetGlobalTerm() < n.state.GlobalTerm {
		term := n.state.GlobalTerm
		n.recordRejectionLocked("snapshot_stale_term")
		n.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "stale global term: %d", term)
	}
	// Provenance: a snapshot rewrites the whole applied prefix, so it may only
	// come from the Global Leader we follow (or our Domain Leader fanning it
	// out), exactly like Replicate.
	if request.GetGlobalTerm() == n.state.GlobalTerm && !n.replicationSenderTrustedLocked(request.GetSenderNodeId()) {
		n.recordRejectionLocked("snapshot_bad_sender")
		n.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "snapshot from untrusted sender")
	}
	if request.GetFanoutToDomain() && (n.domainRole != Leader || n.state.DomainLeader.NodeID != n.local.ID) {
		n.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "target is not current domain leader")
	}
	installed := installSnapshotState(&n.state, request.GetLastIncludedIndex(), request.GetLastIncludedTerm(),
		request.GetLastIncludedRequestId(), sm, results)
	if installed {
		// Any pending Fast Return certificate at or below the boundary refers to
		// an entry that is now committed (and possibly compacted); the global
		// response path has long settled those requests.
		for idx := range n.pendingFast {
			if idx <= request.GetLastIncludedIndex() {
				delete(n.pendingFast, idx)
			}
		}
	}
	if err := n.persistLocked(); err != nil {
		n.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "snapshot persistence failed")
	}
	lastIndex := n.state.LastLogIndex()
	term := n.state.GlobalTerm
	n.mu.Unlock()

	response := &cdraftv1.InstallSnapshotResponse{GlobalTerm: term, LastIndex: lastIndex, Installed: installed}
	if !request.GetFanoutToDomain() || !installed {
		return response, nil
	}

	acks := 1
	var ackMu sync.Mutex
	var wait sync.WaitGroup
	for _, member := range n.config.DomainMembers(n.local.DomainCode) {
		if member.ID == n.local.ID {
			continue
		}
		wait.Add(1)
		go func(peerID string) {
			defer wait.Done()
			followerRequest := proto.Clone(request).(*cdraftv1.InstallSnapshotRequest)
			followerRequest.FanoutToDomain = false
			_ = n.callNode(ctx, peerID, func(callCtx context.Context, conn *grpc.ClientConn) error {
				resp, err := cdraftv1.NewReplicationClient(conn).InstallSnapshot(callCtx, followerRequest)
				if err == nil && resp.GetInstalled() {
					ackMu.Lock()
					acks++
					ackMu.Unlock()
				}
				return err
			})
		}(member.ID)
	}
	wait.Wait()
	quorum := acks >= majority(len(n.config.DomainMembers(n.local.DomainCode)))
	if quorum {
		n.mu.Lock()
		n.state.DomainQuorumIndex[n.local.DomainCode] = max(n.state.DomainQuorumIndex[n.local.DomainCode], request.GetLastIncludedIndex())
		persistErr := n.persistLocked()
		n.mu.Unlock()
		if persistErr != nil {
			return nil, status.Error(codes.Unavailable, "snapshot persistence failed")
		}
	}
	response.Quorum = quorum
	return response, nil
}

// buildSnapshotRequestLocked freezes this node's applied prefix into an
// InstallSnapshot request. The boundary is the APPLIED index (not the snapshot
// floor), so the receiver lands as close to the head as the sender can prove.
func (n *RPCNode) buildSnapshotRequestLocked() (*cdraftv1.InstallSnapshotRequest, error) {
	lastIndex := n.state.AppliedIndex
	if lastIndex == 0 {
		return nil, fmt.Errorf("nothing applied to snapshot")
	}
	var lastTerm uint64
	var lastRequestID string
	if entry, ok := findEntryByIndex(n.state.Log, lastIndex); ok {
		lastTerm, lastRequestID = entry.GlobalTerm, entry.RequestID
	} else if lastIndex == n.state.SnapshotLastIndex {
		lastTerm, lastRequestID = n.state.SnapshotLastTerm, n.state.SnapshotLastRequestID
	} else {
		return nil, fmt.Errorf("applied boundary %d not identifiable", lastIndex)
	}
	smJSON, err := json.Marshal(n.state.StateMachine)
	if err != nil {
		return nil, err
	}
	resJSON, err := json.Marshal(n.state.Results)
	if err != nil {
		return nil, err
	}
	return &cdraftv1.InstallSnapshotRequest{
		GlobalTerm: n.state.GlobalTerm, SenderNodeId: n.local.ID,
		LastIncludedIndex: lastIndex, LastIncludedTerm: lastTerm, LastIncludedRequestId: lastRequestID,
		StateMachine: smJSON, Results: resJSON, FanoutToDomain: true,
	}, nil
}

// sendInstallSnapshot ships this node's applied prefix to the target Domain
// Leader (with in-domain fanout). It is the catch-up fallback used when the
// tail a lagging domain needs has already been compacted away.
func (n *RPCNode) sendInstallSnapshot(ctx context.Context, nodeID string) (*cdraftv1.InstallSnapshotResponse, error) {
	n.mu.Lock()
	request, err := n.buildSnapshotRequestLocked()
	n.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if nodeID == n.local.ID {
		return n.InstallSnapshot(ctx, request)
	}
	var response *cdraftv1.InstallSnapshotResponse
	err = n.callNode(ctx, nodeID, func(callCtx context.Context, conn *grpc.ClientConn) error {
		var callErr error
		response, callErr = cdraftv1.NewReplicationClient(conn).InstallSnapshot(callCtx, request)
		return callErr
	})
	return response, err
}

// snapshotFloor is the last log index this node has compacted away; entries at
// or below it can no longer be streamed and require InstallSnapshot.
func (n *RPCNode) snapshotFloor() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.SnapshotLastIndex
}

func (n *RPCNode) Write(ctx context.Context, request *cdraftv1.ClientWriteRequest) (*cdraftv1.ClientResult, error) {
	requestID := request.GetRequestId()
	originDomain := request.GetOriginDomain()
	finishWrite := n.traceStart(requestID, "server.write.total", originDomain)
	defer finishWrite()
	finishInbound := n.traceStart(requestID, "server.client_link.in", originDomain)
	if err := n.sleepLink(ctx, request.GetOriginDomain(), n.local.DomainCode); err != nil {
		finishInbound("deadline")
		return nil, status.Error(codes.DeadlineExceeded, "client request did not reach Global Leader")
	}
	finishInbound()
	defer func() {
		finishOutbound := n.traceStart(requestID, "server.client_link.out", originDomain)
		n.sleepLinkWithoutCancel(n.local.DomainCode, request.GetOriginDomain())
		finishOutbound()
	}()

	finishCheck := n.traceStart(requestID, "server.global_leader_check", "")
	n.mu.Lock()
	if n.stage != Serving || !n.servesAsGlobalLeaderLocked() {
		redirect := n.globalRedirectAddressLocked()
		n.recordRejectionLocked("write_not_global_leader")
		n.mu.Unlock()
		finishCheck("redirect")
		// During a migration window the new Global Leader may not be known yet.
		// Surface a retryable gRPC error so the idempotent client keeps retrying
		// (with the stable requestId) until it can be redirected to the new GL.
		if redirect == "" {
			return nil, status.Error(codes.Unavailable, ErrNotGlobalLeader.Error())
		}
		return &cdraftv1.ClientResult{RequestId: request.GetRequestId(), RedirectAddress: redirect, Error: ErrNotGlobalLeader.Error()}, nil
	}
	finishCheck("ok")
	// Safe-handoff soft pause: while draining toward a migration barrier we stop
	// ordering NEW writes (freezing the barrier so the target can catch up), but
	// still serve already-ordered requestIds idempotently (retries/in-flight).
	if n.draining {
		_, settled := n.state.Results[request.GetRequestId()]
		_, ordered := findEntryByRequest(n.state.Log, request.GetRequestId())
		if !settled && !ordered {
			n.recordRejectionLocked("write_draining")
			n.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "global leader migration in progress")
		}
	}
	if request.GetOriginDomain() != "" {
		n.stats.record(request.GetOriginDomain(), false, time.Now())
	}
	if final, ok := n.state.Results[request.GetRequestId()]; ok {
		entry, exists := findEntryByRequest(n.state.Log, request.GetRequestId())
		if exists {
			if entry.Command.Key != request.GetKey() || entry.Command.Value != request.GetValue() ||
				entry.OriginDomain != request.GetOriginDomain() {
				n.mu.Unlock()
				return nil, status.Error(codes.InvalidArgument, "requestId reused with different command")
			}
		} else if final.GlobalIndex > n.state.SnapshotLastIndex ||
			final.Result != commandResult(Command{Key: request.GetKey(), Value: request.GetValue()}) {
			// Either the entry should still be in the log but is not, or the
			// compacted entry's deterministic result does not match the command
			// being retried: the requestId was reused.
			n.mu.Unlock()
			return nil, status.Error(codes.InvalidArgument, "requestId reused with different command")
		}
		// Otherwise the entry was compacted into the snapshot; the committed,
		// idempotent result is authoritative for a retried request.
		n.mu.Unlock()
		n.traceMark(requestID, "server.write.idempotent", "", "")
		return toPBClientResult(final, ""), nil
	}
	command := Command{Key: request.GetKey(), Value: request.GetValue()}
	entry := LogEntry{
		GlobalTerm: n.globalLeader.GlobalTerm, GlobalIndex: n.state.Summary().LastGlobalIndex + 1,
		RequestID: request.GetRequestId(), OriginDomain: request.GetOriginDomain(), Command: command, Result: commandResult(command),
	}
	if existing, ok := findEntryByRequest(n.state.Log, request.GetRequestId()); ok {
		if existing.Command != command || existing.OriginDomain != request.GetOriginDomain() {
			n.mu.Unlock()
			return nil, status.Error(codes.InvalidArgument, "requestId reused with different command")
		}
		entry = existing
	} else {
		finishOrder := n.traceStart(requestID, "server.order_entry", "")
		n.state.Log = append(n.state.Log, entry)
		finishOrder()
		// Ordering the entry in memory is not yet an external commitment: nothing
		// outside this node has been told about it. The persistence barrier is
		// pushed down into replicateAndCommit (which fsyncs once before any
		// externally visible step — Fast Return announce or commitAcrossDomains).
		n.traceMark(requestID, "server.order_recorded", "", "")
	}
	globalDomain := n.local.DomainCode

	// The cross-domain replication and two-domain commit MUST run to completion
	// independently of this client request's lifetime. With Fast Return enabled
	// the origin-domain leader answers the client as soon as it observes its own
	// quorum plus the global-domain announce, so a short-lived client exits and
	// cancels THIS request's context before the Global Leader has gathered its
	// own second-domain evidence. If commit were tied to `ctx`, that early exit
	// would strand the entry uncommitted on the Global Leader, leaving
	// linearizable reads blocked behind the read barrier forever. So we drive
	// commit on a node-scoped background context and only use `ctx` to decide
	// when to answer the (possibly still-waiting) client. A per-requestId guard
	// makes a retried write join the in-flight commit instead of spawning a
	// duplicate.
	done, ok := n.committing[request.GetRequestId()]
	if !ok {
		done = make(chan struct{})
		n.committing[request.GetRequestId()] = done
		commitCtx, commitCancel := context.WithTimeout(context.Background(), commitBudget(n.rpcPolicy))
		go func() {
			defer commitCancel()
			n.replicateAndCommit(commitCtx, entry, request, globalDomain, done)
			n.mu.Lock()
			delete(n.committing, request.GetRequestId())
			n.mu.Unlock()
		}()
	}
	n.mu.Unlock()

	finishWait := n.traceStart(requestID, "server.wait_commit", "")
	select {
	case <-ctx.Done():
		finishWait("deadline")
		// Client gave up or left via Fast Return; the background commit keeps
		// running and will advance the global commit index regardless.
		return nil, status.Error(codes.DeadlineExceeded, "write did not obtain two-domain evidence")
	case <-done:
	}
	finishWait()

	finishResult := n.traceStart(requestID, "server.global_response.result", "")
	n.mu.Lock()
	final, committed := n.state.Results[request.GetRequestId()]
	n.mu.Unlock()
	if !committed {
		finishResult("not-committed")
		return nil, status.Error(codes.Unavailable, ErrNoCommit.Error())
	}
	if n.config.DomainKind(request.GetOriginDomain()) == topology.DomainFloating {
		n.mu.Lock()
		n.metrics.floatingRaceGlobalWins++
		n.mu.Unlock()
	}
	result := toPBClientResult(final, "")
	finishResult()
	return result, nil
}

// replicateAndCommit fans the entry out to every domain, records per-domain
// quorum, fires the Fast Return announce once the global domain is covered, and
// commits across domains as soon as a second domain is reached. It is driven by
// a node-scoped context (NOT a client request context) so the global commit
// always completes even when the client has already left.
//
// `done` is closed exactly once, at the moment two-domain commit is reached
// (releasing any client still waiting for the GlobalResponse after only ~1 RTT,
// not the slowest domain's RTT), or when the fan-out ends without committing.
// After signalling, the loop keeps draining the remaining domain acks so their
// goroutines (and any backfill they trigger) are not abandoned mid-flight.
func (n *RPCNode) replicateAndCommit(ctx context.Context, entry LogEntry, request *cdraftv1.ClientWriteRequest, globalDomain string, done chan struct{}) {
	finishBackground := n.traceStart(entry.RequestID, "server.commit.background", "")
	defer finishBackground()
	signaled := false
	signal := func() {
		if signaled {
			return
		}
		signaled = true
		n.traceMark(entry.RequestID, "server.commit.signal", "", "")
		n.mu.Lock()
		n.signalCommittedWaiterLocked(entry.RequestID)
		n.mu.Unlock()
	}
	defer signal()

	// Durability before announce / commit is provided by:
	//   - Each domain's follower-side replicate_persist (their majority makes
	//     the entry durable in each acking domain).
	//   - The local Commit handler's commit_persist (covers GL's own log,
	//     DomainQuorumIndex, and commit index in one batch before client ack).
	// So we do NOT need a separate persist_before_commit fsync on GL: a Fast
	// Return that fires after announce is backed by two independent domain
	// majorities (responder's own quorum + the GL-domain quorum the announce
	// asserts), both of which are durable on their respective domain followers.
	// committed tracks whether commitAcrossDomains ran, so the deferred drain
	// only fsyncs DomainQuorumIndex updates that the commit path won't capture.
	committed := false
	defer func() {
		if committed {
			return
		}
		n.mu.Lock()
		finishPersist := n.traceStart(entry.RequestID, "server.quorum_progress_persist", "")
		err := n.persistLocked()
		n.mu.Unlock()
		if err != nil {
			finishPersist("error")
			return
		}
		finishPersist("drain")
	}()

	type quorumResult struct {
		domain string
		quorum bool
	}
	leaders := n.domainLeaderSnapshot()
	plan := n.planReturnPath(request.GetOriginDomain(), globalDomain, request.GetReplyRoute())
	if plan.Responder.Valid() {
		n.traceMark(entry.RequestID, "server.return_plan", plan.Responder.DomainID, plan.Responder.NodeID)
	}
	results := make(chan quorumResult, len(leaders))
	for _, leader := range leaders {
		leader := leader
		go func() {
			finishQuorum := n.traceStart(entry.RequestID, "server.domain_quorum", leader.DomainID)
			ok := n.replicateDomainQuorum(ctx, leader, entry, request.GetReplyRoute())
			if ok {
				finishQuorum("quorum")
			} else {
				finishQuorum("no-quorum")
			}
			results <- quorumResult{domain: leader.DomainID, quorum: ok}
		}()
	}

	quorums := make(map[string]bool)
	announced := false
	for range leaders {
		select {
		case <-ctx.Done():
			return
		case result := <-results:
			if result.quorum {
				quorums[result.domain] = true
				n.mu.Lock()
				n.state.DomainQuorumIndex[result.domain] = max(n.state.DomainQuorumIndex[result.domain], entry.GlobalIndex)
				n.mu.Unlock()
				n.traceMark(entry.RequestID, "server.quorum_progress_recorded", result.domain, "")
			}
			if n.fastReturnIsEnabled() && quorums[globalDomain] && !announced && request.GetOriginDomain() != globalDomain {
				if plan.Responder.Valid() {
					announced = true
					go n.announceGlobalDomainQuorum(context.Background(), entry, request.GetReplyRoute(), plan)
				}
			}
			if !signaled && quorums[globalDomain] && hasOtherDomainQuorum(quorums, globalDomain) {
				// commitAcrossDomains drives local Commit, which fsyncs via
				// commit_persist before allowing externally visible commit state.
				// That covers the GL's log + DomainQuorumIndex + commit index in a
				// single batch, so we do not pay an extra fsync here.
				finishCommit := n.traceStart(entry.RequestID, "server.two_domain_commit", "")
				n.commitAcrossDomains(ctx, entry, quorums)
				finishCommit()
				committed = true
				n.mu.Lock()
				delay := n.normalDelay
				n.mu.Unlock()
				if delay > 0 {
					time.Sleep(delay)
				}
				signal()
			}
		}
	}
}

func (n *RPCNode) planReturnPath(originDomain, globalDomain, replyRoute string) returnPlan {
	kind := n.config.DomainKind(originDomain)
	plan := returnPlan{OriginKind: kind, Floating: kind == topology.DomainFloating}
	if originDomain == "" || originDomain == globalDomain || replyRoute == "" {
		return plan
	}
	n.mu.Lock()
	enabled := n.fastReturnEnabled
	leaders := make(map[string]DomainLeaderIdentity, len(n.domainLeaders))
	for domain, leader := range n.domainLeaders {
		leaders[domain] = leader
	}
	n.mu.Unlock()
	if !enabled {
		return plan
	}
	switch kind {
	case topology.DomainConsensus:
		plan.Responder = leaders[originDomain]
	case topology.DomainFloating:
		plan.Responder = n.selectFloatingResponder(originDomain, globalDomain, leaders)
		if plan.Responder.Valid() {
			n.mu.Lock()
			n.metrics.floatingResponderChosen++
			if n.metrics.floatingResponderByDomain == nil {
				n.metrics.floatingResponderByDomain = make(map[string]uint64)
			}
			n.metrics.floatingResponderByDomain[plan.Responder.DomainID]++
			n.mu.Unlock()
		}
	}
	return plan
}

func (n *RPCNode) selectFloatingResponder(originDomain, globalDomain string, leaders map[string]DomainLeaderIdentity) DomainLeaderIdentity {
	now := time.Now()
	if _, ok := n.floatingLatency.oneWay(originDomain, globalDomain, now); !ok {
		return DomainLeaderIdentity{}
	}
	var best DomainLeaderIdentity
	bestCost := 0.0
	for domain, leader := range leaders {
		if domain == globalDomain || !leader.Valid() || !n.config.IsConsensusDomain(domain) {
			continue
		}
		originToResponder, ok := n.floatingLatency.oneWay(originDomain, domain, now)
		if !ok {
			continue
		}
		glToResponder, ok := n.rtt.lookupOneWay(globalDomain, domain)
		if !ok {
			glToResponder = float64(n.oneWayDelay(globalDomain, domain).Milliseconds())
		}
		cost := glToResponder + originToResponder
		if !best.Valid() || cost < bestCost {
			best = leader
			bestCost = cost
		}
	}
	return best
}

// commitBudget bounds how long a background commit may keep retrying cross
// domain replication/backfill before giving up, scaled off the RPC policy so
// real-WAN deployments (longer per-attempt deadlines) get proportionally more
// headroom.
func commitBudget(policy RPCPolicy) time.Duration {
	budget := time.Duration(policy.MaxRetries+1) * policy.Deadline * 4
	if budget < 5*time.Second {
		budget = 5 * time.Second
	}
	return budget
}

// replicateDomainQuorum replicates a single entry to a domain and returns
// whether that domain reached in-domain quorum AT this entry's index.
//
// The base single-entry Replicate path appends strictly contiguously, so a
// domain that has fallen behind (after an election or migration left it
// trailing, or because earlier writes left uncommitted orphan entries on the
// Global Leader) would reject the entry on a gap and could never catch up
// through the normal write path. That used to permanently wedge new writes: the
// leader's log head kept advancing while lagging domains stayed stuck, so the
// leader could never gather a second-domain quorum again.
//
// To make replication self-healing, a gap rejection triggers a backfill of the
// contiguous tail the domain is missing (up to this entry) over the existing
// CatchUp RPC, after which the domain can append in order and ack. We only need
// ONE other domain to reach the entry for the two-domain commit, so a nearby
// (same-term) domain heals even when another domain is hopelessly behind.
// prevLogFor returns the index/term of the entry immediately preceding index in
// this node's log (0,0 for the first index). Used by the Global Leader to stamp
// outgoing replication with the Raft prev-entry consistency anchor.
func (n *RPCNode) prevLogFor(index uint64) (uint64, uint64) {
	if index <= 1 {
		return 0, 0
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if prev, ok := findEntryByIndex(n.state.Log, index-1); ok {
		return index - 1, prev.GlobalTerm
	}
	if index-1 == n.state.SnapshotLastIndex {
		return index - 1, n.state.SnapshotLastTerm
	}
	return index - 1, 0
}

func (n *RPCNode) replicateDomainQuorum(ctx context.Context, leader DomainLeaderIdentity, entry LogEntry, replyRoute string) bool {
	prevIndex, prevTerm := n.prevLogFor(entry.GlobalIndex)
	replicate := &cdraftv1.ReplicateEntry{
		GlobalTerm: entry.GlobalTerm, GlobalIndex: entry.GlobalIndex, RequestId: entry.RequestID,
		OriginDomain: entry.OriginDomain, Key: entry.Command.Key, Value: entry.Command.Value,
		ReplyRoute: replyRoute, SenderNodeId: n.local.ID, FanoutToDomain: true,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm,
	}
	if leader.NodeID == n.local.ID {
		ack, err := n.Replicate(ctx, replicate)
		return err == nil && ack.GetQuorum()
	}
	var ack *cdraftv1.DomainQuorumAck
	err := n.callNode(ctx, leader.NodeID, func(callCtx context.Context, conn *grpc.ClientConn) error {
		var callErr error
		ack, callErr = cdraftv1.NewReplicationClient(conn).Replicate(callCtx, replicate)
		return callErr
	})
	if err == nil && ack.GetQuorum() {
		return true
	}
	// Contiguity gap (or transient miss): probe the target for the index it has
	// actually reached, then backfill exactly the contiguous tail it is missing
	// (starting from its own last index, NOT from the leader's stale per-domain
	// quorum view, which can be 0 right after an election and would otherwise
	// dredge up ancient entries). After backfilling, re-check its in-domain
	// quorum at this entry.
	probe, perr := n.sendCatchUp(ctx, leader.NodeID, entry.GlobalIndex, nil)
	if perr != nil {
		return false
	}
	if probe.GetDomainQuorumIndex() >= entry.GlobalIndex {
		return true
	}
	after := probe.GetLastIndex()
	// The target is behind our compaction floor: the entries it needs are gone
	// from the log, so ship the applied-prefix snapshot first, then stream the
	// remaining tail above it.
	if after < n.snapshotFloor() {
		snap, serr := n.sendInstallSnapshot(ctx, leader.NodeID)
		if serr != nil || !snap.GetInstalled() {
			return false
		}
		after = snap.GetLastIndex()
	}
	tail := n.collectTailEntries(after, entry.GlobalIndex)
	if len(tail) == 0 {
		return false
	}
	resp, cerr := n.sendCatchUp(ctx, leader.NodeID, entry.GlobalIndex, tail)
	return cerr == nil && resp.GetDomainQuorumIndex() >= entry.GlobalIndex
}

func (n *RPCNode) Read(ctx context.Context, request *cdraftv1.ClientReadRequest) (*cdraftv1.ClientReadResponse, error) {
	// A linearizable read is served solely by the Global Leader, so the client
	// pays the same simulated client<->GL round trip as a write does. Inject the
	// directed one-way delays outside the lock so the mutex is never held across
	// a sleep.
	if err := n.sleepLink(ctx, request.GetOriginDomain(), n.local.DomainCode); err != nil {
		return nil, status.Error(codes.DeadlineExceeded, "client read did not reach Global Leader")
	}
	defer n.sleepLinkWithoutCancel(n.local.DomainCode, request.GetOriginDomain())

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stage != Serving || !n.servesAsGlobalLeaderLocked() {
		n.recordRejectionLocked("read_not_global_leader")
		redirect := n.globalRedirectAddressLocked()
		if redirect == "" {
			return nil, status.Error(codes.Unavailable, ErrNotGlobalLeader.Error())
		}
		return &cdraftv1.ClientReadResponse{RedirectAddress: redirect, Error: ErrNotGlobalLeader.Error()}, nil
	}
	if request.GetOriginDomain() != "" {
		n.stats.record(request.GetOriginDomain(), true, time.Now())
	}
	barrier := n.state.DomainQuorumIndex[n.local.DomainCode]
	if n.state.KnownGlobalCommitIndex < barrier || n.state.AppliedIndex < barrier {
		n.recordRejectionLocked("read_barrier")
		return nil, status.Error(codes.Unavailable, ErrReadBarrier.Error())
	}
	return &cdraftv1.ClientReadResponse{Value: n.state.StateMachine[request.GetKey()]}, nil
}

func (n *RPCNode) DiscoverTopology(ctx context.Context, request *cdraftv1.TopologyRequest) (*cdraftv1.TopologyResponse, error) {
	if err := n.sleepLink(ctx, request.GetOriginDomain(), n.local.DomainCode); err != nil {
		return nil, status.Error(codes.DeadlineExceeded, "topology request did not reach node")
	}
	defer n.sleepLinkWithoutCancel(n.local.DomainCode, request.GetOriginDomain())

	n.mu.Lock()
	if n.stage != Serving || !n.servesAsGlobalLeaderLocked() {
		redirect := n.globalRedirectAddressLocked()
		n.mu.Unlock()
		if redirect == "" {
			return nil, status.Error(codes.Unavailable, ErrNotGlobalLeader.Error())
		}
		return &cdraftv1.TopologyResponse{RedirectAddress: redirect, Error: ErrNotGlobalLeader.Error()}, nil
	}
	globalLeader := n.globalLeader
	globalAddr := n.globalLeaderAddressLocked()
	globalTerm := n.state.GlobalTerm
	leaders := make([]DomainLeaderIdentity, 0, len(n.domainLeaders))
	for _, leader := range n.domainLeaders {
		if leader.Valid() {
			leaders = append(leaders, leader)
		}
	}
	n.mu.Unlock()
	ttlMillis := n.floatingLatency.ttlMillis()

	sort.Slice(leaders, func(i, j int) bool { return leaders[i].DomainID < leaders[j].DomainID })
	endpoints := make([]*cdraftv1.DomainEndpoint, 0, len(leaders))
	for _, leader := range leaders {
		node, ok := n.config.Node(leader.NodeID)
		if !ok {
			continue
		}
		endpoints = append(endpoints, &cdraftv1.DomainEndpoint{
			DomainId:           leader.DomainID,
			NodeId:             leader.NodeID,
			ClientAddress:      node.ClientDialAddress(),
			InterDomainAddress: node.InterDomainDialAddress(),
			DomainLeader:       true,
			GlobalLeader:       leader == globalLeader.DomainLeader,
		})
	}
	return &cdraftv1.TopologyResponse{
		GlobalTerm:          globalTerm,
		GlobalLeader:        toPBDomainIdentity(globalLeader.DomainLeader),
		GlobalLeaderAddress: globalAddr,
		DomainLeaders:       endpoints,
		TtlMillis:           ttlMillis,
	}, nil
}

func (n *RPCNode) recordRejectionLocked(reason string) {
	if n.metrics.rejections == nil {
		n.metrics.rejections = make(map[string]uint64)
	}
	n.metrics.rejections[reason]++
}

func (n *RPCNode) Status(_ context.Context, _ *cdraftv1.NodeStatusRequest) (*cdraftv1.NodeStatusResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return &cdraftv1.NodeStatusResponse{
		NodeId: n.local.ID, DomainCode: n.local.DomainCode, Stage: string(n.stage),
		DomainRole: string(n.domainRole), GlobalRole: string(n.globalRole),
		DomainLeader: toPBDomainIdentity(n.state.DomainLeader),
		GlobalLeader: toPBDomainIdentity(n.globalLeader.DomainLeader),
		GlobalTerm:   n.state.GlobalTerm, GlobalLeaderAddress: n.globalLeaderAddressLocked(),
		DomainTerm: n.state.DomainTerm, CommitIndex: n.state.KnownGlobalCommitIndex, AppliedIndex: n.state.AppliedIndex,
		SnapshotIndex: n.state.SnapshotLastIndex,
	}, nil
}

func (n *RPCNode) announceGlobalDomainQuorum(ctx context.Context, entry LogEntry, replyRoute string, plan returnPlan) {
	responder := plan.Responder
	if !responder.Valid() {
		return
	}
	err := n.callNode(ctx, responder.NodeID, func(callCtx context.Context, conn *grpc.ClientConn) error {
		_, err := cdraftv1.NewReplicationClient(conn).AnnounceGlobalDomainQuorum(callCtx, &cdraftv1.GlobalDomainQuorumAck{
			GlobalTerm: entry.GlobalTerm, GlobalIndex: entry.GlobalIndex, GlobalLeaderDomain: n.local.DomainCode,
			RequestId: entry.RequestID, Result: entry.Result, ReplyRoute: replyRoute,
			OriginDomain: entry.OriginDomain, ResponderDomain: responder.DomainID,
		})
		return err
	})
	if err != nil && plan.Floating {
		n.mu.Lock()
		n.metrics.floatingResponderFailed++
		n.mu.Unlock()
	}
}

func (n *RPCNode) commitAcrossDomains(ctx context.Context, entry LogEntry, quorums map[string]bool) {
	evidence := make([]string, 0, len(quorums))
	for domain, quorum := range quorums {
		if quorum {
			evidence = append(evidence, domain)
		}
	}
	sort.Strings(evidence)
	n.mu.Lock()
	leaderTerm := n.state.GlobalTerm
	n.mu.Unlock()
	for _, leader := range n.domainLeaderSnapshot() {
		leader := leader
		notice := &cdraftv1.CommitNotice{
			GlobalTerm: leaderTerm, CommitIndex: entry.GlobalIndex, EvidenceDomains: evidence,
			FanoutToDomain: true, SenderNodeId: n.local.ID,
			CommitTerm: entry.GlobalTerm, CommitRequestId: entry.RequestID,
		}
		if leader.NodeID == n.local.ID {
			_, _ = n.Commit(ctx, notice)
			continue
		}
		go func() {
			_ = n.callNode(context.Background(), leader.NodeID, func(callCtx context.Context, conn *grpc.ClientConn) error {
				_, err := cdraftv1.NewReplicationClient(conn).Commit(callCtx, notice)
				return err
			})
		}()
	}
}

// domainHoldsEntryLocked reports whether this node's domain has reached an
// in-domain quorum AT globalIndex *for the specific entry* identified by
// (globalTerm, requestID). The plain DomainQuorumIndex high-water mark is not
// enough: after a conflict overwrite the same index can hold a different entry,
// so a Fast Return certificate that only checked the index would falsely vouch
// for an entry the domain's majority never actually held.
func (n *RPCNode) domainHoldsEntryLocked(globalIndex, globalTerm uint64, requestID string) bool {
	if n.state.DomainQuorumIndex[n.local.DomainCode] < globalIndex {
		return false
	}
	entry, ok := findEntryByIndex(n.state.Log, globalIndex)
	return ok && entry.GlobalTerm == globalTerm && entry.RequestID == requestID
}

func (n *RPCNode) deliverFastReturn(ctx context.Context, pending pendingFastReturn) {
	finishFast := n.traceStart(pending.RequestID, "server.fast_return.deliver", pending.ResponderDomain)
	defer finishFast()
	if pending.ReplyRoute == "" {
		return
	}
	n.mu.Lock()
	if !n.domainHoldsEntryLocked(pending.GlobalIndex, pending.GlobalTerm, pending.RequestID) ||
		n.state.KnownGlobalCommitIndex+1 < pending.GlobalIndex ||
		pending.GlobalTerm < n.state.GlobalTerm {
		n.mu.Unlock()
		return
	}
	if n.fastSent[pending.RequestID] {
		n.mu.Unlock()
		return
	}
	n.fastSent[pending.RequestID] = true
	delay := n.fastDelay
	n.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	n.sleepLinkWithoutCancel(n.local.DomainCode, pending.OriginDomain)
	policy := n.rpcPolicySnapshot()
	callCtx, cancel := context.WithTimeout(ctx, policy.Deadline)
	defer cancel()
	// Reuse the per-address connection pool instead of dialing a fresh gRPC
	// client per callback: a new grpc.NewClient pays a TCP+HTTP/2 handshake on
	// its first RPC, whose variable latency decides thin-margin Fast Return vs
	// global-leader races (e.g. an origin domain close to the Global Leader).
	// The pooled conn is owned by n.conns and closed on Stop(); do not close it
	// here, and keep it on RPC failure so gRPC can auto-reconnect for reuse.
	conn, err := n.connection(pending.ReplyRoute)
	if err != nil {
		n.mu.Lock()
		n.metrics.fastReturnDegraded++
		if n.config.DomainKind(pending.OriginDomain) == topology.DomainFloating {
			n.metrics.floatingResponderFailed++
		}
		n.mu.Unlock()
		return
	}
	_, callErr := cdraftv1.NewClientCallbackClient(conn).FastReturn(callCtx, &cdraftv1.ClientResult{
		RequestId: pending.RequestID, GlobalTerm: pending.GlobalTerm, GlobalIndex: pending.GlobalIndex,
		Result: pending.Result, Source: string(FastResponse), Committed: true,
		ResponderDomain: pending.ResponderDomain,
	})
	n.mu.Lock()
	if callErr == nil {
		n.metrics.fastReturnSuccess++
		if n.config.DomainKind(pending.OriginDomain) == topology.DomainFloating {
			n.metrics.floatingRaceFastWins++
		}
	} else {
		n.metrics.fastReturnDegraded++
		if n.config.DomainKind(pending.OriginDomain) == topology.DomainFloating {
			n.metrics.floatingResponderFailed++
		}
	}
	n.mu.Unlock()
}

func (n *RPCNode) callNode(ctx context.Context, nodeID string, call func(context.Context, *grpc.ClientConn) error) error {
	peer, ok := n.config.Node(nodeID)
	if !ok {
		return fmt.Errorf("unknown peer %q", nodeID)
	}
	address, err := n.peerAddress(nodeID)
	if err != nil {
		return err
	}
	// Injected partition: the link to this peer is cut, so every attempt fails
	// exactly as a real unreachable peer would (Unavailable, after the one-way
	// link latency so timing stays realistic).
	if n.linkBlocked(nodeID) {
		_ = n.sleepLink(ctx, n.local.DomainCode, peer.DomainCode)
		return status.Error(codes.Unavailable, "injected network partition")
	}
	conn, err := n.connection(address)
	if err != nil {
		return err
	}
	var lastErr error
	policy := n.rpcPolicySnapshot()
	for attempt := 0; attempt < policy.MaxRetries; attempt++ {
		if err := n.sleepLink(ctx, n.local.DomainCode, peer.DomainCode); err != nil {
			return err
		}
		// Injected packet loss: drop this attempt; retries may still succeed.
		if n.dropAttempt() {
			lastErr = status.Error(codes.Unavailable, "injected packet loss")
			continue
		}
		callCtx, cancel := context.WithTimeout(ctx, policy.Deadline)
		lastErr = call(callCtx, conn)
		cancel()
		if err := n.sleepLink(ctx, peer.DomainCode, n.local.DomainCode); err != nil {
			return err
		}
		if lastErr == nil {
			return nil
		}
		if status.Code(lastErr) != codes.Unavailable && status.Code(lastErr) != codes.DeadlineExceeded {
			return lastErr
		}
	}
	return lastErr
}

func (n *RPCNode) sleepLink(ctx context.Context, fromDomain, toDomain string) error {
	delay := n.oneWayDelay(fromDomain, toDomain)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (n *RPCNode) sleepLinkWithoutCancel(fromDomain, toDomain string) {
	delay := n.oneWayDelay(fromDomain, toDomain)
	if delay > 0 {
		time.Sleep(delay)
	}
}

func (n *RPCNode) oneWayDelay(fromDomain, toDomain string) time.Duration {
	n.mu.Lock()
	simulation := n.networkSimulation
	n.mu.Unlock()
	config := topology.Config{NetworkSimulation: simulation}
	return config.OneWayDelay(fromDomain, toDomain)
}

func (n *RPCNode) fastReturnIsEnabled() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.fastReturnEnabled
}

func (n *RPCNode) rpcPolicySnapshot() RPCPolicy {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.rpcPolicy
}

func (n *RPCNode) peerAddress(nodeID string) (string, error) {
	peer, ok := n.config.Node(nodeID)
	if !ok {
		return "", fmt.Errorf("unknown peer %q", nodeID)
	}
	if peer.DomainCode == n.local.DomainCode {
		return peer.ListenAddress, nil
	}
	return peer.InterDomainDialAddress(), nil
}

func (n *RPCNode) connection(address string) (*grpc.ClientConn, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if conn := n.conns[address]; conn != nil {
		return conn, nil
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	n.conns[address] = conn
	return conn, nil
}

func (n *RPCNode) domainLeaderSnapshot() []DomainLeaderIdentity {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]DomainLeaderIdentity, 0, len(n.domainLeaders))
	for _, identity := range n.domainLeaders {
		if identity.Valid() {
			out = append(out, identity)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DomainID < out[j].DomainID })
	return out
}

func (n *RPCNode) observeHigherGlobalTerm(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	wasGlobalLeader := n.globalRole == Leader && n.globalLeader.DomainLeader.NodeID == n.local.ID
	if transitionObserveHigherGlobalTerm(&n.state, &n.globalRole, &n.stage, term) {
		n.globalLeader = GlobalLeaderIdentity{}
		_ = n.persistLocked()
		if wasGlobalLeader {
			log.Printf("cd-raft %s: stepping down as GL: observed higher global term %d", n.local.ID, term)
		}
	}
}

// observeHigherDomainTerm steps this node down from Domain Leader (and any
// dependent global role) when it learns of a higher DomainTerm in its own
// domain — a newer Domain Leader has been elected, so the old one must yield to
// avoid two leaders casting global votes.
func (n *RPCNode) observeHigherDomainTerm(term uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	wasDomainLeader := n.domainRole == Leader && n.state.DomainLeader.NodeID == n.local.ID
	wasGlobalLeader := n.globalRole == Leader && n.globalLeader.DomainLeader.NodeID == n.local.ID
	if !transitionObserveHigherDomainTerm(&n.state, &n.domainRole, term) {
		return
	}
	if wasDomainLeader {
		delete(n.domainLeaders, n.local.DomainCode)
	}
	if wasGlobalLeader {
		n.globalLeader = GlobalLeaderIdentity{}
		transitionRevokeGlobalLeadership(&n.state, &n.globalRole, &n.stage)
	}
	if wasDomainLeader {
		n.stage = DomainElecting
		n.resetDomainDeadlineLocked()
	}
	_ = n.persistLocked()
}

// SetGlobalLeaseEnabled toggles Global Leader read-lease enforcement. It is
// enabled automatically by Run; tests may enable it explicitly to exercise the
// isolation/step-down behaviour.
func (n *RPCNode) SetGlobalLeaseEnabled(enabled bool) {
	n.mu.Lock()
	n.globalLeaseEnabled = enabled
	n.mu.Unlock()
}

// SetGlobalLeaseDuration overrides the lease validity window. It MUST stay
// safely below the minimum global election timeout (currently 600ms) so a new
// Global Leader can never be elected while an old lease is still valid.
func (n *RPCNode) SetGlobalLeaseDuration(d time.Duration) {
	n.mu.Lock()
	if d > 0 {
		n.globalLeaseDuration = d
	}
	n.mu.Unlock()
}

// SetDomainLeaseEnabled toggles Domain Leader lease enforcement (enabled by Run;
// the manual election harness keeps it off).
func (n *RPCNode) SetDomainLeaseEnabled(enabled bool) {
	n.mu.Lock()
	n.domainLeaseEnabled = enabled
	n.mu.Unlock()
}

// SetDomainLeaseDuration overrides the domain lease window (must stay below the
// domain election timeout so a new Domain Leader cannot be elected while an old
// lease is still valid).
func (n *RPCNode) SetDomainLeaseDuration(d time.Duration) {
	n.mu.Lock()
	if d > 0 {
		n.domainLeaseDuration = d
	}
	n.mu.Unlock()
}

func (n *RPCNode) refreshDomainLeaseLocked(sentAt time.Time) {
	if deadline := sentAt.Add(n.domainLeaseDuration); deadline.After(n.domainLeaseDeadline) {
		n.domainLeaseDeadline = deadline
	}
}

// domainLeaseFreshLocked reports whether this node, as Domain Leader, still holds
// a recently-confirmed in-domain majority. When the lease is not enforced (the
// manual election harness), it is always "fresh". A Domain Leader that cannot
// confirm a majority (partition / lost peers) is NOT demoted from domain
// leadership — that would needlessly destroy the domain's availability — but it
// loses the right to cast or grant GLOBAL votes (and to serve as Global Leader),
// which is what prevents an old, majority-less leader and a freshly elected one
// from BOTH wielding their domain's single global vote.
func (n *RPCNode) domainLeaseFreshLocked() bool {
	if !n.domainLeaseEnabled {
		return true
	}
	return time.Now().Before(n.domainLeaseDeadline)
}

// maybeStepDownGlobalOnExpiredDomainLeaseLocked relinquishes GLOBAL leadership
// (only) when a Domain-Leader/Global-Leader can no longer confirm its in-domain
// majority: without a backing domain quorum it must not keep ordering writes or
// serving reads. It deliberately keeps domain leadership so the domain stays a
// valid migration/optimizer candidate and re-converges once peers return.
func (n *RPCNode) maybeStepDownGlobalOnExpiredDomainLeaseLocked(now time.Time) bool {
	if !(n.domainLeaseEnabled && n.domainRole == Leader &&
		n.state.DomainLeader.NodeID == n.local.ID &&
		now.After(n.domainLeaseDeadline) &&
		n.globalRole == Leader && n.globalLeader.DomainLeader.NodeID == n.local.ID) {
		return false
	}
	n.globalLeader = GlobalLeaderIdentity{}
	transitionRevokeGlobalLeadership(&n.state, &n.globalRole, &n.stage)
	n.resetGlobalDeadlineLocked()
	n.recordRejectionLocked("domain_lease_expired")
	return true
}

// refreshGlobalLeaseLocked extends (never shortens) the lease to sentAt+duration.
// sentAt is the time the heartbeat round that produced the quorum acks was
// *sent*, which makes the lease conservative with respect to network delay.
func (n *RPCNode) refreshGlobalLeaseLocked(sentAt time.Time) {
	if deadline := sentAt.Add(n.globalLeaseDuration); deadline.After(n.globalLeaseDeadline) {
		n.globalLeaseDeadline = deadline
	}
}

// maybeStepDownExpiredGlobalLeaseLocked relinquishes global leadership when an
// enforced lease has lapsed (the leader has not heard back from a quorum of
// Domain Leaders within the lease window). This prevents a partitioned/old
// Global Leader from serving stale reads after a new one is elected elsewhere.
// The node stays Domain Leader and re-campaigns once it can reach a quorum
// again. It returns true if it stepped down.
func (n *RPCNode) maybeStepDownExpiredGlobalLeaseLocked(now time.Time) bool {
	if !(n.globalLeaseEnabled && n.globalRole == Leader &&
		n.globalLeader.DomainLeader.NodeID == n.local.ID &&
		now.After(n.globalLeaseDeadline)) {
		return false
	}
	n.globalLeader = GlobalLeaderIdentity{}
	transitionRevokeGlobalLeadership(&n.state, &n.globalRole, &n.stage)
	n.resetGlobalDeadlineLocked()
	n.recordRejectionLocked("global_lease_expired")
	log.Printf("cd-raft %s: stepping down as GL (term %d): read lease lapsed (no quorum of domain-leader confirmations in window)", n.local.ID, n.state.GlobalTerm)
	return true
}

// revokeGlobalLeadershipForHandoffLocked is called by the OUTGOING Global Leader
// at the moment it hands off (a leadership-transfer migration). Because a
// transfer election deliberately bypasses voter stickiness, the only thing that
// keeps the new and old serving leases from overlapping is the incumbent
// explicitly relinquishing first: we step down to Follower and expire our read
// lease NOW, so servesAsGlobalLeaderLocked() turns false before the target can
// begin serving. We also push out our own global election deadline so we do not
// immediately race the handoff with a fresh self-campaign.
func (n *RPCNode) revokeGlobalLeadershipForHandoffLocked() {
	if n.globalRole == Leader && n.globalLeader.DomainLeader.NodeID == n.local.ID {
		n.globalLeader = GlobalLeaderIdentity{}
		transitionRevokeGlobalLeadership(&n.state, &n.globalRole, &n.stage)
	}
	n.globalLeaseDeadline = time.Now()
	n.resetGlobalDeadlineLocked()
	_ = n.persistLocked()
}

// servesAsGlobalLeaderLocked reports whether this node may currently serve
// linearizable client reads/writes: it must be the recorded Global Leader and,
// when the lease is enforced, still hold a fresh lease.
func (n *RPCNode) servesAsGlobalLeaderLocked() bool {
	if n.failed {
		return false
	}
	if n.globalRole != Leader || n.globalLeader.DomainLeader.NodeID != n.local.ID {
		return false
	}
	if n.globalLeaseEnabled && !time.Now().Before(n.globalLeaseDeadline) {
		return false
	}
	return true
}

// globalRedirectAddressLocked returns the address to redirect a client to, or
// "" when the client should simply retry. It never returns our own address: if
// we are the recorded leader but our lease lapsed, we must not bounce the client
// back to ourselves.
func (n *RPCNode) globalRedirectAddressLocked() string {
	if n.globalLeader.DomainLeader.NodeID == n.local.ID {
		return ""
	}
	return n.globalLeaderAddressLocked()
}

func (n *RPCNode) globalLeaderAddressLocked() string {
	if !n.globalLeader.Valid() {
		return ""
	}
	node, ok := n.config.Node(n.globalLeader.DomainLeader.NodeID)
	if !ok {
		return ""
	}
	// Redirects go to clients which may sit in another region, so advertise the
	// client-facing (possibly elastic/public) address, not the intra-domain one.
	return node.ClientDialAddress()
}

// persistLocked durably saves consensus state. A failure is FATAL to this
// node's correctness: it cannot safely keep participating once it is unable to
// persist state it may already be acting on (it would risk acknowledging writes,
// votes, or commits it cannot recover after a crash). On failure it fail-stops —
// marks the node failed, halts it asynchronously, and returns the error so
// ack-critical callers (replicate / commit / vote / heartbeat / Fast Return)
// refuse to confirm un-persisted state instead of lying about durability.
// Best-effort/background callers may ignore the error; the fail-stop side effect
// still quarantines the node.
func (n *RPCNode) persistLocked() error {
	if err := n.store.Save(n.state); err != nil {
		if !n.failed {
			n.failed = true
			log.Printf("cd-raft node %s: FATAL persistence failure: %v; halting node", n.local.ID, err)
			go n.Stop()
		}
		return err
	}
	return nil
}

// Timing parameters. These are the single source of truth for election, lease,
// and heartbeat timing; validateLeaseTiming() asserts the safety relationships
// between them at startup so a misconfiguration cannot silently allow two
// leaders to serve at once.
const (
	// electionDelayFloor/Jitter bound a single randomized election delay to
	// [floor, floor+jitter).
	electionDelayFloor  = 150 * time.Millisecond
	electionDelayJitter = 200 * time.Millisecond
	// A domain/global election fires after this many randomized delays elapse
	// without contact from the respective leader.
	domainElectionDelayFactor = 3
	globalElectionDelayFactor = 4
	// heartbeatInterval is how often a leader refreshes followers/leases.
	heartbeatInterval = 100 * time.Millisecond
)

// minDomainElectionTimeout / minGlobalElectionTimeout are the SHORTEST time a
// rival could possibly be elected (every randomized delay at its floor). Lease
// durations must stay strictly below these so an incumbent's lease always lapses
// before any rival can win.
const (
	minDomainElectionTimeout = domainElectionDelayFactor * electionDelayFloor
	minGlobalElectionTimeout = globalElectionDelayFactor * electionDelayFloor
)

func randomizedElectionDelay() time.Duration {
	return electionDelayFloor + time.Duration(rand.Int63n(int64(electionDelayJitter)))
}

// globalStickyWindow is how long a Domain Leader refuses to grant a competing
// global vote after hearing from a valid Global Leader (Raft leader stickiness).
// It equals the minimum possible global election timeout and MUST be >= the
// leader's read-lease duration so the incumbent's lease always lapses before any
// rival could be elected.
const globalStickyWindow = minGlobalElectionTimeout

func (n *RPCNode) resetDomainDeadlineLocked() {
	n.domainDeadline = time.Now().Add(domainElectionDelayFactor * randomizedElectionDelay())
}

func (n *RPCNode) resetGlobalDeadlineLocked() {
	n.globalDeadline = time.Now().Add(globalElectionDelayFactor * randomizedElectionDelay())
}

// needsGlobalElectionLocked reports whether this node should start a global
// re-campaign because its global deadline lapsed with no Global Leader. It is
// gated on !n.migrating: while WE are driving a handoff the outgoing GL has
// already relinquished its lease, and the re-campaign timer armed at revoke
// would otherwise fire mid-handoff, bump the global term, and tear the
// freshly-elected new GL back down via the higher-term observation path — the
// "flap". The handoff itself installs the new leader; we adopt it on success.
// adoptHandedOffLeaderLocked records the target as the new Global Leader after a
// successful migration handoff, mirroring what a follower would learn from the
// new leader's first heartbeat. Without this, the outgoing GL sits leaderless
// with the re-campaign timer that revoke armed before the handoff round-trip;
// that timer could fire the instant the migrating flag clears — before the new
// GL's first heartbeat crosses the WAN — and flap leadership straight back. We
// only move forward in term (never backward) and re-arm a full global deadline.
func (n *RPCNode) adoptHandedOffLeaderLocked(target DomainLeaderIdentity, newTerm uint64) {
	if newTerm >= n.state.GlobalTerm {
		leader := GlobalLeaderIdentity{DomainLeader: target, GlobalTerm: newTerm}
		n.state.GlobalTerm = newTerm
		n.globalLeader = leader
		n.state.GlobalLeader = leader
	}
	n.lastGlobalContact = time.Now()
	n.resetGlobalDeadlineLocked()
}

func (n *RPCNode) needsGlobalElectionLocked(now time.Time) bool {
	return n.domainRole == Leader &&
		n.globalRole != Leader &&
		!n.globalCampaigning &&
		!n.migrating &&
		now.After(n.globalDeadline)
}

// validateLeaseTiming asserts the timing relationships that guarantee at most one
// serving leader per layer. A violation means the configured lease durations
// could let an old leader keep serving (or voting) while a new one is elected —
// a config-induced dual-leader. It returns an error describing the first
// violated constraint.
func validateLeaseTiming(globalLease, domainLease time.Duration) error {
	switch {
	case globalLease <= 0:
		return fmt.Errorf("global lease duration must be positive, got %s", globalLease)
	case domainLease <= 0:
		return fmt.Errorf("domain lease duration must be positive, got %s", domainLease)
	case globalLease >= minGlobalElectionTimeout:
		return fmt.Errorf("global lease duration (%s) must be < min global election timeout (%s) or two Global Leaders could serve at once",
			globalLease, minGlobalElectionTimeout)
	case domainLease >= minDomainElectionTimeout:
		return fmt.Errorf("domain lease duration (%s) must be < min domain election timeout (%s) or one domain could cast two global votes",
			domainLease, minDomainElectionTimeout)
	case globalLease > globalStickyWindow:
		return fmt.Errorf("global lease duration (%s) must be <= leader-stickiness window (%s) so a rival cannot win before the incumbent lease lapses",
			globalLease, globalStickyWindow)
	case heartbeatInterval >= globalLease:
		return fmt.Errorf("heartbeat interval (%s) must be < global lease duration (%s) so a healthy leader can renew its lease",
			heartbeatInterval, globalLease)
	case heartbeatInterval >= domainLease:
		return fmt.Errorf("heartbeat interval (%s) must be < domain lease duration (%s) so a healthy leader can renew its lease",
			heartbeatInterval, domainLease)
	}
	return nil
}

func hasOtherDomainQuorum(quorums map[string]bool, globalDomain string) bool {
	for domain, quorum := range quorums {
		if domain != globalDomain && quorum {
			return true
		}
	}
	return false
}

func toPBDomainIdentity(identity DomainLeaderIdentity) *cdraftv1.DomainLeaderIdentity {
	return &cdraftv1.DomainLeaderIdentity{DomainId: identity.DomainID, NodeId: identity.NodeID, DomainTerm: identity.DomainTerm}
}

func fromPBDomainIdentity(identity *cdraftv1.DomainLeaderIdentity) DomainLeaderIdentity {
	if identity == nil {
		return DomainLeaderIdentity{}
	}
	return DomainLeaderIdentity{DomainID: identity.GetDomainId(), NodeID: identity.GetNodeId(), DomainTerm: identity.GetDomainTerm()}
}

func toPBLogSummary(summary LogSummary) *cdraftv1.LogSummary {
	return &cdraftv1.LogSummary{LastGlobalTerm: summary.LastGlobalTerm, LastGlobalIndex: summary.LastGlobalIndex}
}

func fromPBLogSummary(summary *cdraftv1.LogSummary) LogSummary {
	if summary == nil {
		return LogSummary{}
	}
	return LogSummary{LastGlobalTerm: summary.GetLastGlobalTerm(), LastGlobalIndex: summary.GetLastGlobalIndex()}
}

func toPBClientResult(result ClientResult, redirect string) *cdraftv1.ClientResult {
	return &cdraftv1.ClientResult{
		RequestId: result.RequestID, GlobalTerm: result.GlobalTerm, GlobalIndex: result.GlobalIndex,
		Result: result.Result, Source: string(result.Source), Committed: result.Committed, RedirectAddress: redirect,
		ResponderDomain: result.ResponderDomain,
	}
}

func fromPBClientResult(result *cdraftv1.ClientResult) ClientResult {
	if result == nil {
		return ClientResult{}
	}
	return ClientResult{
		RequestID: result.GetRequestId(), GlobalTerm: result.GetGlobalTerm(), GlobalIndex: result.GetGlobalIndex(),
		Result: result.GetResult(), Source: ResultSource(result.GetSource()), Committed: result.GetCommitted(),
		ResponderDomain: result.GetResponderDomain(),
	}
}

type domainElectionService struct {
	cdraftv1.UnimplementedDomainElectionServer
	node *RPCNode
}

func (s *domainElectionService) RequestVote(ctx context.Context, request *cdraftv1.DomainVoteRequest) (*cdraftv1.DomainVoteResponse, error) {
	return s.node.RequestVote(ctx, request)
}

func (s *domainElectionService) Heartbeat(ctx context.Context, request *cdraftv1.DomainHeartbeat) (*cdraftv1.DomainVoteResponse, error) {
	return s.node.Heartbeat(ctx, request)
}

func (s *domainElectionService) PublishReady(ctx context.Context, request *cdraftv1.DomainLeaderReady) (*cdraftv1.DomainVoteResponse, error) {
	return s.node.PublishReady(ctx, request)
}

// The generated GlobalElection interface still requires RequestVote and
// Heartbeat, which conflict by name with DomainElection. Separate adapters
// below are registered with the same underlying node.
type globalElectionService struct {
	cdraftv1.UnimplementedGlobalElectionServer
	node *RPCNode
}

func (s *globalElectionService) RequestVote(ctx context.Context, request *cdraftv1.GlobalVoteRequest) (*cdraftv1.GlobalVoteResponse, error) {
	return s.node.RequestVoteGlobalInternal(ctx, request)
}

func (s *globalElectionService) Heartbeat(ctx context.Context, request *cdraftv1.GlobalHeartbeat) (*cdraftv1.GlobalVoteResponse, error) {
	return s.node.HeartbeatGlobalInternal(ctx, request)
}
