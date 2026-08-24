package harness

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	cdraftv1 "github.com/czq/cd-raft/gen/cdraft/v1"
	"github.com/czq/cd-raft/internal/cdraft"
	"github.com/czq/cd-raft/internal/topology"
)

const pollInterval = 50 * time.Millisecond

const (
	StoreProfileMemory        = "memory"
	StoreProfileLevelDBSync   = "leveldb-sync"
	StoreProfileLevelDBNoSync = "leveldb-nosync"
)

// Options configures a real-gRPC CD-Raft harness. When ConfigPath and Config
// are empty, New builds the default local three-domain topology.
type Options struct {
	ConfigPath        string
	Config            topology.Config
	FastReturnEnabled bool
	RPCPolicy         *cdraft.RPCPolicy
	NetworkSimulation *topology.NetworkSimulation
	StatsWindow       time.Duration
	CatchUpTimeout    time.Duration
	StoreProfile      string
	DataDir           string
}

// Harness owns a set of real RPCNode instances plus the client and stores used
// to drive them. Tests and CLIs should interact with nodes through the exported
// methods rather than mutating the maps directly.
type Harness struct {
	Config topology.Config
	Nodes  map[string]*cdraft.RPCNode
	Stores map[string]cdraft.Store
	Client *cdraft.RPCClient

	mu                   sync.Mutex
	started              bool
	closed               bool
	runCtx               context.Context
	runCancel            context.CancelFunc
	rpcPolicy            *cdraft.RPCPolicy
	networkSimulation    *topology.NetworkSimulation
	statsWindow          time.Duration
	catchUpTimeout       time.Duration
	fastReturnConfigured bool
	storeProfile         string
	dataDir              string
	removeDataDir        bool
	storeClosers         []interface{ Close() error }
}

// LeaderInfo is the stable leader summary used in reports.
type LeaderInfo struct {
	NodeID     string `json:"nodeId,omitempty"`
	DomainID   string `json:"domainId,omitempty"`
	GlobalTerm uint64 `json:"globalTerm,omitempty"`
	Address    string `json:"address,omitempty"`
}

// New builds a harness and allocates all node objects. It does not start node
// listeners; call Start or Run when the scenario is ready.
func New(options Options) (*Harness, error) {
	cfg, err := configFromOptions(options)
	if err != nil {
		return nil, err
	}
	cfg.Features.FastReturnEnabled = options.FastReturnEnabled
	if options.NetworkSimulation != nil {
		cfg.NetworkSimulation = *options.NetworkSimulation
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	h := &Harness{
		Config:               cfg,
		Nodes:                make(map[string]*cdraft.RPCNode, len(cfg.Nodes)),
		Stores:               make(map[string]cdraft.Store, len(cfg.Nodes)),
		Client:               cdraft.NewRPCClient(),
		statsWindow:          options.StatsWindow,
		catchUpTimeout:       options.CatchUpTimeout,
		fastReturnConfigured: options.FastReturnEnabled,
		rpcPolicy:            options.RPCPolicy,
		networkSimulation:    options.NetworkSimulation,
		storeProfile:         nonEmpty(options.StoreProfile, StoreProfileMemory),
	}
	if h.storeProfile != StoreProfileMemory && h.storeProfile != StoreProfileLevelDBSync && h.storeProfile != StoreProfileLevelDBNoSync {
		return nil, fmt.Errorf("unknown store profile %q", h.storeProfile)
	}
	if h.storeProfile == StoreProfileLevelDBSync || h.storeProfile == StoreProfileLevelDBNoSync {
		h.dataDir = options.DataDir
		if h.dataDir == "" {
			dir, err := os.MkdirTemp("", "cdraft-harness-leveldb-*")
			if err != nil {
				return nil, err
			}
			h.dataDir = dir
			h.removeDataDir = true
		}
	}
	for _, configured := range cfg.Nodes {
		store, err := h.newStore(configured.ID)
		if err != nil {
			h.Close()
			return nil, err
		}
		node, err := cdraft.NewRPCNode(cfg, configured.ID, store)
		if err != nil {
			h.Close()
			return nil, err
		}
		h.Stores[configured.ID] = store
		h.Nodes[configured.ID] = node
		h.applyNodeSettings(node)
	}
	return h, nil
}

func (h *Harness) newStore(nodeID string) (cdraft.Store, error) {
	switch h.storeProfile {
	case StoreProfileMemory:
		return cdraft.NewMemoryStore(), nil
	case StoreProfileLevelDBSync:
		store, err := cdraft.NewLevelDBStore(filepath.Join(h.dataDir, nodeID))
		if err != nil {
			return nil, err
		}
		h.storeClosers = append(h.storeClosers, store)
		return store, nil
	case StoreProfileLevelDBNoSync:
		store, err := cdraft.NewLevelDBStoreWithSync(filepath.Join(h.dataDir, nodeID), false)
		if err != nil {
			return nil, err
		}
		h.storeClosers = append(h.storeClosers, store)
		return store, nil
	default:
		return nil, fmt.Errorf("unknown store profile %q", h.storeProfile)
	}
}

func configFromOptions(options Options) (topology.Config, error) {
	if options.ConfigPath != "" {
		return topology.Load(options.ConfigPath)
	}
	if len(options.Config.Nodes) > 0 {
		return options.Config, nil
	}
	return DefaultConfig(options.FastReturnEnabled)
}

// DefaultConfig returns a local, static three-domain / three-node topology using
// free loopback ports for both client and inter-domain listeners.
func DefaultConfig(fastReturn bool) (topology.Config, error) {
	var nodes []topology.Node
	for _, domain := range []string{"a", "b", "c"} {
		for i := 1; i <= 3; i++ {
			listen, err := FreeAddress()
			if err != nil {
				return topology.Config{}, err
			}
			inter, err := FreeAddress()
			if err != nil {
				return topology.Config{}, err
			}
			id := fmt.Sprintf("%s%d", domain, i)
			nodes = append(nodes, topology.Node{
				ID: id, DomainCode: domain, ListenAddress: listen, InterDomainAddress: inter,
			})
		}
	}
	cfg := topology.Config{
		ApplicationGroup: topology.ApplicationGroup,
		Nodes:            nodes,
		Features: topology.Features{
			FastReturnEnabled: fastReturn,
		},
	}
	if err := cfg.Validate(); err != nil {
		return topology.Config{}, err
	}
	return cfg, nil
}

// DefaultLatencyProfile mirrors the repeatable three-domain profile used by
// existing latency experiments.
func DefaultLatencyProfile() topology.NetworkSimulation {
	return topology.NetworkSimulation{
		Enabled: true, LocalOneWayDelayMillis: 1,
		InterDomainOneWayMillis: map[string]int{
			"a->b": 50, "b->a": 50,
			"a->c": 80, "c->a": 80,
			"b->c": 60, "c->b": 60,
		},
	}
}

// FreeAddress reserves and releases a loopback TCP address for local harness
// use. The caller should bind it soon after to minimize normal port races.
func FreeAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return address, nil
}

func (h *Harness) applyNodeSettings(node *cdraft.RPCNode) {
	if h.rpcPolicy != nil {
		node.SetRPCPolicy(*h.rpcPolicy)
	}
	if h.networkSimulation != nil {
		node.SetNetworkSimulation(*h.networkSimulation)
	}
	if h.statsWindow > 0 {
		node.SetStatsWindow(h.statsWindow)
	}
	if h.catchUpTimeout > 0 {
		node.SetCatchUpTimeout(h.catchUpTimeout)
	}
	node.SetFastReturnEnabled(h.fastReturnConfigured)
}

// Start opens every node's real gRPC listeners.
func (h *Harness) Start() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return errors.New("harness is closed")
	}
	if h.started {
		h.mu.Unlock()
		return nil
	}
	nodes := h.nodeSnapshotLocked()
	h.mu.Unlock()

	for _, node := range nodes {
		if err := node.Start(); err != nil {
			return err
		}
	}

	h.mu.Lock()
	h.started = true
	h.mu.Unlock()
	return nil
}

// Run starts listeners if needed and then runs every node's live runtime until
// ctx is cancelled or Close is called.
func (h *Harness) Run(ctx context.Context) error {
	if err := h.Start(); err != nil {
		return err
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return errors.New("harness is closed")
	}
	if h.runCancel != nil {
		h.mu.Unlock()
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	h.runCtx = runCtx
	h.runCancel = cancel
	nodes := h.nodeSnapshotLocked()
	h.mu.Unlock()

	for _, node := range nodes {
		go node.Run(runCtx)
	}
	return nil
}

// Close stops the live runtime, all nodes, and the shared client.
func (h *Harness) Close() {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.closed = true
	cancel := h.runCancel
	nodes := h.nodeSnapshotLocked()
	client := h.Client
	closers := append([]interface{ Close() error }(nil), h.storeClosers...)
	dataDir := h.dataDir
	removeDataDir := h.removeDataDir
	h.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if client != nil {
		client.Close()
	}
	for _, node := range nodes {
		node.Stop()
	}
	for _, closer := range closers {
		_ = closer.Close()
	}
	if removeDataDir && dataDir != "" {
		_ = os.RemoveAll(dataDir)
	}
}

func (h *Harness) nodeSnapshotLocked() []*cdraft.RPCNode {
	ids := h.nodeIDsLocked()
	nodes := make([]*cdraft.RPCNode, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, h.Nodes[id])
	}
	return nodes
}

func (h *Harness) nodeIDsLocked() []string {
	ids := make([]string, 0, len(h.Nodes))
	for id := range h.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// NodeIDs returns the configured node IDs in stable order.
func (h *Harness) NodeIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodeIDsLocked()
}

// DomainNodeIDs returns all node IDs in a domain in stable order.
func (h *Harness) DomainNodeIDs(domain string) []string {
	ids := make([]string, 0)
	for _, node := range h.Config.Nodes {
		if node.DomainCode == domain {
			ids = append(ids, node.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// Address returns a node's client-facing dial address.
func (h *Harness) Address(nodeID string) (string, error) {
	node, ok := h.Config.Node(nodeID)
	if !ok {
		return "", fmt.Errorf("unknown node %q", nodeID)
	}
	return node.ClientDialAddress(), nil
}

// EntryAddress returns the first configured node's client-facing address.
func (h *Harness) EntryAddress() (string, error) {
	ids := h.NodeIDs()
	if len(ids) == 0 {
		return "", errors.New("harness has no nodes")
	}
	return h.Address(ids[0])
}

func (h *Harness) resolveTarget(target string) (string, error) {
	if target == "" {
		return h.EntryAddress()
	}
	if node, ok := h.Config.Node(target); ok {
		return node.ClientDialAddress(), nil
	}
	if strings.Contains(target, ":") {
		return target, nil
	}
	return "", fmt.Errorf("unknown target %q", target)
}

// Status queries one node through the real client-facing gRPC API.
func (h *Harness) Status(ctx context.Context, nodeIDOrAddress string) (*cdraftv1.NodeStatusResponse, error) {
	target, err := h.resolveTarget(nodeIDOrAddress)
	if err != nil {
		return nil, err
	}
	return h.Client.Status(ctx, target)
}

// Metrics queries one node's metrics snapshot through the real gRPC API.
func (h *Harness) Metrics(ctx context.Context, nodeIDOrAddress string) (*cdraftv1.MetricsResponse, error) {
	target, err := h.resolveTarget(nodeIDOrAddress)
	if err != nil {
		return nil, err
	}
	return h.Client.Metrics(ctx, target)
}

// WaitServing waits until every configured node reports Serving and agrees on a
// single Global Leader.
func (h *Harness) WaitServing(ctx context.Context) (LeaderInfo, error) {
	var lastErr error
	var lastLeader LeaderInfo
	for {
		leader, ok, err := h.servingSnapshot(ctx)
		if err == nil && ok {
			return leader, nil
		}
		if err != nil {
			lastErr = err
		}
		if leader.NodeID != "" {
			lastLeader = leader
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return lastLeader, fmt.Errorf("wait serving: %w", lastErr)
			}
			return lastLeader, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

func (h *Harness) servingSnapshot(ctx context.Context) (LeaderInfo, bool, error) {
	var leader LeaderInfo
	for _, id := range h.NodeIDs() {
		addr, err := h.Address(id)
		if err != nil {
			return leader, false, err
		}
		status, err := h.Client.Status(ctx, addr)
		if err != nil {
			return leader, false, err
		}
		if status.GetStage() != string(cdraft.Serving) || status.GetGlobalLeader().GetNodeId() == "" {
			return leader, false, nil
		}
		current := leaderFromStatus(status)
		if current.Address == "" {
			current.Address = status.GetGlobalLeaderAddress()
		}
		if leader.NodeID == "" {
			leader = current
			continue
		}
		if leader.NodeID != current.NodeID || leader.GlobalTerm != current.GlobalTerm {
			return leader, false, nil
		}
	}
	return leader, leader.NodeID != "", nil
}

func leaderFromStatus(status *cdraftv1.NodeStatusResponse) LeaderInfo {
	gl := status.GetGlobalLeader()
	return LeaderInfo{
		NodeID:     gl.GetNodeId(),
		DomainID:   gl.GetDomainId(),
		GlobalTerm: status.GetGlobalTerm(),
		Address:    status.GetGlobalLeaderAddress(),
	}
}

// CurrentGlobalLeader returns the first currently observed Serving Global
// Leader. Use WaitServing when convergence is required.
func (h *Harness) CurrentGlobalLeader(ctx context.Context) (LeaderInfo, error) {
	for _, id := range h.NodeIDs() {
		status, err := h.Status(ctx, id)
		if err != nil {
			continue
		}
		if status.GetStage() == string(cdraft.Serving) && status.GetGlobalLeader().GetNodeId() != "" {
			return leaderFromStatus(status), nil
		}
	}
	return LeaderInfo{}, errors.New("no serving global leader observed")
}

// WriteUntilCommitted retries an idempotent write until it commits or budget is
// exhausted. The caller should provide a stable RequestID.
func (h *Harness) WriteUntilCommitted(ctx context.Context, target string, req cdraft.ClientWriteRequest, budget time.Duration) (cdraft.RaceDecision, error) {
	if req.RequestID == "" {
		return cdraft.RaceDecision{}, errors.New("request id is required")
	}
	addr, err := h.resolveTarget(target)
	if err != nil {
		return cdraft.RaceDecision{}, err
	}
	deadline := time.Now().Add(budget)
	var lastErr error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			break
		}
		attemptBudget := time.Until(deadline)
		if attemptBudget > 2*time.Second {
			attemptBudget = 2 * time.Second
		}
		if attemptBudget <= 0 {
			break
		}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptBudget)
		decision, err := h.Client.Write(attemptCtx, addr, req)
		cancel()
		if err == nil && decision.Result.Committed {
			return decision, nil
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(pollInterval)
	}
	if ctx.Err() != nil {
		return cdraft.RaceDecision{}, ctx.Err()
	}
	if lastErr != nil {
		return cdraft.RaceDecision{}, lastErr
	}
	return cdraft.RaceDecision{}, fmt.Errorf("write %s did not commit within %s", req.RequestID, budget)
}

// ReadUntil retries a linearizable read until it returns the wanted value.
func (h *Harness) ReadUntil(ctx context.Context, target, origin, key, want string, budget time.Duration) (string, error) {
	addr, err := h.resolveTarget(target)
	if err != nil {
		return "", err
	}
	deadline := time.Now().Add(budget)
	var lastValue string
	var lastErr error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			break
		}
		attemptBudget := time.Until(deadline)
		if attemptBudget > 2*time.Second {
			attemptBudget = 2 * time.Second
		}
		if attemptBudget <= 0 {
			break
		}
		attemptCtx, cancel := context.WithTimeout(ctx, attemptBudget)
		value, err := h.Client.ReadFrom(attemptCtx, addr, origin, key)
		cancel()
		if err == nil {
			lastValue = value
			if value == want {
				return value, nil
			}
		} else {
			lastErr = err
		}
		time.Sleep(pollInterval)
	}
	if ctx.Err() != nil {
		return lastValue, ctx.Err()
	}
	if lastErr != nil {
		return lastValue, fmt.Errorf("read %q did not return %q within %s (last=%q, err=%w)", key, want, budget, lastValue, lastErr)
	}
	return lastValue, fmt.Errorf("read %q did not return %q within %s (last=%q)", key, want, budget, lastValue)
}

// Move requests a safe Global Leader handoff through the existing telemetry RPC.
func (h *Harness) Move(ctx context.Context, target, targetDomain, reason string) (*cdraftv1.MoveResponse, error) {
	addr, err := h.resolveTarget(target)
	if err != nil {
		return nil, err
	}
	return h.Client.Move(ctx, addr, targetDomain, reason)
}

// Partition installs a symmetric bidirectional partition between two node sets.
func (h *Harness) Partition(groupA, groupB []string) error {
	if err := h.validateNodeIDs(groupA...); err != nil {
		return err
	}
	if err := h.validateNodeIDs(groupB...); err != nil {
		return err
	}
	for _, id := range groupA {
		h.Nodes[id].SetPartitionedFrom(groupB...)
	}
	for _, id := range groupB {
		h.Nodes[id].SetPartitionedFrom(groupA...)
	}
	return nil
}

// Heal clears all injected outbound partitions.
func (h *Harness) Heal() {
	for _, node := range h.Nodes {
		node.HealPartition()
	}
}

// SetDropRate configures probabilistic outbound RPC loss on every node.
func (h *Harness) SetDropRate(rate float64) error {
	if rate < 0 || rate > 1 {
		return fmt.Errorf("drop rate %.3f outside [0,1]", rate)
	}
	for _, node := range h.Nodes {
		node.SetDropRate(rate)
	}
	return nil
}

// SetNetworkSimulation applies a latency profile to every node and records it
// for future restarts.
func (h *Harness) SetNetworkSimulation(simulation topology.NetworkSimulation) {
	h.mu.Lock()
	h.networkSimulation = &simulation
	nodes := h.nodeSnapshotLocked()
	h.mu.Unlock()
	for _, node := range nodes {
		node.SetNetworkSimulation(simulation)
	}
}

// SetRPCPolicy applies a policy to every node and records it for future restarts.
func (h *Harness) SetRPCPolicy(policy cdraft.RPCPolicy) {
	h.mu.Lock()
	h.rpcPolicy = &policy
	nodes := h.nodeSnapshotLocked()
	h.mu.Unlock()
	for _, node := range nodes {
		node.SetRPCPolicy(policy)
	}
}

// SetCatchUpTimeout applies the migration catch-up timeout to every node.
func (h *Harness) SetCatchUpTimeout(timeout time.Duration) {
	h.mu.Lock()
	h.catchUpTimeout = timeout
	nodes := h.nodeSnapshotLocked()
	h.mu.Unlock()
	for _, node := range nodes {
		node.SetCatchUpTimeout(timeout)
	}
}

// Restart replaces a node process while preserving its address and store.
func (h *Harness) Restart(id string) error {
	h.mu.Lock()
	old, ok := h.Nodes[id]
	store := h.Stores[id]
	runCtx := h.runCtx
	h.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown node %q", id)
	}
	if old != nil {
		old.Stop()
	}
	node, err := cdraft.NewRPCNode(h.Config, id, store)
	if err != nil {
		return err
	}
	h.applyNodeSettings(node)
	if err := node.Start(); err != nil {
		return err
	}
	h.mu.Lock()
	h.Nodes[id] = node
	h.mu.Unlock()
	if runCtx != nil {
		go node.Run(runCtx)
	}
	return nil
}

func (h *Harness) validateNodeIDs(ids ...string) error {
	for _, id := range ids {
		if _, ok := h.Nodes[id]; !ok {
			return fmt.Errorf("unknown node %q", id)
		}
	}
	return nil
}
