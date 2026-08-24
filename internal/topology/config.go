package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"
)

const ApplicationGroup = "cd-raft"

type Node struct {
	ID         string `json:"id"`
	DomainCode string `json:"domainCode"`
	// ListenAddress is what same-domain peers and clients dial, and (unless
	// overridden by BindListenAddress) what this node binds its client/intra
	// server to.
	ListenAddress string `json:"listenAddress"`
	// InterDomainAddress is what cross-domain peers dial (unless overridden by
	// PublicInterDomainAddress), and (unless overridden by BindInterDomainAddress)
	// what this node binds its inter-domain server to.
	InterDomainAddress string `json:"interDomainAddress"`

	// The fields below are OPTIONAL and exist for cloud deployments where the
	// address a node binds locally differs from the address peers use to reach
	// it (e.g. an elastic/public IP that is NAT-mapped and cannot be bound
	// directly). Leaving them empty reproduces the original behaviour exactly.

	// BindListenAddress overrides the local bind address of the client/intra
	// server. Defaults to ListenAddress. Set to e.g. "0.0.0.0:7101" to listen on
	// all interfaces while advertising a routable ListenAddress to peers.
	BindListenAddress string `json:"bindListenAddress,omitempty"`
	// BindInterDomainAddress overrides the local bind address of the inter-domain
	// server. Defaults to InterDomainAddress. Set to e.g. "0.0.0.0:8101" when the
	// inter-domain reach address is an elastic IP that cannot be bound directly.
	BindInterDomainAddress string `json:"bindInterDomainAddress,omitempty"`
	// PublicInterDomainAddress is the address cross-domain peers actually dial,
	// typically the node's elastic/public IP and inter-domain port. When set it
	// overrides InterDomainAddress for cross-domain dialing only (binding is
	// unaffected). This is the single field to edit when an elastic IP changes.
	PublicInterDomainAddress string `json:"publicInterDomainAddress,omitempty"`
	// PublicListenAddress is the client-facing address advertised to clients and
	// external tools (GL redirects, cdraft-mover). Set it when ListenAddress is a
	// private/loopback address that remote clients cannot reach, e.g. multiple
	// nodes of one domain co-located on a machine use the private IP as
	// ListenAddress for same-domain peer dialing, while clients in other regions
	// must be redirected to the elastic IP. Defaults to ListenAddress.
	PublicListenAddress string `json:"publicListenAddress,omitempty"`
}

// ClientBindAddress is the local address the client/intra-domain gRPC server
// binds to (BindListenAddress if set, else ListenAddress).
func (n Node) ClientBindAddress() string {
	if n.BindListenAddress != "" {
		return n.BindListenAddress
	}
	return n.ListenAddress
}

// InterDomainBindAddress is the local address the inter-domain gRPC server binds
// to (BindInterDomainAddress if set, else InterDomainAddress).
func (n Node) InterDomainBindAddress() string {
	if n.BindInterDomainAddress != "" {
		return n.BindInterDomainAddress
	}
	return n.InterDomainAddress
}

// InterDomainDialAddress is the address cross-domain peers should dial to reach
// this node (PublicInterDomainAddress if set, else InterDomainAddress).
func (n Node) InterDomainDialAddress() string {
	if n.PublicInterDomainAddress != "" {
		return n.PublicInterDomainAddress
	}
	return n.InterDomainAddress
}

// ClientDialAddress is the address clients and external tools should dial to
// reach this node (PublicListenAddress if set, else ListenAddress). Used for
// Global Leader redirects and by cdraft-mover; same-domain peers keep dialing
// ListenAddress directly.
func (n Node) ClientDialAddress() string {
	if n.PublicListenAddress != "" {
		return n.PublicListenAddress
	}
	return n.ListenAddress
}

type Features struct {
	FastReturnEnabled bool `json:"fastReturnEnabled"`
	// FloatingDomains is an optional allowlist of client-only domains. These
	// domains can contribute client load and latency telemetry, but never create
	// members, leaders, quorum state, or Global Leader candidates.
	FloatingDomains []string `json:"floatingDomains,omitempty"`
	// FloatingTelemetryTTLMillis bounds how long client-reported floating-domain
	// latency measurements can drive responder selection or optimizer input.
	// 0 uses the runtime default.
	FloatingTelemetryTTLMillis uint64 `json:"floatingTelemetryTtlMillis,omitempty"`
	// AllowUnknownFloatingDomains permits experiments to treat unknown non-empty
	// client origins as floating domains without predeclaring them.
	AllowUnknownFloatingDomains bool `json:"allowUnknownFloatingDomains,omitempty"`
	// LogCompactionThreshold enables automatic Raft log compaction: once more
	// than this many applied entries sit above the last snapshot point, the node
	// folds the applied prefix into its snapshot and discards those log entries.
	// 0 disables automatic compaction.
	LogCompactionThreshold uint64 `json:"logCompactionThreshold,omitempty"`
	// LogCompactionRetain is how many of the newest applied entries to KEEP in
	// the log when compacting, so ordinary lagging followers can still be healed
	// by a cheap CatchUp backfill instead of a full InstallSnapshot.
	LogCompactionRetain uint64 `json:"logCompactionRetain,omitempty"`
}

type Unsupported struct {
	ApplicationGroups        int  `json:"applicationGroups,omitempty"`
	Shards                   int  `json:"shards,omitempty"`
	CrossGroupTransactions   bool `json:"crossGroupTransactions,omitempty"`
	RuntimeMembershipChanges bool `json:"runtimeMembershipChanges,omitempty"`
}

type NetworkSimulation struct {
	Enabled                 bool           `json:"enabled"`
	LocalOneWayDelayMillis  int            `json:"localOneWayDelayMillis"`
	InterDomainOneWayMillis map[string]int `json:"interDomainOneWayMillis"`
}

type Config struct {
	ApplicationGroup  string            `json:"applicationGroup"`
	Nodes             []Node            `json:"nodes"`
	Features          Features          `json:"features"`
	NetworkSimulation NetworkSimulation `json:"networkSimulation,omitempty"`
	Unsupported       Unsupported       `json:"unsupported,omitempty"`
}

type DomainKind int

const (
	DomainUnknown DomainKind = iota
	DomainConsensus
	DomainFloating
)

func (k DomainKind) String() string {
	switch k {
	case DomainConsensus:
		return "consensus"
	case DomainFloating:
		return "floating"
	default:
		return "unknown"
	}
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode topology: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.ApplicationGroup != ApplicationGroup {
		return fmt.Errorf("applicationGroup must be %q", ApplicationGroup)
	}
	if c.Unsupported.ApplicationGroups > 1 {
		return errors.New("multiple application-level groups are not supported")
	}
	if c.Unsupported.Shards > 0 {
		return errors.New("sharding is not supported")
	}
	if c.Unsupported.CrossGroupTransactions {
		return errors.New("cross-group transactions are not supported")
	}
	if c.Unsupported.RuntimeMembershipChanges {
		return errors.New("runtime membership changes are not supported")
	}
	if c.NetworkSimulation.LocalOneWayDelayMillis < 0 {
		return errors.New("local one-way delay must not be negative")
	}
	for route, delay := range c.NetworkSimulation.InterDomainOneWayMillis {
		if delay < 0 {
			return fmt.Errorf("inter-domain delay %q must not be negative", route)
		}
		parts := strings.Split(route, "->")
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("inter-domain delay route %q must use from->to", route)
		}
	}
	if len(c.Nodes) == 0 {
		return errors.New("topology has no nodes")
	}

	ids := make(map[string]struct{}, len(c.Nodes))
	listenAddresses := make(map[string]struct{}, len(c.Nodes))
	interDomainAddresses := make(map[string]struct{}, len(c.Nodes))
	domains := make(map[string]int)
	for _, node := range c.Nodes {
		if node.ID == "" {
			return errors.New("node id is empty")
		}
		if node.DomainCode == "" {
			return fmt.Errorf("node %q has empty domainCode", node.ID)
		}
		if node.ListenAddress == "" || node.InterDomainAddress == "" {
			return fmt.Errorf("node %q has an empty address", node.ID)
		}
		// The optional cloud overrides are real sockets the node binds or dials,
		// so they must parse as host:port. ListenAddress/InterDomainAddress are
		// intentionally NOT checked here: the in-memory test model uses opaque
		// (non host:port) labels for them.
		for label, addr := range map[string]string{
			"bindListenAddress":        node.BindListenAddress,
			"bindInterDomainAddress":   node.BindInterDomainAddress,
			"publicInterDomainAddress": node.PublicInterDomainAddress,
			"publicListenAddress":      node.PublicListenAddress,
		} {
			if addr == "" {
				continue
			}
			if _, _, err := net.SplitHostPort(addr); err != nil {
				return fmt.Errorf("node %q has invalid %s %q: %w", node.ID, label, addr, err)
			}
		}
		if _, ok := ids[node.ID]; ok {
			return fmt.Errorf("duplicate node id %q", node.ID)
		}
		if _, ok := listenAddresses[node.ListenAddress]; ok {
			return fmt.Errorf("duplicate listen address %q", node.ListenAddress)
		}
		if _, ok := interDomainAddresses[node.InterDomainAddress]; ok {
			return fmt.Errorf("duplicate inter-domain address %q", node.InterDomainAddress)
		}
		ids[node.ID] = struct{}{}
		listenAddresses[node.ListenAddress] = struct{}{}
		interDomainAddresses[node.InterDomainAddress] = struct{}{}
		domains[node.DomainCode]++
	}
	if len(domains) < 2 {
		return errors.New("CD-Raft requires at least two non-empty domains")
	}
	seenFloating := make(map[string]struct{}, len(c.Features.FloatingDomains))
	for _, domain := range c.Features.FloatingDomains {
		if domain == "" {
			return errors.New("floating domain id is empty")
		}
		if _, ok := domains[domain]; ok {
			return fmt.Errorf("floating domain %q overlaps a consensus domain", domain)
		}
		if _, ok := seenFloating[domain]; ok {
			return fmt.Errorf("duplicate floating domain %q", domain)
		}
		seenFloating[domain] = struct{}{}
	}
	return nil
}

func (c Config) OneWayDelay(fromDomain, toDomain string) time.Duration {
	if !c.NetworkSimulation.Enabled {
		return 0
	}
	if fromDomain == toDomain {
		return time.Duration(c.NetworkSimulation.LocalOneWayDelayMillis) * time.Millisecond
	}
	key := fromDomain + "->" + toDomain
	delay, ok := c.NetworkSimulation.InterDomainOneWayMillis[key]
	if !ok {
		delay = c.NetworkSimulation.InterDomainOneWayMillis[toDomain+"->"+fromDomain]
	}
	return time.Duration(delay) * time.Millisecond
}

func (c Config) Domains() []string {
	return c.ConsensusDomains()
}

func (c Config) ConsensusDomains() []string {
	set := make(map[string]struct{})
	for _, node := range c.Nodes {
		set[node.DomainCode] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for domain := range set {
		out = append(out, domain)
	}
	sort.Strings(out)
	return out
}

func (c Config) FloatingDomains() []string {
	out := append([]string(nil), c.Features.FloatingDomains...)
	sort.Strings(out)
	return out
}

func (c Config) DomainKind(domain string) DomainKind {
	if domain == "" {
		return DomainUnknown
	}
	for _, consensus := range c.ConsensusDomains() {
		if domain == consensus {
			return DomainConsensus
		}
	}
	for _, floating := range c.Features.FloatingDomains {
		if domain == floating {
			return DomainFloating
		}
	}
	if c.Features.AllowUnknownFloatingDomains {
		return DomainFloating
	}
	return DomainUnknown
}

func (c Config) IsConsensusDomain(domain string) bool {
	return c.DomainKind(domain) == DomainConsensus
}

func (c Config) IsFloatingDomain(domain string) bool {
	return c.DomainKind(domain) == DomainFloating
}

func (c Config) Node(id string) (Node, bool) {
	for _, node := range c.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return Node{}, false
}

func (c Config) DomainMembers(domainCode string) []Node {
	var out []Node
	for _, node := range c.Nodes {
		if node.DomainCode == domainCode {
			out = append(out, node)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (c Config) SameDomain(nodeID string) ([]Node, error) {
	node, ok := c.Node(nodeID)
	if !ok {
		return nil, fmt.Errorf("unknown node %q", nodeID)
	}
	return c.DomainMembers(node.DomainCode), nil
}
