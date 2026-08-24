package topology

import (
	"path/filepath"
	"testing"
	"time"
)

func validConfig() Config {
	return Config{
		ApplicationGroup: ApplicationGroup,
		Nodes: []Node{
			{ID: "a1", DomainCode: "a", ListenAddress: "a1-client", InterDomainAddress: "a1-domain"},
			{ID: "a2", DomainCode: "a", ListenAddress: "a2-client", InterDomainAddress: "a2-domain"},
			{ID: "b1", DomainCode: "b", ListenAddress: "b1-client", InterDomainAddress: "b1-domain"},
		},
	}
}

func TestDomainCodeBuildsLocalMembership(t *testing.T) {
	cfg := validConfig()
	members, err := cfg.SameDomain("a2")
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 || members[0].ID != "a1" || members[1].ID != "a2" {
		t.Fatalf("unexpected same-domain members: %#v", members)
	}
	if got := cfg.Domains(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected domains: %v", got)
	}
}

func TestFloatingDomainsAreClientOnly(t *testing.T) {
	cfg := validConfig()
	cfg.Features.FloatingDomains = []string{"edge-x"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := cfg.DomainKind("a"); got != DomainConsensus {
		t.Fatalf("consensus domain kind: %v", got)
	}
	if got := cfg.DomainKind("edge-x"); got != DomainFloating {
		t.Fatalf("floating domain kind: %v", got)
	}
	if got := cfg.DomainKind("unknown"); got != DomainUnknown {
		t.Fatalf("unknown domain kind: %v", got)
	}
	if members := cfg.DomainMembers("edge-x"); len(members) != 0 {
		t.Fatalf("floating domain must not create members: %#v", members)
	}
	if got := cfg.ConsensusDomains(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("floating domain leaked into consensus domains: %v", got)
	}
	if got := cfg.FloatingDomains(); len(got) != 1 || got[0] != "edge-x" {
		t.Fatalf("unexpected floating domains: %v", got)
	}
}

func TestUnknownFloatingDomainExperiments(t *testing.T) {
	cfg := validConfig()
	if got := cfg.DomainKind("edge-x"); got != DomainUnknown {
		t.Fatalf("unknown floating disabled should stay unknown: %v", got)
	}
	cfg.Features.AllowUnknownFloatingDomains = true
	if got := cfg.DomainKind("edge-x"); got != DomainFloating {
		t.Fatalf("unknown floating enabled should classify as floating: %v", got)
	}
}

func TestLoadExampleTopology(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config", "cluster.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.DomainMembers("domain-b")) != 3 {
		t.Fatalf("domain-b membership was not loaded")
	}
	if cfg.Features.FastReturnEnabled {
		t.Fatalf("unsafe feature defaults must remain disabled: %+v", cfg.Features)
	}
}

func TestLoadCloudExampleTopology(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config", "cluster.cloud.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	a1, ok := cfg.Node("a1")
	if !ok {
		t.Fatal("a1 missing from cloud example")
	}
	if a1.InterDomainBindAddress() != "0.0.0.0:8101" {
		t.Fatalf("a1 should bind 0.0.0.0 inter-domain: %q", a1.InterDomainBindAddress())
	}
	if a1.InterDomainDialAddress() != "100.64.0.11:8101" {
		t.Fatalf("cross-domain peers should dial a1's elastic IP: %q", a1.InterDomainDialAddress())
	}
}

func TestRejectUnsupportedAndInvalidTopology(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"duplicate node", func(c *Config) { c.Nodes[1].ID = c.Nodes[0].ID }},
		{"empty domain", func(c *Config) { c.Nodes[0].DomainCode = "" }},
		{"single domain", func(c *Config) { c.Nodes[2].DomainCode = "a" }},
		{"multi group", func(c *Config) { c.Unsupported.ApplicationGroups = 2 }},
		{"sharding", func(c *Config) { c.Unsupported.Shards = 1 }},
		{"cross group transaction", func(c *Config) { c.Unsupported.CrossGroupTransactions = true }},
		{"membership changes", func(c *Config) { c.Unsupported.RuntimeMembershipChanges = true }},
		{"floating overlaps consensus", func(c *Config) { c.Features.FloatingDomains = []string{"a"} }},
		{"empty floating domain", func(c *Config) { c.Features.FloatingDomains = []string{""} }},
		{"duplicate floating domain", func(c *Config) { c.Features.FloatingDomains = []string{"edge-x", "edge-x"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestCloudAddressOverridesAndDefaults(t *testing.T) {
	// No overrides: bind/dial fall back to the legacy single addresses.
	plain := Node{ID: "a1", DomainCode: "a", ListenAddress: "10.0.1.11:7101", InterDomainAddress: "10.0.1.11:8101"}
	if got := plain.ClientBindAddress(); got != "10.0.1.11:7101" {
		t.Fatalf("client bind default: %q", got)
	}
	if got := plain.InterDomainBindAddress(); got != "10.0.1.11:8101" {
		t.Fatalf("inter-domain bind default: %q", got)
	}
	if got := plain.InterDomainDialAddress(); got != "10.0.1.11:8101" {
		t.Fatalf("inter-domain dial default: %q", got)
	}

	if got := plain.ClientDialAddress(); got != "10.0.1.11:7101" {
		t.Fatalf("client dial default: %q", got)
	}

	// Cloud overrides: bind locally, advertise the elastic IP for cross-domain.
	cloud := Node{
		ID: "a1", DomainCode: "a",
		ListenAddress:            "10.0.1.11:7101",
		InterDomainAddress:       "10.0.1.11:8101",
		BindInterDomainAddress:   "0.0.0.0:8101",
		PublicInterDomainAddress: "100.64.0.11:8101",
		PublicListenAddress:      "100.64.0.11:7101",
	}
	if got := cloud.InterDomainBindAddress(); got != "0.0.0.0:8101" {
		t.Fatalf("inter-domain bind override: %q", got)
	}
	if got := cloud.InterDomainDialAddress(); got != "100.64.0.11:8101" {
		t.Fatalf("cross-domain dial should use the elastic IP: %q", got)
	}
	if got := cloud.ClientBindAddress(); got != "10.0.1.11:7101" {
		t.Fatalf("client bind should default to ListenAddress: %q", got)
	}
	if got := cloud.ClientDialAddress(); got != "100.64.0.11:7101" {
		t.Fatalf("clients/redirects should use the public listen address: %q", got)
	}
}

func TestRejectInvalidCloudAddress(t *testing.T) {
	cfg := validConfig()
	cfg.Nodes[0].PublicInterDomainAddress = "100.64.0.11" // missing :port
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for publicInterDomainAddress without a port")
	}
}

func TestNetworkSimulationUsesDirectedOneWayDelays(t *testing.T) {
	cfg := validConfig()
	cfg.NetworkSimulation = NetworkSimulation{
		Enabled: true, LocalOneWayDelayMillis: 2,
		InterDomainOneWayMillis: map[string]int{"a->b": 40, "b->a": 60, "edge-x->a": 80, "b->edge-x": 90},
	}
	cfg.Features.FloatingDomains = []string{"edge-x"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := cfg.OneWayDelay("a", "a"); got != 2*time.Millisecond {
		t.Fatalf("unexpected local delay: %v", got)
	}
	if got := cfg.OneWayDelay("a", "b"); got != 40*time.Millisecond {
		t.Fatalf("unexpected a->b delay: %v", got)
	}
	if got := cfg.OneWayDelay("b", "a"); got != 60*time.Millisecond {
		t.Fatalf("unexpected b->a delay: %v", got)
	}
	if got := cfg.OneWayDelay("edge-x", "a"); got != 80*time.Millisecond {
		t.Fatalf("unexpected floating->consensus delay: %v", got)
	}
	if got := cfg.OneWayDelay("edge-x", "b"); got != 90*time.Millisecond {
		t.Fatalf("unexpected reversed consensus->floating delay: %v", got)
	}
}
