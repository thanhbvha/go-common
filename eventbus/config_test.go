package eventbus

import (
	"strings"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestTopologyName(t *testing.T) {
	topologies := []Topology{
		{Namespace: "a.b", JobType: "c", Kind: TaskTopology},
		{Namespace: "a", JobType: "b.c", Kind: TaskTopology},
		{Namespace: "acme", JobType: "event.foo", Kind: TaskTopology},
		{Namespace: "acme", JobType: "foo", Kind: EventTopology},
		{Namespace: "tenant_一", JobType: "orders/paid.*", Kind: EventTopology},
	}
	seen := make(map[string]Topology, len(topologies))
	for _, topo := range topologies {
		name := TopologyName(topo)
		if !strings.HasPrefix(name, "eb2.") {
			t.Fatalf("TopologyName(%+v) = %q, want v2 marker", topo, name)
		}
		if prior, ok := seen[name]; ok {
			t.Fatalf("distinct topologies collide: %+v and %+v => %q", prior, topo, name)
		}
		seen[name] = topo
	}

	base := Topology{Namespace: "a.b", JobType: "c", Kind: EventTopology}
	groups := []string{"", "x.y", "x", "y_一", "event.foo"}
	consumerNames := make(map[string]string, len(groups))
	for _, group := range groups {
		name := base.ConsumerName(group)
		if prior, ok := consumerNames[name]; ok {
			t.Fatalf("subscriber groups %q and %q collide as %q", prior, group, name)
		}
		consumerNames[name] = group
	}

	legacy := Topology{Namespace: "eventbus", JobType: "email", Kind: EventTopology, Options: map[string]any{
		transport.ResourceNameVersionOption: transport.ResourceNameVersionLegacy,
	}}
	if got := legacy.Name(); got != "eventbus.event.email" {
		t.Fatalf("legacy topology name = %q", got)
	}
}

func TestConfigValidate(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("DefaultConfig should be valid: %v", err)
	}

	cfg.Namespace = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Config with empty namespace should use the default: %v", err)
	}
	if cfg.Namespace != DefaultNamespace {
		t.Errorf("Config namespace = %q, expected default %q", cfg.Namespace, DefaultNamespace)
	}
}

func TestConfigValidateMaxAttemptsCompatibility(t *testing.T) {
	tests := []struct {
		name         string
		cfg          Config
		wantAttempts int
	}{
		{name: "default", cfg: DefaultConfig(), wantAttempts: 5},
		{name: "canonical field", cfg: Config{MaxAttempts: 7}, wantAttempts: 7},
		{name: "deprecated field", cfg: Config{MaxDeliveries: 3}, wantAttempts: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if tt.cfg.MaxAttempts != tt.wantAttempts {
				t.Fatalf("MaxAttempts = %d, want %d", tt.cfg.MaxAttempts, tt.wantAttempts)
			}
		})
	}
}
