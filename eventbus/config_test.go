package eventbus

import "testing"

func TestTopologyName(t *testing.T) {
	tests := []struct {
		ns       string
		jobType  string
		kind     TopologyKind
		expected string
	}{
		{"eventbus", "email", TaskTopology, "eventbus.email"},
		{"", "email", TaskTopology, "email"},
		{"custom", "payment.process", TaskTopology, "custom.payment.process"},
		{"eventbus", "email", EventTopology, "eventbus.event.email"},
		{"", "email", EventTopology, "event.email"},
	}

	for _, tc := range tests {
		topo := Topology{Namespace: tc.ns, JobType: tc.jobType, Kind: tc.kind}
		name := TopologyName(topo)
		if name != tc.expected {
			t.Errorf("TopologyName(%q, %q, %s) = %q, expected %q", tc.ns, tc.jobType, tc.kind, name, tc.expected)
		}
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
