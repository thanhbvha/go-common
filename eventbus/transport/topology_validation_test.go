package transport

import (
	"errors"
	"testing"
)

func TestTopologyValidate(t *testing.T) {
	tests := []struct {
		name     string
		topology Topology
		wantErr  bool
	}{
		{name: "task", topology: Topology{JobType: "task", Kind: TaskTopology}},
		{name: "event", topology: Topology{JobType: "event", Kind: EventTopology}},
		{name: "empty name", topology: Topology{Kind: TaskTopology}, wantErr: true},
		{name: "padded name", topology: Topology{JobType: " task", Kind: TaskTopology}, wantErr: true},
		{name: "invalid kind", topology: Topology{JobType: "task", Kind: TopologyKind(99)}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.topology.Validate()
			if tt.wantErr && !errors.Is(err, ErrInvalidTopology) {
				t.Fatalf("Validate error = %v, want ErrInvalidTopology", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}
