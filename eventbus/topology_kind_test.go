package eventbus

import (
	"context"
	"errors"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestTopologyKindConflict(t *testing.T) {
	tests := []struct {
		name  string
		first func(Bus) error
		next  func(Bus) error
	}{
		{
			name: "task then event",
			first: func(bus Bus) error {
				return bus.Register("shared", func(context.Context, Job) error { return nil })
			},
			next: func(bus Bus) error {
				return bus.Subscribe("shared", "subscriber", func(context.Context, Job) error { return nil })
			},
		},
		{
			name: "event then task",
			first: func(bus Bus) error {
				return bus.Subscribe("shared", "subscriber", func(context.Context, Job) error { return nil })
			},
			next: func(bus Bus) error {
				return bus.Register("shared", func(context.Context, Job) error { return nil })
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bus, err := Open(DefaultConfig(), WithTransport(&topologyRecordingTransport{}))
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.first(bus); err != nil {
				t.Fatalf("first registration: %v", err)
			}
			if err := tt.next(bus); !errors.Is(err, ErrTopologyKindConflict) {
				t.Fatalf("second registration error = %v, want ErrTopologyKindConflict", err)
			}
		})
	}
}

func TestTaskAndEventPublishUseIsolatedTopologies(t *testing.T) {
	recorder := &topologyRecordingTransport{published: make(chan transport.Topology, 2)}
	taskBus, err := Open(DefaultConfig(), WithTransport(recorder))
	if err != nil {
		t.Fatal(err)
	}
	eventBus, err := Open(DefaultConfig(), WithTransport(recorder))
	if err != nil {
		t.Fatal(err)
	}

	if err := taskBus.Register("shared", func(context.Context, Job) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := eventBus.Subscribe("shared", "subscriber", func(context.Context, Job) error { return nil }); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := taskBus.Enqueue(ctx, "shared", "task payload"); err != nil {
		t.Fatal(err)
	}
	if err := eventBus.Publish(ctx, "shared", "event payload"); err != nil {
		t.Fatal(err)
	}

	taskTopology := <-recorder.published
	eventTopology := <-recorder.published
	if taskTopology.Kind != TaskTopology {
		t.Fatalf("Enqueue topology kind = %s, want task", taskTopology.Kind)
	}
	if eventTopology.Kind != EventTopology {
		t.Fatalf("Publish topology kind = %s, want event", eventTopology.Kind)
	}
	if taskTopology.Name() == eventTopology.Name() {
		t.Fatalf("task and event physical names collide: %q", taskTopology.Name())
	}
}

type topologyRecordingTransport struct {
	benchmarkTransport
	published chan transport.Topology
	ensured   chan transport.Topology
}

func (t *topologyRecordingTransport) EnsureTopology(_ context.Context, topo transport.Topology) error {
	if t.ensured != nil {
		t.ensured <- topo
	}
	return nil
}

func (t *topologyRecordingTransport) Publish(_ context.Context, topo transport.Topology, _ transport.Message) error {
	if t.published != nil {
		t.published <- topo
	}
	return nil
}
