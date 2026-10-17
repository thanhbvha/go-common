package eventbus

import "github.com/thanhbvha/go-common/eventbus/transport"

// Topology aliases the transport.Topology.
type Topology = transport.Topology

type TopologyKind = transport.TopologyKind

const (
	TaskTopology  = transport.TaskTopology
	EventTopology = transport.EventTopology
)

// DefaultNamespace is the default namespace to segregate eventbus from queue* modules.
const DefaultNamespace = "eventbus"

// TopologyName returns a string representation of the topology.
func TopologyName(t Topology) string {
	return t.Name()
}

// TopologyOption allows configuring topology-specific options.
type TopologyOption func(*Topology)

// WithOption adds a generic option to the topology.
func WithOption(key string, value any) TopologyOption {
	return func(t *Topology) {
		if t.Options == nil {
			t.Options = make(map[string]any)
		}
		t.Options[key] = value
	}
}

// WithConcurrency overrides the global concurrency for this specific topology.
func WithConcurrency(concurrency int) TopologyOption {
	return WithOption("eventbus.concurrency", concurrency)
}

// WithPrefetch overrides the global number of deliveries that may wait in the
// bounded intake queue for this topology or subscriber.
func WithPrefetch(prefetch int) TopologyOption {
	return WithOption("eventbus.prefetch", prefetch)
}

// WithMaxAttempts overrides the global MaxAttempts for this specific topology.
func WithMaxAttempts(max int) TopologyOption {
	return WithOption("eventbus.max_attempts", max)
}

// WithMaxDeliveries overrides the global MaxAttempts for this specific topology.
//
// Deprecated: use WithMaxAttempts.
func WithMaxDeliveries(max int) TopologyOption {
	return WithMaxAttempts(max)
}
