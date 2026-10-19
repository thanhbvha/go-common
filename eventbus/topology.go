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

// RateLimit configures a local token bucket for one task or subscriber.
type RateLimit struct {
	PerSecond float64
	Burst     int
}

// WithRateLimit bounds handler starts per second in this process. It does not
// coordinate a distributed quota across EventBus instances.
func WithRateLimit(perSecond float64, burst int) TopologyOption {
	return WithOption("eventbus.rate_limit", RateLimit{PerSecond: perSecond, Burst: burst})
}

// WithOrderingKeyConcurrency bounds simultaneous handlers sharing one nonempty
// ordering key. Keys are isolated per local task or subscriber session.
func WithOrderingKeyConcurrency(limit int) TopologyOption {
	return WithOption("eventbus.ordering_key_concurrency", limit)
}

// WithMaxPayloadBytes overrides the serialized envelope limit for one topology.
func WithMaxPayloadBytes(max int) TopologyOption {
	return WithOption("eventbus.max_payload_bytes", max)
}

// WithLegacyResourceNames explicitly selects the pre-v2 separator-joined
// broker resource layout for a migration window. Legacy names are collision
// prone; new deployments must use the v2 default.
func WithLegacyResourceNames() TopologyOption {
	return WithOption(transport.ResourceNameVersionOption, transport.ResourceNameVersionLegacy)
}

// WithMaxDeliveries overrides the global MaxAttempts for this specific topology.
//
// Deprecated: use WithMaxAttempts.
func WithMaxDeliveries(max int) TopologyOption {
	return WithMaxAttempts(max)
}
