package nats

import (
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus"
)

const (
	natsEventLayoutOption = "nats.event_layout"
	natsTargetedLayoutV1  = "targeted-v1"
)

// WithMaxAge sets the maximum age for messages in the NATS stream.
// Default is no limit (or NATS default).
func WithMaxAge(maxAge time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("nats.maxage", maxAge)
}

// WithReplicas sets the number of replicas when auto-creating the NATS stream.
// Default is 1.
func WithReplicas(replicas int) eventbus.TopologyOption {
	return eventbus.WithOption("nats.replicas", replicas)
}

// WithRetention sets the retention policy for the NATS stream.
// Use gonats.WorkQueuePolicy for pure work-queues to save disk space.
// Use gonats.LimitsPolicy (default) or InterestPolicy for Fan-out Pub/Sub.
func WithRetention(policy gonats.RetentionPolicy) eventbus.TopologyOption {
	return eventbus.WithOption("nats.retention", policy)
}

// WithAckWait sets the time NATS will wait for a message to be ACKed before redelivering it.
// Default is 5 minutes.
func WithAckWait(wait time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("nats.ack_wait", wait)
}

// WithBatchSize sets the number of messages to fetch at once from NATS.
// Default is 1. Increase for higher throughput if your processing is fast.
func WithBatchSize(size int) eventbus.TopologyOption {
	return eventbus.WithOption("nats.batch_size", size)
}

// WithFetchTimeout sets the maximum time NATS will block waiting for a message.
// A shorter timeout (e.g. 2s) allows the worker loop to recover from silent connection drops.
// Default is 2 seconds.
func WithFetchTimeout(wait time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("nats.fetch_timeout", wait)
}

// WithDelayBatchSize bounds successful delayed publications per scheduler pass.
// The default is 100 and the maximum is 1000.
func WithDelayBatchSize(size int) eventbus.TopologyOption {
	return eventbus.WithOption("nats.delay_batch_size", size)
}

// WithDelayLease sets how long one scheduler instance owns a delayed record
// before another instance may recover it. The default is 30 seconds.
func WithDelayLease(lease time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("nats.delay_lease", lease)
}

// WithDuplicateWindow sets the broker deduplication bound used by immediate,
// delayed, retry-lane, and DLQ publications. The default is two minutes.
func WithDuplicateWindow(window time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("nats.duplicate_window", window)
}

// WithEventLayoutTargeted opts an event topology into subscriber-targeted
// retries. Older NATS drivers reject this unknown option, fencing them out of
// mixed-version deployments before they can broadcast retry work.
func WithEventLayoutTargeted() eventbus.TopologyOption {
	return eventbus.WithOption(natsEventLayoutOption, natsTargetedLayoutV1)
}
