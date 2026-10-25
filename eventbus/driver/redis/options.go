package redis

import (
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	redisEventLayoutOption = "redis.event_layout"
	redisTargetedLayoutV1  = "targeted-v1"

	defaultMaxReadyEntries   int64 = 1_000_000
	defaultMaxDelayedEntries int64 = 1_000_000
	defaultMaxDLQEntries     int64 = 50_000
	defaultRetentionBatch    int64 = 10_000
	defaultRetentionInterval       = 30 * time.Second
	schedulerLaneScanMax           = 16
)

// WithEventLayoutTargeted opts an event topology into subscriber-targeted
// retries. Older Redis drivers reject this Redis-specific option, preventing
// mixed-version deployments from silently using incompatible retry layouts.
func WithEventLayoutTargeted() eventbus.TopologyOption {
	return eventbus.WithOption(redisEventLayoutOption, redisTargetedLayoutV1)
}

// WithMaxLen requests the retired lossy ready-stream cap. Validation rejects
// this option because MAXLEN can remove accepted work ahead of slow groups.
//
// Deprecated: configure broker capacity and explicit safe retention instead.
func WithMaxLen(maxLen int64) eventbus.TopologyOption {
	return eventbus.WithOption("redis.maxlen", maxLen)
}

// WithDLQMaxLen sets the approximate maximum length for the Dead-Letter Queue stream.
// Default is 50,000.
func WithDLQMaxLen(maxLen int64) eventbus.TopologyOption {
	return eventbus.WithOption("redis.dlq_maxlen", maxLen)
}

// WithBatchSize sets the number of messages to fetch at once from Redis (XREADGROUP).
// Default is 1. Increase for higher throughput if your processing is fast.
func WithBatchSize(size int64) eventbus.TopologyOption {
	return eventbus.WithOption("redis.batch_size", size)
}

// WithBlock sets the max duration to block when polling for new messages.
// A higher value (e.g., 2s) reduces Redis CPU load during idle times.
// Default is 2 seconds.
func WithBlock(d time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("redis.block", d)
}

// WithAckWait sets the time Redis will wait before assuming a worker crashed
// and re-queueing the message (via XAUTOCLAIM).
// Default is 5 minutes.
func WithAckWait(wait time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("redis.ack_wait", wait)
}

// WithDelayBatchSize sets the number of delayed messages to sweep into the main stream per second.
// Default is 100. Increase if you expect massive spikes of delayed jobs maturing at the exact same second.
func WithDelayBatchSize(size int) eventbus.TopologyOption {
	return eventbus.WithOption("redis.delay_batch_size", size)
}

// WithMaxReadyEntries bounds the physical ready stream. Reaching the bound
// returns transport.ErrCapacityExceeded; it never trims unread work.
func WithMaxReadyEntries(limit int64) eventbus.TopologyOption {
	return eventbus.WithOption("redis.max_ready_entries", limit)
}

// WithMaxDelayedEntries bounds scheduled entries awaiting dispatch.
func WithMaxDelayedEntries(limit int64) eventbus.TopologyOption {
	return eventbus.WithOption("redis.max_delayed_entries", limit)
}

// WithMaxDLQEntries bounds dead-letter records. A full DLQ leaves the source
// delivery unsettled so it can be retried after operator intervention.
func WithMaxDLQEntries(limit int64) eventbus.TopologyOption {
	return eventbus.WithOption("redis.max_dlq_entries", limit)
}

// WithRetentionInterval sets how often the shared retention coordinator
// considers this topology.
func WithRetentionInterval(interval time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("redis.retention_interval", interval)
}

// WithRetentionBatchSize bounds entries examined by one MINID trim pass.
func WithRetentionBatchSize(size int64) eventbus.TopologyOption {
	return eventbus.WithOption("redis.retention_batch_size", size)
}

// WithRetentionMinAge retains acknowledged entries for at least age.
func WithRetentionMinAge(age time.Duration) eventbus.TopologyOption {
	return eventbus.WithOption("redis.retention_min_age", age)
}

func redisInt64Option(topo transport.Topology, key string, fallback int64) int64 {
	if value, ok := topo.Options[key].(int64); ok && value > 0 {
		return value
	}
	return fallback
}

func maxReadyEntries(topo transport.Topology) int64 {
	return redisInt64Option(topo, "redis.max_ready_entries", defaultMaxReadyEntries)
}

func maxDelayedEntries(topo transport.Topology) int64 {
	return redisInt64Option(topo, "redis.max_delayed_entries", defaultMaxDelayedEntries)
}

func maxDLQEntries(topo transport.Topology) int64 {
	return redisInt64Option(topo, "redis.max_dlq_entries", defaultMaxDLQEntries)
}
