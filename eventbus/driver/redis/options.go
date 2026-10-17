package redis

import (
	"time"

	"github.com/thanhbvha/go-common/eventbus"
)

// WithMaxLen sets the approximate maximum length for the Redis stream.
// This prevents the stream from growing indefinitely and consuming all memory.
// Default is 100,000.
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
