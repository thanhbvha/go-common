package nats

import (
	"time"

	gonats "github.com/nats-io/nats.go"
)

// ============================================================
// Stream options
// ============================================================

// StreamOption is a functional option that modifies a *nats.StreamConfig.
type StreamOption func(*gonats.StreamConfig)

// WithRetention sets the retention policy for the stream.
func WithRetention(r RetentionPolicy) StreamOption {
	return func(cfg *gonats.StreamConfig) {
		switch r {
		case WorkQueuePolicy:
			cfg.Retention = gonats.WorkQueuePolicy
		case InterestPolicy:
			cfg.Retention = gonats.InterestPolicy
		default:
			cfg.Retention = gonats.LimitsPolicy
		}
	}
}

// WithStorage overrides the storage backend for this stream.
func WithStorage(s StorageType) StreamOption {
	return func(cfg *gonats.StreamConfig) {
		if s == FileStorage {
			cfg.Storage = gonats.FileStorage
		} else {
			cfg.Storage = gonats.MemoryStorage
		}
	}
}

// WithMaxMsgs sets the maximum number of messages the stream will retain.
// Oldest messages are removed when the limit is exceeded.
func WithMaxMsgs(n int64) StreamOption {
	return func(cfg *gonats.StreamConfig) { cfg.MaxMsgs = n }
}

// WithMaxBytes sets the maximum total bytes the stream will retain.
func WithMaxBytes(n int64) StreamOption {
	return func(cfg *gonats.StreamConfig) { cfg.MaxBytes = n }
}

// WithMaxAge sets the maximum age of messages before they are removed.
func WithMaxAge(d time.Duration) StreamOption {
	return func(cfg *gonats.StreamConfig) { cfg.MaxAge = d }
}

// WithMaxMsgSize sets the maximum size in bytes for a single message.
func WithMaxMsgSize(n int32) StreamOption {
	return func(cfg *gonats.StreamConfig) { cfg.MaxMsgSize = n }
}

// WithReplicas sets the number of replicas for the stream.
// Requires a NATS cluster with at least n servers.
func WithReplicas(n int) StreamOption {
	return func(cfg *gonats.StreamConfig) { cfg.Replicas = n }
}

// WithSubjects replaces the stream's subject filter list.
func WithSubjects(subjects ...string) StreamOption {
	return func(cfg *gonats.StreamConfig) { cfg.Subjects = subjects }
}

// WithNoAck disables server-side acknowledgement for the stream.
// Use this for high-throughput, best-effort delivery where lost messages
// are acceptable (fire-and-forget).
func WithNoAck() StreamOption {
	return func(cfg *gonats.StreamConfig) { cfg.NoAck = true }
}

// WithDuplicateWindow sets the time window for deduplicating published messages.
// Messages with the same Nats-Msg-Id header within this window are deduplicated.
func WithDuplicateWindow(d time.Duration) StreamOption {
	return func(cfg *gonats.StreamConfig) { cfg.Duplicates = d }
}

// WithDescription adds a human-readable description to the stream.
func WithDescription(s string) StreamOption {
	return func(cfg *gonats.StreamConfig) { cfg.Description = s }
}

// ============================================================
// Consumer options
// ============================================================

// ConsumerOption is a functional option that modifies a *nats.ConsumerConfig.
type ConsumerOption func(*gonats.ConsumerConfig)

// WithDurable sets the durable name for the consumer.
// A durable consumer survives server restarts and client disconnects.
func WithDurable(name string) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) { cfg.Durable = name }
}

// WithAckWait sets how long the server waits for an ACK before redelivering.
// Equivalent to the Redis Stream pending-entry idle timeout before XClaim.
// Default: 30s.
func WithAckWait(d time.Duration) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) { cfg.AckWait = d }
}

// WithMaxDeliver sets the maximum number of delivery attempts before the
// message is considered undeliverable. Equivalent to Redis Stream MaxRetries.
// Set to -1 for unlimited redelivery.
func WithMaxDeliver(n int) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) { cfg.MaxDeliver = n }
}

// WithMaxAckPending sets the maximum number of messages the consumer may have
// outstanding (delivered but not yet ACK-ed) at any time.
func WithMaxAckPending(n int) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) { cfg.MaxAckPending = n }
}

// WithDeliverPolicy controls which messages the consumer receives first.
func WithDeliverPolicy(p gonats.DeliverPolicy) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) { cfg.DeliverPolicy = p }
}

// WithFilterSubject restricts the consumer to a single subject within the stream.
// Equivalent to subscribing to a specific Redis Stream key rather than all keys.
func WithFilterSubject(s string) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) { cfg.FilterSubject = s }
}

// WithStartSequence configures the consumer to begin at a specific sequence number.
// Equivalent to starting XRange from a specific message ID.
func WithStartSequence(seq uint64) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) {
		cfg.DeliverPolicy = gonats.DeliverByStartSequencePolicy
		cfg.OptStartSeq = seq
	}
}

// WithStartTime configures the consumer to begin at a specific timestamp.
func WithStartTime(t time.Time) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) {
		cfg.DeliverPolicy = gonats.DeliverByStartTimePolicy
		cfg.OptStartTime = &t
	}
}

// WithRateLimit sets a maximum message delivery rate (messages per second).
func WithRateLimit(msgsPerSec uint64) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) { cfg.RateLimit = msgsPerSec }
}

// WithConsumerDescription adds a human-readable description to the consumer.
func WithConsumerDescription(s string) ConsumerOption {
	return func(cfg *gonats.ConsumerConfig) { cfg.Description = s }
}

// ============================================================
// Subscribe options
// ============================================================

// SubOption is a functional option for JS push subscriptions.
type SubOption func(*subOptions)

type subOptions struct {
	manualAck     bool
	maxAckPending int
}

// WithManualAck disables automatic ACK so the caller controls when each
// message is acknowledged. Required when using Msg.Ack/Nak/Term explicitly.
func WithManualAck() SubOption {
	return func(o *subOptions) { o.manualAck = true }
}

// WithMaxAckPendingSub sets the maximum number of outstanding (unACK-ed)
// messages for a push subscription.
func WithMaxAckPendingSub(n int) SubOption {
	return func(o *subOptions) { o.maxAckPending = n }
}

// ============================================================
// Fetch options
// ============================================================

// FetchOption is a functional option for PullSubscription.Fetch calls.
type FetchOption func(*fetchOptions)

type fetchOptions struct {
	timeout time.Duration
}

// WithFetchTimeout sets how long Fetch blocks waiting for messages.
// Default: 5s. Pass 0 to use FetchNoWait instead.
func WithFetchTimeout(d time.Duration) FetchOption {
	return func(o *fetchOptions) { o.timeout = d }
}

// ============================================================
// KV options
// ============================================================

// KVOption is a functional option for KV bucket creation.
type KVOption func(*gonats.KeyValueConfig)

// WithKVStorage overrides the storage backend for the KV bucket.
func WithKVStorage(s StorageType) KVOption {
	return func(cfg *gonats.KeyValueConfig) {
		if s == FileStorage {
			cfg.Storage = gonats.FileStorage
		} else {
			cfg.Storage = gonats.MemoryStorage
		}
	}
}

// WithKVMaxValueSize sets the maximum size in bytes for a single KV value.
func WithKVMaxValueSize(n int32) KVOption {
	return func(cfg *gonats.KeyValueConfig) { cfg.MaxValueSize = n }
}

// WithKVHistory sets the number of historical revisions to keep per key.
// Must be between 1 and 64. Default: 1 (only current value).
func WithKVHistory(n uint8) KVOption {
	return func(cfg *gonats.KeyValueConfig) { cfg.History = n }
}

// WithKVTTL sets the maximum age of entries in the KV bucket.
// Entries older than this TTL are removed automatically.
func WithKVTTL(d time.Duration) KVOption {
	return func(cfg *gonats.KeyValueConfig) { cfg.TTL = d }
}

// WithKVReplicas sets the replication factor for the KV bucket.
func WithKVReplicas(n int) KVOption {
	return func(cfg *gonats.KeyValueConfig) { cfg.Replicas = n }
}

// WithKVDescription adds a human-readable description to the KV bucket.
func WithKVDescription(s string) KVOption {
	return func(cfg *gonats.KeyValueConfig) { cfg.Description = s }
}
