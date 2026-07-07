// Package queue_nats implements a highly reliable, NATS JetStream-backed job queue.
// It provides feature parity with the Redis-backed queue, including delayed execution,
// per-type worker pools, automatic retries with exponential backoff, and a dead-letter
// queue (DLQ) for exhausted jobs.
//
// Basic usage:
//
//	nc := nats.MustConnect(ctx, nats.DefaultConfig())
//	cfg := queue_nats.DefaultConfig()
//	q := queue_nats.New(nc, cfg)
//
//	q.RegisterJobType("email", queue_nats.JobTypeOptions{Concurrency: 4, MaxRetry: 5})
//	q.RegisterHandler("email", func(job queue_nats.Job) error { /* … */ return nil })
//
//	q.Start(ctx)
//	defer q.Stop()
//
//	q.Enqueue(ctx, "email", payload)
package queue_nats

import (
	"context"
	"time"

	"github.com/thanhbvha/go-common/nats"
)

// Logger is the logging interface consumed by Queue.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	InfoAsync(msg string, args ...any)
	ErrorAsync(msg string, args ...any)
	WarnAsync(msg string, args ...any)
}

// NATSStreamer defines the minimal NATS API required by Queue.
// *nats.Client from github.com/thanhbvha/go-common/nats satisfies this interface.
type NATSStreamer interface {
	AddStream(ctx context.Context, name string, subjects []string, opts ...nats.StreamOption) error
	JSPublish(ctx context.Context, subject string, data []byte) (*nats.PubAck, error)
	AddConsumer(ctx context.Context, stream, durable string, opts ...nats.ConsumerOption) error
	PullSubscribe(subject, durable string, opts ...nats.SubOption) (*nats.PullSubscription, error)
	Fetch(ps *nats.PullSubscription, count int, opts ...nats.FetchOption) ([]*nats.Msg, error)
	KVCreate(ctx context.Context, bucket string, opts ...nats.KVOption) error
	KVCreate2(ctx context.Context, bucket, key string, value []byte) (uint64, error)
}

// Config holds all tuneable parameters for a Queue instance using NATS.
type Config struct {
	// StreamPrefix is prepended to all stream names created by this queue.
	// Default: "queue_stream". (NATS recommends avoiding colons for stream names, use underscores)
	StreamPrefix string

	// DefaultGroup is the durable consumer group name when a job type has no custom group.
	// Default: "workers".
	DefaultGroup string

	// DefaultMaxAge is the maximum age of messages before they are removed from the stream.
	// Default: 15 days.
	DefaultMaxAge time.Duration

	// DefaultMaxRetry is the number of retries before a job is moved to the DLQ.
	// Default: 3.
	DefaultMaxRetry int

	// DefaultWorkerCount is the fallback worker pool size used when no job types
	// are registered. Default: 5.
	DefaultWorkerCount int

	// --- Delayed job settings ---

	// DelayedStreamSubject is the subject that receives delayed jobs.
	// We use a stream for delayed jobs and a consumer with NakWithDelay.
	// Default: "queue_delayed".
	DelayedStreamSubject string

	// --- Dead-letter queue settings ---

	// DLQStreamName is the NATS stream that receives exhausted jobs.
	// Default: "queue_dlq".
	DLQStreamName string

	// DLQRetention is the age after which DLQ entries are deleted.
	// NATS JetStream handles this automatically via MaxAge.
	// Default: 15 days.
	DLQRetention time.Duration

	// --- Consumer tuning ---

	// AckWait is the time NATS waits for a message to be ACK-ed before re-delivering it.
	// This replaces Redis stuck-job reclaim loop.
	// Default: 5 minutes.
	AckWait time.Duration

	// Logger receives operational log events. Set to nil to suppress logging.
	Logger Logger
}

// DefaultConfig returns a Config pre-populated with production-ready defaults.
func DefaultConfig() Config {
	return Config{
		StreamPrefix:         "queue_stream",
		DefaultGroup:         "workers",
		DefaultMaxAge:        15 * 24 * time.Hour,
		DefaultMaxRetry:      3,
		DefaultWorkerCount:   5,

		DelayedStreamSubject: "queue_delayed",

		DLQStreamName:        "queue_dlq",
		DLQRetention:         15 * 24 * time.Hour,

		AckWait:              5 * time.Minute,
	}
}

// JobTypeOptions configures a specific job type registered with Queue.RegisterJobType.
type JobTypeOptions struct {
	// Concurrency is the number of parallel worker goroutines. Default: 1.
	Concurrency int

	// MaxRetry overrides Config.DefaultMaxRetry for this type. 0 = use default.
	MaxRetry int

	// MaxAge overrides Config.DefaultMaxAge for this type's stream. 0 = use default.
	MaxAge time.Duration

	// BatchSize is the number of messages fetched per Fetch call. Default: 1.
	BatchSize int

	// StreamName overrides the auto-generated stream name. Empty = auto.
	StreamName string

	// Group overrides the auto-generated consumer durable name. Empty = auto.
	Group string
}

// jobTypeConfig is the resolved, internal representation of a registered type.
type jobTypeConfig struct {
	Concurrency int
	MaxRetry    int
	StreamName  string
	Group       string
	MaxAge      time.Duration
	BatchSize   int
}
