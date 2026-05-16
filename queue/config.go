// Package queue implements a durable, Redis-Streams-backed job queue with
// support for delayed execution, per-type worker pools, automatic retry with
// exponential back-off, and a dead-letter queue (DLQ) for exhausted jobs.
//
// The queue package depends only on a RedisStreamer interface, so it is not
// coupled to any specific Redis client implementation. The *redis.Client
// provided by github.com/thanhbvha/go-common/redis satisfies this interface
// out of the box.
//
// Basic usage:
//
//	rdb := redis.MustConnect(ctx, redis.DefaultConfig())
//	redis.SetDefault(rdb)
//
//	cfg := queue.DefaultConfig()
//	q := queue.New(rdb, cfg)
//
//	q.RegisterJobType("email", queue.JobTypeOptions{Concurrency: 4, MaxRetry: 5})
//	q.RegisterHandler("email", func(job queue.Job) error { /* … */ return nil })
//
//	q.Start(ctx)
//	defer q.Stop()
//
//	q.Enqueue(ctx, "email", payload)
package queue

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Logger is the logging interface consumed by Queue. It mirrors the interface
// in the sibling redis package so both can be satisfied by the same logger
// without an import cycle.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// RedisStreamer defines the minimal Redis Streams API required by Queue.
// *redis.Client from github.com/thanhbvha/go-common/redis satisfies this
// interface, as does any compatible mock for testing.
type RedisStreamer interface {
	BuildKey(key string) string
	XAdd(ctx context.Context, args *goredis.XAddArgs) (string, error)
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) error
	XReadGroup(ctx context.Context, group, consumer string, streams []string, count int64, block time.Duration) ([]goredis.XStream, error)
	XAck(ctx context.Context, stream, group string, ids ...string) (int64, error)
	XDel(ctx context.Context, stream string, ids ...string) (int64, error)
	XRangeN(ctx context.Context, stream, start, stop string, count int64) ([]goredis.XMessage, error)
	XPendingExt(ctx context.Context, stream, group, start, end string, count int64, idle time.Duration) ([]goredis.XPendingExt, error)
	XClaim(ctx context.Context, args *goredis.XClaimArgs) ([]goredis.XMessage, error)
	XAutoClaim(ctx context.Context, args *goredis.XAutoClaimArgs) ([]goredis.XMessage, string, error)
	SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) (bool, error)
	Exists(ctx context.Context, keys ...string) (int64, error)
	Type(ctx context.Context, key string) (string, error)
	Info(ctx context.Context, section ...string) (string, error)
}

// Config holds all tuneable parameters for a Queue instance.
// Use DefaultConfig for a pre-filled value and override only what you need.
type Config struct {
	// StreamPrefix is prepended to all stream names created by this queue.
	// Default: "queue:stream".
	StreamPrefix string

	// DefaultGroup is the consumer group name when a job type has no custom group.
	// Default: "workers".
	DefaultGroup string

	// DefaultMaxLen is the approximate maximum number of entries kept in each
	// stream (MAXLEN ~ trimming). Default: 20_000.
	DefaultMaxLen int64

	// DefaultMaxRetry is the number of retries before a job is moved to the DLQ.
	// Default: 3.
	DefaultMaxRetry int

	// DefaultWorkerCount is the fallback worker pool size used when no job types
	// are registered. Default: 5.
	DefaultWorkerCount int

	// --- Delayed job settings ---

	// DelayedStreamKey is the Redis stream that holds deferred jobs.
	// Default: "queue:stream:delayed".
	DelayedStreamKey string

	// DelayCheckInterval controls how often the delay dispatcher wakes up.
	// Default: 2s.
	DelayCheckInterval time.Duration

	// --- Dead-letter queue settings ---

	// DLQStreamKey is the Redis stream that receives exhausted jobs.
	// Default: "queue:stream:dlq".
	DLQStreamKey string

	// DLQMaxLen is the approximate maximum number of entries kept in the DLQ.
	// Default: 50_000.
	DLQMaxLen int64

	// DLQRetention is the age after which DLQ entries are deleted.
	// Default: 15 days.
	DLQRetention time.Duration

	// DLQCleanupInterval controls how often the DLQ cleanup goroutine runs.
	// Default: 5 minutes.
	DLQCleanupInterval time.Duration

	// --- Stuck-job reclaim settings ---

	// ReclaimMinIdle is the minimum time a pending message must be idle before
	// it is eligible for reclaiming. Default: 5 minutes.
	ReclaimMinIdle time.Duration

	// ReclaimInterval controls how often each reclaim goroutine runs.
	// Default: 1 minute.
	ReclaimInterval time.Duration

	// ReclaimBatchSize is the maximum number of pending messages processed in
	// a single reclaim cycle. Default: 100.
	ReclaimBatchSize int64

	// Logger receives operational log events. Set to nil to suppress logging.
	Logger Logger
}

// DefaultConfig returns a Config pre-populated with production-ready defaults.
func DefaultConfig() Config {
	return Config{
		StreamPrefix:       "queue:stream",
		DefaultGroup:       "workers",
		DefaultMaxLen:      20_000,
		DefaultMaxRetry:    3,
		DefaultWorkerCount: 5,

		DelayedStreamKey:   "queue:stream:delayed",
		DelayCheckInterval: 2 * time.Second,

		DLQStreamKey:       "queue:stream:dlq",
		DLQMaxLen:          50_000,
		DLQRetention:       15 * 24 * time.Hour,
		DLQCleanupInterval: 5 * time.Minute,

		ReclaimMinIdle:   5 * time.Minute,
		ReclaimInterval:  1 * time.Minute,
		ReclaimBatchSize: 100,
	}
}

// JobTypeOptions configures a specific job type registered with Queue.RegisterJobType.
type JobTypeOptions struct {
	// Concurrency is the number of parallel worker goroutines. Default: 1.
	Concurrency int

	// MaxRetry overrides Config.DefaultMaxRetry for this type. 0 = use default.
	MaxRetry int

	// MaxLen overrides Config.DefaultMaxLen for this type. 0 = use default.
	MaxLen int64

	// BatchSize is the number of messages fetched per XReadGroup call. Default: 1.
	BatchSize int64

	// StreamKey overrides the auto-generated stream key. Empty = auto.
	StreamKey string

	// Group overrides the auto-generated consumer group name. Empty = auto.
	Group string
}

// jobTypeConfig is the resolved, internal representation of a registered type.
type jobTypeConfig struct {
	Concurrency int
	MaxRetry    int
	StreamKey   string
	Group       string
	MaxLen      int64
	BatchSize   int64
}
