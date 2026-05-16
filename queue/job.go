package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// JobHandler is a function that processes a single Job.
// Return a non-nil error to signal failure; the queue will retry up to
// MaxRetry times before routing the job to the dead-letter queue.
type JobHandler func(job Job) error

// Job represents a unit of work dispatched through the queue.
type Job struct {
	// ID is a globally unique identifier assigned at enqueue time.
	ID string `json:"id"`
	// Type identifies the handler that should process this job.
	Type string `json:"type"`
	// Data is the arbitrary payload passed to the handler.
	Data interface{} `json:"data"`
	// Retry is the number of times this job has been attempted.
	Retry int `json:"retry"`
	// MaxRetry is the maximum number of attempts before DLQ routing.
	MaxRetry int `json:"max_retry"`
	// Delay is the number of seconds to wait before first execution.
	// Set via WithDelay option; 0 means immediate.
	Delay int64 `json:"delay"`
	// MaxLen is the approximate maximum stream length for this job's stream.
	MaxLen int64 `json:"max_len"`
	// CreatedAt is the wall-clock time the job was enqueued.
	CreatedAt time.Time `json:"created_at"`
}

// JobOption is a functional option applied to a Job before it is enqueued.
type JobOption func(*Job)

// WithDelay sets the number of seconds to wait before the job is dispatched.
func WithDelay(seconds int64) JobOption {
	return func(j *Job) { j.Delay = seconds }
}

// WithMaxRetry overrides the maximum number of retry attempts for the job.
func WithMaxRetry(max int) JobOption {
	return func(j *Job) { j.MaxRetry = max }
}

// WithMaxLen overrides the approximate stream length cap for the job's stream.
func WithMaxLen(max int64) JobOption {
	return func(j *Job) { j.MaxLen = max }
}

// Enqueue adds a job of the given type to the queue for immediate processing.
// The target stream is determined by the registered JobTypeOptions for jobType;
// if the stream does not yet exist the job falls back to the default stream.
func (q *Queue) Enqueue(ctx context.Context, jobType string, data interface{}, opts ...JobOption) error {
	cfg := q.resolveType(jobType)
	job := q.buildJob(jobType, data, cfg, opts)

	if job.Delay > 0 {
		return q.pushDelayed(ctx, job)
	}
	return q.pushToStream(ctx, job, cfg)
}

// EnqueueDelayed adds a job that will not be dispatched until delay has
// elapsed. Under the hood the job is placed in the delayed stream and
// moved to the target stream by the delay-dispatcher goroutine.
func (q *Queue) EnqueueDelayed(ctx context.Context, jobType string, data interface{}, delay time.Duration, opts ...JobOption) error {
	opts = append(opts, WithDelay(int64(delay.Seconds())))
	return q.Enqueue(ctx, jobType, data, opts...)
}

// EnqueueUnique adds a job only if no job with the same dedupeKey has been
// enqueued within ttl. Duplicate submissions within the window are silently
// dropped (no error is returned).
//
// Deduplication is implemented with a Redis SET NX key so it works across
// multiple application instances.
func (q *Queue) EnqueueUnique(ctx context.Context, jobType string, dedupeKey string, data interface{}, ttl time.Duration, opts ...JobOption) error {
	lockKey := q.redis.BuildKey(fmt.Sprintf("queue:dedupe:%s", dedupeKey))

	set, err := q.redis.SetNX(ctx, lockKey, "1", ttl)
	if err != nil {
		return fmt.Errorf("queue: deduplication check failed: %w", err)
	}
	if !set {
		// Duplicate — skip silently.
		return nil
	}

	return q.Enqueue(ctx, jobType, data, opts...)
}

// ---- internal helpers ----

// buildJob constructs a Job with defaults resolved from cfg and any opts applied.
func (q *Queue) buildJob(jobType string, data interface{}, cfg jobTypeConfig, opts []JobOption) *Job {
	job := &Job{
		ID:        uuid.NewString(),
		Type:      jobType,
		Data:      data,
		MaxRetry:  cfg.MaxRetry,
		MaxLen:    cfg.MaxLen,
		CreatedAt: time.Now(),
	}
	for _, opt := range opts {
		opt(job)
	}
	return job
}

// pushDelayed routes job to the delayed stream with a run_at timestamp.
func (q *Queue) pushDelayed(ctx context.Context, job *Job) error {
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: failed to marshal delayed job: %w", err)
	}

	runAt := time.Now().Add(time.Duration(job.Delay) * time.Second).Unix()
	delayedKey := q.redis.BuildKey(q.cfg.DelayedStreamKey)

	if _, err := q.redis.XAdd(ctx, &goredis.XAddArgs{
		Stream: delayedKey,
		Values: map[string]interface{}{
			"run_at": runAt,
			"job":    jobBytes,
		},
	}); err != nil {
		return fmt.Errorf("queue: failed to push to delayed stream: %w", err)
	}
	return nil
}

// pushToStream routes job directly to its target stream.
func (q *Queue) pushToStream(ctx context.Context, job *Job, cfg jobTypeConfig) error {
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: failed to marshal job: %w", err)
	}

	streamKey := q.redis.BuildKey(cfg.StreamKey)

	// Verify the stream exists; fall back to default if not.
	exists, _ := q.redis.Exists(ctx, streamKey)
	if exists == 0 {
		defaultCfg := q.resolveType("default")
		streamKey = q.redis.BuildKey(defaultCfg.StreamKey)
		cfg.MaxLen = defaultCfg.MaxLen
	}

	if _, err := q.redis.XAdd(ctx, &goredis.XAddArgs{
		Stream: streamKey,
		MaxLen: cfg.MaxLen,
		Approx: true,
		Values: map[string]interface{}{"job": jobBytes},
	}); err != nil {
		return fmt.Errorf("queue: failed to push job to stream %s: %w", streamKey, err)
	}
	return nil
}

// retryOrDLQ re-enqueues job for another attempt or routes it to the DLQ when
// MaxRetry is exhausted.
func (q *Queue) retryOrDLQ(ctx context.Context, job Job, streamKey string, cfg jobTypeConfig) {
	job.Retry++
	if job.MaxRetry == 0 {
		job.MaxRetry = cfg.MaxRetry
	}

	jobBytes, _ := json.Marshal(job)

	if job.Retry <= job.MaxRetry {
		if _, err := q.redis.XAdd(ctx, &goredis.XAddArgs{
			Stream: streamKey,
			MaxLen: cfg.MaxLen,
			Approx: true,
			Values: map[string]interface{}{"job": jobBytes},
		}); err == nil {
			q.logInfo("queue: job scheduled for retry",
				"job_id", job.ID, "type", job.Type,
				"retry", job.Retry, "max_retry", job.MaxRetry)
		}
		return
	}

	dlqKey := q.redis.BuildKey(q.cfg.DLQStreamKey)
	if _, err := q.redis.XAdd(ctx, &goredis.XAddArgs{
		Stream: dlqKey,
		MaxLen: q.cfg.DLQMaxLen,
		Approx: true,
		Values: map[string]interface{}{"job": jobBytes},
	}); err == nil {
		q.logInfo("queue: job moved to DLQ",
			"job_id", job.ID, "type", job.Type,
			"retry", job.Retry, "max_retry", job.MaxRetry)
	}
}

// ack acknowledges ids and logs any error. Not finding an already-ACK'd message
// is logged as a warning rather than an error.
func (q *Queue) ack(ctx context.Context, streamKey, group, jobType, consumer string, ids []string) {
	if len(ids) == 0 {
		return
	}
	count, err := q.redis.XAck(ctx, streamKey, group, ids...)
	if err != nil {
		q.logError("queue: failed to acknowledge messages",
			"consumer", consumer, "stream", streamKey, "group", group,
			"job_type", jobType, "err", err.Error())
		return
	}
	if count == 0 {
		q.logWarn("queue: no messages acknowledged (possibly already ACK'd)",
			"consumer", consumer, "stream", streamKey, "group", group,
			"attempted", len(ids), "job_type", jobType)
	}
}

// executeHandler routes job to its registered handler, recovering from panics.
func (q *Queue) executeHandler(job Job, workerName string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in job handler: %v", r)
			q.logError("queue: panic recovered",
				"worker", workerName, "job_id", job.ID, "type", job.Type, "panic", r)
		}
	}()

	q.mu.RLock()
	handler, ok := q.handlers[job.Type]
	q.mu.RUnlock()

	if !ok {
		return fmt.Errorf("queue: no handler registered for job type %q", job.Type)
	}

	q.logInfo("queue: processing job",
		"worker", workerName, "job_id", job.ID, "type", job.Type,
		"retry", job.Retry, "max_retry", job.MaxRetry,
		"created_at", job.CreatedAt.Format(time.RFC3339))

	return handler(job)
}
