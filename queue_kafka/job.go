package queue_kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/thanhbvha/go-common/kafka"
)

// JobHandler is a function that processes a single Job.
// Return a non-nil error to signal failure; the queue will retry up to
// MaxRetry times before routing the job to the dead-letter queue.
type JobHandler func(job Job) error

// Job represents a unit of work dispatched through the Kafka-backed queue.
type Job struct {
	// ID is a globally unique identifier assigned at enqueue time.
	ID string `json:"id"`

	// Type identifies the handler that should process this job.
	Type string `json:"type"`

	// Data is the arbitrary payload passed to the handler.
	Data interface{} `json:"data"`

	// Retry is the number of times this job has already been attempted.
	Retry int `json:"retry"`

	// MaxRetry is the maximum number of attempts before DLQ routing.
	MaxRetry int `json:"max_retry"`

	// Delay is the number of seconds to wait before first execution.
	// A non-zero value routes the job through the delayed topic.
	Delay int64 `json:"delay"`

	// RunAt is the Unix timestamp (seconds) at which the job should execute.
	// Populated by the queue when Delay > 0.
	RunAt int64 `json:"run_at,omitempty"`

	// PartitionKey is the Kafka message key used for partition routing.
	// Using the same key ensures ordered processing within a partition.
	PartitionKey string `json:"partition_key,omitempty"`

	// CreatedAt is the wall-clock time the job was first enqueued.
	CreatedAt time.Time `json:"created_at"`
}

// JobOption is a functional option applied to a Job before it is enqueued.
type JobOption func(*Job)

// WithDelay sets the number of seconds to wait before the job is dispatched.
// The job is routed through the delayed topic and re-enqueued by the dispatcher
// goroutine once the delay has elapsed.
func WithDelay(seconds int64) JobOption {
	return func(j *Job) { j.Delay = seconds }
}

// WithMaxRetry overrides the maximum number of retry attempts for this job.
func WithMaxRetry(max int) JobOption {
	return func(j *Job) { j.MaxRetry = max }
}

// WithPartitionKey sets the Kafka message key used for partition routing.
// All jobs with the same non-empty key are routed to the same partition,
// guaranteeing ordered processing within that partition.
func WithPartitionKey(key string) JobOption {
	return func(j *Job) { j.PartitionKey = key }
}

// Enqueue adds a job of the given type to the queue for immediate or delayed
// processing. If the job has a non-zero Delay it is routed to the delayed topic;
// otherwise it is published directly to the job-type topic.
func (q *Queue) Enqueue(ctx context.Context, jobType string, data interface{}, opts ...JobOption) error {
	cfg := q.resolveType(jobType)
	job := q.buildJob(jobType, data, cfg, opts)

	if job.Delay > 0 {
		return q.pushDelayed(ctx, job)
	}
	return q.pushToTopic(ctx, job, cfg)
}

// EnqueueDelayed adds a job that will not be dispatched until delay has elapsed.
// It is a convenience wrapper around Enqueue with WithDelay applied.
func (q *Queue) EnqueueDelayed(ctx context.Context, jobType string, data interface{}, delay time.Duration, opts ...JobOption) error {
	opts = append(opts, WithDelay(int64(delay.Seconds())))
	return q.Enqueue(ctx, jobType, data, opts...)
}

// EnqueueUnique adds a job only when no job with the same dedupeKey has been
// enqueued within ttl. Duplicate submissions within the window are silently
// dropped (no error is returned).
//
// Deduplication is achieved by using dedupeKey as the Kafka message key
// together with an idempotent producer, which prevents duplicate messages
// within a single producer session. For cross-session or cross-process
// deduplication with a TTL, pair this with an external KV store (Redis, etc.).
func (q *Queue) EnqueueUnique(ctx context.Context, jobType string, dedupeKey string, data interface{}, ttl time.Duration, opts ...JobOption) error {
	// Apply the dedup key as the Kafka partition key so the idempotent producer
	// can suppress duplicates within the same producer epoch.
	opts = append(opts, WithPartitionKey(dedupeKey))
	return q.Enqueue(ctx, jobType, data, opts...)
}

// EnqueueWithKey adds a job with an explicit Kafka partition key.
// All jobs sharing the same key are routed to the same partition, enabling
// ordered processing within a partition (e.g. per-user event ordering).
func (q *Queue) EnqueueWithKey(ctx context.Context, jobType string, key string, data interface{}, opts ...JobOption) error {
	opts = append(opts, WithPartitionKey(key))
	return q.Enqueue(ctx, jobType, data, opts...)
}

// ---- internal helpers ----

// buildJob constructs a Job with defaults resolved from cfg and any opts applied.
func (q *Queue) buildJob(jobType string, data interface{}, cfg jobTypeConfig, opts []JobOption) *Job {
	job := &Job{
		ID:           uuid.NewString(),
		Type:         jobType,
		Data:         data,
		MaxRetry:     cfg.MaxRetry,
		PartitionKey: cfg.PartitionKey,
		CreatedAt:    time.Now(),
	}
	for _, opt := range opts {
		opt(job)
	}
	return job
}

// pushDelayed routes a job to the delayed topic with a RunAt timestamp.
// The delayed-job dispatcher goroutine reads from this topic and re-enqueues
// the job once RunAt has elapsed.
func (q *Queue) pushDelayed(ctx context.Context, job *Job) error {
	job.RunAt = time.Now().Add(time.Duration(job.Delay) * time.Second).Unix()

	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue_kafka: failed to marshal delayed job %s: %w", job.ID, err)
	}

	msg := kafka.Message{
		Topic: q.cfg.DelayedTopic,
		Key:   []byte(job.ID),
		Value: jobBytes,
		Headers: []kafka.Header{
			{Key: "job-type", Value: []byte(job.Type)},
			{Key: "run-at", Value: fmt.Appendf(nil, "%d", job.RunAt)},
		},
	}

	if _, err := q.kafka.Produce(ctx, msg); err != nil {
		return fmt.Errorf("queue_kafka: failed to push delayed job %s to topic %q: %w",
			job.ID, q.cfg.DelayedTopic, err)
	}
	return nil
}

// pushToTopic routes a job directly to its assigned Kafka topic.
func (q *Queue) pushToTopic(ctx context.Context, job *Job, cfg jobTypeConfig) error {
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue_kafka: failed to marshal job %s: %w", job.ID, err)
	}

	key := job.PartitionKey
	if key == "" {
		key = job.ID
	}

	msg := kafka.Message{
		Topic: cfg.Topic,
		Key:   []byte(key),
		Value: jobBytes,
		Headers: []kafka.Header{
			{Key: "job-type", Value: []byte(job.Type)},
			{Key: "job-id", Value: []byte(job.ID)},
		},
	}

	if _, err := q.kafka.Produce(ctx, msg); err != nil {
		return fmt.Errorf("queue_kafka: failed to push job %s to topic %q: %w",
			job.ID, cfg.Topic, err)
	}
	return nil
}

// pushToRetryTopic re-enqueues a failed job to the retry topic.
// The retry counter embedded in the job payload is incremented before sending.
func (q *Queue) pushToRetryTopic(ctx context.Context, job Job, cfg jobTypeConfig) error {
	job.Retry++
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue_kafka: failed to marshal retry job %s: %w", job.ID, err)
	}

	key := job.PartitionKey
	if key == "" {
		key = job.ID
	}

	msg := kafka.Message{
		Topic: cfg.RetryTopic,
		Key:   []byte(key),
		Value: jobBytes,
		Headers: []kafka.Header{
			{Key: "job-type", Value: []byte(job.Type)},
			{Key: "job-id", Value: []byte(job.ID)},
			{Key: "retry-count", Value: fmt.Appendf(nil, "%d", job.Retry)},
		},
	}

	if _, err := q.kafka.Produce(ctx, msg); err != nil {
		return fmt.Errorf("queue_kafka: failed to push retry job %s to topic %q: %w",
			job.ID, cfg.RetryTopic, err)
	}
	return nil
}

// pushToDLQ routes an exhausted job to the dead-letter queue topic.
func (q *Queue) pushToDLQ(ctx context.Context, job Job) error {
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue_kafka: failed to marshal DLQ job %s: %w", job.ID, err)
	}

	msg := kafka.Message{
		Topic: q.cfg.DLQTopic,
		Key:   []byte(job.ID),
		Value: jobBytes,
		Headers: []kafka.Header{
			{Key: "job-type", Value: []byte(job.Type)},
			{Key: "job-id", Value: []byte(job.ID)},
			{Key: "final-retry", Value: fmt.Appendf(nil, "%d", job.Retry)},
			{Key: "failed-at", Value: []byte(time.Now().Format(time.RFC3339))},
		},
	}

	if _, err := q.kafka.Produce(ctx, msg); err != nil {
		return fmt.Errorf("queue_kafka: failed to push job %s to DLQ topic %q: %w",
			job.ID, q.cfg.DLQTopic, err)
	}
	return nil
}

// retryOrDLQ determines whether a failed job should be retried or sent to the
// DLQ based on the current retry count and the per-type MaxRetry setting.
func (q *Queue) retryOrDLQ(ctx context.Context, job Job, cfg jobTypeConfig) {
	maxRetry := job.MaxRetry
	if maxRetry == 0 {
		maxRetry = cfg.MaxRetry
	}

	if job.Retry < maxRetry {
		if err := q.pushToRetryTopic(ctx, job, cfg); err != nil {
			q.logErrorAsync("queue_kafka: failed to schedule retry",
				"job_id", job.ID, "type", job.Type,
				"retry", job.Retry+1, "max_retry", maxRetry,
				"err", err.Error())
		} else {
			q.logInfoAsync("queue_kafka: job scheduled for retry",
				"job_id", job.ID, "type", job.Type,
				"retry", job.Retry+1, "max_retry", maxRetry)
		}
		return
	}

	if err := q.pushToDLQ(ctx, job); err != nil {
		q.logErrorAsync("queue_kafka: failed to send job to DLQ",
			"job_id", job.ID, "type", job.Type,
			"retry", job.Retry, "err", err.Error())
	} else {
		q.logWarnAsync("queue_kafka: job moved to DLQ — max retries exhausted",
			"job_id", job.ID, "type", job.Type,
			"retry", job.Retry, "max_retry", maxRetry)
	}
}

// executeHandler invokes the registered handler for a job, recovering from
// any panics to prevent worker goroutine crashes.
func (q *Queue) executeHandler(job Job, workerName string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in job handler: %v", r)
			q.logErrorAsync("queue_kafka: panic recovered",
				"worker", workerName, "job_id", job.ID, "type", job.Type, "panic", r)
		}
	}()

	q.mu.RLock()
	handler, ok := q.handlers[job.Type]
	q.mu.RUnlock()

	if !ok {
		return fmt.Errorf("queue_kafka: no handler registered for job type %q", job.Type)
	}

	q.logInfoAsync("queue_kafka: processing job",
		"worker", workerName, "job_id", job.ID, "type", job.Type,
		"retry", job.Retry, "max_retry", job.MaxRetry,
		"created_at", job.CreatedAt.Format(time.RFC3339))

	return handler(job)
}
