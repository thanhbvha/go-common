package eventbus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

// Headers map for propagating tracing, metadata, etc.
type Headers map[string]string

// Envelope represents the internal wire format v3 for all jobs.
// It uses json.RawMessage for the Payload to preserve large int64 precision (Bug C2).
type Envelope struct {
	V           int             `json:"v"`            // version, always 3 for newly encoded messages
	ID          string          `json:"id"`           // unique job ID
	Kind        TopologyKind    `json:"kind"`         // task or event topology
	Type        string          `json:"type"`         // job type name
	Payload     json.RawMessage `json:"payload"`      // raw json payload
	Attempt     int             `json:"attempt"`      // current attempt (1-indexed)
	MaxAttempts int             `json:"max_attempts"` // total permitted handler executions
	// MaxRetry is populated only while decoding a v2 envelope.
	//
	// Deprecated: use MaxAttempts.
	MaxRetry    int     `json:"max_retry,omitempty"`
	CreatedAt   int64   `json:"created_at"`              // creation time in unix milliseconds
	RunAt       int64   `json:"run_at"`                  // scheduled run time in unix milliseconds
	OrderingKey string  `json:"ordering_key,omitempty"`  // optional partition/ordering key
	DedupeKey   string  `json:"dedupe_key,omitempty"`    // optional deduplication key
	DedupeTTL   int64   `json:"dedupe_ttl_ns,omitempty"` // reservation lifetime in nanoseconds
	Headers     Headers `json:"headers,omitempty"`       // optional headers (e.g. traceparent)
}

// Job is the public interface for handlers to access job context and payload.
type Job struct {
	env *Envelope
}

// ID returns the unique identifier for this job.
func (j *Job) ID() string { return j.env.ID }

// Type returns the job type.
func (j *Job) Type() string { return j.env.Type }

// Kind returns whether this job belongs to a task or event topology.
func (j *Job) Kind() TopologyKind { return j.env.Kind }

// Attempt returns the current 1-indexed handler attempt.
func (j *Job) Attempt() int { return j.env.Attempt }

// GetAttempt returns the current 1-indexed handler attempt.
//
// Deprecated: use Attempt.
func (j *Job) GetAttempt() int { return j.Attempt() }

// MaxAttempts returns the total number of permitted handler executions.
func (j *Job) MaxAttempts() int { return j.env.MaxAttempts }

// MaxRetry returns the total number of permitted handler executions.
//
// Deprecated: use MaxAttempts.
func (j *Job) MaxRetry() int { return j.MaxAttempts() }

// Decode unmarshals the raw JSON payload into the provided value `v`.
// Because we use json.RawMessage, large int64 IDs (>2^53) are decoded exactly
// without float64 precision loss (Fixing Bug C2).
func (j *Job) Decode(v any) error {
	return json.Unmarshal(j.env.Payload, v)
}

// Header returns the value of the header with the given key, or empty string.
func (j *Job) Header(key string) string {
	if j.env.Headers == nil {
		return ""
	}
	return j.env.Headers[key]
}

// JobOptions are used when enqueuing a job.
type JobOptions struct {
	Delay       time.Duration
	RunAt       time.Time
	OrderingKey string
	DedupeKey   string
	DedupeTTL   time.Duration
	Headers     Headers
}

// JobOption is a functional option for customizing JobOptions.
type JobOption func(*JobOptions)

const MaxBatchJobs = 1000

// BatchJob is one task publication in EnqueueBatch.
type BatchJob struct {
	Payload any
	Options []JobOption
}

// BatchError reports the first failed item in a non-atomic batch. Items before
// Index may already be durable; items at and after Index were not all accepted.
type BatchError struct {
	Index int
	Err   error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("eventbus: batch item %d: %v", e.Index, e.Err)
}

func (e *BatchError) Unwrap() error { return e.Err }

func applyJobOptions(options *JobOptions, opts []JobOption) error {
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("%w: nil job option", ErrInvalidOption)
		}
		opt(options)
	}
	if options.Delay < 0 {
		return fmt.Errorf("%w: delay must not be negative", ErrInvalidOption)
	}
	if options.Delay > 0 && !options.RunAt.IsZero() {
		return fmt.Errorf("%w: delay and run-at cannot both be set", ErrInvalidOption)
	}
	if options.DedupeTTL < 0 {
		return fmt.Errorf("%w: dedupe TTL must not be negative", ErrInvalidOption)
	}
	if options.DedupeTTL > 0 && options.DedupeKey == "" {
		return fmt.Errorf("%w: dedupe TTL requires a dedupe key", ErrInvalidOption)
	}
	for key := range options.Headers {
		if strings.TrimSpace(key) == "" || key != strings.TrimSpace(key) {
			return fmt.Errorf("%w: header key %q is blank or malformed", ErrInvalidOption, key)
		}
	}
	return nil
}

func WithDelay(d time.Duration) JobOption {
	return func(o *JobOptions) { o.Delay = d }
}

func WithRunAt(t time.Time) JobOption {
	return func(o *JobOptions) { o.RunAt = t }
}

func WithOrderingKey(key string) JobOption {
	return func(o *JobOptions) { o.OrderingKey = key }
}

func WithDedupeKey(key string) JobOption {
	return func(o *JobOptions) { o.DedupeKey = key }
}

// WithDedupeTTL sets how long the deduplication reservation remains active.
// A zero value retains the compatibility default of 24 hours.
func WithDedupeTTL(ttl time.Duration) JobOption {
	return func(o *JobOptions) { o.DedupeTTL = ttl }
}

func WithHeader(key, val string) JobOption {
	return func(o *JobOptions) {
		if o.Headers == nil {
			o.Headers = make(Headers)
		}
		o.Headers[key] = val
	}
}

// Handler is the signature for job processing functions.
type Handler func(ctx context.Context, job Job) error
