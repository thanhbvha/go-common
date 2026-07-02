package queue_nats

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/nats"
)

// Queue manages a set of NATS JetStream-backed worker pools, a delayed-job
// dispatcher, and built-in redelivery mechanics.
// Create with New; register job types and handlers before calling Start.
type Queue struct {
	cfg      Config
	nats     NATSStreamer
	jobTypes map[string]jobTypeConfig
	handlers map[string]JobHandler
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// New creates a Queue backed by the given NATSStreamer and configured by cfg.
// The queue is idle until Start is called.
func New(natsClient NATSStreamer, cfg Config) *Queue {
	return &Queue{
		cfg:      cfg,
		nats:     natsClient,
		jobTypes: make(map[string]jobTypeConfig),
		handlers: make(map[string]JobHandler),
	}
}

// RegisterJobType declares a named job type with its worker configuration.
// Must be called before Start. Calling with the same name overwrites the
// previous registration.
func (q *Queue) RegisterJobType(name string, opts JobTypeOptions) {
	q.mu.Lock()
	defer q.mu.Unlock()

	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	maxRetry := opts.MaxRetry
	if maxRetry <= 0 {
		maxRetry = q.cfg.DefaultMaxRetry
	}
	maxAge := opts.MaxAge
	if maxAge <= 0 {
		maxAge = q.cfg.DefaultMaxAge
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	streamName := opts.StreamName
	if streamName == "" {
		streamName = fmt.Sprintf("%s_%s", q.cfg.StreamPrefix, name)
	}
	group := opts.Group
	if group == "" {
		group = fmt.Sprintf("%s_%s", q.cfg.DefaultGroup, name)
	}

	q.jobTypes[name] = jobTypeConfig{
		Concurrency: concurrency,
		MaxRetry:    maxRetry,
		StreamName:  streamName,
		Group:       group,
		MaxAge:      maxAge,
		BatchSize:   batchSize,
	}
}

// RegisterHandler binds handler to jobType.
// Must be called before Start.
func (q *Queue) RegisterHandler(jobType string, handler JobHandler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[jobType] = handler
}

// Start initialises NATS streams and consumer groups, then launches all background goroutines.
func (q *Queue) Start(ctx context.Context) {
	q.ctx, q.cancel = context.WithCancel(ctx)

	q.mu.RLock()
	types := q.snapshotTypes()
	q.mu.RUnlock()

	// Ensure streams and consumer groups exist for every registered type.
	for name, cfg := range types {
		subject := fmt.Sprintf("%s.%s", q.cfg.StreamPrefix, name)

		// Create stream
		if err := q.nats.AddStream(q.ctx, cfg.StreamName, []string{subject}, nats.WithMaxAge(cfg.MaxAge), nats.WithRetention(nats.WorkQueuePolicy)); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				q.logError("queue: failed to create stream",
					"stream", cfg.StreamName, "subject", subject, "err", err.Error())
			}
		}

		// Create durable consumer
		if err := q.nats.AddConsumer(q.ctx, cfg.StreamName, cfg.Group, nats.WithFilterSubject(subject), nats.WithAckWait(q.cfg.AckWait)); err != nil {
			if !strings.Contains(err.Error(), "already exists") {
				q.logError("queue: failed to create consumer group",
					"stream", cfg.StreamName, "group", cfg.Group, "err", err.Error())
			}
		}
	}

	// Ensure the default stream/group exists.
	defaultCfg := q.resolveType("default")
	defaultSubject := fmt.Sprintf("%s.default", q.cfg.StreamPrefix)

	if err := q.nats.AddStream(q.ctx, defaultCfg.StreamName, []string{defaultSubject}, nats.WithMaxAge(defaultCfg.MaxAge), nats.WithRetention(nats.WorkQueuePolicy)); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			q.logError("queue: failed to create default stream",
				"stream", defaultCfg.StreamName, "subject", defaultSubject, "err", err.Error())
		}
	}

	if err := q.nats.AddConsumer(q.ctx, defaultCfg.StreamName, defaultCfg.Group, nats.WithFilterSubject(defaultSubject), nats.WithAckWait(q.cfg.AckWait)); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			q.logError("queue: failed to create default consumer group",
				"stream", defaultCfg.StreamName, "group", defaultCfg.Group, "err", err.Error())
		}
	}

	// Ensure Delayed stream and consumer
	if err := q.nats.AddStream(q.ctx, "queue_delayed", []string{q.cfg.DelayedStreamSubject}, nats.WithMaxAge(q.cfg.DefaultMaxAge), nats.WithRetention(nats.WorkQueuePolicy)); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			q.logError("queue: failed to create delayed stream", "err", err.Error())
		}
	}
	if err := q.nats.AddConsumer(q.ctx, "queue_delayed", "delayed-dispatcher", nats.WithFilterSubject(q.cfg.DelayedStreamSubject), nats.WithAckWait(1*time.Minute)); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			q.logError("queue: failed to create delayed consumer group", "err", err.Error())
		}
	}

	// Ensure DLQ stream
	if err := q.nats.AddStream(q.ctx, q.cfg.DLQStreamName, []string{q.cfg.DLQStreamName}, nats.WithMaxAge(q.cfg.DLQRetention), nats.WithRetention(nats.WorkQueuePolicy)); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			q.logError("queue: failed to create DLQ stream", "err", err.Error())
		}
	}

	// Delayed-job dispatcher.
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.delayedDispatchLoop(q.ctx)
	}()

	// Note: NATS handles Stuck Jobs via AckWait redelivery and DLQ Cleanup via MaxAge.
	// So we omit reclaimLoop and dlqCleanupLoop.

	// Worker goroutines for each registered type.
	for name, cfg := range types {
		for i := 0; i < cfg.Concurrency; i++ {
			q.wg.Add(1)
			go func(n string, c jobTypeConfig, id int) {
				defer q.wg.Done()
				q.workerLoop(q.ctx, n, c, id)
			}(name, cfg, i)
		}
	}

	// Default workers when no types are registered; always at least one.
	if len(types) == 0 {
		count := q.cfg.DefaultWorkerCount
		if count <= 0 {
			count = 1
		}
		for i := 0; i < count; i++ {
			q.wg.Add(1)
			go func(id int) {
				defer q.wg.Done()
				q.workerLoop(q.ctx, "default", defaultCfg, id)
			}(i)
		}
	} else {
		// Always keep one default worker running.
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			q.workerLoop(q.ctx, "default", defaultCfg, 0)
		}()
	}
}

// Stop signals all goroutines to shut down and waits for them to finish.
func (q *Queue) Stop() {
	if q.cancel != nil {
		q.cancel()
	}

	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		q.logInfo("queue: all workers stopped gracefully")
	case <-q.ctx.Done():
	}
}

// resolveType returns the jobTypeConfig for name, falling back to defaults.
func (q *Queue) resolveType(name string) jobTypeConfig {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if cfg, ok := q.jobTypes[name]; ok {
		return cfg
	}

	streamName := fmt.Sprintf("%s_default", q.cfg.StreamPrefix)
	if name != "default" {
		streamName = fmt.Sprintf("%s_%s", q.cfg.StreamPrefix, name)
	}

	return jobTypeConfig{
		Concurrency: q.cfg.DefaultWorkerCount,
		MaxRetry:    q.cfg.DefaultMaxRetry,
		StreamName:  streamName,
		Group:       q.cfg.DefaultGroup,
		MaxAge:      q.cfg.DefaultMaxAge,
		BatchSize:   1,
	}
}

// snapshotTypes returns a shallow copy of the jobTypes map for safe iteration.
func (q *Queue) snapshotTypes() map[string]jobTypeConfig {
	snapshot := make(map[string]jobTypeConfig, len(q.jobTypes))
	for k, v := range q.jobTypes {
		snapshot[k] = v
	}
	return snapshot
}

// ---- Internal logging helpers ----

func (q *Queue) logInfo(msg string, args ...any) {
	if q.cfg.Logger != nil {
		q.cfg.Logger.Info(msg, args...)
	}
}

func (q *Queue) logError(msg string, args ...any) {
	if q.cfg.Logger != nil {
		q.cfg.Logger.Error(msg, args...)
	}
}

func (q *Queue) logInfoAsync(msg string, args ...any) {
	if q.cfg.Logger != nil {
		q.cfg.Logger.InfoAsync(msg, args...)
	}
}

func (q *Queue) logErrorAsync(msg string, args ...any) {
	if q.cfg.Logger != nil {
		q.cfg.Logger.ErrorAsync(msg, args...)
	}
}
