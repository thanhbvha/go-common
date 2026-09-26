package queue_kafka

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Queue manages a set of Kafka-backed worker pools, a delayed-job dispatcher,
// and a dead-letter queue (DLQ) topic. Create with New; register job types and
// handlers before calling Start.
type Queue struct {
	cfg      Config
	kafka    KafkaStreamer
	jobTypes map[string]jobTypeConfig
	handlers map[string]JobHandler
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// New creates a Queue backed by the given KafkaStreamer and configured by cfg.
// The queue is idle until Start is called.
func New(kafkaClient KafkaStreamer, cfg Config) *Queue {
	return &Queue{
		cfg:      cfg,
		kafka:    kafkaClient,
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
	retentionMs := opts.RetentionMs
	if retentionMs <= 0 {
		retentionMs = q.cfg.DefaultRetentionMs
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	topic := opts.Topic
	if topic == "" {
		topic = fmt.Sprintf("%s.%s", q.cfg.TopicPrefix, name)
	}
	retryTopic := opts.RetryTopic
	if retryTopic == "" {
		retryTopic = fmt.Sprintf("%s.%s.retry", q.cfg.TopicPrefix, name)
	}
	group := opts.Group
	if group == "" {
		group = fmt.Sprintf("%s.%s", q.cfg.DefaultGroup, name)
	}

	q.jobTypes[name] = jobTypeConfig{
		Concurrency:  concurrency,
		MaxRetry:     maxRetry,
		RetentionMs:  retentionMs,
		BatchSize:    batchSize,
		Topic:        topic,
		RetryTopic:   retryTopic,
		Group:        group,
		PartitionKey: opts.PartitionKey,
	}
}

// RegisterHandler binds handler to jobType. Jobs of that type are dispatched
// to handler from every worker goroutine assigned to the type.
// Must be called before Start.
func (q *Queue) RegisterHandler(jobType string, handler JobHandler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[jobType] = handler
}

// Start ensures required Kafka topics exist, then launches all background
// goroutines: worker pools for each registered type, the delayed-job
// dispatcher, and retry-topic consumers. ctx controls the lifetime of all
// goroutines; cancel it (or call Stop) to begin a graceful shutdown.
func (q *Queue) Start(ctx context.Context) {
	q.ctx, q.cancel = context.WithCancel(ctx)

	q.mu.RLock()
	types := q.snapshotTypes()
	q.mu.RUnlock()

	retentionStr := func(ms int64) map[string]string {
		return map[string]string{
			"retention.ms": fmt.Sprintf("%d", ms),
		}
	}

	// Ensure Kafka topics exist for every registered job type.
	for _, cfg := range types {
		_ = q.kafka.EnsureTopic(q.ctx, cfg.Topic,
			q.cfg.DefaultPartitions, q.cfg.DefaultReplicationFactor, retentionStr(cfg.RetentionMs))
		_ = q.kafka.EnsureTopic(q.ctx, cfg.RetryTopic,
			q.cfg.DefaultPartitions, q.cfg.DefaultReplicationFactor, retentionStr(cfg.RetentionMs))
	}

	// Ensure default, delayed, and DLQ topics exist.
	defaultCfg := q.resolveType("default")
	_ = q.kafka.EnsureTopic(q.ctx, defaultCfg.Topic,
		q.cfg.DefaultPartitions, q.cfg.DefaultReplicationFactor, retentionStr(defaultCfg.RetentionMs))
	_ = q.kafka.EnsureTopic(q.ctx, q.cfg.DelayedTopic,
		q.cfg.DefaultPartitions, q.cfg.DefaultReplicationFactor, retentionStr(q.cfg.DefaultRetentionMs))
	_ = q.kafka.EnsureTopic(q.ctx, q.cfg.DLQTopic,
		q.cfg.DefaultPartitions, q.cfg.DefaultReplicationFactor,
		map[string]string{"retention.ms": fmt.Sprintf("%d", q.cfg.DLQRetentionMs)})

	// Launch the delayed-job dispatcher.
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.delayedDispatchLoop(q.ctx)
	}()

	// Launch worker goroutines for each registered type (main topic + retry topic).
	for name, cfg := range types {
		// Workers for the main topic.
		for i := 0; i < cfg.Concurrency; i++ {
			q.wg.Add(1)
			go func(n string, c jobTypeConfig, id int) {
				defer q.wg.Done()
				q.workerLoop(q.ctx, n, c, id, c.Topic)
			}(name, cfg, i)
		}
		// One additional worker per type for the retry topic.
		q.wg.Add(1)
		go func(n string, c jobTypeConfig) {
			defer q.wg.Done()
			q.workerLoop(q.ctx, n, c, 0, c.RetryTopic)
		}(name, cfg)
	}

	// Fall back to default workers when no types are registered.
	if len(types) == 0 {
		count := q.cfg.DefaultWorkerCount
		if count <= 0 {
			count = 1
		}
		for i := 0; i < count; i++ {
			q.wg.Add(1)
			go func(id int) {
				defer q.wg.Done()
				q.workerLoop(q.ctx, "default", defaultCfg, id, defaultCfg.Topic)
			}(i)
		}
	} else {
		// Always keep at least one default worker running.
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			q.workerLoop(q.ctx, "default", defaultCfg, 0, defaultCfg.Topic)
		}()
	}
}

// Stop signals all goroutines to shut down and waits up to ShutdownTimeout for
// them to finish processing in-flight jobs.
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
		q.logInfo("queue_kafka: all workers stopped gracefully")
	case <-time.After(q.cfg.ShutdownTimeout):
		q.logWarn("queue_kafka: shutdown timeout reached, forcing exit")
	}
}

// resolveType returns the jobTypeConfig for name, falling back to defaults when
// the type has not been explicitly registered.
func (q *Queue) resolveType(name string) jobTypeConfig {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if cfg, ok := q.jobTypes[name]; ok {
		return cfg
	}

	topic := fmt.Sprintf("%s.default", q.cfg.TopicPrefix)
	if name != "default" {
		topic = fmt.Sprintf("%s.%s", q.cfg.TopicPrefix, name)
	}

	return jobTypeConfig{
		Concurrency: q.cfg.DefaultWorkerCount,
		MaxRetry:    q.cfg.DefaultMaxRetry,
		RetentionMs: q.cfg.DefaultRetentionMs,
		BatchSize:   1,
		Topic:       topic,
		RetryTopic:  topic + ".retry",
		Group:       fmt.Sprintf("%s.%s", q.cfg.DefaultGroup, name),
	}
}

// snapshotTypes returns a shallow copy of the jobTypes map for safe iteration
// outside of the mutex.
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

func (q *Queue) logWarn(msg string, args ...any) {
	if q.cfg.Logger != nil {
		q.cfg.Logger.Warn(msg, args...)
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

func (q *Queue) logWarnAsync(msg string, args ...any) {
	if q.cfg.Logger != nil {
		q.cfg.Logger.WarnAsync(msg, args...)
	}
}
