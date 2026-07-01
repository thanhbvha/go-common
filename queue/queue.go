package queue

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// Queue manages a set of Redis-Streams-backed worker pools, a delayed-job
// dispatcher, a DLQ cleanup routine, and a per-stream stuck-job reclaimer.
// Create with New; register job types and handlers before calling Start.
type Queue struct {
	cfg      Config
	redis    RedisStreamer
	jobTypes map[string]jobTypeConfig
	handlers map[string]JobHandler
	mu       sync.RWMutex
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// New creates a Queue backed by the given RedisStreamer and configured by cfg.
// The queue is idle until Start is called.
func New(redisClient RedisStreamer, cfg Config) *Queue {
	return &Queue{
		cfg:      cfg,
		redis:    redisClient,
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
	maxLen := opts.MaxLen
	if maxLen <= 0 {
		maxLen = q.cfg.DefaultMaxLen
	}
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	streamKey := opts.StreamKey
	if streamKey == "" {
		streamKey = fmt.Sprintf("%s:%s", q.cfg.StreamPrefix, name)
	}
	group := opts.Group
	if group == "" {
		group = fmt.Sprintf("%s-%s", q.cfg.DefaultGroup, name)
	}

	q.jobTypes[name] = jobTypeConfig{
		Concurrency: concurrency,
		MaxRetry:    maxRetry,
		StreamKey:   streamKey,
		Group:       group,
		MaxLen:      maxLen,
		BatchSize:   batchSize,
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

// Start initialises consumer groups, then launches all background goroutines:
// one per registered worker, plus the delayed-job dispatcher, DLQ cleanup,
// and per-stream reclaim loops. ctx controls the lifetime of all goroutines;
// cancel it (or call Stop) to begin a graceful shutdown.
func (q *Queue) Start(ctx context.Context) {
	q.ctx, q.cancel = context.WithCancel(ctx)

	q.mu.RLock()
	types := q.snapshotTypes()
	q.mu.RUnlock()

	// Ensure consumer groups exist for every registered type.
	for _, cfg := range types {
		streamKey := q.redis.BuildKey(cfg.StreamKey)
		if err := q.redis.XGroupCreateMkStream(q.ctx, streamKey, cfg.Group, "$"); err != nil &&
			!strings.Contains(err.Error(), "BUSYGROUP") {
			q.logError("queue: failed to create consumer group",
				"stream", streamKey, "group", cfg.Group, "err", err.Error())
		}
	}

	// Ensure the default stream/group exists.
	defaultCfg := q.resolveType("default")
	defaultStream := q.redis.BuildKey(defaultCfg.StreamKey)
	if err := q.redis.XGroupCreateMkStream(q.ctx, defaultStream, defaultCfg.Group, "$"); err != nil &&
		!strings.Contains(err.Error(), "BUSYGROUP") {
		q.logError("queue: failed to create default consumer group",
			"stream", defaultStream, "group", defaultCfg.Group, "err", err.Error())
	}

	// Launch reclaim goroutines.
	for name, cfg := range types {
		streamKey := q.redis.BuildKey(cfg.StreamKey)
		q.wg.Add(1)
		go func(n string, sk, grp string) {
			defer q.wg.Done()
			q.reclaimLoop(q.ctx, sk, grp, fmt.Sprintf("reclaimer-%s", n))
		}(name, streamKey, cfg.Group)
	}
	// Reclaim for the default stream.
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.reclaimLoop(q.ctx, defaultStream, defaultCfg.Group, "reclaimer-default")
	}()

	// Delayed-job dispatcher.
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.delayedDispatchLoop(q.ctx)
	}()

	// DLQ cleanup.
	q.wg.Add(1)
	go func() {
		defer q.wg.Done()
		q.dlqCleanupLoop(q.ctx)
	}()

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

// Stop signals all goroutines to shut down and waits up to 10 seconds for
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

	streamKey := fmt.Sprintf("%s:default", q.cfg.StreamPrefix)
	if name != "default" {
		streamKey = fmt.Sprintf("%s:%s", q.cfg.StreamPrefix, name)
	}

	return jobTypeConfig{
		Concurrency: q.cfg.DefaultWorkerCount,
		MaxRetry:    q.cfg.DefaultMaxRetry,
		StreamKey:   streamKey,
		Group:       q.cfg.DefaultGroup,
		MaxLen:      q.cfg.DefaultMaxLen,
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
