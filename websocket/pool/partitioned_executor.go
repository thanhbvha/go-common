package pool

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

var (
	ErrExecutorFull    = errors.New("websocket executor queue full")
	ErrExecutorStopped = errors.New("websocket executor stopped")
)

// ExecutorStats is a snapshot of bounded executor activity.
type ExecutorStats struct {
	Partitions  int
	QueueSize   int
	QueueLength int
	Submitted   uint64
	Completed   uint64
	Rejected    uint64
	Panicked    uint64
	InFlight    int64
	Accepting   bool
}

type executorTask struct {
	ctx context.Context
	run func(context.Context) error
}

// PartitionedExecutor preserves FIFO execution for the same key while allowing
// independent keys assigned to different partitions to execute concurrently.
type PartitionedExecutor struct {
	queues    []chan executorTask
	mu        sync.RWMutex
	accepting bool
	wg        sync.WaitGroup
	submitted atomic.Uint64
	completed atomic.Uint64
	rejected  atomic.Uint64
	panicked  atomic.Uint64
	inFlight  atomic.Int64
}

func NewPartitionedExecutor(partitions, queueSize int) *PartitionedExecutor {
	if partitions <= 0 {
		partitions = 1
	}
	if queueSize <= 0 {
		queueSize = 256
	}
	e := &PartitionedExecutor{
		queues:    make([]chan executorTask, partitions),
		accepting: true,
	}
	for i := range e.queues {
		e.queues[i] = make(chan executorTask, queueSize)
		e.wg.Add(1)
		go e.run(e.queues[i])
	}
	return e
}

func (e *PartitionedExecutor) Submit(ctx context.Context, key string, run func(context.Context) error) error {
	if run == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	queue := e.queues[partitionIndex(key, len(e.queues))]
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.accepting {
		e.rejected.Add(1)
		return ErrExecutorStopped
	}
	select {
	case <-ctx.Done():
		e.rejected.Add(1)
		return ctx.Err()
	case queue <- executorTask{ctx: ctx, run: run}:
		e.submitted.Add(1)
		return nil
	default:
		e.rejected.Add(1)
		return ErrExecutorFull
	}
}

func (e *PartitionedExecutor) run(queue <-chan executorTask) {
	defer e.wg.Done()
	for task := range queue {
		e.inFlight.Add(1)
		func() {
			defer func() {
				if recover() != nil {
					e.panicked.Add(1)
				}
			}()
			_ = task.run(task.ctx)
		}()
		e.inFlight.Add(-1)
		e.completed.Add(1)
	}
}

func (e *PartitionedExecutor) Shutdown(ctx context.Context) error {
	e.mu.Lock()
	if e.accepting {
		e.accepting = false
		for _, queue := range e.queues {
			close(queue)
		}
	}
	e.mu.Unlock()

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *PartitionedExecutor) Stats() ExecutorStats {
	e.mu.RLock()
	accepting := e.accepting
	queueLength := 0
	queueSize := 0
	for _, queue := range e.queues {
		queueLength += len(queue)
		queueSize += cap(queue)
	}
	e.mu.RUnlock()
	return ExecutorStats{
		Partitions:  len(e.queues),
		QueueSize:   queueSize,
		QueueLength: queueLength,
		Submitted:   e.submitted.Load(),
		Completed:   e.completed.Load(),
		Rejected:    e.rejected.Load(),
		Panicked:    e.panicked.Load(),
		InFlight:    e.inFlight.Load(),
		Accepting:   accepting,
	}
}

func partitionIndex(key string, partitions int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(partitions))
}
