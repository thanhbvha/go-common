// Package pool provides memory and concurrency pooling utilities.
//
// It includes a Goroutine pool to limit the number of active workers processing
// WebSocket messages, and a sync.Pool-based byte buffer pool to minimize allocations
// and reduce GC pressure during high-throughput message broadcasting.
package pool

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/logger"
)

// WorkerPool manages a dynamic pool of worker goroutines for high-performance concurrent processing.
type WorkerPool struct {
	workers    int32
	maxWorkers int32
	taskQueue  chan func()
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewWorkerPool instantiates a WorkerPool with the specified maximum number of concurrent workers.
// If maxWorkers is less than or equal to 0, it defaults to two times the number of CPU cores.
func NewWorkerPool(maxWorkers int) *WorkerPool {
	if maxWorkers <= 0 {
		maxWorkers = runtime.NumCPU() * 2 // Default to 2x CPU cores
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		maxWorkers: int32(maxWorkers),
		taskQueue:  make(chan func(), maxWorkers*10), // Buffer for pending tasks
		ctx:        ctx,
		cancel:     cancel,
	}

	// Start initial workers
	initialWorkers := maxWorkers / 4
	if initialWorkers < 1 {
		initialWorkers = 1
	}

	for i := 0; i < initialWorkers; i++ {
		pool.addWorker()
	}

	return pool
}

// Submit enqueues a task for execution in the pool. Returns true if the task was successfully scheduled,
// or false if the queue is full and the pool cannot scale further.
func (p *WorkerPool) Submit(task func()) bool {
	select {
	case p.taskQueue <- task:
		p.scaleWorkers()
		return true
	default:
		// Queue is full, try to add more workers if possible
		if p.canAddWorker() {
			p.addWorker()
			select {
			case p.taskQueue <- task:
				return true
			default:
				return false
			}
		}
		return false
	}
}

// SubmitWithTimeout attempts to enqueue a task within the given timeout duration.
// Returns true on success, or false if the timeout expires or the pool context is canceled.
func (p *WorkerPool) SubmitWithTimeout(task func(), timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case p.taskQueue <- task:
		p.scaleWorkers()
		return true
	case <-timer.C:
		return false
	case <-p.ctx.Done():
		return false
	}
}

// canAddWorker checks if the current worker count is below the maximum allowed.
func (p *WorkerPool) canAddWorker() bool {
	current := atomic.LoadInt32(&p.workers)
	max := atomic.LoadInt32(&p.maxWorkers)
	return current < max
}

// addWorker launches a new worker goroutine.
func (p *WorkerPool) addWorker() {
	if !p.canAddWorker() {
		return
	}

	atomic.AddInt32(&p.workers, 1)
	p.wg.Add(1)

	go func() {
		defer func() {
			atomic.AddInt32(&p.workers, -1)
			p.wg.Done()
		}()

		// Worker idle timeout of 30 seconds
		idleTimer := time.NewTimer(30 * time.Second)
		defer idleTimer.Stop()

		for {
			select {
			case <-p.ctx.Done():
				return
			case task := <-p.taskQueue:
				if !idleTimer.Stop() {
					select {
					case <-idleTimer.C:
					default:
					}
				}
				idleTimer.Reset(30 * time.Second)

				// Execute task with panic recovery
				func() {
					defer func() {
						if r := recover(); r != nil {
							logger.ErrorAsync("[AddWorker] Panic in worker pool task", "error", r)
						}
					}()
					task()
				}()

			case <-idleTimer.C:
				// Worker has been idle, check if it is safe to shut down
				if p.shouldReduceWorkers() {
					return
				}
				idleTimer.Reset(30 * time.Second)
			}
		}
	}()
}

// scaleWorkers scales up worker count if there are many pending tasks in the queue.
func (p *WorkerPool) scaleWorkers() {
	queueLen := len(p.taskQueue)
	currentWorkers := int(atomic.LoadInt32(&p.workers))
	maxWorkers := int(atomic.LoadInt32(&p.maxWorkers))

	if queueLen > currentWorkers*2 && currentWorkers < maxWorkers {
		workersToAdd := queueLen/2 - currentWorkers
		if workersToAdd > maxWorkers-currentWorkers {
			workersToAdd = maxWorkers - currentWorkers
		}

		for i := 0; i < workersToAdd; i++ {
			p.addWorker()
		}
	}
}

// shouldReduceWorkers decides if a worker can exit when idle.
func (p *WorkerPool) shouldReduceWorkers() bool {
	queueLen := len(p.taskQueue)
	currentWorkers := int(atomic.LoadInt32(&p.workers))

	minWorkers := int(atomic.LoadInt32(&p.maxWorkers)) / 4
	if minWorkers < 1 {
		minWorkers = 1
	}

	return queueLen < currentWorkers/4 && currentWorkers > minWorkers
}

// GetStats returns current status information for the WorkerPool.
func (p *WorkerPool) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"workers":     atomic.LoadInt32(&p.workers),
		"maxWorkers":  atomic.LoadInt32(&p.maxWorkers),
		"queueLength": len(p.taskQueue),
		"queueCap":    cap(p.taskQueue),
	}
}

// Shutdown gracefully stops all workers and waits for active tasks to complete.
// If the specified timeout expires, it returns immediately.
func (p *WorkerPool) Shutdown(timeout time.Duration) {
	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		// Success
	case <-timer.C:
		// Timeout
	}
}

var (
	globalPool *WorkerPool
	poolOnce   sync.Once
)

// GetGlobalPool returns the default singleton WorkerPool with 4x CPU cores capacity.
func GetGlobalPool() *WorkerPool {
	poolOnce.Do(func() {
		globalPool = NewWorkerPool(runtime.NumCPU() * 4)
	})
	return globalPool
}
