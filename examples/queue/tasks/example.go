package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/queue"
	"github.com/thanhbvha/go-common/queue/registry"
)

func init() {
	logger.InfoAsync("Registering ExampleJobHandler to queue")

	// Register the task to the central registry instead of the global queue.
	registry.Register("example_job_type", queue.JobTypeOptions{
		Concurrency: 10,
		MaxRetry:    3,
		MaxLen:      50000,
		BatchSize:   10,
	}, ExampleJobHandler)

	// Initialize the singleton worker pool for database writes.
	dbWorkerInstance = NewDBWorkerPool()
	dbWorkerInstance.StartWorkerPool(10, dbWorkerInstance.WriteData)
	logger.InfoAsync("DBWorkerPool initialized", "concurrency", 10)
}

// ExampleJobHandler processes incoming jobs from the queue.
func ExampleJobHandler(job queue.Job) error {
	// Simulate converting interface{} to map
	jobData, ok := job.Data.(map[string]interface{})
	if !ok {
		logger.ErrorAsync("Failed to convert job data to map")
		return fmt.Errorf("invalid job data format")
	}

	jobData["isVietnam"] = "no"
	if countryCode, ok := jobData["countryCode"].(string); ok {
		if countryCode == "VN" || countryCode == "VI" || countryCode == "VNM" {
			jobData["isVietnam"] = "yes"
		}
	}

	jobData["createDate"] = time.Now().Unix()

	// Submit to the internal worker pool for DB writing
	dbWorkerInstance.SubmitToWorkerPool(jobData)

	return nil
}

// Global singleton instance for this specific task's DB worker pool
var dbWorkerInstance *DBWorkerPool

// DBWorkerPool manages an internal worker pool to batch/write data to DB.
type DBWorkerPool struct {
	writeChan chan interface{}
	ctx       context.Context
	cancel    context.CancelFunc
}

func NewDBWorkerPool() *DBWorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &DBWorkerPool{
		writeChan: make(chan interface{}, 10000),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// WriteData simulates writing the parsed data into a database.
func (pool *DBWorkerPool) WriteData(data interface{}) error {
	// In reality, you'd insert to MongoDB or another DB here.
	logger.InfoAsync("Successfully inserted log data to DB", "data", data)
	time.Sleep(10 * time.Millisecond) // Simulate DB I/O
	return nil
}

// StartWorkerPool starts background workers that read from writeChan.
func (pool *DBWorkerPool) StartWorkerPool(concurrency int, doWrite func(data interface{}) error) {
	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer func() {
				if r := recover(); r != nil {
					logger.ErrorAsync("DB worker panic recovered", "panic", r, "worker_id", workerID)
				}
			}()

			for {
				select {
				case <-pool.ctx.Done():
					logger.InfoAsync("DB worker shutdown requested", "worker_id", workerID)
					return
				case data, ok := <-pool.writeChan:
					if !ok {
						logger.InfoAsync("DB worker channel closed", "worker_id", workerID)
						return
					}

					if err := doWrite(data); err != nil {
						logger.ErrorAsync("Error writing data to DB", "error", err, "worker_id", workerID)
					}
				}
			}
		}(i + 1)
	}
}

// SubmitToWorkerPool enqueues data for the internal DB workers.
func (pool *DBWorkerPool) SubmitToWorkerPool(data interface{}) {
	select {
	case pool.writeChan <- data:
		logger.DebugAsync("Data submitted to DB worker pool", "data", data)
	case <-time.After(5 * time.Second):
		logger.ErrorAsync("Timeout submitting data to DB worker pool", "data", data)
	case <-pool.ctx.Done():
		logger.ErrorAsync("Worker pool is shutting down, dropping data", "data", data)
	}
}

// Shutdown gracefully shuts down the DB worker pool.
func (pool *DBWorkerPool) Shutdown(timeout time.Duration) error {
	logger.InfoAsync("Shutting down DB worker pool")
	pool.cancel()
	close(pool.writeChan)

	done := make(chan bool)
	go func() {
		// Wait slightly for pending workers to drain the channel.
		time.Sleep(100 * time.Millisecond)
		done <- true
	}()

	select {
	case <-done:
		logger.InfoAsync("DB worker pool shutdown completed")
		return nil
	case <-time.After(timeout):
		logger.ErrorAsync("DB worker pool shutdown timeout")
		return fmt.Errorf("shutdown timeout")
	}
}

// GetDBWorkerPool exports the instance to be shut down gracefully in main.
func GetDBWorkerPool() *DBWorkerPool {
	return dbWorkerInstance
}
