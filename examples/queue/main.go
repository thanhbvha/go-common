package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/queue"
	"github.com/thanhbvha/go-common/queue/registry"
	gored "github.com/thanhbvha/go-common/redis"
)

// =====================================================================
// 1. TASK REGISTRATION (via init)
// =====================================================================

func init() {
	// Note for AI/Developers: In a real project, this init() function and the handlers below
	// should be placed in a separate package (e.g. `tasks/email.go`) and imported into main.go 
	// using a blank import `_ "your_project/tasks"`. We place it here in main.go solely 
	// so you can see the complete code in one file.

	// Register the task to the central registry.
	registry.Register("example_job_type", queue.JobTypeOptions{
		Concurrency: 10,
		MaxRetry:    3,
		MaxLen:      50000,
		BatchSize:   10,
	}, ExampleJobHandler)

	// Initialize the singleton worker pool for database writes.
	dbWorkerInstance = NewDBWorkerPool()
	// StartWorkerPool runs asynchronously
	go func() {
		dbWorkerInstance.StartWorkerPool(10, dbWorkerInstance.WriteData)
	}()
}

// =====================================================================
// 2. MAIN FUNCTION (Setup Redis & Start Queue)
// =====================================================================

func main() {
	fmt.Println("=== Queue (Redis-based) Module Example ===")
	
	// 1. Initialize Logger
	logOpts := logger.DefaultOptions()
	logOpts.Level = 0 // Info
	logOpts.StdOut = true
	log := logger.New(logOpts)
	logger.SetDefault(log)
	defer logger.Close()

	// 2. Initialize Redis
	redisCfg := gored.DefaultConfig()
	redisCfg.Host = "localhost"
	redisCfg.Port = 6379
	redisCfg.Prefix = "myapp:"
	redisCfg.Logger = log

	rdb := gored.New(redisCfg)
	ctx := context.Background()
	if err := rdb.Connect(ctx); err != nil {
		logger.Error("Failed to connect to Redis", "error", err)
		os.Exit(1)
	}
	gored.SetDefault(rdb)
	defer rdb.Close()
	logger.Info("Connected to Redis successfully.")

	// 3. Setup Queue
	qCfg := queue.DefaultConfig()
	qCfg.Logger = log
	// IMPORTANT: ShutdownTimeout determines how long q.Stop() will wait for
	// workers to finish in-flight jobs before forcing an exit.
	qCfg.ShutdownTimeout = 10 * time.Second 
	q := queue.New(rdb, qCfg)

	// 4. Autoload Tasks
	// Apply all tasks registered via init() into our specific Queue instance.
	registry.ApplyToQueue(q)

	// 5. Graceful Shutdown Setup
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	shutdownDone := make(chan struct{})
	go func() {
		<-quit
		logger.InfoAsync("Queue Worker shutting down...")

		// Stop the queue workers from pulling new jobs
		q.Stop()

		// Stop custom internal worker pools inside tasks
		if inst := GetDBWorkerPool(); inst != nil {
			inst.Shutdown(5 * time.Second)
		}

		// Close Redis connection
		if err := rdb.Close(); err != nil {
			logger.ErrorAsync("Error closing Redis connection", "error", err)
		}

		logger.InfoAsync("Queue Worker stopped.")
		close(shutdownDone)
	}()

	// 6. Start the Queue workers
	logger.InfoAsync("Queue Worker starting...")
	q.Start(context.Background())

	// Push a sample job to test the system
	err := q.Enqueue(context.Background(), "example_job_type", map[string]interface{}{
		"countryCode": "VN",
		"userId":      12345,
	})
	if err != nil {
		logger.Error("Failed to enqueue sample job", "err", err)
	} else {
		logger.Info("Sample job enqueued successfully.")
	}

	// Wait until an interrupt signal is received
	<-shutdownDone
}

// =====================================================================
// 3. JOB HANDLER IMPLEMENTATION
// =====================================================================

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
