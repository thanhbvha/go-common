package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thanhbvha/go-common/example/queue/registry"
	"github.com/thanhbvha/go-common/example/queue/tasks"
	_ "github.com/thanhbvha/go-common/example/queue/tasks" // Blank import to run init() and register tasks
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/queue"
	gored "github.com/thanhbvha/go-common/redis"
)

func main() {
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

	// 3. Autoload Tasks
	// The blank import `_ "github.com/thanhbvha/go-common/example/queue/tasks"` at the top
	// ensures that the init() functions in the tasks package execute
	// and register their configurations and handlers into our central registry.

	// 4. Setup Queue
	qCfg := queue.DefaultConfig()
	qCfg.Logger = log
	q := queue.New(rdb, qCfg)

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
		if inst := tasks.GetDBWorkerPool(); inst != nil {
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
