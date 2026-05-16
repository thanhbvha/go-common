// Package example demonstrates how to wire up the three packages in
// github.com/thanhbvha/go-common (logger, redis, queue) in a single binary.
//
// Run:
//
//	go run ./queue/example/main.go
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
	gored "github.com/thanhbvha/go-common/redis"
)

func main() {
	// ---- 1. Logger ----
	log := logger.New(logger.Options{
		Level:  0, // slog.LevelInfo
		StdOut: true,
		// Uncomment to enable file rotation:
		// File: &logger.FileOptions{
		//     Path:       "app.log",
		//     MaxSizeMB:  50,
		//     MaxBackups: 7,
		//     MaxAgeDays: 30,
		//     Compress:   true,
		// },
	})
	logger.SetDefault(log)
	defer logger.Close()

	// ---- 2. Redis ----
	redisCfg := gored.DefaultConfig()
	redisCfg.Host = getEnv("REDIS_HOST", "localhost")
	redisCfg.Port = 6379
	redisCfg.Prefix = "myapp:"
	redisCfg.Logger = log // inject the logger

	rdb := gored.New(redisCfg)
	ctx := context.Background()
	if err := rdb.Connect(ctx); err != nil {
		logger.Error("redis: connection failed", "err", err)
		os.Exit(1)
	}
	gored.SetDefault(rdb)
	defer gored.Close() //nolint:errcheck

	logger.Info("redis connected")

	// ---- 3. Queue ----
	qCfg := queue.DefaultConfig()
	qCfg.Logger = log // inject the same logger

	q := queue.New(rdb, qCfg)

	// Register job types.
	q.RegisterJobType("email", queue.JobTypeOptions{
		Concurrency: 3,
		MaxRetry:    5,
		BatchSize:   5,
	})
	q.RegisterJobType("notification", queue.JobTypeOptions{
		Concurrency: 2,
		MaxRetry:    3,
	})

	// Register handlers.
	q.RegisterHandler("email", func(job queue.Job) error {
		logger.Info("processing email job", "job_id", job.ID, "data", job.Data)
		time.Sleep(50 * time.Millisecond) // simulate work
		return nil
	})
	q.RegisterHandler("notification", func(job queue.Job) error {
		logger.Info("processing notification job", "job_id", job.ID, "data", job.Data)
		return nil
	})

	q.Start(ctx)
	defer q.Stop()

	// Enqueue some jobs.
	for i := 0; i < 5; i++ {
		if err := q.Enqueue(ctx, "email", map[string]any{
			"to":      fmt.Sprintf("user%d@example.com", i),
			"subject": "Welcome!",
		}); err != nil {
			logger.Error("enqueue failed", "err", err)
		}
	}

	// Enqueue a delayed job.
	if err := q.EnqueueDelayed(ctx, "notification", map[string]any{
		"user_id": 42, "message": "Your report is ready",
	}, 10*time.Second); err != nil {
		logger.Error("enqueue delayed failed", "err", err)
	}

	// Enqueue a unique (deduplicated) job.
	if err := q.EnqueueUnique(ctx, "email", "welcome-user-42",
		map[string]any{"to": "vip@example.com"}, 30*time.Minute); err != nil {
		logger.Error("enqueue unique failed", "err", err)
	}

	// Wait for OS signal.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down…")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
