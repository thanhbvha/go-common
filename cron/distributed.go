package cron

import (
	"context"
	"log"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/thanhbvha/go-common/cache"
)

// DistributedConfig contains configuration for a distributed job.
type DistributedConfig struct {
	// JobName is a unique identifier for the job across the cluster.
	JobName string
	// Schedule is the cron expression (e.g. "0/5 * * * * *").
	Schedule string
	// LockTTL is how long the Redis lock should be held.
	// It should be slightly less than the cron interval to ensure the lock expires
	// before the next run, but long enough to prevent other nodes from acquiring it simultaneously.
	LockTTL time.Duration
	// RunFunc is the actual job logic to execute.
	RunFunc func(ctx context.Context)
}

// AddDistributedJob adds a job that will only run on one node at a time.
// It uses cache.RedisLock to perform Leader Election.
func (s *Scheduler) AddDistributedJob(cfg DistributedConfig, redisLock *cache.RedisLock) (cron.EntryID, error) {
	lockKey := "cron:lock:" + cfg.JobName

	wrapper := func() {
		ctx := context.Background()
		
		// Attempt to acquire the lock. If false, another node is already running this job.
		acquired, err := redisLock.Acquire(ctx, lockKey, cfg.LockTTL)
		if err != nil {
			log.Printf("[Cron] [%s] Error acquiring lock: %v", cfg.JobName, err)
			return
		}

		if !acquired {
			// Another node won the election. Skip execution.
			log.Printf("[Cron] [%s] Skipped (Another node is running this job)", cfg.JobName)
			return
		}

		// We won the election! Run the job.
		log.Printf("[Cron] [%s] Started execution (Lock acquired)", cfg.JobName)
		startTime := time.Now()

		// Execute the actual user function
		cfg.RunFunc(ctx)

		duration := time.Since(startTime)
		log.Printf("[Cron] [%s] Completed execution in %s", cfg.JobName, duration)

		// Note: We deliberately DO NOT release the lock here!
		// The lock will naturally expire after LockTTL.
		// If we release it immediately after a fast job, another node whose clock is slightly
		// behind might trigger its cron, see the lock is free, and run the job again!
		// Relying on TTL ensures strict "At most once per interval" execution across the cluster.
	}

	return s.AddFunc(cfg.Schedule, wrapper)
}
