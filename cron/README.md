# Cron Module (Distributed Job Scheduler)

The `cron` module is an enterprise-grade job scheduler built on top of `robfig/cron/v3`. It provides seamless support for **Distributed Cron Jobs** (Leader Election), ensuring that scheduled tasks run exactly once across your entire microservices cluster.

## Features

1. **Second-Level Precision**: Supports standard Quartz-like cron expressions with second-level precision (e.g., `*/5 * * * * *` for every 5 seconds).
2. **Distributed Locks (Leader Election)**: If you deploy your application to 10 servers, all 10 servers will attempt to run the cron job at the same time. The `cron` module automatically uses Redis to elect a single "Leader" server to execute the job, preventing duplicate execution.
3. **Graceful Logs**: Automatically logs the start time, completion time, and exact execution duration of your jobs.

## Installation

```bash
go get github.com/thanhbvha/go-common/cron
go get github.com/thanhbvha/go-common/cache
```

## Usage

```go
package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/cache"
	"github.com/thanhbvha/go-common/cron"
)

func main() {
	// 1. Connect to Redis
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	
	// 2. Initialize RedisLock (used for Leader Election)
	redisLock := cache.NewRedisLock(rdb)

	// 3. Create the Scheduler
	scheduler := cron.NewScheduler()

	// 4. Define your Distributed Job
	jobCfg := cron.DistributedConfig{
		JobName:  "send_daily_emails",
		Schedule: "0 0 8 * * *", // Run every day at 08:00:00
		LockTTL:  1 * time.Minute, // Must be less than the interval to allow the next run
		RunFunc: func(ctx context.Context) {
			// Do heavy lifting here. 
			// Even if you have 10 servers, only ONE server will execute this block.
			println("Sending emails to millions of users...")
		},
	}

	// 5. Register and Start
	scheduler.AddDistributedJob(jobCfg, redisLock)
	scheduler.Start()

	// Block main thread
	select {}
}
```

## How it works

When the cron schedule triggers (e.g., 08:00:00), every server in your cluster attempts to acquire a Redis Lock (`SETNX`) with the key `cron:lock:send_daily_emails`.
- The first server to hit Redis successfully acquires the lock and becomes the "Leader". It executes your `RunFunc`.
- The other servers will fail to acquire the lock. They will silently skip execution and wait for the next cron interval.
- The lock automatically expires after `LockTTL` to ensure that the next execution (e.g., tomorrow at 08:00:00) can happen normally.
