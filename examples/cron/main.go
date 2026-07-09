package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/cache"
	"github.com/thanhbvha/go-common/cron"
)

func main() {
	fmt.Println("=== Cron Module (Distributed Job) Example ===")

	// 1. Connect to Redis (Used for Leader Election via Distributed Lock)
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 2. Initialize RedisLock
	redisLock := cache.NewRedisLock(rdb)

	// 3. Initialize Scheduler
	scheduler := cron.NewScheduler()

	// 4. Define a Distributed Job
	jobCfg := cron.DistributedConfig{
		JobName:  "daily_report_generator",
		// Run every 10 seconds for demonstration purposes
		Schedule: "*/10 * * * * *", 
		// TTL should be slightly less than the interval (e.g., 9s for a 10s interval)
		LockTTL:  9 * time.Second,
		RunFunc: func(ctx context.Context) {
			fmt.Println("--------------------------------------------------")
			fmt.Printf("[%s] 🚀 RUNNING HEAVY JOB: Generating Report...\n", time.Now().Format("15:04:05"))
			time.Sleep(3 * time.Second) // Simulate heavy work
			fmt.Printf("[%s] ✅ JOB COMPLETED.\n", time.Now().Format("15:04:05"))
			fmt.Println("--------------------------------------------------")
		},
	}

	// 5. Add Job to Scheduler
	scheduler.AddDistributedJob(jobCfg, redisLock)

	// 6. Start Scheduler
	scheduler.Start()

	// Keep the application running until stopped
	fmt.Println("Scheduler is running. Press Ctrl+C to stop.")
	fmt.Println("Try running multiple instances of this program simultaneously!")
	fmt.Println("You will see that only ONE instance executes the job every 10 seconds.")
	
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	scheduler.Stop()
	log.Println("Application stopped.")
}
