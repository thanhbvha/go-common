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
	fmt.Println("=== Cron Module Examples ===")
	fmt.Println("Uncomment one of the functions below to run a specific example.")

	RunStandardCronExample()
	// RunDistributedCronExample()
}

// =====================================================================
// 1. STANDARD CRON (Local Node Execution)
// =====================================================================
func RunStandardCronExample() {
	fmt.Println("\n--- 1. Testing Standard Local Cron ---")
	
	// Initialize Scheduler
	scheduler := cron.NewScheduler()

	// Add a standard job that runs every 5 seconds
	_, err := scheduler.AddFunc("*/5 * * * * *", func() {
		fmt.Printf("[%s] 🕒 Standard Job: Running on this local machine.\n", time.Now().Format("15:04:05"))
	})
	if err != nil {
		log.Fatalf("Failed to add standard cron job: %v", err)
	}

	scheduler.Start()

	fmt.Println("Standard Scheduler is running. Press Ctrl+C to stop.")
	waitForInterrupt(scheduler)
}

// =====================================================================
// 2. DISTRIBUTED CRON (Multi-Node with Redis Leader Election)
// =====================================================================
func RunDistributedCronExample() {
	fmt.Println("\n--- 2. Testing Distributed Cron ---")

	// 1. Connect to Redis (Used for Leader Election via Distributed Lock)
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// 2. Initialize RedisLock
	redisLock := cache.NewRedisLock(rdb)

	// 3. Initialize Scheduler
	scheduler := cron.NewScheduler()

	// 4. Define a Distributed Job
	jobCfg := cron.DistributedConfig{
		JobName:  "daily_report_generator",
		// Schedule: Run every 10 seconds for demonstration purposes
		Schedule: "*/10 * * * * *", 
		// CRITICAL: LockTTL must ALWAYS be slightly less than the cron interval to ensure
		// the lock expires just in time for the next tick, avoiding skipped executions.
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

	fmt.Println("Distributed Scheduler is running. Press Ctrl+C to stop.")
	fmt.Println("Try running multiple instances of this program simultaneously!")
	fmt.Println("You will see that only ONE instance executes the job every 10 seconds.")
	
	waitForInterrupt(scheduler)
}

// ---------------- Helpers ----------------

func waitForInterrupt(scheduler *cron.Scheduler) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c

	scheduler.Stop()
	log.Println("Application stopped.")
}
