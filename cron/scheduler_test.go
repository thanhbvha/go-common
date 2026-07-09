package cron

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/cache"
)

func TestDistributedJob_SingleExecution(t *testing.T) {
	// 1. Start Miniredis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to start miniredis: %v", err)
	}
	defer mr.Close()

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	// 2. Setup Scheduler and RedisLock
	scheduler := NewScheduler()
	scheduler.Start()
	defer scheduler.Stop()

	// We create 3 different locks simulating 3 different server nodes.
	lockNode1 := cache.NewRedisLock(rdb)
	lockNode2 := cache.NewRedisLock(rdb)
	lockNode3 := cache.NewRedisLock(rdb)

	var executionCount int32

	jobFunc := func(ctx context.Context) {
		atomic.AddInt32(&executionCount, 1)
	}

	// 3. Register the exact same job on all 3 nodes (simulating 3 pods)
	// Cron schedule: Run every second (* * * * * *)
	cfg := DistributedConfig{
		JobName:  "test-job",
		Schedule: "* * * * * *",
		LockTTL:  1 * time.Second, // Lock prevents other nodes from running it in the same second
		RunFunc:  jobFunc,
	}

	scheduler.AddDistributedJob(cfg, lockNode1)
	scheduler.AddDistributedJob(cfg, lockNode2)
	scheduler.AddDistributedJob(cfg, lockNode3)

	// 4. Wait for 2.5 seconds to allow the cron to trigger twice
	// Since we are using miniredis, we must explicitly advance its clock alongside Go's clock.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		
		for i := 0; i < 25; i++ { // 25 * 100ms = 2.5s
			<-ticker.C
			mr.FastForward(100 * time.Millisecond)
		}
	}()
	wg.Wait()

	// 5. Assertions
	count := atomic.LoadInt32(&executionCount)

	// In 2.5 seconds, a 1-second interval cron will fire either 2 or 3 times depending on 
	// when exactly the scheduler started within the current second.
	// Even though we have 3 nodes registered, it should ONLY execute 1 time per second across the entire cluster,
	// because the RedisLock ensures Leader Election.
	if count < 2 || count > 3 {
		t.Errorf("Expected job to execute 2 or 3 times across the cluster, got %d", count)
	}
}
