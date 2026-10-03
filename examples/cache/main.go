package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/thanhbvha/go-common/cache"
	myredis "github.com/thanhbvha/go-common/redis"
)

func main() {
	fmt.Println("=== Cache Module Examples ===")
	fmt.Println("Uncomment one of the functions below to run a specific example.")

	RunMemoryCacheExample()
	RunJSONCacheExample()
	// RunRedisCacheExample()
	// RunDistributedLockExample()
}

// =====================================================================
// 1. LOCAL MEMORY CACHE (Ristretto - Ultra Fast, Single Node)
// =====================================================================
func RunMemoryCacheExample() {
	fmt.Println("\n--- 1. Testing Memory Cache (Ristretto) ---")
	ctx := context.Background()

	// Initialize local cache with a maximum capacity of 100MB
	memCache, err := cache.NewMemoryCache(100 * 1024 * 1024)
	if err != nil {
		log.Fatalf("Failed to init Memory Cache: %v", err)
	}
	defer memCache.Close()

	// Set data with a 2-second TTL
	memCache.Set(ctx, "local_key", "Super Fast Data", 2*time.Second)

	// Ristretto handles admissions asynchronously to achieve high performance,
	// so we sleep briefly before reading in this example.
	time.Sleep(50 * time.Millisecond)

	val, err := memCache.Get(ctx, "local_key")
	if err != nil {
		fmt.Println("Error retrieving Memory Cache:", err)
	} else {
		fmt.Printf("Memory Cache hit: [local_key] = %s\n", val)
	}
}

// =====================================================================
// 2. REMOTE REDIS CACHE (Distributed, Multi-Node)
// =====================================================================
func RunRedisCacheExample() {
	fmt.Println("\n--- 2. Testing Remote Redis Cache ---")
	ctx := context.Background()

	// Initialize the core redis module of go-common
	redisClient := myredis.MustConnect(ctx, myredis.Config{
		Mode:     myredis.ModeSingle,
		Host:     "localhost",
		Port:     6379,
		PoolSize: 10,
	})
	// CRITICAL: Always use defer Close() for remote connections to avoid pool exhaustion
	defer redisClient.Close()

	// Initialize Redis Cache, using the Native Client from the redis module
	remoteCache := cache.NewRedisCache(redisClient.Native())

	remoteCache.Set(ctx, "remote_key", "Distributed Data", 10*time.Minute)

	val, err := remoteCache.Get(ctx, "remote_key")
	if err != nil {
		fmt.Println("Error retrieving Redis Cache:", err)
	} else {
		fmt.Printf("Redis Cache hit: [remote_key] = %s\n", val)
	}
}

// =====================================================================
// 3. DISTRIBUTED LOCK (Redlock Algorithm for Mutex across nodes)
// =====================================================================
func RunDistributedLockExample() {
	fmt.Println("\n--- 3. Testing Distributed Lock ---")
	ctx := context.Background()

	// Initialize Redis Client
	redisClient := myredis.MustConnect(ctx, myredis.Config{
		Mode:     myredis.ModeSingle,
		Host:     "localhost",
		Port:     6379,
		PoolSize: 10,
	})
	defer redisClient.Close()

	// Initialize 2 workers competing for the same lock
	worker1 := cache.NewRedisLock(redisClient.Native())
	worker2 := cache.NewRedisLock(redisClient.Native())

	lockKey := "cron_job_daily_report"

	// Ensure old lock is cleared for this demo
	remoteCache := cache.NewRedisCache(redisClient.Native())
	remoteCache.Delete(ctx, lockKey)

	// Worker 1 attempts to acquire the lock for 5 seconds
	acquired1, err := worker1.Acquire(ctx, lockKey, 5*time.Second)
	if acquired1 {
		fmt.Println("Worker 1: Successfully acquired the Lock!")
	} else if err != nil {
		fmt.Printf("Worker 1: Error acquiring lock: %v\n", err)
	}

	// Worker 2 attempts to acquire the lock (Will definitely fail because Worker 1 holds it)
	acquired2, _ := worker2.Acquire(ctx, lockKey, 5*time.Second)
	if !acquired2 {
		fmt.Println("Worker 2: Failed! The Lock is currently held by another node.")
	}

	// Worker 1 is taking too long and needs to extend the lock by 10 seconds
	extended, _ := worker1.Extend(ctx, lockKey, 10*time.Second)
	if extended {
		fmt.Println("Worker 1: Successfully extended the Lock for another 10 seconds.")
	}

	// Worker 1 finishes processing and releases the Lock (Safely via Lua script)
	err = worker1.Release(ctx, lockKey)
	if err == nil {
		fmt.Println("Worker 1: Successfully released the Lock.")
	}

	// Now Worker 2 attempts to acquire the Lock again
	acquired2, _ = worker2.Acquire(ctx, lockKey, 5*time.Second)
	if acquired2 {
		fmt.Println("Worker 2: Successfully acquired the Lock this time!")
		worker2.Release(ctx, lockKey)
	}
}

// =====================================================================
// 4. JSON CACHE HELPERS (Store Structs transparently)
// =====================================================================
type UserProfile struct {
	ID    int      `json:"id"`
	Name  string   `json:"name"`
	Roles []string `json:"roles"`
}

func RunJSONCacheExample() {
	fmt.Println("\n--- 4. Testing JSON Cache Helpers ---")
	ctx := context.Background()

	memCache, _ := cache.NewMemoryCache(100 * 1024 * 1024)
	defer memCache.Close()

	user := UserProfile{
		ID:    1,
		Name:  "Admin",
		Roles: []string{"admin", "editor"},
	}

	// Store struct as JSON automatically
	err := cache.SetJSON(ctx, memCache, "user:1", user, 5*time.Minute)
	if err != nil {
		log.Fatalf("SetJSON failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // Ristretto async wait

	// Retrieve JSON into struct pointer
	var fetched UserProfile
	err = cache.GetJSON(ctx, memCache, "user:1", &fetched)
	if err != nil {
		log.Fatalf("GetJSON failed: %v", err)
	}

	fmt.Printf("JSON Cache hit: %+v\n", fetched)
}
