# Cache Module

The `cache` module provides an enterprise-grade caching and distributed locking solution for microservices. It is designed to be highly concurrent, memory-safe, and framework-agnostic.

## Features

1. **Local Memory Cache (`MemoryCache`)**: 
   - Powered by [Ristretto](https://github.com/dgraph-io/ristretto).
   - High performance and lock-free admission.
   - Built-in `MaxCost` to prevent Out-Of-Memory (OOM) issues.
   - Built-in TTL (Time-To-Live).
   - TinyLFU eviction policy for superior cache hit ratios.
2. **Distributed Cache (`RedisCache`)**:
   - Reuses existing `redis.UniversalClient`.
3. **Distributed Lock (`RedisLock`)**:
   - Atomic mutual exclusion across multiple nodes.
   - Prevents race conditions.
   - Safe token-based Release/Extend logic (Redlock concept) via Lua scripts.

## Installation

This module automatically pulls the required dependencies:
```bash
go get github.com/thanhbvha/go-common/cache
```

## Basic Usage

### 1. Local In-Memory Cache (Ristretto)

```go
package main

import (
	"context"
	"fmt"
	"time"
	"github.com/thanhbvha/go-common/cache"
)

func main() {
	ctx := context.Background()

	// Initialize cache with a maximum capacity of 100MB
	memCache, err := cache.NewMemoryCache(100 * 1024 * 1024)
	if err != nil {
		panic(err)
	}

	// Set a value with a 5-minute TTL
	memCache.Set(ctx, "session:123", "user_data_here", 5*time.Minute)

	// Note: Ristretto handles admissions asynchronously. Allow a tiny sleep in tests.
	time.Sleep(10 * time.Millisecond)

	// Retrieve the value
	val, err := memCache.Get(ctx, "session:123")
	if err == cache.ErrNotFound {
		fmt.Println("Cache miss")
	} else {
		fmt.Println("Cache hit:", val)
	}
}
```

### 2. Distributed Lock

Used to ensure only one worker processes a specific task in a distributed environment (e.g., cron jobs, payment processing).

```go
package main

import (
	"context"
	"fmt"
	"time"
	"github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/cache"
)

func processPayment(ctx context.Context, redisClient redis.UniversalClient, orderID string) {
	lockKey := "lock:payment:" + orderID
	
	// Initialize a new lock instance for this operation
	lock := cache.NewRedisLock(redisClient)

	// Attempt to acquire the lock for 10 seconds
	acquired, err := lock.Acquire(ctx, lockKey, 10*time.Second)
	if err != nil || !acquired {
		fmt.Println("Payment is already being processed by another node.")
		return
	}

	// ALWAYS ensure the lock is released when the function finishes
	defer lock.Release(ctx, lockKey)

	// -----------------------------
	// DO CRITICAL WORK HERE
	// -----------------------------
	fmt.Println("Processing payment exclusively for order:", orderID)

	// If the work takes longer than expected, you can extend the lock:
	// lock.Extend(ctx, lockKey, 10*time.Second)
}
```
