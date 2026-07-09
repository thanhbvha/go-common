package cache

import (
	"context"
	"testing"
	"time"
)

func TestRedisLock_AcquireRelease(t *testing.T) {
	client := setupRedisTestClient(t) // Reuse setup from redis_cache_test.go
	defer client.Close()

	ctx := context.Background()
	lockKey := "test_lock_key"

	lock1 := NewRedisLock(client)
	lock2 := NewRedisLock(client)

	// Clean up before test
	client.Del(ctx, lockKey)

	// 1. Lock1 acquires successfully
	success, err := lock1.Acquire(ctx, lockKey, 5*time.Second)
	if err != nil || !success {
		t.Fatalf("Lock1 failed to acquire: %v", err)
	}

	// 2. Lock2 attempts to acquire and should fail
	success, err = lock2.Acquire(ctx, lockKey, 5*time.Second)
	if err != nil {
		t.Fatalf("Lock2 error during acquire: %v", err)
	}
	if success {
		t.Fatal("Lock2 unexpectedly acquired the lock")
	}

	// 3. Lock2 attempts to release Lock1's lock and should fail
	err = lock2.Release(ctx, lockKey)
	if err == nil {
		t.Fatal("Lock2 unexpectedly released Lock1's lock")
	}

	// 4. Lock1 releases its own lock successfully
	err = lock1.Release(ctx, lockKey)
	if err != nil {
		t.Fatalf("Lock1 failed to release: %v", err)
	}

	// 5. Lock2 can now acquire the lock
	success, err = lock2.Acquire(ctx, lockKey, 5*time.Second)
	if err != nil || !success {
		t.Fatalf("Lock2 failed to acquire after Lock1 released: %v", err)
	}
	lock2.Release(ctx, lockKey)
}

func TestRedisLock_Extend(t *testing.T) {
	client := setupRedisTestClient(t)
	defer client.Close()

	ctx := context.Background()
	lockKey := "test_lock_extend_key"

	lock := NewRedisLock(client)
	client.Del(ctx, lockKey)

	success, _ := lock.Acquire(ctx, lockKey, 2*time.Second)
	if !success {
		t.Fatal("Failed to acquire lock")
	}

	// Extend the TTL
	success, err := lock.Extend(ctx, lockKey, 10*time.Second)
	if err != nil || !success {
		t.Fatalf("Failed to extend lock: %v", err)
	}

	// Verify new TTL
	ttl, _ := client.TTL(ctx, lockKey).Result()
	if ttl < 5*time.Second {
		t.Errorf("Expected extended TTL, got %v", ttl)
	}

	lock.Release(ctx, lockKey)
}
