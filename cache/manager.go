// Package cache provides a multi-level caching system and distributed lock implementation.
//
// It defines a unified Cache interface that can be implemented by both local memory
// engines (like Ristretto) and remote distributed engines (like Redis). It also
// provides a DistributedLock interface for concurrency control across nodes.
//
// Basic usage:
//
//	localCache, _ := cache.NewMemoryCache(100 * 1024 * 1024) // 100MB
//	localCache.Set(ctx, "key", "value", 5*time.Minute)
//	val, err := localCache.Get(ctx, "key")
package cache

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound is returned when a requested key does not exist in the cache.
	ErrNotFound = errors.New("cache: key not found")
)

// Cache defines the standard behavior for a caching engine (either local or remote).
type Cache interface {
	// Get retrieves a value from the cache. Returns an error (e.g., ErrNotFound) if not found.
	Get(ctx context.Context, key string) (string, error)

	// Set stores a value in the cache with an optional Time-To-Live (TTL).
	// A TTL of 0 means the key does not expire.
	Set(ctx context.Context, key string, value string, ttl time.Duration) error

	// Delete removes a key from the cache.
	Delete(ctx context.Context, key string) error

	// Clear completely flushes the cache.
	Clear(ctx context.Context) error
}

// DistributedLock defines the behavior for a cluster-wide mutual exclusion lock.
type DistributedLock interface {
	// Acquire attempts to acquire the lock with a given TTL.
	// Returns true if successfully acquired, false otherwise.
	Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// Release releases the lock.
	Release(ctx context.Context, key string) error

	// Extend attempts to extend the TTL of a lock currently held by the caller.
	Extend(ctx context.Context, key string, ttl time.Duration) (bool, error)
}
