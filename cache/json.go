// Package cache provides JSON-aware helper functions built on top of the Cache interface.
// These helpers automatically serialize/deserialize values using encoding/json,
// allowing any JSON-serializable type to be stored without manual marshaling.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// GetJSON retrieves a value from the cache and deserializes it from JSON into dest.
// dest must be a pointer. Returns ErrNotFound if the key does not exist.
//
// Example:
//
//	var user UserProfile
//	err := cache.GetJSON(ctx, myCache, "user:123", &user)
//	if errors.Is(err, cache.ErrNotFound) { ... }
func GetJSON[T any](ctx context.Context, c Cache, key string, dest *T) error {
	raw, err := c.Get(ctx, key)
	if err != nil {
		return err // includes ErrNotFound
	}
	if err := json.Unmarshal([]byte(raw), dest); err != nil {
		return fmt.Errorf("cache: failed to unmarshal value for key %q: %w", key, err)
	}
	return nil
}

// SetJSON serializes value to JSON and stores it in the cache with the given TTL.
// A TTL of 0 means the key does not expire.
//
// Example:
//
//	err := cache.SetJSON(ctx, myCache, "user:123", user, 5*time.Minute)
func SetJSON[T any](ctx context.Context, c Cache, key string, value T, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("cache: failed to marshal value for key %q: %w", key, err)
	}
	return c.Set(ctx, key, string(data), ttl)
}

// GetOrSetJSON retrieves a value from cache; on miss, calls fetch() to compute
// the value, stores it, and returns it. Useful for the cache-aside pattern.
//
// Example:
//
//	user, err := cache.GetOrSetJSON(ctx, myCache, "user:123", 5*time.Minute, func() (*User, error) {
//	    return userRepo.FindByID(ctx, 123)
//	})
func GetOrSetJSON[T any](ctx context.Context, c Cache, key string, ttl time.Duration, fetch func() (T, error)) (T, error) {
	var dest T
	err := GetJSON(ctx, c, key, &dest)
	if err == nil {
		return dest, nil // cache hit
	}
	if err != ErrNotFound {
		return dest, err // unexpected error
	}

	// Cache miss — fetch from source
	value, err := fetch()
	if err != nil {
		return dest, err
	}

	// Store in cache (best-effort, don't fail on cache write error)
	_ = SetJSON(ctx, c, key, value, ttl)

	return value, nil
}
