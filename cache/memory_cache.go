package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
)

// MemoryCache implements the Cache interface using dgraph-io/ristretto.
type MemoryCache struct {
	cache *ristretto.Cache
}

// NewMemoryCache creates a new in-memory cache with bounded max cost (in bytes).
// Note: It is highly concurrent and uses TinyLFU for optimal cache admission.
func NewMemoryCache(maxCostBytes int64) (*MemoryCache, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     maxCostBytes,
		BufferItems: 64,      // number of keys per Get buffer.
		Metrics:     false,   // set to true for profiling (has performance overhead).
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init ristretto cache: %w", err)
	}

	return &MemoryCache{
		cache: cache,
	}, nil
}

// Get retrieves a string value from the cache.
func (m *MemoryCache) Get(ctx context.Context, key string) (string, error) {
	val, found := m.cache.Get(key)
	if !found {
		return "", ErrNotFound
	}

	strVal, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("cache value is not a string")
	}

	return strVal, nil
}

// Set stores a string value in the cache. 
// Ristretto requires a 'cost' for each item. We use the length of the string as the cost.
func (m *MemoryCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	cost := int64(len(value))
	var success bool

	if ttl > 0 {
		success = m.cache.SetWithTTL(key, value, cost, ttl)
	} else {
		success = m.cache.Set(key, value, cost)
	}

	if !success {
		return fmt.Errorf("failed to set key %s in cache (dropped by policy)", key)
	}

	return nil
}

// Delete removes a key from the cache.
func (m *MemoryCache) Delete(ctx context.Context, key string) error {
	m.cache.Del(key)
	return nil
}

// Clear completely flushes the cache.
func (m *MemoryCache) Clear(ctx context.Context) error {
	m.cache.Clear()
	return nil
}
