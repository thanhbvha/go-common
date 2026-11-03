package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
)

// MemoryCache implements the Cache interface using dgraph-io/ristretto.
type MemoryCache struct {
	cache  *ristretto.Cache
	mu     sync.RWMutex
	closed bool
}

// NewMemoryCache creates a new in-memory cache with bounded max cost (in bytes).
// Note: It is highly concurrent and uses TinyLFU for optimal cache admission.
// MemoryOptions controls Ristretto metadata. MaxCost accounts values and entry
// overhead; process RSS also includes the admission sketch, queues and runtime.
type MemoryOptions struct {
	NumCounters int64
	BufferItems int64
	Metrics     bool
}

func NewMemoryCache(maxCostBytes int64, options ...MemoryOptions) (*MemoryCache, error) {
	if maxCostBytes <= 0 || len(options) > 1 {
		return nil, fmt.Errorf("cache: positive capacity and at most one options value required")
	}
	opts := MemoryOptions{NumCounters: min(max(min(maxCostBytes/64, 100000)*10, 1000), 1000000), BufferItems: 64}
	if len(options) == 1 {
		opts = options[0]
		if opts.NumCounters == 0 {
			opts.NumCounters = min(max(min(maxCostBytes/64, 100000)*10, 1000), 1000000)
		}
		if opts.BufferItems == 0 {
			opts.BufferItems = 64
		}
	}
	if opts.NumCounters < 1 || opts.BufferItems < 1 {
		return nil, fmt.Errorf("cache: counters and buffer items must be positive")
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: opts.NumCounters, // frequency sketch size, independent of value cost.
		MaxCost:     maxCostBytes,
		BufferItems: opts.BufferItems, // number of keys per Get buffer.
		Metrics:     opts.Metrics,     // set to true for profiling (has performance overhead).
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return "", ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl < 0 {
		return fmt.Errorf("cache: TTL must not be negative")
	}
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.cache.Del(key)
	return nil
}

// Clear completely flushes the cache.
func (m *MemoryCache) Clear(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.cache.Clear()
	return nil
}

// Close releases resources held by the Ristretto cache, stopping its background goroutines.
func (m *MemoryCache) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	m.cache.Close()
	return nil
}

// Metrics returns Ristretto's concurrent statistics, or nil when disabled.
// Enable via MemoryOptions.Metrics and read the counters before Close.
func (m *MemoryCache) Metrics() *ristretto.Metrics { return m.cache.Metrics }
