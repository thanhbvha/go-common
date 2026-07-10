package graphql

import (
	"context"
	"sync"
	"time"
)

// BatchFunc is a function that the developer must implement.
// It receives a list of IDs, queries the DB once, and returns a map[ID]Data.
type BatchFunc[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// ConfigDL configures a DataLoader
type ConfigDL struct {
	Wait     time.Duration // Maximum wait time to batch requests (Default: 16ms)
	MaxBatch int           // Maximum number of IDs in a single DB query (Default: 100)
}

// DataLoader prevents N+1 issues by batching individual data load requests into a single aggregate request.
type DataLoader[K comparable, V any] struct {
	batchFn  BatchFunc[K, V]
	wait     time.Duration
	maxBatch int

	mu    sync.Mutex
	cache map[K]V
	batch *batch[K, V]
}

type batch[K comparable, V any] struct {
	keys    []K
	done    chan struct{}
	results map[K]V
	err     error
}

// NewDataLoader initializes a new DataLoader
func NewDataLoader[K comparable, V any](batchFn BatchFunc[K, V], opts ...ConfigDL) *DataLoader[K, V] {
	wait := 16 * time.Millisecond
	maxBatch := 100
	if len(opts) > 0 {
		if opts[0].Wait > 0 {
			wait = opts[0].Wait
		}
		if opts[0].MaxBatch > 0 {
			maxBatch = opts[0].MaxBatch
		}
	}
	return &DataLoader[K, V]{
		batchFn:  batchFn,
		wait:     wait,
		maxBatch: maxBatch,
		cache:    make(map[K]V),
	}
}

// Load loads data for 1 key. If the key is already in the cache, it returns immediately.
// Otherwise, it waits a few milliseconds to batch with other Load() calls.
func (l *DataLoader[K, V]) Load(ctx context.Context, key K) (V, error) {
	l.mu.Lock()

	// 1. Check L1 Cache (lives only in the current request)
	if val, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return val, nil
	}

	// 2. Create or join an accumulating Batch
	var b *batch[K, V]
	if l.batch == nil || len(l.batch.keys) >= l.maxBatch {
		b = &batch[K, V]{
			done:    make(chan struct{}),
			results: make(map[K]V),
		}
		l.batch = b
		go l.dispatchBatch(ctx, b)
	} else {
		b = l.batch
	}

	b.keys = append(b.keys, key)
	l.mu.Unlock()

	// 3. Block this goroutine to wait for the Batch to finish querying the DB
	<-b.done

	if b.err != nil {
		var zero V
		return zero, b.err
	}

	val, ok := b.results[key]
	if !ok {
		// Key does not exist in the DB, return Zero value and nil error
		var zero V
		return zero, nil
	}

	return val, nil
}

// LoadAll supports loading multiple keys at once
func (l *DataLoader[K, V]) LoadAll(ctx context.Context, keys []K) ([]V, []error) {
	results := make([]V, len(keys))
	errors := make([]error, len(keys))
	
	var wg sync.WaitGroup
	wg.Add(len(keys))
	
	for i, key := range keys {
		go func(i int, key K) {
			defer wg.Done()
			results[i], errors[i] = l.Load(ctx, key)
		}(i, key)
	}
	
	wg.Wait()
	return results, errors
}

func (l *DataLoader[K, V]) dispatchBatch(ctx context.Context, b *batch[K, V]) {
	// Wait an additional period to accumulate more IDs
	time.Sleep(l.wait)

	l.mu.Lock()
	if l.batch == b {
		l.batch = nil // Reset batch so subsequent Loads create a new batch
	}
	l.mu.Unlock()

	// Filter duplicate IDs to avoid redundant queries
	keys := make([]K, 0, len(b.keys))
	seen := make(map[K]bool)
	for _, k := range b.keys {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}

	// Execute Batch Function provided by Dev (Call DB/Redis)
	res, err := l.batchFn(ctx, keys)
	b.results = res
	b.err = err

	// Put results into Cache
	if err == nil && res != nil {
		l.mu.Lock()
		for k, v := range res {
			l.cache[k] = v
		}
		l.mu.Unlock()
	}

	// Wake up all goroutines waiting on <-b.done
	close(b.done)
}
