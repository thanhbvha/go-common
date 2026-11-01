package graphql

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/vikstrous/dataloadgen"
)

// BatchFunc returns values by key. Missing entries become ErrNotFound.
type BatchFunc[K comparable, V any] func(context.Context, []K) (map[K]V, error)

var (
	ErrNotFound    = errors.New("graphql: key not found")
	ErrLoaderLimit = errors.New("graphql: loader key budget exceeded")
)

// LoaderConfig bounds work for one request. Zero uses defaults; negative is invalid.
type LoaderConfig struct {
	Wait                 time.Duration
	MaxBatch             int
	MaxKeys              int
	MaxConcurrentBatches int
}

func DefaultLoaderConfig() LoaderConfig {
	return LoaderConfig{Wait: time.Millisecond, MaxBatch: 100, MaxKeys: 1000, MaxConcurrentBatches: 4}
}

// DataLoader is request-scoped, including its memoized errors. Waiting for a
// result requires the backend to complete or honor the request context.
type DataLoader[K comparable, V any] struct {
	ctx      context.Context
	loader   *dataloadgen.Loader[K, V]
	mu       sync.Mutex
	cached   map[K]bool
	admitted int
	maxKeys  int
}

func NewDataLoader[K comparable, V any](ctx context.Context, fn BatchFunc[K, V], cfg LoaderConfig) (*DataLoader[K, V], error) {
	if ctx == nil || fn == nil {
		return nil, errors.New("graphql: request context and batch function are required")
	}
	if cfg.Wait < 0 || cfg.MaxBatch < 0 || cfg.MaxKeys < 0 || cfg.MaxConcurrentBatches < 0 {
		return nil, errors.New("graphql: loader limits must not be negative")
	}
	defaults := DefaultLoaderConfig()
	if cfg.Wait == 0 {
		cfg.Wait = defaults.Wait
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = defaults.MaxBatch
	}
	if cfg.MaxKeys == 0 {
		cfg.MaxKeys = defaults.MaxKeys
	}
	if cfg.MaxConcurrentBatches == 0 {
		cfg.MaxConcurrentBatches = defaults.MaxConcurrentBatches
	}
	slots := make(chan struct{}, cfg.MaxConcurrentBatches)
	l := &DataLoader[K, V]{ctx: ctx, cached: make(map[K]bool), maxKeys: cfg.MaxKeys}
	fetch := func(_ context.Context, keys []K) (values []V, errs []error) {
		// Catch before the dependency's recovery, which includes the panic value.
		defer func() {
			if p := recover(); p != nil {
				values = nil
				errs = []error{RecoverFunc(ctx, p)}
			}
		}()
		select {
		case <-ctx.Done():
			return nil, []error{ctx.Err()}
		case slots <- struct{}{}:
		}
		defer func() { <-slots }()
		if err := ctx.Err(); err != nil {
			return nil, []error{err}
		}
		res, err := fn(ctx, keys)
		values = make([]V, len(keys))
		errs = make([]error, len(keys))
		for i, k := range keys {
			if err != nil {
				errs[i] = err
				continue
			}
			var found bool
			values[i], found = res[k]
			if !found {
				errs[i] = ErrNotFound
			}
		}
		return values, errs
	}
	l.loader = dataloadgen.NewLoader(fetch, dataloadgen.WithWait(cfg.Wait), dataloadgen.WithBatchCapacity(cfg.MaxBatch))
	return l, nil
}

func (l *DataLoader[K, V]) enqueue(ctx context.Context, key K) func() (V, error) {
	failed := func(err error) func() (V, error) { return func() (V, error) { var zero V; return zero, err } }
	if err := ctx.Err(); err != nil {
		return failed(err)
	}
	if err := l.ctx.Err(); err != nil {
		return failed(err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.cached[key] {
		if l.admitted >= l.maxKeys {
			return failed(ErrLoaderLimit)
		}
		l.admitted++
		l.cached[key] = true
	}
	return l.loader.LoadThunk(l.ctx, key)
}
func (l *DataLoader[K, V]) Load(ctx context.Context, key K) (V, error) {
	return l.enqueue(ctx, key)()
}

// LoadAll enqueues before waiting, without allocating a goroutine for each key.
// Results and errors always correspond to the input order, including duplicates.
func (l *DataLoader[K, V]) LoadAll(ctx context.Context, keys []K) ([]V, []error) {
	thunks := make([]func() (V, error), len(keys))
	for i, k := range keys {
		thunks[i] = l.enqueue(ctx, k)
	}
	values := make([]V, len(keys))
	errs := make([]error, len(keys))
	for i, thunk := range thunks {
		values[i], errs[i] = thunk()
	}
	return values, errs
}

// Clear permits an explicit retry. It does not reset the lifetime key budget.
func (l *DataLoader[K, V]) Clear(key K) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.cached, key)
	l.loader.Clear(key)
}
func (l *DataLoader[K, V]) ClearAll() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cached = make(map[K]bool)
	l.loader.ClearAll()
}

// Prime never replaces an existing value; false also indicates an exhausted budget.
func (l *DataLoader[K, V]) Prime(key K, value V) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ctx.Err() != nil || l.cached[key] || l.admitted >= l.maxKeys {
		return false
	}
	l.cached[key] = true
	l.admitted++
	return l.loader.Prime(key, value)
}
