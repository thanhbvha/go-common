package eventbus

import (
	"context"
	"sync"

	"golang.org/x/time/rate"
)

type processingPolicy struct {
	rate *rate.Limiter
	keys *keyedConcurrency
}

func newProcessingPolicy(topo Topology) *processingPolicy {
	policy := &processingPolicy{}
	if configured, ok := topo.Options["eventbus.rate_limit"].(RateLimit); ok {
		policy.rate = rate.NewLimiter(rate.Limit(configured.PerSecond), configured.Burst)
	}
	if limit, ok := topo.Options["eventbus.ordering_key_concurrency"].(int); ok {
		policy.keys = &keyedConcurrency{limit: limit, entries: make(map[string]*keyEntry)}
	}
	return policy
}

func (p *processingPolicy) wait(ctx context.Context, key string) (func(), error) {
	if p == nil {
		return func() {}, nil
	}
	release := func() {}
	if p.keys != nil && key != "" {
		var err error
		release, err = p.keys.acquire(ctx, key)
		if err != nil {
			return nil, err
		}
	}
	if p.rate != nil {
		if err := p.rate.Wait(ctx); err != nil {
			release()
			return nil, err
		}
	}
	return release, nil
}

type keyedConcurrency struct {
	mu      sync.Mutex
	limit   int
	entries map[string]*keyEntry
}

type keyEntry struct {
	semaphore chan struct{}
	users     int
}

func (k *keyedConcurrency) acquire(ctx context.Context, key string) (func(), error) {
	k.mu.Lock()
	entry := k.entries[key]
	if entry == nil {
		entry = &keyEntry{semaphore: make(chan struct{}, k.limit)}
		k.entries[key] = entry
	}
	entry.users++
	k.mu.Unlock()
	select {
	case entry.semaphore <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { k.release(key, entry, true) }) }, nil
	case <-ctx.Done():
		k.release(key, entry, false)
		return nil, ctx.Err()
	}
}

func (k *keyedConcurrency) release(key string, entry *keyEntry, acquired bool) {
	if acquired {
		<-entry.semaphore
	}
	k.mu.Lock()
	entry.users--
	if entry.users == 0 {
		delete(k.entries, key)
	}
	k.mu.Unlock()
}

func (e *engine) policyFor(key consumerKey) *processingPolicy {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.policies[key]
}
