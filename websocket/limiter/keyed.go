package limiter

import (
	"sync"
	"time"
)

// KeyedRateLimiter applies an independent token bucket to each key (typically a
// client IP), so one abusive source cannot exhaust a shared budget. Memory is
// bounded: idle buckets expire, and once maxKeys live buckets exist further new
// keys share a single overflow bucket instead of growing the map.
type KeyedRateLimiter struct {
	rate     float64
	burst    int64
	maxKeys  int
	idle     time.Duration
	mu       sync.Mutex
	buckets  map[string]*keyedBucket
	overflow *RateLimiter
	sweepAt  time.Time
	now      func() time.Time
}

type keyedBucket struct {
	limiter *RateLimiter
	seen    time.Time
}

// NewKeyedRateLimiter allows rate events per second with the given burst for
// every key. maxKeys <= 0 selects 100000; idle <= 0 selects 5 minutes.
func NewKeyedRateLimiter(rate float64, burst int64, maxKeys int, idle time.Duration) *KeyedRateLimiter {
	if maxKeys <= 0 {
		maxKeys = 100000
	}
	if idle <= 0 {
		idle = 5 * time.Minute
	}
	return &KeyedRateLimiter{
		rate: rate, burst: burst, maxKeys: maxKeys, idle: idle,
		buckets:  make(map[string]*keyedBucket),
		overflow: NewRateLimiter(rate, burst),
		now:      time.Now,
	}
}

// Allow reports whether key may perform one more operation.
func (k *KeyedRateLimiter) Allow(key string) bool {
	now := k.now()
	k.mu.Lock()
	if now.After(k.sweepAt) {
		k.sweepLocked(now)
	}
	bucket := k.buckets[key]
	if bucket == nil {
		if len(k.buckets) >= k.maxKeys {
			k.mu.Unlock()
			return k.overflow.Allow()
		}
		bucket = &keyedBucket{limiter: NewRateLimiter(k.rate, k.burst)}
		k.buckets[key] = bucket
	}
	bucket.seen = now
	k.mu.Unlock()
	return bucket.limiter.Allow()
}

func (k *KeyedRateLimiter) sweepLocked(now time.Time) {
	for key, bucket := range k.buckets {
		if now.Sub(bucket.seen) > k.idle {
			delete(k.buckets, key)
		}
	}
	k.sweepAt = now.Add(k.idle / 4)
}

// Len reports the number of tracked keys.
func (k *KeyedRateLimiter) Len() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.buckets)
}
