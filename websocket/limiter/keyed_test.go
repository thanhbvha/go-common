package limiter

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestKeyedRateLimiterIsolatesKeys(t *testing.T) {
	limiter := NewKeyedRateLimiter(0.001, 2, 0, 0)
	if !limiter.Allow("attacker") || !limiter.Allow("attacker") {
		t.Fatal("burst not honoured")
	}
	if limiter.Allow("attacker") {
		t.Fatal("attacker exceeded its burst")
	}
	if !limiter.Allow("honest") {
		t.Fatal("honest key starved by another key's exhaustion")
	}
}

func TestKeyedRateLimiterExpiresIdleKeys(t *testing.T) {
	clock := time.Now()
	limiter := NewKeyedRateLimiter(1, 1, 0, time.Minute)
	limiter.now = func() time.Time { return clock }
	for i := 0; i < 50; i++ {
		limiter.Allow("ip-" + strconv.Itoa(i))
	}
	if limiter.Len() != 50 {
		t.Fatalf("len = %d", limiter.Len())
	}
	clock = clock.Add(2 * time.Minute)
	limiter.Allow("fresh")
	if limiter.Len() != 1 {
		t.Fatalf("idle keys not swept: len = %d", limiter.Len())
	}
}

func TestKeyedRateLimiterBoundsMemoryWithSharedOverflow(t *testing.T) {
	limiter := NewKeyedRateLimiter(0.001, 3, 10, time.Hour)
	for i := 0; i < 10; i++ {
		limiter.Allow("known-" + strconv.Itoa(i))
	}
	allowed := 0
	for i := 0; i < 1000; i++ {
		if limiter.Allow("spray-" + strconv.Itoa(i)) {
			allowed++
		}
	}
	if limiter.Len() != 10 {
		t.Fatalf("map grew past maxKeys: %d", limiter.Len())
	}
	if allowed > 3 {
		t.Fatalf("overflow bucket let %d unknown keys through, want <= burst 3", allowed)
	}
	if !limiter.Allow("known-0") && limiter.Allow("known-1") {
		t.Fatal("tracked keys must keep their own buckets")
	}
}

func TestKeyedRateLimiterConcurrent(t *testing.T) {
	limiter := NewKeyedRateLimiter(1000, 1000, 64, time.Minute)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				limiter.Allow("k" + strconv.Itoa((g+i)%100))
			}
		}(g)
	}
	wg.Wait()
	if limiter.Len() > 64 {
		t.Fatalf("len = %d", limiter.Len())
	}
}
