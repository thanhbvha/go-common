package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProcessingPolicyRateLimitHonorsBurstAndCancellation(t *testing.T) {
	policy := newProcessingPolicy(Topology{Options: map[string]any{
		"eventbus.rate_limit": RateLimit{PerSecond: 1, Burst: 2},
	}})

	for i := 0; i < 2; i++ {
		release, err := policy.wait(context.Background(), "")
		if err != nil {
			t.Fatalf("burst token %d: %v", i+1, err)
		}
		release()
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := policy.wait(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("exhausted limiter error = %v, want context.Canceled", err)
	}
}

func TestKeyedConcurrencySerializesOnlyMatchingKeysAndReclaimsEntries(t *testing.T) {
	keys := &keyedConcurrency{limit: 1, entries: make(map[string]*keyEntry)}
	releaseA, err := keys.acquire(context.Background(), "a")
	if err != nil {
		t.Fatalf("first key a acquire: %v", err)
	}

	acquiredA := make(chan func(), 1)
	go func() {
		release, acquireErr := keys.acquire(context.Background(), "a")
		if acquireErr == nil {
			acquiredA <- release
		}
	}()
	select {
	case <-acquiredA:
		t.Fatal("second matching key acquired before the first released")
	case <-time.After(25 * time.Millisecond):
	}

	releaseB, err := keys.acquire(context.Background(), "b")
	if err != nil {
		t.Fatalf("independent key b acquire: %v", err)
	}
	releaseB()
	releaseA()

	var releaseSecond func()
	select {
	case releaseSecond = <-acquiredA:
	case <-time.After(time.Second):
		t.Fatal("matching key did not acquire after release")
	}
	releaseSecond()

	keys.mu.Lock()
	defer keys.mu.Unlock()
	if len(keys.entries) != 0 {
		t.Fatalf("retained key entries = %d, want 0", len(keys.entries))
	}
}

func TestKeyedConcurrencyCanceledWaiterIsReclaimed(t *testing.T) {
	keys := &keyedConcurrency{limit: 1, entries: make(map[string]*keyEntry)}
	release, err := keys.acquire(context.Background(), "a")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := keys.acquire(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquire error = %v, want context.Canceled", err)
	}
	release()

	keys.mu.Lock()
	defer keys.mu.Unlock()
	if len(keys.entries) != 0 {
		t.Fatalf("retained key entries = %d, want 0", len(keys.entries))
	}
}
