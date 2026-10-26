package eventbus

import (
	"testing"
	"time"
)

func TestBug_C6_DelayUnder1Second(t *testing.T) {
	now := time.Now()
	clock := &mockClock{now: now}
	codec := NewCodec(clock)

	delay := 500 * time.Millisecond
	_, env, err := codec.EncodeTopology(TaskTopology, "test", "data", 3, WithDelay(delay))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	expectedRunAt := now.Add(delay).UnixMilli()
	if env.RunAt != expectedRunAt {
		t.Fatalf("Bug C6 NOT fixed! Expected run_at %d, got %d", expectedRunAt, env.RunAt)
	}

	t.Logf("Bug C6 fixed: Sub-second delay preserved. run_at=%d, expected=%d", env.RunAt, expectedRunAt)
}

func TestBug_C7_RetryPolicy(t *testing.T) {
	policy := ExponentialBackoff{
		Initial: 1 * time.Second,
		Max:     1 * time.Hour,
		Factor:  2.0,
		Jitter:  false, // disable jitter for deterministic test
	}

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{10, 512 * time.Second},
	}

	for _, tc := range tests {
		delay := policy.NextBackoff(tc.attempt)
		if delay != tc.expected {
			t.Fatalf("Bug C7 NOT fixed! Attempt %d: expected %v, got %v", tc.attempt, tc.expected, delay)
		}
	}
	t.Logf("Bug C7 fixed: Backoff is calculated correctly and consistently.")
}

type mockClock struct {
	now time.Time
}

func (m *mockClock) Now() time.Time {
	return m.now
}
