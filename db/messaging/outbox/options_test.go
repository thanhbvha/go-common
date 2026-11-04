package outbox

import (
	"testing"
	"time"
)

func TestRetryDelayBoundsAndOverflow(t *testing.T) {
	for attempt := 1; attempt <= 1000; attempt++ {
		delay := RetryDelay(attempt, time.Second, 5*time.Minute)
		if delay < time.Second/2 || delay > 5*time.Minute {
			t.Fatal(delay)
		}
	}
	if d := RetryDelay(1000, 1<<61, 1<<62); d < 1<<61 || d > 1<<62 {
		t.Fatal("overflow", d)
	}
}
