package eventbus

import (
	"math/rand/v2"
	"time"
)

// RetryPolicy defines how to calculate the backoff duration for the next attempt.
type RetryPolicy interface {
	// NextBackoff returns the duration to wait before the next attempt.
	// `attempt` is 1-indexed (1 means it's the first retry after initial failure).
	NextBackoff(attempt int) time.Duration
}

// ConstantBackoff retries with a fixed delay.
type ConstantBackoff struct {
	Delay time.Duration
}

func (c ConstantBackoff) NextBackoff(attempt int) time.Duration {
	return c.Delay
}

// ExponentialBackoff retries with exponentially increasing delays.
type ExponentialBackoff struct {
	Initial time.Duration
	Max     time.Duration
	Factor  float64
	Jitter  bool
}

// DefaultExponentialBackoff returns a standard exponential backoff.
func DefaultExponentialBackoff() ExponentialBackoff {
	return ExponentialBackoff{
		Initial: 1 * time.Second,
		Max:     1 * time.Hour,
		Factor:  2.0,
		Jitter:  true,
	}
}

func (e ExponentialBackoff) NextBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	delay := float64(e.Initial)
	for i := 1; i < attempt; i++ {
		delay *= e.Factor
	}

	if delay > float64(e.Max) {
		delay = float64(e.Max)
	}

	res := time.Duration(delay)
	if e.Jitter {
		// apply up to 20% jitter
		jitterMax := float64(res) * 0.2
		jitter := rand.Float64() * jitterMax
		res += time.Duration(jitter)
	}
	
	if res > e.Max {
		res = e.Max
	}
	
	return res
}
