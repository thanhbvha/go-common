package outbox

import (
	"math/rand/v2"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
)

type Config struct {
	Concurrency       int
	PollInterval      time.Duration
	PublishTimeout    time.Duration
	SettlementTimeout time.Duration
	Lease             time.Duration
	MaxAttempts       int
	RetryMin          time.Duration
	RetryMax          time.Duration
	Observer          messaging.Observer
}

func DefaultConfig() Config {
	return Config{Concurrency: 4, PollInterval: time.Second, PublishTimeout: 10 * time.Second,
		SettlementTimeout: 5 * time.Second, Lease: 30 * time.Second, MaxAttempts: 20,
		RetryMin: time.Second, RetryMax: 5 * time.Minute}
}
func (c Config) Validate() error {
	if c.Concurrency < 1 || c.Concurrency > 1000 || c.MaxAttempts < 1 || c.MaxAttempts > 1000000 ||
		c.PollInterval <= 0 || c.PublishTimeout <= 0 || c.SettlementTimeout <= 0 ||
		c.Lease <= 0 || c.Lease > 24*time.Hour || c.PublishTimeout >= c.Lease ||
		c.SettlementTimeout >= c.Lease-c.PublishTimeout || c.RetryMin <= 0 ||
		c.RetryMax < c.RetryMin || c.RetryMax > 24*time.Hour {
		return messaging.ErrInvalidConfig
	}
	return nil
}

// RetryDelay uses equal jitter and caps without overflowing time.Duration.
func RetryDelay(attempt int, minimum, maximum time.Duration) time.Duration {
	if minimum <= 0 || maximum < minimum {
		return 0
	}
	d := minimum
	for i := 1; i < attempt && d < maximum; i++ {
		if d > maximum/2 {
			d = maximum
			break
		}
		d *= 2
	}
	if d > maximum {
		d = maximum
	}
	return d/2 + time.Duration(rand.Int64N(int64(d-d/2)+1))
}
