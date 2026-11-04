package messaging

import (
	"context"
	"time"
)

type Stats struct {
	Pending, InFlight, Dead int64
	OldestPendingAge        time.Duration
}

// Observation is safe operational metadata; it never includes payloads, IDs,
// tenant names or driver errors. Observers must be fast and concurrency-safe.
type Observation struct {
	Operation string
	Outcome   string
	Duration  time.Duration
	Attempt   int
	Stats     *Stats
}
type Observer interface {
	Observe(context.Context, Observation)
}

// Observe isolates instrumentation panics from transaction settlement.
func Observe(ctx context.Context, observer Observer, event Observation) {
	if observer == nil {
		return
	}
	defer func() { _ = recover() }()
	observer.Observe(ctx, event)
}
