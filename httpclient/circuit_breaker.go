package httpclient

import (
	"github.com/sony/gobreaker/v2"
)

// setupCircuitBreaker configures and returns a gobreaker instance.
func setupCircuitBreaker(cfg CBConfig) *gobreaker.CircuitBreaker[[]byte] {
	if !cfg.Enabled {
		return nil
	}

	st := gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Do not trip if we haven't reached the minimum request threshold
			if counts.Requests < cfg.ReadyToTripMinRequests {
				return false
			}

			// Calculate failure ratio
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return failureRatio >= cfg.ReadyToTripFailRatio
		},
		OnStateChange: cfg.OnStateChange,
	}

	return gobreaker.NewCircuitBreaker[[]byte](st)
}
