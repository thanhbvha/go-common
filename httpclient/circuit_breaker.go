package httpclient

import (
	"context"
	"errors"
	"github.com/sony/gobreaker/v2"
)

func setupCircuitBreaker(cfg CBConfig) *gobreaker.TwoStepCircuitBreaker[any] {
	if !cfg.Enabled {
		return nil
	}
	return gobreaker.NewTwoStepCircuitBreaker[any](gobreaker.Settings{
		Name: cfg.Name, MaxRequests: cfg.MaxRequests, Interval: cfg.Interval, Timeout: cfg.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.Requests >= cfg.ReadyToTripMinRequests && counts.Requests > 0 && float64(counts.TotalFailures)/float64(counts.Requests) >= cfg.ReadyToTripFailRatio
		},
		IsExcluded:    func(err error) bool { return errors.Is(err, context.Canceled) },
		OnStateChange: cfg.OnStateChange,
	})
}
