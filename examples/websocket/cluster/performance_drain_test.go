//go:build integration

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/eventbus/transport"
)

func TestPerformanceDrainRequiresObservedLagAndSettlement(t *testing.T) {
	good := transport.TopologyStats{LagKnown: true}
	for _, scenario := range []string{"unknown", "missing", "backend-error", "dlq", "in-flight", "ready", "drained", "save-error"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			calls := 0
			snapshot := func(context.Context) ([]transport.TopologyStats, error) {
				calls++
				a, b := good, good
				switch scenario {
				case "unknown":
					a.LagKnown = false
				case "missing":
					return nil, nil
				case "backend-error":
					return nil, errors.New("stats unavailable")
				case "dlq":
					b.DLQ = 1
				case "in-flight":
					a.InFlight = 1
					a.Lag = 1
				case "ready":
					a.Pending = 1
					a.Lag = 1
				}
				return []transport.TopologyStats{a, b}, nil
			}
			saved := false
			_, err := waitPerformanceDrain(ctx, snapshot, func([]transport.TopologyStats) error {
				saved = true
				if scenario == "save-error" {
					return errors.New("save failed")
				}
				return nil
			})
			if scenario == "drained" {
				if err != nil || !saved {
					t.Fatalf("drained err=%v saved=%v", err, saved)
				}
			} else if err == nil {
				t.Fatal("invalid drain accepted")
			}
			if (scenario == "in-flight" || scenario == "ready") && calls < 2 {
				t.Fatal("did not wait for settlement")
			}
		})
	}
}
