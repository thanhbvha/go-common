package eventbus_test

import (
	"context"
	bus "github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/driver/memory"
	bridge "github.com/thanhbvha/go-common/websocket/integration/eventbus"
	"github.com/thanhbvha/go-common/websocket/routing"
	"sync/atomic"
	"testing"
	"time"
)

type staticRegistry struct {
	routing.SessionRegistry
	location routing.Location
}

func (r staticRegistry) Resolve(context.Context, string, string, string) ([]routing.Location, error) {
	return []routing.Location{r.location}, nil
}

type resultTransport struct {
	routing.DeliveryTransport
	attempts  atomic.Int64
	failUntil int64
}

func (t *resultTransport) Deliver(_ context.Context, _ string, c routing.DeliveryCommand) (routing.DeliveryResult, error) {
	n := t.attempts.Add(1)
	status := routing.Enqueued
	if n <= t.failUntil {
		status = routing.QueueFull
	}
	return routing.DeliveryResult{Outcomes: []routing.Outcome{{Location: c.Sessions[0], Status: status}}}, nil
}
func TestBridgeRetriesAndDLQ(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail int64
		dlq  bool
	}{{"retry-success", 1, false}, {"exhausted", 100, true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			transport := &resultTransport{failUntil: tc.fail}
			router, _ := routing.NewRouter(routing.RouterConfig{Registry: staticRegistry{location: routing.Location{TenantID: "t", UserID: "u", SessionID: "s", NodeID: "n", Generation: "g"}}, Transport: transport})
			cfg := bus.DefaultConfig()
			cfg.MaxAttempts = 2
			cfg.RetryPolicy = bus.ConstantBackoff{Delay: time.Millisecond}
			b, err := bus.Open(cfg, bus.WithTransport(memory.New()))
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close(context.Background())
			integration, err := bridge.New(bridge.Config{Bus: b, Router: router})
			if err != nil {
				t.Fatal(err)
			}
			if err = b.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err = integration.PublishNotification(ctx, bridge.Notification{MessageID: "retry", TenantID: "t", UserID: "u", Payload: []byte("message")}); err != nil {
				t.Fatal(err)
			}
			for {
				stats, err := b.SubscriberStats(ctx, "ws.notification", "realtime-router")
				if err != nil {
					t.Fatal(err)
				}
				if transport.attempts.Load() >= 2 && stats.InFlight == 0 && stats.Pending == 0 && stats.Delayed == 0 {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("settlement timeout: %+v", stats)
				case <-time.After(time.Millisecond):
				}
			}
			count, err := b.CountDeadLetters(ctx, bus.EventRef("ws.notification"))
			if err != nil {
				t.Fatal(err)
			}
			if (count > 0) != tc.dlq {
				t.Fatalf("DLQ=%d expected=%v", count, tc.dlq)
			}
			if transport.attempts.Load() != 2 {
				t.Fatalf("attempts=%d", transport.attempts.Load())
			}
		})
	}
}
