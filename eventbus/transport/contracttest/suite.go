// Package contracttest provides reusable behavioral checks for EventBus
// transports. Driver packages supply only lifecycle and deterministic clock
// controls; the assertions remain transport-independent.
package contracttest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

// Harness adapts a transport to the shared contract suite.
type Harness struct {
	New           func(*testing.T) transport.Transport
	Topology      func(testName string, kind transport.TopologyKind) transport.Topology
	DispatchDue   func(context.Context, transport.Transport, transport.Topology, time.Time) (int64, error)
	ExpireDedupe  func(*testing.T, time.Duration)
	ReceiveWithin time.Duration
}

// Run executes the durable transport contract that is independent of core
// handler classification. Core tests separately cover panic, timeout, attempt,
// reconnect, and drain semantics once a transport honors this boundary.
func Run(t *testing.T, h Harness) {
	t.Helper()
	if h.New == nil || h.Topology == nil || h.DispatchDue == nil {
		t.Fatal("contracttest: incomplete harness")
	}
	if h.ReceiveWithin <= 0 {
		h.ReceiveWithin = 5 * time.Second
	}

	t.Run("publish_consume_ack", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		mustPublish(t, ctx, tr, topo, "ack")
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		delivery := active.Delivery
		if string(delivery.Message().Payload) != "ack" || delivery.DeliveryAttempt() < 1 {
			t.Fatalf("delivery = %#v, attempt %d", delivery.Message(), delivery.DeliveryAttempt())
		}
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		active.Close()
		assertStats(t, ctx, tr, topo, 0, 0, 0, 0)
	})

	t.Run("retry_transfer", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		mustPublish(t, ctx, tr, topo, "source")
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		delivery := active.Delivery
		retryAt := time.Now().Add(time.Minute)
		if err := delivery.Settle(ctx, transport.Decision{
			Kind: transport.DecisionRetry, Next: transport.Message{Payload: []byte("retry")}, RetryAt: retryAt,
		}); err != nil {
			t.Fatal(err)
		}
		active.Close()
		assertStats(t, ctx, tr, topo, 0, 1, 0, 0)
		if moved, err := h.DispatchDue(ctx, tr, topo, retryAt.Add(delayPrecision(tr))); err != nil || moved != 1 {
			t.Fatalf("dispatch due = %d, %v", moved, err)
		}
		active = receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		retried := active.Delivery
		if string(retried.Message().Payload) != "retry" {
			t.Fatalf("retry payload = %q", retried.Message().Payload)
		}
		if err := retried.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		active.Close()
	})

	t.Run("dead_letter_admin", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		mustPublish(t, ctx, tr, topo, "dead")
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		delivery := active.Delivery
		if err := delivery.Settle(ctx, transport.Decision{
			Kind: transport.DecisionDeadLetter, Reason: "contract", Cause: errors.New("synthetic"), ErrorCode: "synthetic", Attempts: 1,
		}); err != nil {
			t.Fatal(err)
		}
		active.Close()
		admin, ok := tr.(transport.DLQAdmin)
		if !ok {
			t.Fatal("DLQAdmin not advertised")
		}
		if count, err := admin.Count(ctx, topo); err != nil || count != 1 {
			t.Fatalf("DLQ count = %d, %v", count, err)
		}
		records, err := admin.List(ctx, topo, 0, 10)
		if err != nil || len(records) != 1 || records[0].ID == "" || records[0].Reason != "contract" || records[0].LastError != "synthetic" {
			t.Fatalf("DLQ records = %#v, %v", records, err)
		}
		if err := admin.Requeue(ctx, topo, []string{records[0].ID}); err != nil {
			t.Fatal(err)
		}
		if count, err := admin.Count(ctx, topo); err != nil || count != 0 {
			t.Fatalf("DLQ after requeue = %d, %v", count, err)
		}
		active = receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		requeued := active.Delivery
		if string(requeued.Message().Payload) != "dead" {
			t.Fatalf("requeued payload = %q", requeued.Message().Payload)
		}
		if err := requeued.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		active.Close()
	})

	t.Run("delayed_delivery", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		runAt := time.Now().Add(time.Minute)
		if err := tr.PublishAt(ctx, topo, transport.Message{Payload: []byte("later")}, runAt); err != nil {
			t.Fatal(err)
		}
		earlyBy := delayPrecision(tr)
		if moved, err := h.DispatchDue(ctx, tr, topo, runAt.Add(-earlyBy)); err != nil || moved != 0 {
			t.Fatalf("early dispatch = %d, %v", moved, err)
		}
		assertStats(t, ctx, tr, topo, 0, 1, 0, 0)
		if moved, err := h.DispatchDue(ctx, tr, topo, runAt.Add(earlyBy)); err != nil || moved != 1 {
			t.Fatalf("due dispatch = %d, %v", moved, err)
		}
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		delivery := active.Delivery
		if string(delivery.Message().Payload) != "later" {
			t.Fatalf("delayed payload = %q", delivery.Message().Payload)
		}
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		active.Close()
	})

	t.Run("event_fanout_groups", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.EventTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		if provisioner, ok := tr.(transport.SubscriptionProvisioner); ok {
			for _, group := range []string{"alpha", "beta"} {
				if err := provisioner.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: group}); err != nil {
					t.Fatal(err)
				}
			}
		}
		mustPublish(t, ctx, tr, topo, "event")
		for _, group := range []string{"alpha", "beta"} {
			active := receive(t, tr, transport.Subscription{Topology: topo, Group: group}, h.ReceiveWithin)
			delivery := active.Delivery
			if string(delivery.Message().Payload) != "event" {
				t.Fatalf("subscriber %s payload = %q", group, delivery.Message().Payload)
			}
			if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
				t.Fatal(err)
			}
			active.Close()
		}
		assertStats(t, ctx, tr, topo, 0, 0, 0, 0)
	})

	t.Run("dedupe_scope_expiry_release", func(t *testing.T) {
		tr := h.New(t)
		deduper, ok := tr.(transport.TopologyDeduper)
		if !ok {
			t.Skip("transport does not advertise deduplication")
		}
		if h.ExpireDedupe == nil {
			t.Fatal("contracttest: dedupe transport requires ExpireDedupe")
		}
		ctx := context.Background()
		first := h.Topology(t.Name()+"-first", transport.TaskTopology)
		second := h.Topology(t.Name()+"-second", transport.TaskTopology)
		mustEnsure(t, ctx, tr, first)
		mustEnsure(t, ctx, tr, second)
		const ttl = 50 * time.Millisecond
		for _, topo := range []transport.Topology{first, second} {
			if acquired, err := deduper.ReserveTopology(ctx, topo, "key", ttl); err != nil || !acquired {
				t.Fatalf("initial reserve = %v, %v", acquired, err)
			}
			if acquired, err := deduper.ReserveTopology(ctx, topo, "key", ttl); err != nil || acquired {
				t.Fatalf("duplicate reserve = %v, %v", acquired, err)
			}
		}
		if err := deduper.ReleaseTopology(ctx, first, "key"); err != nil {
			t.Fatal(err)
		}
		if acquired, err := deduper.ReserveTopology(ctx, first, "key", ttl); err != nil || !acquired {
			t.Fatalf("reserve after release = %v, %v", acquired, err)
		}
		h.ExpireDedupe(t, ttl)
		for _, topo := range []transport.Topology{first, second} {
			if acquired, err := deduper.ReserveTopology(ctx, topo, "key", ttl); err != nil || !acquired {
				t.Fatalf("reserve after expiry = %v, %v", acquired, err)
			}
		}
	})

	t.Run("malformed_payload_remains_settleable", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		mustPublish(t, ctx, tr, topo, "{not-json")
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		delivery := active.Delivery
		if string(delivery.Message().Payload) != "{not-json" {
			t.Fatal("transport altered malformed payload")
		}
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionDeadLetter, Reason: "decode"}); err != nil {
			t.Fatal(err)
		}
		active.Close()
		assertStats(t, ctx, tr, topo, 0, 0, 0, 1)
	})
}

func delayPrecision(tr transport.Transport) time.Duration {
	precision := tr.Capabilities().DelayPrecision
	if precision <= 0 {
		return time.Millisecond
	}
	return precision
}

func mustEnsure(t *testing.T, ctx context.Context, tr transport.Transport, topo transport.Topology) {
	t.Helper()
	if err := tr.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
}

func mustPublish(t *testing.T, ctx context.Context, tr transport.Transport, topo transport.Topology, payload string) {
	t.Helper()
	if err := tr.Publish(ctx, topo, transport.Message{ID: payload, Type: topo.JobType, Payload: []byte(payload)}); err != nil {
		t.Fatal(err)
	}
}

type activeDelivery struct {
	transport.Delivery
	close func()
}

func (d *activeDelivery) Close() { d.close() }

func receive(t *testing.T, tr transport.Transport, sub transport.Subscription, timeout time.Duration) *activeDelivery {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	deliveries := make(chan transport.Delivery, 1)
	done := make(chan error, 1)
	go func() {
		done <- tr.Consume(ctx, sub, func(delivery transport.Delivery) {
			select {
			case deliveries <- delivery:
			default:
			}
		})
	}()
	var delivery transport.Delivery
	select {
	case delivery = <-deliveries:
	case err := <-done:
		cancel()
		t.Fatalf("consume ended before delivery: %v", err)
	case <-time.After(timeout):
		cancel()
		t.Fatal("timed out waiting for delivery")
	}
	var once sync.Once
	active := &activeDelivery{Delivery: delivery}
	active.close = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("consume shutdown: %v", err)
				}
			case <-time.After(timeout):
				t.Error("consume did not stop after cancellation")
			}
		})
	}
	t.Cleanup(active.Close)
	return active
}

func assertStats(t *testing.T, ctx context.Context, tr transport.Transport, topo transport.Topology, pending, delayed, inFlight, dlq int64) {
	t.Helper()
	reader, ok := tr.(transport.StatsReader)
	if !ok {
		t.Fatal("StatsReader not advertised")
	}
	stats, err := reader.Stats(ctx, topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != pending || stats.Delayed != delayed || stats.InFlight != inFlight || stats.DLQ != dlq || stats.Lag != pending+inFlight || !stats.LagKnown {
		t.Fatalf("stats = %s", fmt.Sprintf("%+v", stats))
	}
}
