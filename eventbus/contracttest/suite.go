// Package contracttest provides reusable behavioral checks for the public
// EventBus contract over real transports.
package contracttest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

// Harness adapts a real transport to the Bus-level contract suite.
type Harness struct {
	New                        func(*testing.T) transport.Transport
	Topology                   func(testName string, kind transport.TopologyKind) transport.Topology
	DispatchDue                func(context.Context, transport.Transport, transport.Topology, time.Time) (int64, error)
	PrepareUnsettledRedelivery func(*testing.T, transport.Topology)
	DurableRedelivery          bool
	ReceiveWithin              time.Duration
}

// Run executes handler classification and shutdown scenarios through the
// public Bus while using harness hooks only for broker clock/control surfaces.
func Run(t *testing.T, h Harness) {
	t.Helper()
	if h.New == nil || h.Topology == nil || h.DispatchDue == nil {
		t.Fatal("eventbus contracttest: incomplete harness")
	}
	if h.ReceiveWithin <= 0 {
		h.ReceiveWithin = 5 * time.Second
	}

	t.Run("handler_retry_exact_max_attempts_and_final_dlq", func(t *testing.T) {
		attempts := make(chan int, 3)
		tr, bus, topo := newTaskBus(t, h, eventbus.DefaultConfig(), eventbus.WithMaxAttempts(3), func(_ context.Context, job eventbus.Job) error {
			attempts <- job.Attempt()
			return errors.New("synthetic retry")
		})
		start(t, bus)
		mustEnqueue(t, bus, topo.JobType)
		for want := 1; want <= 3; want++ {
			if got := receive(t, attempts, h.ReceiveWithin); got != want {
				t.Fatalf("handler attempt = %d, want %d", got, want)
			}
			if want < 3 {
				waitStats(t, bus, topo, h.ReceiveWithin, func(stats transport.TopologyStats) bool { return stats.Delayed == 1 })
				dispatch(t, h, tr, topo, time.Now().Add(24*time.Hour), 1)
			}
		}
		record := waitDLQ(t, bus, topo, h.ReceiveWithin)
		if record.Reason != "max_attempts_exceeded" || record.Attempts != 3 {
			t.Fatalf("dead letter = %+v", record)
		}
		drain(t, bus, h.ReceiveWithin)
	})

	t.Run("explicit_retry_after", func(t *testing.T) {
		calls := make(chan int, 2)
		tr, bus, topo := newTaskBus(t, h, eventbus.DefaultConfig(), eventbus.WithMaxAttempts(2), func(_ context.Context, job eventbus.Job) error {
			calls <- job.Attempt()
			if job.Attempt() == 1 {
				return eventbus.MarkRetryAfter(errors.New("synthetic retry after"), time.Hour)
			}
			return nil
		})
		start(t, bus)
		mustEnqueue(t, bus, topo.JobType)
		if got := receive(t, calls, h.ReceiveWithin); got != 1 {
			t.Fatalf("first attempt = %d", got)
		}
		waitStats(t, bus, topo, h.ReceiveWithin, func(stats transport.TopologyStats) bool { return stats.Delayed == 1 })
		dispatch(t, h, tr, topo, time.Now().Add(30*time.Minute), 0)
		dispatch(t, h, tr, topo, time.Now().Add(2*time.Hour), 1)
		if got := receive(t, calls, h.ReceiveWithin); got != 2 {
			t.Fatalf("retry attempt = %d", got)
		}
		waitStats(t, bus, topo, h.ReceiveWithin, settled)
		drain(t, bus, h.ReceiveWithin)
	})

	t.Run("permanent_error_directly_to_dlq", func(t *testing.T) {
		var calls atomic.Int32
		_, bus, topo := newTaskBus(t, h, eventbus.DefaultConfig(), eventbus.WithMaxAttempts(3), func(context.Context, eventbus.Job) error {
			calls.Add(1)
			return eventbus.MarkPermanent(errors.New("synthetic permanent"))
		})
		start(t, bus)
		mustEnqueue(t, bus, topo.JobType)
		record := waitDLQ(t, bus, topo, h.ReceiveWithin)
		if calls.Load() != 1 || record.Reason != "permanent_error" || record.Attempts != 1 {
			t.Fatalf("calls = %d, dead letter = %+v", calls.Load(), record)
		}
		drain(t, bus, h.ReceiveWithin)
	})

	t.Run("panic_recovery_and_worker_survival", func(t *testing.T) {
		calls := make(chan string, 3)
		tr, bus, topo := newTaskBus(t, h, eventbus.DefaultConfig(), eventbus.WithMaxAttempts(2), func(_ context.Context, job eventbus.Job) error {
			var value string
			if err := job.Decode(&value); err != nil {
				return err
			}
			calls <- fmt.Sprintf("%s:%d", value, job.Attempt())
			if value == "panic" && job.Attempt() == 1 {
				panic("synthetic")
			}
			return nil
		})
		start(t, bus)
		if err := bus.Enqueue(context.Background(), topo.JobType, "panic"); err != nil {
			t.Fatal(err)
		}
		if err := bus.Enqueue(context.Background(), topo.JobType, "survivor"); err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for len(seen) < 2 {
			seen[receive(t, calls, h.ReceiveWithin)] = true
		}
		if !seen["panic:1"] || !seen["survivor:1"] {
			t.Fatalf("initial calls = %v", seen)
		}
		waitStats(t, bus, topo, h.ReceiveWithin, func(stats transport.TopologyStats) bool { return stats.Delayed == 1 })
		dispatch(t, h, tr, topo, time.Now().Add(24*time.Hour), 1)
		if got := receive(t, calls, h.ReceiveWithin); got != "panic:2" {
			t.Fatalf("panic retry = %q", got)
		}
		waitStats(t, bus, topo, h.ReceiveWithin, settled)
		drain(t, bus, h.ReceiveWithin)
	})

	t.Run("handler_timeout_retries_then_dlq", func(t *testing.T) {
		cfg := eventbus.DefaultConfig()
		cfg.ProcessTimeout = 25 * time.Millisecond
		calls := make(chan int, 2)
		tr, bus, topo := newTaskBus(t, h, cfg, eventbus.WithMaxAttempts(2), func(ctx context.Context, job eventbus.Job) error {
			calls <- job.Attempt()
			<-ctx.Done()
			return nil
		})
		start(t, bus)
		mustEnqueue(t, bus, topo.JobType)
		if got := receive(t, calls, h.ReceiveWithin); got != 1 {
			t.Fatalf("first attempt = %d", got)
		}
		waitStats(t, bus, topo, h.ReceiveWithin, func(stats transport.TopologyStats) bool { return stats.Delayed == 1 })
		dispatch(t, h, tr, topo, time.Now().Add(24*time.Hour), 1)
		if got := receive(t, calls, h.ReceiveWithin); got != 2 {
			t.Fatalf("second attempt = %d", got)
		}
		record := waitDLQ(t, bus, topo, h.ReceiveWithin)
		if record.Reason != "max_attempts_exceeded" || record.Attempts != 2 {
			t.Fatalf("dead letter = %+v", record)
		}
		drain(t, bus, h.ReceiveWithin)
	})

	for _, malformed := range []struct {
		name, reason string
		payload      func(transport.Topology) []byte
	}{
		{name: "malformed", reason: "decode_error", payload: func(transport.Topology) []byte { return []byte("{") }},
		{name: "wrong_route", reason: "topology_type_mismatch", payload: wrongRouteEnvelope},
	} {
		t.Run(malformed.name+"_envelope_suppresses_handler_and_has_stable_dlq_reason", func(t *testing.T) {
			var calls atomic.Int32
			tr, bus, topo := newTaskBus(t, h, eventbus.DefaultConfig(), nil, func(context.Context, eventbus.Job) error {
				calls.Add(1)
				return nil
			})
			start(t, bus)
			if err := tr.Publish(context.Background(), topo, transport.Message{ID: "contract-raw", Payload: malformed.payload(topo)}); err != nil {
				t.Fatal(err)
			}
			record := waitDLQ(t, bus, topo, h.ReceiveWithin)
			if calls.Load() != 0 || record.Reason != malformed.reason {
				t.Fatalf("handler calls = %d, dead letter = %+v", calls.Load(), record)
			}
			drain(t, bus, h.ReceiveWithin)
		})
	}

	t.Run("graceful_drain_waits_and_acks_in_flight", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		_, bus, topo := newTaskBus(t, h, eventbus.DefaultConfig(), nil, func(context.Context, eventbus.Job) error {
			close(entered)
			<-release
			return nil
		})
		start(t, bus)
		mustEnqueue(t, bus, topo.JobType)
		receive(t, entered, h.ReceiveWithin)
		result := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), h.ReceiveWithin)
		defer cancel()
		go func() { result <- bus.Drain(ctx) }()
		select {
		case err := <-result:
			t.Fatalf("Drain returned before handler release: %v", err)
		default:
		}
		close(release)
		if err := receive(t, result, h.ReceiveWithin); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("forced_deadline_redelivers_unsettled_source", func(t *testing.T) {
		if !h.DurableRedelivery || h.PrepareUnsettledRedelivery == nil {
			t.Skip("harness has no deterministic durable-redelivery control")
		}
		entered := make(chan struct{})
		_, bus, topo := newTaskBus(t, h, eventbus.DefaultConfig(), nil, func(ctx context.Context, _ eventbus.Job) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		})
		start(t, bus)
		mustEnqueue(t, bus, topo.JobType)
		receive(t, entered, h.ReceiveWithin)
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		err := bus.Drain(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Drain = %v, want deadline exceeded", err)
		}
		h.PrepareUnsettledRedelivery(t, topo)

		attempts := make(chan int, 1)
		second := h.New(t)
		next, err := openTaskBus(second, topo, eventbus.DefaultConfig(), nil, func(_ context.Context, job eventbus.Job) error {
			attempts <- job.Attempt()
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		start(t, next)
		if got := receive(t, attempts, h.ReceiveWithin); got < 1 {
			t.Fatalf("redelivered handler attempt = %d, want a valid attempt", got)
		}
		waitStats(t, next, topo, h.ReceiveWithin, settled)
		drain(t, next, h.ReceiveWithin)
	})
}

func newTaskBus(t *testing.T, h Harness, cfg eventbus.Config, option eventbus.TopologyOption, handler eventbus.Handler) (transport.Transport, eventbus.Bus, transport.Topology) {
	t.Helper()
	tr := h.New(t)
	topo := h.Topology(t.Name(), transport.TaskTopology)
	bus, err := openTaskBus(tr, topo, cfg, option, handler)
	if err != nil {
		t.Fatal(err)
	}
	return tr, bus, topo
}

func openTaskBus(tr transport.Transport, topo transport.Topology, cfg eventbus.Config, option eventbus.TopologyOption, handler eventbus.Handler) (eventbus.Bus, error) {
	cfg.Namespace = topo.Namespace
	bus, err := eventbus.Open(cfg, eventbus.WithTransport(tr))
	if err != nil {
		return nil, err
	}
	options := make([]eventbus.TopologyOption, 0, len(topo.Options)+1)
	for key, value := range topo.Options {
		options = append(options, eventbus.WithOption(key, value))
	}
	if option != nil {
		options = append(options, option)
	}
	if err := bus.DeclareTask(topo.JobType, options...); err != nil {
		return nil, err
	}
	if err := bus.HandleTask(topo.JobType, handler); err != nil {
		return nil, err
	}
	return bus, nil
}

func start(t *testing.T, bus eventbus.Bus) {
	t.Helper()
	if err := bus.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func mustEnqueue(t *testing.T, bus eventbus.Bus, name string) {
	t.Helper()
	if err := bus.Enqueue(context.Background(), name, "payload"); err != nil {
		t.Fatal(err)
	}
}

func dispatch(t *testing.T, h Harness, tr transport.Transport, topo transport.Topology, now time.Time, want int64) {
	t.Helper()
	got, err := h.DispatchDue(context.Background(), tr, topo, now)
	if err != nil || (want == 0 && got != 0) || (want > 0 && got < want) {
		t.Fatalf("dispatch due = %d, %v; want %s%d", got, err, map[bool]string{true: "at least ", false: ""}[want > 0], want)
	}
}

func waitDLQ(t *testing.T, bus eventbus.Bus, topo transport.Topology, within time.Duration) transport.DeadLetter {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		count, err := bus.CountDeadLetters(context.Background(), eventbus.TaskRef(topo.JobType))
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for dead letter")
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		page, err := bus.ListDeadLetters(context.Background(), eventbus.TaskRef(topo.JobType), "", 10)
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatal(err)
			}
			time.Sleep(5 * time.Millisecond)
			continue
		}
		if len(page.Records) > 0 {
			return page.Records[0]
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitStats(t *testing.T, bus eventbus.Bus, topo transport.Topology, within time.Duration, ready func(transport.TopologyStats) bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		stats, err := bus.TopologyStats(context.Background(), eventbus.TaskRef(topo.JobType))
		if err != nil {
			t.Fatal(err)
		}
		if ready(stats) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for stats, last = %+v", stats)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func settled(stats transport.TopologyStats) bool {
	return stats.Pending == 0 && stats.Delayed == 0 && stats.InFlight == 0 && stats.DLQ == 0
}

func drain(t *testing.T, bus eventbus.Bus, within time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	if err := bus.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

func wrongRouteEnvelope(topo transport.Topology) []byte {
	payload, _, err := eventbus.NewCodec(nil).EncodeTopology(eventbus.TaskTopology, topo.JobType+"_other", "payload", 1)
	if err != nil {
		panic(err)
	}
	return payload
}

func receive[T any](t *testing.T, ch <-chan T, within time.Duration) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(within):
		var zero T
		t.Fatalf("timed out waiting for %s", strings.TrimPrefix(t.Name(), "Test"))
		return zero
	}
}
