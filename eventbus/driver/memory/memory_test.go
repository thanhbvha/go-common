package memory

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestEventPublishBeforeConsumeReachesProvisionedGroups(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "fanout", Kind: transport.EventTopology}
	for _, group := range []string{"alpha", "beta"} {
		if err := backend.EnsureSubscription(context.Background(), transport.Subscription{Topology: topo, Group: group}); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{ID: "event", Payload: []byte("event")}); err != nil {
		t.Fatal(err)
	}
	for _, group := range []string{"alpha", "beta"} {
		delivery, stop := receiveMemory(t, backend, transport.Subscription{Topology: topo, Group: group})
		if string(delivery.Message().Payload) != "event" {
			t.Fatalf("group %s payload=%q", group, delivery.Message().Payload)
		}
		if err := delivery.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		stop()
	}
}

func TestEventPublishWithoutProcessLocalSubscribersIsUnsupported(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "no-subscribers", Kind: transport.EventTopology}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{}); !errors.Is(err, transport.ErrNotSupported) {
		t.Fatalf("publish error=%v", err)
	}
	if err := backend.PublishAt(context.Background(), topo, transport.Message{}, time.Now().Add(time.Hour)); !errors.Is(err, transport.ErrNotSupported) {
		t.Fatalf("delayed publish error=%v", err)
	}
}

func TestFanoutCapacityFailureIsAtomic(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "fanout-capacity", Kind: transport.EventTopology, Options: map[string]any{
		"memory.max_ready_entries": 1,
	}}
	for _, group := range []string{"full", "empty"} {
		if err := backend.EnsureSubscription(context.Background(), transport.Subscription{Topology: topo, Group: group}); err != nil {
			t.Fatal(err)
		}
	}
	backend.mu.Lock()
	backend.topologies[topo.Name()].groups["full"].ready = append(backend.topologies[topo.Name()].groups["full"].ready, memoryQueued{message: transport.Message{ID: "existing"}})
	backend.mu.Unlock()
	err := backend.Publish(context.Background(), topo, transport.Message{ID: "rejected"})
	if !errors.Is(err, transport.ErrCapacityExceeded) {
		t.Fatalf("publish error=%v", err)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if len(backend.topologies[topo.Name()].groups["full"].ready) != 1 || len(backend.topologies[topo.Name()].groups["empty"].ready) != 0 {
		t.Fatal("capacity failure partially fanned out")
	}
}

func TestConcurrentPublishRespectsCapacity(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "concurrent", Options: map[string]any{"memory.max_ready_entries": 10}}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 50)
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- backend.Publish(context.Background(), topo, transport.Message{Payload: []byte("x")})
		}()
	}
	wg.Wait()
	close(errs)
	accepted := 0
	for err := range errs {
		if err == nil {
			accepted++
		} else if !errors.Is(err, transport.ErrCapacityExceeded) {
			t.Fatal(err)
		}
	}
	if accepted != 10 {
		t.Fatalf("accepted=%d", accepted)
	}
}

func TestDelayedSchedulerCapacityOrderAndClose(t *testing.T) {
	before := runtime.NumGoroutine()
	backend := New().(*memoryTransport)
	topo := transport.Topology{Namespace: "memory", JobType: "delayed", Options: map[string]any{
		"memory.max_delayed_entries": 2,
	}}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := backend.PublishAt(context.Background(), topo, transport.Message{ID: "second", Payload: []byte("second")}, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := backend.PublishAt(context.Background(), topo, transport.Message{ID: "first", Payload: []byte("first")}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := backend.PublishAt(context.Background(), topo, transport.Message{ID: "full"}, now.Add(3*time.Hour)); !errors.Is(err, transport.ErrCapacityExceeded) {
		t.Fatalf("capacity error=%v", err)
	}
	if moved, err := backend.dispatchDue(context.Background(), now.Add(90*time.Minute)); err != nil || moved != 1 {
		t.Fatalf("dispatch=%d err=%v", moved, err)
	}
	delivery, stop := receiveMemory(t, backend, transport.Subscription{Topology: topo})
	if delivery.Message().ID != "first" {
		t.Fatalf("first delivery=%q", delivery.Message().ID)
	}
	if err := delivery.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
	stop()
	if err := backend.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{}); !errors.Is(err, errMemoryClosed) {
		t.Fatalf("post-close publish=%v", err)
	}
	if after := runtime.NumGoroutine(); after > before+4 {
		t.Fatalf("goroutines grew from %d to %d", before, after)
	}
}

func TestDelayedSchedulerAutomaticallyDispatchesAndReportsBackpressure(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "scheduler-health", Options: map[string]any{
		"memory.max_ready_entries": 1,
	}}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{ID: "blocking"}); err != nil {
		t.Fatal(err)
	}
	if err := backend.PublishAt(context.Background(), topo, transport.Message{ID: "delayed"}, time.Now().Add(10*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for backend.SchedulerStatus().Retries == 0 {
		select {
		case <-deadline:
			t.Fatal("scheduler did not expose capacity backpressure")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	status := backend.SchedulerStatus()
	if status.LastError == "" || status.Pending != 1 {
		t.Fatalf("degraded status=%+v", status)
	}
	blocking, stop := receiveMemory(t, backend, transport.Subscription{Topology: topo})
	if err := blocking.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
	stop()
	delayed, stopDelayed := receiveMemory(t, backend, transport.Subscription{Topology: topo})
	defer stopDelayed()
	if delayed.Message().ID != "delayed" {
		t.Fatalf("delayed ID=%q", delayed.Message().ID)
	}
	status = backend.SchedulerStatus()
	if status.LastError != "" || status.LastSuccess.IsZero() || status.Pending != 0 {
		t.Fatalf("recovered status=%+v", status)
	}
}

func TestCancellationRedeliversUnsettledMessage(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "redelivery"}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{ID: "same"}); err != nil {
		t.Fatal(err)
	}
	first, stop := receiveMemory(t, backend, transport.Subscription{Topology: topo})
	if first.DeliveryAttempt() != 1 {
		t.Fatalf("first attempt=%d", first.DeliveryAttempt())
	}
	stop()
	second, stopSecond := receiveMemory(t, backend, transport.Subscription{Topology: topo})
	defer stopSecond()
	if second.Message().ID != "same" || second.DeliveryAttempt() != 2 {
		t.Fatalf("redelivery=%q attempt=%d", second.Message().ID, second.DeliveryAttempt())
	}
	if err := second.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
}

func TestEventRetryReturnsOnlyToSourceGroup(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "retry-isolation", Kind: transport.EventTopology}
	for _, group := range []string{"alpha", "beta"} {
		if err := backend.EnsureSubscription(context.Background(), transport.Subscription{Topology: topo, Group: group}); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	alpha, stopAlpha := receiveMemory(t, backend, transport.Subscription{Topology: topo, Group: "alpha"})
	if err := alpha.Settle(context.Background(), transport.Decision{Kind: transport.DecisionRetry, Next: transport.Message{ID: "retry", Payload: []byte("retry")}, RetryAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	stopAlpha()
	beta, stopBeta := receiveMemory(t, backend, transport.Subscription{Topology: topo, Group: "beta"})
	if err := beta.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
	stopBeta()
	retry, stopRetry := receiveMemory(t, backend, transport.Subscription{Topology: topo, Group: "alpha"})
	defer stopRetry()
	if retry.Message().ID != "retry" {
		t.Fatalf("retry ID=%q", retry.Message().ID)
	}
	backend.mu.Lock()
	betaPending := len(backend.topologies[topo.Name()].groups["beta"].ready)
	backend.mu.Unlock()
	if betaPending != 0 {
		t.Fatal("retry fanned out to unrelated subscriber")
	}
}

func TestDLQCapacityFailurePreservesSourceAndRequeueIsAtomic(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "dlq-capacity", Options: map[string]any{
		"memory.max_ready_entries": 1, "memory.max_dlq_entries": 1,
	}}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	backend.dlqs[topo.Name()] = append(backend.dlqs[topo.Name()], transport.DeadLetter{ID: "existing"})
	backend.mu.Unlock()
	if err := backend.Publish(context.Background(), topo, transport.Message{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	delivery, stop := receiveMemory(t, backend, transport.Subscription{Topology: topo})
	err := delivery.Settle(context.Background(), transport.Decision{Kind: transport.DecisionDeadLetter})
	if !errors.Is(err, transport.ErrCapacityExceeded) {
		t.Fatalf("DLQ capacity error=%v", err)
	}
	stop()
	redelivered, stopRedelivery := receiveMemory(t, backend, transport.Subscription{Topology: topo})
	if redelivered.Message().ID != "source" {
		t.Fatalf("redelivered ID=%q", redelivered.Message().ID)
	}
	stopRedelivery()

	backend.mu.Lock()
	backend.dlqs[topo.Name()] = []transport.DeadLetter{{ID: "admin", Message: transport.Message{ID: "requeue"}}}
	group := backend.topologies[topo.Name()].groups[""]
	group.ready = []memoryQueued{{message: transport.Message{ID: "full"}}}
	group.inFlight = make(map[uint64]memoryInFlight)
	backend.mu.Unlock()
	if err := backend.Requeue(context.Background(), topo, []string{"admin"}); !errors.Is(err, transport.ErrCapacityExceeded) {
		t.Fatalf("requeue capacity error=%v", err)
	}
	if count, _ := backend.Count(context.Background(), topo); count != 1 {
		t.Fatalf("failed requeue removed DLQ record: %d", count)
	}
}

func TestEventDLQRequeueReturnsOnlyToSourceGroup(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "dlq-isolation", Kind: transport.EventTopology}
	for _, group := range []string{"alpha", "beta"} {
		if err := backend.EnsureSubscription(context.Background(), transport.Subscription{Topology: topo, Group: group}); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{ID: "source"}); err != nil {
		t.Fatal(err)
	}
	alpha, stopAlpha := receiveMemory(t, backend, transport.Subscription{Topology: topo, Group: "alpha"})
	if err := alpha.Settle(context.Background(), transport.Decision{Kind: transport.DecisionDeadLetter, Reason: "test"}); err != nil {
		t.Fatal(err)
	}
	stopAlpha()
	beta, stopBeta := receiveMemory(t, backend, transport.Subscription{Topology: topo, Group: "beta"})
	if err := beta.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
	stopBeta()
	records, err := backend.List(context.Background(), topo, 0, 10)
	if err != nil || len(records) != 1 || records[0].SourceGroup != "alpha" {
		t.Fatalf("records=%+v err=%v", records, err)
	}
	if err := backend.Requeue(context.Background(), topo, []string{records[0].ID}); err != nil {
		t.Fatal(err)
	}
	requeued, stopRequeued := receiveMemory(t, backend, transport.Subscription{Topology: topo, Group: "alpha"})
	defer stopRequeued()
	if requeued.Message().ID != "source" {
		t.Fatalf("requeued ID=%q", requeued.Message().ID)
	}
	backend.mu.Lock()
	betaPending := len(backend.topologies[topo.Name()].groups["beta"].ready)
	backend.mu.Unlock()
	if betaPending != 0 {
		t.Fatal("DLQ requeue fanned out to unrelated subscriber")
	}
}

func TestStatsReportQueuedInFlightDelayedAndDLQ(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: "memory", JobType: "stats"}
	if err := backend.EnsureTopology(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(context.Background(), topo, transport.Message{ID: "active"}); err != nil {
		t.Fatal(err)
	}
	delivery, stop := receiveMemory(t, backend, transport.Subscription{Topology: topo})
	defer stop()
	backend.mu.Lock()
	backend.topologies[topo.Name()].groups[""].ready = append(backend.topologies[topo.Name()].groups[""].ready, memoryQueued{
		message: transport.Message{ID: "ready"}, readyAt: time.Now(),
	})
	backend.mu.Unlock()
	if err := backend.PublishAt(context.Background(), topo, transport.Message{ID: "delayed"}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	backend.mu.Lock()
	backend.dlqs[topo.Name()] = append(backend.dlqs[topo.Name()], transport.DeadLetter{ID: "dlq"})
	backend.mu.Unlock()
	stats, err := backend.Stats(context.Background(), topo)
	if err != nil || stats.Pending != 1 || stats.InFlight != 1 || stats.Delayed != 1 || stats.DLQ != 1 || stats.Lag != 2 || !stats.LagKnown || !stats.OldestAgeKnown {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	if err := delivery.Settle(context.Background(), transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
}

func TestTopologyScopedDedupeIsolation(t *testing.T) {
	backend := New().(*memoryTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	first := transport.Topology{Namespace: "one", JobType: "same"}
	second := transport.Topology{Namespace: "two", JobType: "same"}
	for _, topo := range []transport.Topology{first, second} {
		acquired, err := backend.ReserveTopology(context.Background(), topo, "key", time.Minute)
		if err != nil || !acquired {
			t.Fatalf("reserve %s=%t err=%v", topo.Name(), acquired, err)
		}
	}
}

func receiveMemory(t *testing.T, backend *memoryTransport, sub transport.Subscription) (transport.Delivery, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	deliveries := make(chan transport.Delivery, 1)
	done := make(chan error, 1)
	go func() {
		done <- backend.Consume(ctx, sub, func(delivery transport.Delivery) { deliveries <- delivery })
	}()
	select {
	case delivery := <-deliveries:
		return delivery, func() {
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("consumer did not stop")
			}
		}
	case err := <-done:
		cancel()
		t.Fatalf("consumer stopped: %v", err)
	case <-time.After(time.Second):
		cancel()
		t.Fatal("delivery timeout")
	}
	return nil, nil
}
