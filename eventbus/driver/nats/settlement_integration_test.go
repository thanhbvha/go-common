//go:build integration

package nats

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestNATSHeartbeatPreventsAckWaitRedelivery(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	probe := &heartbeatProbeTransport{Transport: backend}
	cfg := eventbus.DefaultConfig()
	cfg.Namespace = fmt.Sprintf("heartbeat_%d", time.Now().UnixNano())
	cfg.ProcessTimeout = 2 * time.Second
	cfg.Concurrency = 1
	cfg.Prefetch = 1
	bus, err := eventbus.Open(cfg, eventbus.WithTransport(probe))
	if err != nil {
		t.Fatal(err)
	}
	const task = "long_running"
	if err := bus.DeclareTask(task, WithAckWait(600*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	completed := make(chan struct{}, 1)
	if err := bus.HandleTask(task, func(context.Context, eventbus.Job) error {
		calls.Add(1)
		time.Sleep(1600 * time.Millisecond)
		completed <- struct{}{}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := bus.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := bus.Enqueue(ctx, task, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal("long-running handler did not complete")
	}
	time.Sleep(800 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1 (heartbeats=%d failures=%d)", got, probe.calls.Load(), probe.failures.Load())
	}
	if probe.calls.Load() < 3 {
		t.Fatalf("heartbeat calls = %d, want at least 3", probe.calls.Load())
	}
	topo := transport.Topology{Namespace: cfg.Namespace, JobType: task, Options: map[string]any{"nats.ack_wait": 600 * time.Millisecond}}
	info, err := js.ConsumerInfo(backend.streamName(topo), backend.consumerName(transport.Subscription{Topology: topo}))
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 0 || info.NumRedelivered != 0 {
		t.Fatalf("consumer ack pending/redelivered = %d/%d", info.NumAckPending, info.NumRedelivered)
	}
	if err := bus.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

type heartbeatProbeTransport struct {
	transport.Transport
	calls    atomic.Int32
	failures atomic.Int32
}

func (p *heartbeatProbeTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	return p.Transport.Consume(ctx, sub, func(delivery transport.Delivery) {
		lease := delivery.(transport.LeaseDelivery)
		deliver(&heartbeatProbeDelivery{Delivery: delivery, probe: p, interval: lease.HeartbeatInterval()})
	})
}

type heartbeatProbeDelivery struct {
	transport.Delivery
	probe    *heartbeatProbeTransport
	interval time.Duration
}

func (d *heartbeatProbeDelivery) Heartbeat(ctx context.Context) error {
	d.probe.calls.Add(1)
	err := d.Delivery.Heartbeat(ctx)
	if err != nil {
		d.probe.failures.Add(1)
	}
	return err
}

func (d *heartbeatProbeDelivery) HeartbeatInterval() time.Duration { return d.interval }

func TestNATSRetryTransferIsIdempotentBeforeSourceAck(t *testing.T) {
	js, backend, topo, kv, delivery, stop := settlementFixture(t, "retry")
	defer stop()
	ctx := context.Background()
	injected := errors.New("synthetic failure after durable transfer")
	backend.afterTransfer = func(kind transport.DecisionKind) error {
		if kind != transport.DecisionRetry {
			t.Fatalf("transfer hook kind = %s", kind)
		}
		return injected
	}
	decision := transport.Decision{
		Kind:    transport.DecisionRetry,
		Next:    transport.Message{ID: "logical-id", Payload: []byte("retry-payload")},
		RetryAt: time.Now().Add(time.Hour),
	}
	if err := delivery.Settle(ctx, decision); !errors.Is(err, injected) {
		t.Fatalf("first settlement = %v", err)
	}
	assertConsumerPending(t, js, backend, topo, 1)
	if got := len(scheduledKeys(t, kv)); got != 1 {
		t.Fatalf("scheduled retry records = %d, want 1", got)
	}
	if got := transferMarkerCount(t, kv); got != 1 {
		t.Fatalf("retry transfer markers = %d, want 1", got)
	}

	backend.afterTransfer = nil
	if err := delivery.Settle(ctx, decision); err != nil {
		t.Fatal(err)
	}
	assertConsumerPending(t, js, backend, topo, 0)
	if got := len(scheduledKeys(t, kv)); got != 1 {
		t.Fatalf("retried settlement created %d scheduled records, want 1", got)
	}
	if published, err := backend.dispatchDue(ctx, topo, kv, decision.RetryAt.Add(time.Second)); err != nil || published != 1 {
		t.Fatalf("retry dispatch = %d, %v", published, err)
	}
	if got := transferMarkerCount(t, kv); got != 0 {
		t.Fatalf("completed retry transfer markers = %d, want 0", got)
	}
	if info, err := js.StreamInfo(backend.streamName(topo)); err != nil || info.State.Msgs != 2 {
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("source + retry records = %d, want 2", info.State.Msgs)
	}
}

func TestNATSDLQTransferIsIdempotentBeforeSourceTerm(t *testing.T) {
	js, backend, topo, _, delivery, stop := settlementFixture(t, "dlq")
	defer stop()
	ctx := context.Background()
	injected := errors.New("synthetic failure after durable transfer")
	backend.afterTransfer = func(kind transport.DecisionKind) error {
		if kind != transport.DecisionDeadLetter {
			t.Fatalf("transfer hook kind = %s", kind)
		}
		return injected
	}
	decision := transport.Decision{Kind: transport.DecisionDeadLetter, Reason: "synthetic", Cause: errors.New("failed")}
	if err := delivery.Settle(ctx, decision); !errors.Is(err, injected) {
		t.Fatalf("first settlement = %v", err)
	}
	assertConsumerPending(t, js, backend, topo, 1)
	info, err := js.StreamInfo(backend.dlqStreamName(topo))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("DLQ records after transfer = %d, want 1", info.State.Msgs)
	}

	backend.afterTransfer = nil
	if err := delivery.Settle(ctx, decision); err != nil {
		t.Fatal(err)
	}
	assertConsumerPending(t, js, backend, topo, 0)
	info, err = js.StreamInfo(backend.dlqStreamName(topo))
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Fatalf("retried DLQ transfer records = %d, want 1", info.State.Msgs)
	}
}

func TestNATSHeartbeatAndAckHonorCanceledContext(t *testing.T) {
	_, _, _, _, delivery, stop := settlementFixture(t, "context")
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := delivery.Heartbeat(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("heartbeat = %v, want context cancellation", err)
	}
	if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ACK = %v, want context cancellation", err)
	}
}

func TestNATSDestinationFailureLeavesSourcePending(t *testing.T) {
	for _, kind := range []transport.DecisionKind{transport.DecisionRetry, transport.DecisionDeadLetter} {
		t.Run(kind.String(), func(t *testing.T) {
			js, backend, topo, _, delivery, stop := settlementFixture(t, kind.String())
			defer stop()
			if kind == transport.DecisionRetry {
				if err := js.DeleteKeyValue(backend.kvBucketName(topo)); err != nil {
					t.Fatal(err)
				}
			} else if err := js.DeleteStream(backend.dlqStreamName(topo)); err != nil {
				t.Fatal(err)
			}
			decision := transport.Decision{Kind: kind, Next: transport.Message{ID: "logical-id", Payload: []byte("retry")}, RetryAt: time.Now().Add(time.Hour)}
			if err := delivery.Settle(context.Background(), decision); err == nil {
				t.Fatal("settlement succeeded without a durable destination")
			}
			assertConsumerPending(t, js, backend, topo, 1)
		})
	}
}

func settlementFixture(t *testing.T, suffix string) (gonats.JetStreamContext, *natsTransport, transport.Topology, gonats.KeyValue, *natsDelivery, func()) {
	t.Helper()
	s, js := provisioningJetStream(t)
	t.Cleanup(s.Shutdown)
	backend := New(js).(*natsTransport)
	topo := transport.Topology{Namespace: fmt.Sprintf("settlement_%s_%d", suffix, time.Now().UnixNano()), JobType: "job"}
	ctx := context.Background()
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(ctx, topo, transport.Message{ID: "logical-id", Payload: []byte("source")}); err != nil {
		t.Fatal(err)
	}
	consumeCtx, cancel := context.WithCancel(ctx)
	deliveries := make(chan *natsDelivery, 1)
	done := make(chan error, 1)
	sub := transport.Subscription{Topology: topo, Concurrency: 1, Prefetch: 1}
	go func() {
		done <- backend.Consume(consumeCtx, sub, func(d transport.Delivery) {
			deliveries <- d.(*natsDelivery)
		})
	}()
	var delivery *natsDelivery
	select {
	case delivery = <-deliveries:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for source delivery")
	}
	kv, err := backend.getKV(backend.kvBucketName(topo))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop")
		}
	}
	return js, backend, topo, kv, delivery, stop
}

func assertConsumerPending(t *testing.T, js gonats.JetStreamContext, backend *natsTransport, topo transport.Topology, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		info, err := js.ConsumerInfo(backend.streamName(topo), backend.consumerName(transport.Subscription{Topology: topo}))
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("consumer pending = %d, want %d", info.NumAckPending, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func transferMarkerCount(t *testing.T, kv gonats.KeyValue) int {
	t.Helper()
	keys, err := kv.Keys()
	if err != nil && !errors.Is(err, gonats.ErrNoKeysFound) {
		t.Fatal(err)
	}
	count := 0
	for _, key := range keys {
		if len(key) > len("transfer.") && key[:len("transfer.")] == "transfer." {
			count++
		}
	}
	return count
}
