//go:build integration

package nats

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

// DS-21/22: an unsettled delivery expires at AckWait and returns with broker
// delivery history incremented. No extra delivery is required to discover it.
func TestNATSAckWaitExpiryRedeliversWithAttempt(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{
		Namespace: fmt.Sprintf("ackwait_%d", time.Now().UnixNano()), JobType: "job",
		Options: map[string]any{"nats.ack_wait": 200 * time.Millisecond},
	}
	ctx := context.Background()
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Publish(ctx, topo, transport.Message{ID: "redeliver", Payload: []byte("payload")}); err != nil {
		t.Fatal(err)
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	deliveries := make(chan transport.Delivery, 2)
	done := make(chan error, 1)
	go func() {
		done <- backend.Consume(consumeCtx, transport.Subscription{Topology: topo, Concurrency: 1, Prefetch: 1}, func(delivery transport.Delivery) {
			deliveries <- delivery
		})
	}()
	first := awaitNATSDelivery(t, deliveries)
	if first.DeliveryAttempt() != 1 {
		t.Fatalf("first attempt = %d, want 1", first.DeliveryAttempt())
	}
	second := awaitNATSDelivery(t, deliveries)
	if second.Message().ID != first.Message().ID || second.DeliveryAttempt() < 2 {
		t.Fatalf("redelivery = id %q attempt %d", second.Message().ID, second.DeliveryAttempt())
	}
	if err := second.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
	cancel()
	awaitNATSConsumeStop(t, done)
}

// MaxAckPending is the broker-side bound: once concurrency+prefetch deliveries
// are unsettled, the next record remains pending until capacity is released.
func TestNATSMaxAckPendingSaturatesAndResumes(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	topo := transport.Topology{Namespace: fmt.Sprintf("saturation_%d", time.Now().UnixNano()), JobType: "job"}
	ctx := context.Background()
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := backend.Publish(ctx, topo, transport.Message{ID: fmt.Sprintf("job-%d", i), Payload: []byte("payload")}); err != nil {
			t.Fatal(err)
		}
	}

	sub := transport.Subscription{Topology: topo, Concurrency: 1, Prefetch: 1}
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	deliveries := make(chan transport.Delivery, 3)
	done := make(chan error, 1)
	go func() {
		done <- backend.Consume(consumeCtx, sub, func(delivery transport.Delivery) { deliveries <- delivery })
	}()
	first := awaitNATSDelivery(t, deliveries)
	second := awaitNATSDelivery(t, deliveries)
	waitForNATSConsumerState(t, js, backend, sub, 2, 1)
	select {
	case third := <-deliveries:
		t.Fatalf("delivery %q exceeded MaxAckPending before capacity release", third.Message().ID)
	case <-time.After(250 * time.Millisecond):
	}
	if err := first.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
		t.Fatal(err)
	}
	third := awaitNATSDelivery(t, deliveries)
	for _, delivery := range []transport.Delivery{second, third} {
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	awaitNATSConsumeStop(t, done)
}

// DS-60: a server outage is observable, and after the same durable store
// returns the supervised consumer resumes processing.
func TestNATSDisconnectReconnectResumesConsumer(t *testing.T) {
	storeDir := t.TempDir()
	first := startRestartableNATSServer(t, server.RANDOM_PORT, storeDir)
	port := first.Addr().(*net.TCPAddr).Port
	disconnected := make(chan struct{}, 1)
	reconnected := make(chan struct{}, 1)
	nc, err := gonats.Connect(first.ClientURL(),
		gonats.ReconnectWait(25*time.Millisecond), gonats.MaxReconnects(-1),
		gonats.DisconnectErrHandler(func(*gonats.Conn, error) {
			select {
			case disconnected <- struct{}{}:
			default:
			}
		}),
		gonats.ReconnectHandler(func(*gonats.Conn) {
			select {
			case reconnected <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		first.Shutdown()
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		first.Shutdown()
		t.Fatal(err)
	}
	backend := New(js).(*natsTransport)
	cfg := eventbus.DefaultConfig()
	cfg.Namespace = fmt.Sprintf("reconnect_%d", time.Now().UnixNano())
	cfg.Concurrency = 1
	cfg.Prefetch = 1
	bus, err := eventbus.Open(cfg, eventbus.WithTransport(backend))
	if err != nil {
		first.Shutdown()
		t.Fatal(err)
	}
	const task = "job"
	if err := bus.DeclareTask(task); err != nil {
		first.Shutdown()
		t.Fatal(err)
	}
	var handled atomic.Int32
	handledCh := make(chan struct{}, 2)
	if err := bus.HandleTask(task, func(context.Context, eventbus.Job) error {
		handled.Add(1)
		handledCh <- struct{}{}
		return nil
	}); err != nil {
		first.Shutdown()
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := bus.Start(ctx); err != nil {
		first.Shutdown()
		t.Fatal(err)
	}
	if err := bus.Enqueue(ctx, task, "before-outage"); err != nil {
		first.Shutdown()
		t.Fatal(err)
	}
	awaitSignal(t, handledCh, "pre-outage handler")

	first.Shutdown()
	first.WaitForShutdown()
	awaitSignal(t, disconnected, "NATS disconnect callback")
	second := startRestartableNATSServer(t, port, storeDir)
	defer second.Shutdown()
	awaitSignal(t, reconnected, "NATS reconnect callback")
	if err := bus.Enqueue(ctx, task, "after-outage"); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, handledCh, "post-outage handler")
	if got := handled.Load(); got != 2 {
		t.Fatalf("handler calls = %d, want 2", got)
	}
	if err := bus.Drain(ctx); err != nil {
		t.Fatal(err)
	}
}

func startRestartableNATSServer(t *testing.T, port int, storeDir string) *server.Server {
	t.Helper()
	s, err := server.NewServer(&server.Options{Port: port, JetStream: true, StoreDir: storeDir})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		t.Fatal("NATS server failed to start")
	}
	return s
}

func awaitNATSDelivery(t *testing.T, deliveries <-chan transport.Delivery) transport.Delivery {
	t.Helper()
	select {
	case delivery := <-deliveries:
		return delivery
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for NATS delivery")
		return nil
	}
}

func awaitNATSConsumeStop(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("consume shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("NATS consumer did not stop")
	}
}

func waitForNATSConsumerState(t *testing.T, js gonats.JetStreamContext, backend *natsTransport, sub transport.Subscription, ackPending int, pending uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		info, err := js.ConsumerInfo(backend.streamName(sub.Topology), backend.consumerName(sub))
		if err == nil && info.NumAckPending == ackPending && info.NumPending == pending {
			return
		}
		if time.Now().After(deadline) {
			if err != nil {
				t.Fatalf("consumer state error = %v; want %d/%d", err, ackPending, pending)
			}
			t.Fatalf("consumer state = ack-pending %d pending %d; want %d/%d", info.NumAckPending, info.NumPending, ackPending, pending)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func awaitSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
