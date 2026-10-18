package eventbus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type topologyControlTransport struct {
	benchmarkTransport
	started    chan int
	stopped    chan int
	sessions   atomic.Int32
	deliverOne atomic.Bool
	delivery   transport.Delivery
}

func (t *topologyControlTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	session := int(t.sessions.Add(1))
	t.started <- session
	if t.delivery != nil && t.deliverOne.CompareAndSwap(false, true) {
		deliver(t.delivery)
	}
	<-ctx.Done()
	t.stopped <- session
	if sub.WaitForHandlers != nil {
		sub.WaitForHandlers()
	}
	return ctx.Err()
}

func TestPauseAndResumeTopologyRestartsIntake(t *testing.T) {
	backend := &topologyControlTransport{started: make(chan int, 4), stopped: make(chan int, 4)}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("controlled"); err != nil {
		t.Fatal(err)
	}
	if err := bus.HandleTask("controlled", func(context.Context, Job) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := bus.Pause(context.Background(), "controlled"); !errors.Is(err, ErrBusNotRunning) {
		t.Fatalf("Pause before Start = %v", err)
	}
	if err := bus.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitInt(t, backend.started, 1)
	if err := bus.Pause(context.Background(), "controlled"); err != nil {
		t.Fatal(err)
	}
	awaitInt(t, backend.stopped, 1)
	select {
	case session := <-backend.started:
		t.Fatalf("session %d started while paused", session)
	default:
	}
	if err := bus.Resume(context.Background(), "controlled"); err != nil {
		t.Fatal(err)
	}
	awaitInt(t, backend.started, 2)
	if err := bus.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDrainTopologyWaitsForAcceptedDeliveryAndBusCanResume(t *testing.T) {
	payload, _, err := NewCodec(nil).EncodeTopology(TaskTopology, "controlled", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	handlerStarted := make(chan struct{})
	delivery := &workerPoolDelivery{message: transport.Message{Payload: payload}, settled: make(chan struct{})}
	backend := &topologyControlTransport{
		started: make(chan int, 4), stopped: make(chan int, 4), delivery: delivery,
	}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("controlled"); err != nil {
		t.Fatal(err)
	}
	if err := bus.HandleTask("controlled", func(context.Context, Job) error {
		close(handlerStarted)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := bus.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	awaitInt(t, backend.started, 1)
	awaitSignal(t, handlerStarted, "handler did not start")
	result := make(chan error, 1)
	go func() { result <- bus.DrainTopology(context.Background(), "controlled") }()
	awaitInt(t, backend.stopped, 1)
	select {
	case err := <-result:
		t.Fatalf("topology drained before handler completed: %v", err)
	default:
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, delivery.settled, "delivery was not settled")
	if err := bus.Resume(context.Background(), "controlled"); err != nil {
		t.Fatal(err)
	}
	awaitInt(t, backend.started, 2)
	if err := bus.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func awaitInt(t *testing.T, ch <-chan int, want int) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("value = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %d", want)
	}
}

func awaitSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}
