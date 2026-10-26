package eventbus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type workerPoolTransport struct {
	benchmarkTransport
	deliveries []transport.Delivery
	started    chan struct{}
	accepted   chan int
	sub        chan transport.Subscription
}

func (t *workerPoolTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	if t.sub != nil {
		t.sub <- sub
	}
	close(t.started)
	for i, delivery := range t.deliveries {
		deliver(delivery)
		t.accepted <- i + 1
	}
	<-ctx.Done()
	return ctx.Err()
}

type workerPoolDelivery struct {
	message transport.Message
	guard   transport.SettlementGuard
	settled chan struct{}
}

func (d *workerPoolDelivery) Message() transport.Message { return d.message }
func (*workerPoolDelivery) DeliveryAttempt() int         { return 1 }
func (*workerPoolDelivery) Heartbeat(context.Context) error {
	return nil
}
func (d *workerPoolDelivery) Settle(_ context.Context, decision transport.Decision) error {
	return d.guard.Settle(decision, func() error {
		close(d.settled)
		return nil
	})
}

func TestWorkerPoolBoundsConcurrencyAndPrefetch(t *testing.T) {
	const (
		concurrency = 2
		prefetch    = 1
		deliveryN   = 6
	)

	codec := NewCodec(nil)
	deliveries := make([]transport.Delivery, 0, deliveryN)
	settled := make([]<-chan struct{}, 0, deliveryN)
	for i := 0; i < deliveryN; i++ {
		payload, env, err := codec.EncodeTopology(TaskTopology, "bounded", i, 1)
		if err != nil {
			t.Fatalf("EncodeTopology: %v", err)
		}
		done := make(chan struct{})
		deliveries = append(deliveries, &workerPoolDelivery{
			message: transport.Message{ID: env.ID, Type: env.Type, Payload: payload},
			settled: done,
		})
		settled = append(settled, done)
	}

	backend := &workerPoolTransport{
		deliveries: deliveries,
		started:    make(chan struct{}),
		accepted:   make(chan int, deliveryN),
		sub:        make(chan transport.Subscription, 1),
	}
	cfg := DefaultConfig()
	cfg.Concurrency = 9
	cfg.Prefetch = 9
	instance, err := Open(cfg, WithTransport(backend))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	release := make(chan struct{})
	started := make(chan struct{}, deliveryN)
	var active atomic.Int32
	var maxActive atomic.Int32
	handler := func(context.Context, Job) error {
		current := active.Add(1)
		for observed := maxActive.Load(); current > observed; observed = maxActive.Load() {
			if maxActive.CompareAndSwap(observed, current) {
				break
			}
		}
		started <- struct{}{}
		<-release
		active.Add(-1)
		return nil
	}
	if err := instance.DeclareTask("bounded", WithConcurrency(concurrency), WithPrefetch(prefetch)); err != nil {
		t.Fatalf("DeclareTask: %v", err)
	}
	if err := instance.HandleTask("bounded", handler); err != nil {
		t.Fatalf("HandleTask: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := instance.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("consumer did not start")
	}
	select {
	case sub := <-backend.sub:
		if sub.Concurrency != concurrency || sub.Prefetch != prefetch {
			t.Fatalf("subscription capacity = %d + %d, want %d + %d", sub.Concurrency, sub.Prefetch, concurrency, prefetch)
		}
	case <-time.After(time.Second):
		t.Fatal("transport did not receive subscription capacity")
	}

	// Two deliveries execute and one waits in the bounded queue. The transport
	// callback for the fourth delivery must remain blocked until capacity frees.
	for want := 1; want <= concurrency+prefetch; want++ {
		select {
		case got := <-backend.accepted:
			if got != want {
				t.Fatalf("accepted delivery = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("delivery %d was not accepted", want)
		}
	}
	for i := 0; i < concurrency; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("worker %d did not start", i+1)
		}
	}
	select {
	case got := <-backend.accepted:
		t.Fatalf("delivery %d exceeded concurrency plus prefetch bound", got)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	for i, done := range settled {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("delivery %d was not settled", i+1)
		}
	}
	if got := maxActive.Load(); got != concurrency {
		t.Fatalf("maximum active handlers = %d, want %d", got, concurrency)
	}
	if err := instance.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}
