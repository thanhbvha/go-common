package eventbus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type drainTransport struct {
	benchmarkTransport
	deliveries    []transport.Delivery
	intakeStopped chan struct{}
	closed        chan struct{}
	closes        atomic.Int32
}

type drainSettlementDelivery struct {
	workerPoolDelivery
	started   chan context.Context
	release   chan struct{}
	confirmed atomic.Bool
}

func (d *drainSettlementDelivery) Settle(ctx context.Context, _ transport.Decision) error {
	d.started <- ctx
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.release:
		d.confirmed.Store(true)
		return nil
	}
}

func (b *drainTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	for _, d := range b.deliveries {
		deliver(d)
	}
	<-ctx.Done()
	close(b.intakeStopped)
	// Model Kafka's session-owned settlement client: keep it alive until the
	// engine has joined handlers, without keeping intake alive.
	if sub.WaitForHandlers != nil {
		sub.WaitForHandlers()
	}
	return ctx.Err()
}

func (b *drainTransport) Close(context.Context) error {
	b.closes.Add(1)
	close(b.closed)
	return nil
}

func drainBus(t *testing.T, n int, handler Handler) (Bus, *drainTransport, []*workerPoolDelivery) {
	t.Helper()
	backend := &drainTransport{intakeStopped: make(chan struct{}), closed: make(chan struct{})}
	deliveries := make([]*workerPoolDelivery, n)
	for i := range n {
		payload, _, err := NewCodec(nil).EncodeTopology(TaskTopology, "drain", i, 3)
		if err != nil {
			t.Fatal(err)
		}
		d := &workerPoolDelivery{message: transport.Message{Payload: payload}, settled: make(chan struct{})}
		deliveries[i] = d
		backend.deliveries = append(backend.deliveries, d)
	}
	cfg := DefaultConfig()
	cfg.Concurrency = 1
	cfg.ProcessTimeout = time.Hour
	b, err := Open(cfg, WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.DeclareTask("drain"); err != nil {
		t.Fatal(err)
	}
	if err := b.HandleTask("drain", handler); err != nil {
		t.Fatal(err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return b, backend, deliveries
}

// DS-71/72: stop intake first, preserve handler contexts, drain accepted
// prefetch, and settle every delivery before transport/session cleanup.
func TestDrainPreservesHandlersAndSettlesPrefetch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		contexts := make(chan context.Context, 2)
		b, backend, deliveries := drainBus(t, 2, func(ctx context.Context, _ Job) error {
			contexts <- ctx
			<-release
			return nil
		})
		synctest.Wait()
		handlerCtx := <-contexts
		result := make(chan error, 1)
		go func() { result <- b.Drain(context.Background()) }()
		<-backend.intakeStopped
		synctest.Wait()
		if handlerCtx.Err() != nil {
			t.Fatal("drain canceled active handler")
		}
		select {
		case <-backend.closed:
			t.Fatal("transport closed before settlement")
		default:
		}
		close(release)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		for _, d := range deliveries {
			select {
			case <-d.settled:
			default:
				t.Fatal("accepted delivery not settled")
			}
		}
		if backend.closes.Load() != 1 {
			t.Fatal("transport not closed exactly once")
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

// DS-73: force-cancel only at the deadline, leave active and buffered sources
// unsettled, and never ACK a late success from an uncooperative handler.
func TestDrainDeadlineLeavesSourcesUnsettled(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		t.Run(map[bool]string{false: "cooperative", true: "late_success"}[lateSuccess], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				contexts := make(chan context.Context, 2)
				var calls atomic.Int32
				b, backend, deliveries := drainBus(t, 2, func(ctx context.Context, _ Job) error {
					calls.Add(1)
					contexts <- ctx
					if lateSuccess {
						<-release
						return nil
					}
					<-ctx.Done()
					return ctx.Err()
				})
				synctest.Wait()
				handlerCtx := <-contexts
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- b.Drain(ctx) }()
				<-backend.intakeStopped
				synctest.Wait()
				if handlerCtx.Err() != nil {
					t.Fatal("worker canceled before deadline")
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Drain = %v", err)
				}
				if handlerCtx.Err() == nil {
					t.Fatal("deadline did not cancel worker")
				}
				<-backend.closed
				close(release)
				synctest.Wait()
				if calls.Load() != 1 {
					t.Fatal("buffered handler executed after force cancellation")
				}
				for _, d := range deliveries {
					select {
					case <-d.settled:
						t.Fatal("forced delivery settled instead of remaining redeliverable")
					default:
					}
				}
				if err := b.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Close = %v", err)
				}
			})
		})
	}
}

func TestDrainWaiterCancellationDoesNotCancelWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		contexts := make(chan context.Context, 1)
		b, backend, _ := drainBus(t, 1, func(ctx context.Context, _ Job) error { contexts <- ctx; <-release; return nil })
		synctest.Wait()
		handlerCtx := <-contexts
		result := make(chan error, 1)
		go func() { result <- b.Drain(context.Background()) }()
		<-backend.intakeStopped
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := b.Stop(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Stop = %v", err)
		}
		if handlerCtx.Err() != nil {
			t.Fatal("secondary waiter canceled worker")
		}
		close(release)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})
}

// DS-41/42/72/73: graceful shutdown waits for broker confirmation, while a
// forced deadline cancels settlement and never records it as confirmed.
func TestDrainWaitsForSettlementOrCancelsAtDeadline(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed", true: "deadline"}[force], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				payload, _, err := NewCodec(nil).EncodeTopology(TaskTopology, "task", 1, 3)
				if err != nil {
					t.Fatal(err)
				}
				d := &drainSettlementDelivery{
					workerPoolDelivery: workerPoolDelivery{message: transport.Message{Payload: payload}},
					started:            make(chan context.Context, 1), release: make(chan struct{}),
				}
				backend := &drainTransport{deliveries: []transport.Delivery{d}, intakeStopped: make(chan struct{}), closed: make(chan struct{})}
				b, err := Open(DefaultConfig(), WithTransport(backend))
				if err != nil {
					t.Fatal(err)
				}
				if err := b.DeclareTask("task"); err != nil {
					t.Fatal(err)
				}
				if err := b.HandleTask("task", func(context.Context, Job) error { return nil }); err != nil {
					t.Fatal(err)
				}
				if err := b.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				settleCtx := <-d.started
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- b.Drain(ctx) }()
				<-backend.intakeStopped
				synctest.Wait()
				if settleCtx.Err() != nil {
					t.Fatal("intake cancellation interrupted settlement")
				}
				select {
				case <-backend.closed:
					t.Fatal("transport closed before settlement")
				default:
				}
				if force {
					time.Sleep(time.Second)
					synctest.Wait()
					if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("Drain = %v", err)
					}
					if settleCtx.Err() == nil || d.confirmed.Load() {
						t.Fatal("forced settlement was not canceled")
					}
					<-backend.closed
				} else {
					close(d.release)
					if err := <-result; err != nil {
						t.Fatal(err)
					}
					if !d.confirmed.Load() {
						t.Fatal("drain completed without broker confirmation")
					}
				}
			})
		})
	}
}
