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

type supervisorTransport struct {
	benchmarkTransport
	consume func(context.Context, transport.Subscription, func(transport.Delivery)) error
}

func (t *supervisorTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	return t.consume(ctx, sub, deliver)
}

func startTestSupervisor(t *testing.T, backend *supervisorTransport, handler Handler) (*engine, <-chan struct{}) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Concurrency = 1
	e := newEngine(cfg, backend, NewCodec(nil))
	if err := e.Register("supervised", handler); err != nil {
		t.Fatal(err)
	}
	atomic.StoreInt32(&e.state, stateRunning)
	done := make(chan struct{})
	e.wg.Add(1)
	go func() {
		e.consumeLoop(TaskTopology, "supervised", "", handler)
		close(done)
	}()
	t.Cleanup(e.cancel)
	return e, done
}

// DS-74: cancellation interrupts reconnect waits and every session return,
// including nil and session-local cancellation, is supervised without spinning.
func TestSupervisorReconnectAndCancellation(t *testing.T) {
	for _, sessionErr := range []error{errors.New("disconnected"), nil, context.Canceled} {
		t.Run(stringError(sessionErr), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				backend := &supervisorTransport{consume: func(context.Context, transport.Subscription, func(transport.Delivery)) error {
					calls.Add(1)
					return sessionErr
				}}
				e, done := startTestSupervisor(t, backend, func(context.Context, Job) error { return nil })
				synctest.Wait()
				if got := calls.Load(); got != 1 {
					t.Fatalf("sessions before backoff = %d, want 1", got)
				}
				// Virtual time: first jittered backoff is 100–120ms, the next
				// is at least 200ms. No wall-clock sleep or broker is involved.
				time.Sleep(150 * time.Millisecond)
				synctest.Wait()
				if got := calls.Load(); got != 2 {
					t.Fatalf("sessions after first backoff = %d, want 2", got)
				}
				e.cancel()
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Fatal("cancellation did not interrupt reconnect backoff")
				}
			})
		})
	}
}

func stringError(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}

// DS-40/DS-74: a replacement session cannot overlap workers or heartbeats
// belonging to the failed session; accepted deliveries finish settlement first.
func TestSupervisorJoinsSessionBeforeReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		payload, _, err := NewCodec(nil).EncodeTopology(TaskTopology, "supervised", 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		delivery := &workerPoolDelivery{
			message: transport.Message{Payload: payload},
			settled: make(chan struct{}),
		}
		var calls atomic.Int32
		backend := &supervisorTransport{consume: func(ctx context.Context, _ transport.Subscription, deliver func(transport.Delivery)) error {
			if calls.Add(1) == 1 {
				deliver(delivery)
				return errors.New("session lost")
			}
			select {
			case <-delivery.settled:
			default:
				t.Error("reconnected before previous delivery settled")
			}
			<-ctx.Done()
			return ctx.Err()
		}}
		release := make(chan struct{})
		e, done := startTestSupervisor(t, backend, func(context.Context, Job) error {
			<-release
			return nil
		})
		synctest.Wait()
		time.Sleep(time.Second)
		if got := calls.Load(); got != 1 {
			t.Fatalf("sessions while previous worker is active = %d, want 1", got)
		}
		close(release)
		synctest.Wait()
		time.Sleep(150 * time.Millisecond)
		synctest.Wait()
		if got := calls.Load(); got != 2 {
			t.Fatalf("sessions after worker cleanup = %d, want 2", got)
		}
		e.cancel()
		<-done
	})
}
