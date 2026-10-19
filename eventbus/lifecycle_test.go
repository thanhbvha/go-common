package eventbus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type lifecycleTransport struct {
	benchmarkTransport
	ensure             func(context.Context, transport.Topology) error
	close              func(context.Context) error
	consumes           atomic.Int32
	closes             atomic.Int32
	provisions         atomic.Int32
	expectedProvisions atomic.Int32
	prematureConsume   atomic.Bool
}

func (b *lifecycleTransport) EnsureTopology(ctx context.Context, topo transport.Topology) error {
	if b.ensure != nil {
		return b.ensure(ctx, topo)
	}
	return nil
}

func (b *lifecycleTransport) Consume(ctx context.Context, _ transport.Subscription, _ func(transport.Delivery)) error {
	if expected := b.expectedProvisions.Load(); expected > 0 && b.provisions.Load() < expected {
		b.prematureConsume.Store(true)
	}
	b.consumes.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func (b *lifecycleTransport) EnsureSubscription(context.Context, transport.Subscription) error {
	b.provisions.Add(1)
	return nil
}

func (b *lifecycleTransport) Close(ctx context.Context) error {
	b.closes.Add(1)
	if b.close != nil {
		return b.close(ctx)
	}
	return nil
}

func TestStartProvisionsEverySubscriptionBeforeConsumers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := &lifecycleTransport{}
		backend.expectedProvisions.Store(2)
		b := lifecycleBus(t, backend)
		if err := b.DeclareEvent("event"); err != nil {
			t.Fatal(err)
		}
		if err := b.Subscribe("event", "subscriber", func(context.Context, Job) error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := b.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if backend.provisions.Load() != 2 || backend.consumes.Load() != 2 || backend.prematureConsume.Load() {
			t.Fatalf("provisions=%d consumes=%d premature=%t", backend.provisions.Load(), backend.consumes.Load(), backend.prematureConsume.Load())
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func lifecycleBus(t *testing.T, backend *lifecycleTransport) Bus {
	t.Helper()
	b, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Register("task", func(context.Context, Job) error { return nil }); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	return b
}

// DS-70: no consumer runs before every topology is provisioned, and a failure
// or canceled startup leaves a retryable bus, including after partial success.
func TestStartProvisionFailureIsRetryable(t *testing.T) {
	for _, kind := range []TopologyKind{TaskTopology, EventTopology} {
		t.Run(kind.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				failure := errors.New("provision failed")
				fail := true
				backend := &lifecycleTransport{ensure: func(_ context.Context, topo transport.Topology) error {
					if fail && topo.Kind == kind {
						return failure
					}
					return nil
				}}
				b := lifecycleBus(t, backend)
				if err := b.DeclareEvent("event"); err != nil {
					t.Fatal(err)
				}
				if err := b.Start(context.Background()); !errors.Is(err, failure) {
					t.Fatalf("Start = %v, want provisioning error", err)
				}
				synctest.Wait()
				if backend.consumes.Load() != 0 {
					t.Fatal("consumer started before provisioning completed")
				}
				if err := b.DeclareTask("after_failure"); err != nil {
					t.Fatal(err)
				}
				fail = false
				if err := b.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				synctest.Wait()
				if backend.consumes.Load() != 1 {
					t.Fatal("retry did not start exactly one consumer")
				}
				if err := b.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestConcurrentStartAndRegistrationDuringProvision(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		backend := &lifecycleTransport{ensure: func(context.Context, transport.Topology) error {
			<-release
			return nil
		}}
		b := lifecycleBus(t, backend)
		result := make(chan error, 1)
		go func() { result <- b.Start(context.Background()) }()
		synctest.Wait()
		if err := b.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("concurrent Start = %v", err)
		}
		for _, register := range []func() error{
			func() error { return b.DeclareTask("late") },
			func() error { return b.DeclareEvent("late") },
			func() error { return b.HandleTask("task", func(context.Context, Job) error { return nil }) },
			func() error { return b.Register("late", func(context.Context, Job) error { return nil }) },
			func() error {
				return b.Subscribe("late", "subscriber", func(context.Context, Job) error { return nil })
			},
		} {
			if err := register(); !errors.Is(err, ErrAlreadyStarted) {
				t.Fatalf("registration during Start = %v", err)
			}
		}
		close(release)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCloseInterruptsStartup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := &lifecycleTransport{ensure: func(ctx context.Context, _ transport.Topology) error {
			<-ctx.Done()
			return ctx.Err()
		}}
		b := lifecycleBus(t, backend)
		result := make(chan error, 1)
		go func() { result <- b.Start(context.Background()) }()
		synctest.Wait()
		if err := b.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("interrupted Start = %v", err)
		}
		if backend.consumes.Load() != 0 {
			t.Fatal("consumer started after shutdown")
		}
		if err := b.Start(context.Background()); !errors.Is(err, ErrBusClosed) {
			t.Fatalf("Start after Close = %v", err)
		}
	})
}

func TestConcurrentShutdownSharesResult(t *testing.T) {
	for _, started := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_start", true: "running"}[started], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				failure := errors.New("close failed")
				backend := &lifecycleTransport{close: func(context.Context) error { <-release; return failure }}
				b := lifecycleBus(t, backend)
				if started {
					if err := b.Start(context.Background()); err != nil {
						t.Fatal(err)
					}
				}
				results := make(chan error, 2)
				go func() { results <- b.Stop(context.Background()) }()
				go func() { results <- b.Close(context.Background()) }()
				synctest.Wait()
				if backend.closes.Load() != 1 {
					t.Fatal("concurrent shutdown did not share one Close")
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := b.Close(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled waiter = %v", err)
				}
				if err := b.Start(context.Background()); !errors.Is(err, ErrBusClosed) {
					t.Fatalf("Start during shutdown = %v", err)
				}
				close(release)
				for range 2 {
					if err := <-results; !errors.Is(err, failure) {
						t.Fatalf("shutdown = %v", err)
					}
				}
				if err := b.Close(context.Background()); !errors.Is(err, failure) {
					t.Fatalf("repeated Close = %v", err)
				}
				if backend.closes.Load() != 1 {
					t.Fatal("transport closed more than once")
				}
			})
		})
	}
}

func TestCanceledStartIsRetryable(t *testing.T) {
	b := lifecycleBus(t, &lifecycleTransport{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Start(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start = %v", err)
	}
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupContextCancellationDuringProvisionIsRetryable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fail := true
		backend := &lifecycleTransport{ensure: func(ctx context.Context, _ transport.Topology) error {
			if fail {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}}
		b := lifecycleBus(t, backend)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- b.Start(ctx) }()
		synctest.Wait()
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Start = %v", err)
		}
		fail = false
		if err := b.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := b.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
