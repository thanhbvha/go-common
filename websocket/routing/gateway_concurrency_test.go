package routing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type concurrentRegistry struct {
	SessionRegistry
	join   func(context.Context, Location) error
	remove func(context.Context, Location) error
	renew  func(context.Context, []Location) error
}

func (r *concurrentRegistry) Register(_ context.Context, l Location, _ time.Duration) (Location, error) {
	return l, nil
}
func (r *concurrentRegistry) Renew(ctx context.Context, ls []Location, _ time.Duration) error {
	if r.renew != nil {
		return r.renew(ctx, ls)
	}
	return nil
}
func (r *concurrentRegistry) JoinRoom(ctx context.Context, l Location, _ string, _ time.Duration) error {
	if r.join != nil {
		return r.join(ctx, l)
	}
	return nil
}
func (*concurrentRegistry) LeaveRoom(context.Context, Location, string) error { return nil }
func (r *concurrentRegistry) Remove(ctx context.Context, l Location) error {
	if r.remove != nil {
		return r.remove(ctx, l)
	}
	return nil
}
func concurrencyGateway(t *testing.T, r SessionRegistry, limit int) *Gateway {
	t.Helper()
	g, err := NewGateway(GatewayConfig{Registry: r, Transport: readyTransport{}, RegistryConcurrency: limit})
	if err != nil {
		t.Fatal(err)
	}
	g.leaseUntil.Store(time.Now().Add(time.Hour).UnixNano())
	t.Cleanup(g.cancel)
	return g
}
func registerConcurrentSession(t *testing.T, g *Gateway, id string) Location {
	t.Helper()
	l, err := g.Register(context.Background(), Location{TenantID: "t", UserID: "u", SessionID: id}, func([]byte) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestSessionLockWaitHonorsDeadlineAndDoesNotBlockOtherSessions(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	r := &concurrentRegistry{join: func(ctx context.Context, _ Location) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	g := concurrencyGateway(t, r, 4)
	one, two := registerConcurrentSession(t, g, "one"), registerConcurrentSession(t, g, "two")
	joined := make(chan error, 1)
	go func() { joined <- g.JoinRoom(context.Background(), one, "room") }()
	<-entered
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.Remove(ctx, one) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Remove ignored lock-wait deadline")
	}
	if g.Stats().Sessions != 1 {
		t.Fatal("canceled cleanup did not detach locally")
	}
	if err := g.Remove(ctx, two); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	// Use a fresh budget: this session must not wait for the blocked join.
	if err := g.Remove(context.Background(), two); err != nil {
		t.Fatal(err)
	}
	// Do not wait for joined until release is closed by the defer.
	t.Cleanup(func() {
		if err := <-joined; !errors.Is(err, ErrStale) {
			t.Errorf("late join=%v", err)
		}
	})
}

func TestRegistryCleanupRunsConcurrentlyWithinLimit(t *testing.T) {
	entered, release := make(chan struct{}, 8), make(chan struct{})
	var active, maximum atomic.Int32
	r := &concurrentRegistry{remove: func(ctx context.Context, _ Location) error {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			old := maximum.Load()
			if n <= old || maximum.CompareAndSwap(old, n) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	g := concurrencyGateway(t, r, 4)
	var ls []Location
	for i := range 8 {
		ls = append(ls, registerConcurrentSession(t, g, fmt.Sprint(i)))
	}
	var wg sync.WaitGroup
	for _, l := range ls {
		wg.Go(func() {
			if err := g.Remove(context.Background(), l); err != nil {
				t.Error(err)
			}
		})
	}
	for range 4 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			wg.Wait()
			t.Fatal("independent cleanup serialized")
		}
	}
	if maximum.Load() != 4 {
		t.Errorf("peak registry concurrency=%d", maximum.Load())
	}
	close(release)
	wg.Wait()
	if maximum.Load() > 4 || g.Stats().Sessions != 0 {
		t.Fatal("registry limit or local cleanup failed")
	}
}

func TestRenewAndRemoveKeepRemoteOperationsOrdered(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var renewing atomic.Bool
	r := &concurrentRegistry{renew: func(_ context.Context, ls []Location) error {
		if len(ls) > 0 {
			renewing.Store(true)
			close(entered)
			<-release
			renewing.Store(false)
		}
		return nil
	}, remove: func(context.Context, Location) error {
		if renewing.Load() {
			t.Error("remove raced in-flight renewal")
		}
		return nil
	}}
	g := concurrencyGateway(t, r, 4)
	l := registerConcurrentSession(t, g, "one")
	renewed := make(chan struct{})
	go func() { g.renew(); close(renewed) }()
	<-entered
	removed := make(chan error, 1)
	go func() { removed <- g.Remove(context.Background(), l) }()
	deadline := time.After(time.Second)
	for g.Stats().Sessions != 0 {
		select {
		case <-deadline:
			close(release)
			t.Fatal("local removal waited on renewal")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(release)
	<-renewed
	if err := <-removed; err != nil {
		t.Fatal(err)
	}
	if g.Stats().Sessions != 0 {
		t.Fatal("renewal restored removed local session")
	}
}

func deliveryCommand(g *Gateway, l Location, id string) DeliveryCommand {
	return DeliveryCommand{Version: 1, MessageID: id, TenantID: l.TenantID, TargetNode: g.NodeID(), Sessions: []Location{l}, Payload: []byte(id), ExpiresAt: time.Now().Add(time.Second)}
}

func TestGatewayEnqueueDoesNotHoldIndexOrOtherSessionLocks(t *testing.T) {
	g := concurrencyGateway(t, &concurrentRegistry{}, 4)
	entered, release := make(chan struct{}), make(chan struct{})
	l, err := g.Register(context.Background(), Location{TenantID: "t", UserID: "u", SessionID: "slow"}, func([]byte) error { close(entered); <-release; return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	other := registerConcurrentSession(t, g, "other")
	done := make(chan DeliveryResult, 1)
	go func() { done <- g.Handle(context.Background(), deliveryCommand(g, l, "one")) }()
	<-entered
	finished := make(chan struct{})
	go func() {
		g.Stats()
		res := g.Handle(context.Background(), deliveryCommand(g, other, "two"))
		if res.Count(Enqueued) != 1 {
			t.Error(res)
		}
		if err := g.Remove(context.Background(), l); err != nil {
			t.Error(err)
		}
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		close(release)
		<-done
		t.Fatal("enqueue held a shared gateway lock")
	}
	close(release)
	<-done
}

func TestGatewayConcurrentDedupeAndRejectedEnqueueRetry(t *testing.T) {
	g := concurrencyGateway(t, &concurrentRegistry{}, 4)
	var calls atomic.Int32
	l, err := g.Register(context.Background(), Location{TenantID: "t", UserID: "u", SessionID: "s"}, func([]byte) error {
		if calls.Add(1) == 1 {
			return ErrQueueFull
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	c := deliveryCommand(g, l, "stable")
	if g.Handle(context.Background(), c).Count(QueueFull) != 1 {
		t.Fatal("expected rejection")
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if g.Handle(context.Background(), c).Count(Enqueued) != 1 {
				t.Error("duplicate did not report acceptance")
			}
		})
	}
	wg.Wait()
	if calls.Load() != 2 || g.Stats().Enqueued != 1 {
		t.Fatalf("duplicate/rejected enqueue accounting: calls=%d stats=%+v", calls.Load(), g.Stats())
	}
	replacement := registerConcurrentSession(t, g, "s")
	if g.Handle(context.Background(), deliveryCommand(g, replacement, "stable")).Count(Enqueued) != 1 {
		t.Fatal("dedupe crossed generations")
	}
}

type closingRegistry struct {
	concurrentRegistry
	entered, release chan struct{}
	removed          atomic.Int32
}

func (r *closingRegistry) Register(_ context.Context, l Location, _ time.Duration) (Location, error) {
	close(r.entered)
	<-r.release // Model a remote commit whose response arrives after cancellation.
	return l, nil
}
func (r *closingRegistry) Remove(context.Context, Location) error { r.removed.Add(1); return nil }

type closeTransport struct {
	DeliveryTransport
	closes atomic.Int32
}

func (*closeTransport) Ready() bool                                          { return true }
func (*closeTransport) Start(context.Context, string, DeliveryHandler) error { return nil }
func (t *closeTransport) Close(context.Context) error                        { t.closes.Add(1); return nil }

func TestGatewayCloseWaitsForRegistrationAndCleansLateCommit(t *testing.T) {
	r := &closingRegistry{entered: make(chan struct{}), release: make(chan struct{})}
	transport := &closeTransport{}
	g, err := NewGateway(GatewayConfig{Registry: r, Transport: transport, LeaseTTL: 2 * time.Hour, RenewInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err = g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	registered := make(chan error, 1)
	go func() {
		_, err := g.Register(context.Background(), Location{TenantID: "t", UserID: "u", SessionID: "s"}, func([]byte) error { return nil }, nil)
		registered <- err
	}()
	<-r.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err = g.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		close(r.release)
		t.Fatalf("close did not wait: %v", err)
	}
	if transport.closes.Load() != 0 {
		t.Error("transport closed before in-flight registry operation settled")
	}
	close(r.release)
	if err = <-registered; !errors.Is(err, context.Canceled) {
		t.Fatalf("late registration succeeded: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	if err = g.Close(ctx2); err != nil {
		t.Fatal(err)
	}
	if g.Stats().Sessions != 0 || r.removed.Load() != 1 {
		t.Fatalf("late commit leaked: sessions=%d removed=%d", g.Stats().Sessions, r.removed.Load())
	}
	if _, err = g.Register(ctx2, Location{}, func([]byte) error { return nil }, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatal("closed gateway accepted new work")
	}
}

func TestRenewSkipsBusySessionAndExpiredLocalLeaseRejectsDelivery(t *testing.T) {
	var renewed []Location
	r := &concurrentRegistry{renew: func(_ context.Context, ls []Location) error { renewed = append(renewed, ls...); return nil }}
	g := concurrencyGateway(t, r, 4)
	busy, live := registerConcurrentSession(t, g, "busy"), registerConcurrentSession(t, g, "live")
	unlock, err := g.sessionLocks.acquire(context.Background(), busy.Key())
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	g.mu.Lock()
	g.sessions[busy.Key()].leaseUntil.Store(time.Now().Add(-time.Second).UnixNano())
	g.mu.Unlock()
	g.renew()
	if len(renewed) != 1 || renewed[0].Generation != live.Generation || !g.Ready() {
		t.Fatalf("busy session blocked unrelated renewal: %+v", renewed)
	}
	if g.Handle(context.Background(), deliveryCommand(g, busy, "expired")).Count(Unavailable) != 1 {
		t.Fatal("expired busy session accepted delivery")
	}
	if g.Handle(context.Background(), deliveryCommand(g, live, "live")).Count(Enqueued) != 1 {
		t.Fatal("healthy session lost delivery")
	}
}

func TestRenewAllBusyStillRequiresRegistryHealth(t *testing.T) {
	r := &concurrentRegistry{renew: func(context.Context, []Location) error { return ErrUnavailable }}
	g := concurrencyGateway(t, r, 4)
	l := registerConcurrentSession(t, g, "busy")
	unlock, err := g.sessionLocks.acquire(context.Background(), l.Key())
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	before := g.leaseUntil.Load()
	g.renew()
	if g.leaseUntil.Load() != before || g.Stats().LeaseFailures != 1 {
		t.Fatal("skipped sessions were mistaken for a successful registry probe")
	}
}
