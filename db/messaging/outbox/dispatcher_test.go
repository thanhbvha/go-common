package outbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
)

type publisherFunc func(context.Context, messaging.Message) error

func (f publisherFunc) Publish(ctx context.Context, m messaging.Message) error { return f(ctx, m) }

type fakeStore struct {
	mu                       sync.Mutex
	claims                   []Claim
	claimError               error
	published, retried, dead int
	settleError              error
	observed                 chan string
}

func (f *fakeStore) Claim(_ context.Context, r ClaimRequest) ([]Claim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Limit != 1 {
		return nil, messaging.ErrInvalidConfig
	}
	if f.claimError != nil {
		return nil, f.claimError
	}
	if len(f.claims) == 0 {
		return nil, nil
	}
	c := f.claims[0]
	f.claims = f.claims[1:]
	return []Claim{c}, nil
}
func (f *fakeStore) finish(kind string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch kind {
	case "published":
		f.published++
	case "retry":
		f.retried++
	case "dead":
		f.dead++
	}
	if f.observed != nil {
		f.observed <- kind
	}
	return f.settleError
}
func (f *fakeStore) MarkPublished(context.Context, Claim) error { return f.finish("published") }
func (f *fakeStore) Retry(context.Context, Claim, time.Duration, string) error {
	return f.finish("retry")
}
func (f *fakeStore) Quarantine(context.Context, Claim, string) error { return f.finish("dead") }
func fastConfig() Config {
	c := DefaultConfig()
	c.PollInterval = time.Millisecond
	c.RetryMin = time.Millisecond
	c.RetryMax = 2 * time.Millisecond
	return c
}
func waitEvent(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not progress")
		return ""
	}
}

func TestDispatcherPublicationAndRetryDecisions(t *testing.T) {
	for _, scenario := range []struct {
		name      string
		attempt   int
		pubErr    error
		settleErr error
		want      string
	}{{"accepted", 1, nil, nil, "published"}, {"ambiguous acceptance", 1, context.DeadlineExceeded, nil, "retry"}, {"exhausted", 20, errors.New("broker down"), nil, "dead"}, {"lost settlement response", 1, nil, errors.New("DB down"), "published"}, {"lease lost", 1, nil, messaging.ErrLeaseLost, "published"}} {
		t.Run(scenario.name, func(t *testing.T) {
			m := testMessage()
			f := &fakeStore{claims: []Claim{{Message: m, Attempt: scenario.attempt, Token: "token"}}, settleError: scenario.settleErr, observed: make(chan string, 2)}
			d, err := NewDispatcher(f, publisherFunc(func(_ context.Context, got messaging.Message) error {
				if got.ID != m.ID {
					t.Error("identity changed")
				}
				return scenario.pubErr
			}), fastConfig())
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- d.Run(t.Context()) }()
			if got := waitEvent(t, f.observed); got != scenario.want {
				t.Fatal(got)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := d.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDispatcherDrainKeepsAcceptedWork(t *testing.T) {
	f := &fakeStore{claims: []Claim{{Message: testMessage(), Attempt: 1}}, observed: make(chan string, 2)}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	d, _ := NewDispatcher(f, publisherFunc(func(ctx context.Context, _ messaging.Message) error {
		calls.Add(1)
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), fastConfig())
	run := make(chan error, 1)
	go func() { run <- d.Run(t.Context()) }()
	<-started
	drain := make(chan error, 1)
	go func() { drain <- d.Drain(t.Context()) }()
	select {
	case err := <-drain:
		t.Fatal("drain returned before accepted work", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-drain; err != nil {
		t.Fatal(err)
	}
	if err := <-run; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("extra publication")
	}
}
func TestDispatcherForcedDrainAndStartGuards(t *testing.T) {
	f := &fakeStore{claims: []Claim{{Message: testMessage(), Attempt: 1}}, observed: make(chan string, 2)}
	started := make(chan struct{})
	d, _ := NewDispatcher(f, publisherFunc(func(ctx context.Context, _ messaging.Message) error { close(started); <-ctx.Done(); return ctx.Err() }), fastConfig())
	done := make(chan error, 1)
	go func() { done <- d.Run(t.Context()) }()
	<-started
	if err := d.Run(t.Context()); !errors.Is(err, messaging.ErrAlreadyRunning) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := d.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := d.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := d.Run(t.Context()); !errors.Is(err, messaging.ErrClosed) {
		t.Fatal(err)
	}
}
func TestDispatcherFatalClaimAndPanic(t *testing.T) {
	for _, panicPublish := range []bool{false, true} {
		f := &fakeStore{claimError: messaging.ErrSchema}
		if panicPublish {
			f.claimError = nil
			f.claims = []Claim{{Message: testMessage(), Attempt: 1}}
		}
		d, _ := NewDispatcher(f, publisherFunc(func(context.Context, messaging.Message) error { panic("private details") }), fastConfig())
		err := d.Run(t.Context())
		if !errors.Is(err, messaging.ErrSchema) && !errors.Is(err, messaging.ErrInfrastructurePanic) {
			t.Fatal(err)
		}
	}
}
func TestDispatcherRecoversClaimOutage(t *testing.T) {
	f := &fakeStore{claimError: errors.New("temporary DB outage"), observed: make(chan string, 2)}
	d, _ := NewDispatcher(f, publisherFunc(func(context.Context, messaging.Message) error { return nil }), fastConfig())
	done := make(chan error, 1)
	go func() { done <- d.Run(t.Context()) }()
	f.mu.Lock()
	f.claimError = nil
	f.claims = []Claim{{Message: testMessage(), Attempt: 1}}
	f.mu.Unlock()
	waitEvent(t, f.observed)
	if err := d.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func testMessage() messaging.Message {
	return messaging.Message{ID: "stable", Namespace: "test", Kind: messaging.Event, Type: "created", SchemaVersion: 1, Payload: []byte(`{"id":9223372036854775807,"amount":1}`)}
}
