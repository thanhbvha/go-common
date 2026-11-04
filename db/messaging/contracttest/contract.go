// Package contracttest supplies a reusable transaction/ownership test suite for
// messaging database drivers. It is test support, not a runtime dependency.
package contracttest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
)

type Store interface {
	outbox.Store
	outbox.Admin
}
type Fixture[T any] struct {
	Namespace string
	Store     Store
	Inbox     inbox.Processor[T]
	Within    func(context.Context, func(T) error) error
	Append    func(T, messaging.Message) error
	Effect    func(T, string) error
	Count     func(context.Context, string) int
	// Expire changes only an isolated fixture's DB clock bookkeeping.
	Expire func(context.Context, messaging.Identity)
}

// Run requires fresh isolated fixtures per scenario, with migration already
// applied. PostgreSQL, MySQL and MongoDB drivers run this same suite.
func Run[T any](t *testing.T, fixture func(*testing.T) Fixture[T]) {
	t.Helper()
	message := func(f Fixture[T], id string) messaging.Message {
		return messaging.Message{ID: id, Namespace: f.Namespace, TenantID: "tenant-a", Kind: messaging.Event, Type: "order.created", SchemaVersion: 1, Payload: []byte(`{"id":9223372036854775807,"amount":2}`)}
	}
	appendMessage := func(t *testing.T, f Fixture[T], m messaging.Message) {
		t.Helper()
		if err := f.Within(t.Context(), func(tx T) error { return f.Append(tx, m) }); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("atomic producer rollback and immutable replay", func(t *testing.T) {
		f := fixture(t)
		ctx := t.Context()
		m := message(f, "atomic")
		sentinel := errors.New("rollback")
		err := f.Within(ctx, func(tx T) error {
			if err := f.Effect(tx, "atomic"); err != nil {
				return err
			}
			if err := f.Append(tx, m); err != nil {
				return err
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) || f.Count(ctx, "atomic") != 0 {
			t.Fatal("business rollback failed", err)
		}
		if _, err := f.Store.Get(ctx, m.TenantID, m.ID); !errors.Is(err, messaging.ErrNotFound) {
			t.Fatal("outbox rollback failed", err)
		}
		if err := f.Within(ctx, func(tx T) error {
			if err := f.Effect(tx, "atomic"); err != nil {
				return err
			}
			return f.Append(tx, m)
		}); err != nil {
			t.Fatal(err)
		}
		appendMessage(t, f, m)
		if f.Count(ctx, "atomic") != 1 {
			t.Fatal("business commit failed")
		}
		changed := m
		changed.Payload = []byte(`{"amount":3}`)
		if err := f.Within(ctx, func(tx T) error { return f.Append(tx, changed) }); !errors.Is(err, messaging.ErrIdentityConflict) {
			t.Fatal("identity conflict not rejected", err)
		}
		changed = m
		changed.Headers = map[string]string{"destination": "different"}
		if err := f.Within(ctx, func(tx T) error { return f.Append(tx, changed) }); !errors.Is(err, messaging.ErrIdentityConflict) {
			t.Fatal("routing conflict not rejected", err)
		}
	})
	t.Run("concurrent inbox and valid redelivery", func(t *testing.T) {
		f := fixture(t)
		ctx := t.Context()
		m := message(f, "inbox")
		hash, _ := m.Fingerprint()
		receipt := inbox.Receipt{Identity: m.Identity(), Consumer: "accounting", Fingerprint: hash}
		var wg sync.WaitGroup
		failures := make(chan error, 12)
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := f.Inbox.Consume(ctx, receipt, func(tx T) error { return f.Effect(tx, "inbox") })
				if err != nil {
					failures <- err
				}
			}()
		}
		wg.Wait()
		close(failures)
		for err := range failures {
			t.Fatal(err)
		}
		if f.Count(ctx, "inbox") != 1 {
			t.Fatal("duplicate business execution")
		}
		r, err := f.Inbox.Consume(ctx, receipt, func(T) error { t.Error("committed replay invoked handler"); return nil })
		if err != nil || r != inbox.Duplicate {
			t.Fatal(r, err)
		}
		changed := receipt
		changed.Fingerprint = fmt.Sprintf("%064d", 1)
		if _, err := f.Inbox.Consume(ctx, changed, func(T) error { return nil }); !errors.Is(err, messaging.ErrIdentityConflict) {
			t.Fatal("receipt conflict not rejected", err)
		}
		for _, r := range []inbox.Receipt{{Identity: messaging.Identity{Namespace: f.Namespace, TenantID: "tenant-b", ID: m.ID}, Consumer: receipt.Consumer, Fingerprint: hash}, {Identity: m.Identity(), Consumer: "shipping", Fingerprint: hash}} {
			if _, err := f.Inbox.Consume(ctx, r, func(tx T) error { return f.Effect(tx, "inbox") }); err != nil {
				t.Fatal(err)
			}
		}
		if f.Count(ctx, "inbox") != 3 {
			t.Fatal("tenant/consumer isolation failed")
		}
	})
	t.Run("consumer rollback panic cancel and next outbox", func(t *testing.T) {
		f := fixture(t)
		ctx := t.Context()
		m := message(f, "rollback")
		hash, _ := m.Fingerprint()
		r := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: hash}
		sentinel := errors.New("business failure")
		if _, err := f.Inbox.Consume(ctx, r, func(tx T) error {
			if err := f.Effect(tx, "rollback"); err != nil {
				return err
			}
			return sentinel
		}); !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Error("panic did not propagate")
				}
			}()
			_, _ = f.Inbox.Consume(ctx, r, func(tx T) error {
				if err := f.Effect(tx, "rollback"); err != nil {
					return err
				}
				panic("fixture panic")
			})
		}()
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := f.Inbox.Consume(cancelled, r, func(T) error { t.Error("canceled handler called"); return nil }); err == nil {
			t.Fatal("cancellation ignored")
		}
		if f.Count(ctx, "rollback") != 0 {
			t.Fatal("failed handler committed")
		}
		result, err := f.Inbox.Consume(ctx, r, func(tx T) error {
			if err := f.Effect(tx, "rollback"); err != nil {
				return err
			}
			return f.Append(tx, message(f, "next"))
		})
		if err != nil || result != inbox.Applied {
			t.Fatal(result, err)
		}
		if _, err := f.Store.Get(ctx, m.TenantID, "next"); err != nil {
			t.Fatal("next outbox missing", err)
		}
	})
	t.Run("claim fencing recovery exhaustion and replay", func(t *testing.T) {
		f := fixture(t)
		ctx := t.Context()
		m := message(f, "crash")
		appendMessage(t, f, m)
		req := outbox.ClaimRequest{Limit: 1, MaxAttempts: 2, Lease: time.Minute}
		claims, err := f.Store.Claim(ctx, req)
		if err != nil || len(claims) != 1 {
			t.Fatal(claims, err)
		}
		first := claims[0]
		if other, err := f.Store.Claim(ctx, req); err != nil || len(other) != 0 {
			t.Fatal("live claim stolen", other, err)
		}
		f.Expire(ctx, m.Identity())
		if err := f.Store.MarkPublished(ctx, first); !errors.Is(err, messaging.ErrLeaseLost) {
			t.Fatal("expired claim settled", err)
		}
		claims, err = f.Store.Claim(ctx, req)
		if err != nil || len(claims) != 1 || claims[0].Attempt != 2 || claims[0].Token == first.Token {
			t.Fatal(claims, err)
		}
		if err := f.Store.Retry(ctx, first, 0, "failure"); !errors.Is(err, messaging.ErrLeaseLost) {
			t.Fatal("old token changed new claim", err)
		}
		f.Expire(ctx, m.Identity())
		_, err = f.Store.Claim(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		record, err := f.Store.Get(ctx, m.TenantID, m.ID)
		if err != nil || record.State != outbox.Dead {
			t.Fatal(record, err)
		}
		count, err := f.Store.Replay(ctx, m.TenantID, []string{m.ID})
		if err != nil || count != 1 {
			t.Fatal(count, err)
		}
		record, err = f.Store.Get(ctx, m.TenantID, m.ID)
		if err != nil || record.ReplayCount != 1 || record.ReplayedAt == nil || record.Attempts != 0 {
			t.Fatal(record, err)
		}
		claims, err = f.Store.Claim(ctx, req)
		if err != nil || len(claims) != 1 || claims[0].Message.ID != m.ID {
			t.Fatal(claims, err)
		}
		if err := f.Store.MarkPublished(ctx, claims[0]); err != nil {
			t.Fatal(err)
		}
		if count, err := f.Store.Replay(ctx, m.TenantID, []string{m.ID}); err != nil || count != 0 {
			t.Fatal("published row replayed", count, err)
		}
	})
	t.Run("parallel claims do not overlap", func(t *testing.T) {
		f := fixture(t)
		for i := 0; i < 16; i++ {
			appendMessage(t, f, message(f, fmt.Sprintf("id-%02d", i)))
		}
		var wg sync.WaitGroup
		results := make(chan []outbox.Claim, 8)
		failures := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, e := f.Store.Claim(t.Context(), outbox.ClaimRequest{Limit: 2, MaxAttempts: 20, Lease: time.Minute})
				if e != nil {
					failures <- e
				}
				results <- c
			}()
		}
		wg.Wait()
		close(results)
		close(failures)
		for err := range failures {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for batch := range results {
			for _, c := range batch {
				if seen[c.Message.ID] {
					t.Fatal("overlapping claim")
				}
				seen[c.Message.ID] = true
			}
		}
		if len(seen) != 16 {
			t.Fatal("missing claims", len(seen))
		}
	})
}
