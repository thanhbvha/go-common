package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// A bounded, concurrency-safe hook. Do not retain an unbounded event history,
// write to a non-thread-safe output stream or call shutdown from this callback.
type exampleMetrics struct {
	started       atomic.Uint64
	attempts      atomic.Uint64
	finished      atomic.Uint64
	unknown       atomic.Uint64
	durationNanos atomic.Int64
}

func (m *exampleMetrics) record(event mongodb.TransactionEvent) {
	switch event.Phase {
	case "started":
		m.started.Add(1)
	case "attempt":
		m.attempts.Add(1)
	case "finished":
		m.finished.Add(1)
		m.durationNanos.Add(int64(event.Duration))
		if event.Outcome == "unknown" {
			m.unknown.Add(1)
		}
	}
}

func (s *exampleState) observability(ctx context.Context) error {
	db, err := s.manager.Lookup("orders")
	if err != nil {
		return err
	}
	if err := db.CreateCollection(ctx, "observability_records"); err != nil {
		return err
	}
	statsBefore := s.manager.TransactionStats()
	startedBefore, attemptsBefore, finishedBefore := s.metrics.started.Load(), s.metrics.attempts.Load(), s.metrics.finished.Load()
	durationBefore := s.metrics.durationNanos.Load()
	// Successful read transaction.
	if err := s.manager.WithTransactionAcross(ctx, []string{"orders"}, func(tx *mongodb.Transaction) error {
		r, err := mongodb.NewTransactionRepository[Sample](tx, "orders", "samples")
		if err != nil {
			return err
		}
		_, err = r.Count(tx.Context(), bson.M{"group": "fixture"})
		return err
	}); err != nil {
		return err
	}
	// Business failure rolls back a real write and records failed, not committed.
	rollback := errors.New("expected observation rollback")
	err = s.manager.WithTransactionAcross(ctx, []string{"orders"}, func(tx *mongodb.Transaction) error {
		r, err := mongodb.NewTransactionRepository[Sample](tx, "orders", "observability_records")
		if err != nil {
			return err
		}
		if _, err := r.InsertOne(tx.Context(), &Sample{ID: 1, Code: "rollback"}); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		return errors.New("observation rollback error was lost")
	}
	// Demo-only transient label: driver re-executes the callback after rollback.
	attempts := 0
	if err := s.manager.WithTransactionAcross(ctx, []string{"orders"}, func(tx *mongodb.Transaction) error {
		attempts++
		r, err := mongodb.NewTransactionRepository[Sample](tx, "orders", "observability_records")
		if err != nil {
			return err
		}
		if _, err := r.InsertOne(tx.Context(), &Sample{ID: 2, Code: "retry"}); err != nil {
			return err
		}
		if attempts == 1 {
			return mongo.CommandError{Code: 112, Message: "demo-only retry", Labels: []string{"TransientTransactionError"}}
		}
		return nil
	}); err != nil {
		return err
	}
	stats := s.manager.TransactionStats()
	if stats.Started-statsBefore.Started != 3 || stats.Committed-statsBefore.Committed != 2 || stats.Failed-statsBefore.Failed != 1 || stats.CallbackAttempts-statsBefore.CallbackAttempts != 4 || stats.Retries-statsBefore.Retries != 1 || stats.Active != 0 {
		return errors.New("transaction stats differ from verified outcomes")
	}
	if s.metrics.started.Load()-startedBefore != 3 || s.metrics.attempts.Load()-attemptsBefore != 4 || s.metrics.finished.Load()-finishedBefore != 3 || s.metrics.durationNanos.Load() <= durationBefore {
		return errors.New("transaction hook events were not recorded")
	}
	r := mongodb.NewRepository[Sample](db, "observability_records")
	if exists, err := r.Exists(ctx, bson.M{"_id": 1}); err != nil || exists {
		return errors.Join(errors.New("failed transaction persisted"), err)
	}
	if n, err := r.Count(ctx, bson.M{"_id": 2}); err != nil || n != 1 {
		return errors.Join(errors.New("retry persisted an incorrect number of records"), err)
	}
	fmt.Fprintln(s.out, "  stats delta: started=3 committed=2 failed=1 attempts=4 retries=1 active=0")
	fmt.Fprintln(s.out, "  concurrency-safe hook captured started/attempt/finished and duration; unknown commit is a separate outcome")
	return nil
}
