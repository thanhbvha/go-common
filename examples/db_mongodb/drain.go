package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// drain uses a separate manager so demonstrating shutdown cannot interrupt the
// remaining scenarios. Its data still belongs to this run's temporary databases.
func (s *exampleState) drain(ctx context.Context) (result error) {
	owner := mongodb.NewManager()
	if err := owner.Init(ctx, s.configs, "orders"); err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, owner.DisconnectAll(cleanup))
	}()
	db, err := s.manager.Lookup("orders")
	if err != nil {
		return err
	}
	if err := db.CreateCollection(ctx, "drain_records"); err != nil {
		return err
	}
	verify := mongodb.NewRepository[Sample](db, "drain_records")
	workerCtx, cancelWorker := context.WithCancel(ctx)
	drainCtx, cancelDrain := context.WithCancel(ctx)
	entered, release := make(chan struct{}), make(chan struct{})
	var signalEntered, signalRelease sync.Once
	transactionDone, drainDone := make(chan error, 1), make(chan error, 1)
	transactionCollected, drainStarted, drainCollected := false, false, false
	// Release/wait before the outer manager cleanup, even on an error path.
	defer func() {
		cancelDrain()
		signalRelease.Do(func() { close(release) })
		cancelWorker()
		if !transactionCollected {
			result = errors.Join(result, waitExampleResult(transactionDone))
		}
		if drainStarted && !drainCollected {
			if err := waitExampleResult(drainDone); err != nil && !errors.Is(err, context.Canceled) {
				result = errors.Join(result, err)
			}
		}
	}()
	go func() {
		transactionDone <- owner.WithTransactionAcross(workerCtx, []string{"orders"}, func(tx *mongodb.Transaction) error {
			r, err := mongodb.NewTransactionRepository[Sample](tx, "orders", "drain_records")
			if err != nil {
				return err
			}
			if _, err := r.InsertOne(tx.Context(), &Sample{ID: 1, Code: "drain-commit"}); err != nil {
				return err
			}
			signalEntered.Do(func() { close(entered) })
			select {
			case <-release:
				return nil
			case <-tx.Context().Done():
				return tx.Context().Err()
			}
		})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		return ctx.Err()
	}
	drainStarted = true
	go func() { drainDone <- owner.DisconnectAll(drainCtx) }()
	if err := waitUntilClosing(ctx, owner); err != nil {
		return err
	}
	// DisconnectAll now holds the lifecycle gate while the callback is blocked.
	// A canceled AddConnection must leave this gate promptly without publishing.
	short, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	err = owner.AddConnection(short, "must-not-publish", s.configs["orders"], false)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		return errors.New("lifecycle gate ignored waiting context")
	}
	cancelDrain()
	err = waitExampleResult(drainDone)
	drainCollected = true
	if !errors.Is(err, context.Canceled) {
		return errors.New("first drain did not honor cancellation")
	}
	// A timed-out drain also retains the clients, allowing a later retry.
	short, cancel = context.WithTimeout(ctx, 30*time.Millisecond)
	err = owner.DisconnectAll(short)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		return errors.New("drain did not wait for the active transaction")
	}
	if owner.TransactionStats().Active != 1 {
		return errors.New("active transaction disappeared during drain")
	}
	called := false
	err = owner.WithTransaction(ctx, "orders", func(context.Context) error { called = true; return nil })
	if !errors.Is(err, mongodb.ErrManagerClosing) || called {
		return errors.New("closing manager accepted a new transaction")
	}
	if _, err := owner.Lookup("orders"); !errors.Is(err, mongodb.ErrManagerClosing) {
		return errors.New("closing manager accepted a lookup")
	}
	fmt.Fprintln(s.out, "  lifecycle wait cancelled; drain timeout retained clients; new transactions/lookups rejected")
	signalRelease.Do(func() { close(release) })
	err = waitExampleResult(transactionDone)
	transactionCollected = true
	if err != nil {
		return err
	}
	if err := owner.DisconnectAll(ctx); err != nil {
		return err
	}
	if n, err := verify.Count(ctx, bson.M{"_id": 1}); err != nil || n != 1 {
		return errors.Join(errors.New("drain interrupted the commit"), err)
	}
	if _, err := owner.Lookup("orders"); err == nil {
		return errors.New("shutdown retained a closed binding")
	}
	fmt.Fprintln(s.out, "  active transaction committed once; shutdown retry closed its client successfully")
	return nil
}

func waitExampleResult(done <-chan error) error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return errors.New("example worker did not stop within its cleanup budget")
	}
}

func waitUntilClosing(ctx context.Context, owner *mongodb.Manager) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := owner.Lookup("orders"); errors.Is(err, mongodb.ErrManagerClosing) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
