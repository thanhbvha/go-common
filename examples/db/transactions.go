package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/thanhbvha/go-common/v2/db/orm"
)

func demoTransactions(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		r, err := f.repository()
		if err != nil {
			return err
		}
		row := patient("tx-committed", "one")
		isolation := sql.LevelSerializable
		if f.client.Capabilities().SupportsIsolation(sql.LevelRepeatableRead) {
			isolation = sql.LevelRepeatableRead
		}
		var escaped *orm.Repository[Patient]
		var transactionContext context.Context
		err = f.client.WithTransaction(ctx, &sql.TxOptions{Isolation: isolation}, func(tx *orm.Transaction) error {
			transactionContext = tx.Context()
			bound, err := r.InTransaction(tx)
			if err != nil {
				return err
			}
			escaped = bound
			if err := bound.Create(tx.Context(), &row); err != nil {
				return err
			}
			if err := bound.PatchByID(tx.Context(), row.ID, map[string]any{"status": "ready"}); err != nil {
				return err
			}
			// Low-level construction is appropriate for a NEW policy. Existing policies
			// should always be retained through rootRepository.InTransaction instead.
			visits, err := orm.NewTransactionRepository[Visit](tx, orm.RepositoryOptions{})
			if err != nil {
				return err
			}
			if err := visits.Create(tx.Context(), &Visit{PatientID: row.ID, Note: "Created atomically"}); err != nil {
				return err
			}
			if _, err := r.FindByID(tx.Context(), row.ID); !errors.Is(err, orm.ErrTransactionContext) {
				return fmt.Errorf("root repository accepted transaction context")
			}
			if _, err := bound.FindByID(ctx, row.ID); !errors.Is(err, orm.ErrTransactionContext) {
				return fmt.Errorf("transaction repository accepted unrelated context")
			}
			nested := f.client.WithTransaction(tx.Context(), nil, func(*orm.Transaction) error { return nil })
			if err := expect(nested, orm.ErrNestedTransaction); err != nil {
				return err
			}
			native, err := tx.DB()
			if err != nil {
				return err
			}
			var count int64
			return nativeFailure("transaction native count", native.Model(&Patient{}).Count(&count).Error)
		})
		if err != nil {
			return err
		}
		if _, err := escaped.FindByID(transactionContext, row.ID); !errors.Is(err, orm.ErrTransactionClosed) {
			return fmt.Errorf("transaction scope remained active")
		}
		rollback := errors.New("example requested rollback")
		err = f.client.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
			bound, e := r.InTransaction(tx)
			if e != nil {
				return e
			}
			record := patient("tx-rolled-back", "one")
			if e := bound.Create(tx.Context(), &record); e != nil {
				return e
			}
			return rollback
		})
		if err := expect(err, rollback); err != nil {
			return err
		}
		describeError(err)
		if _, err := r.FindOne(ctx, byExternalID("tx-rolled-back")); !errors.Is(err, orm.ErrNotFound) {
			return fmt.Errorf("rollback persisted data")
		}
		if err := demonstratePanicRollback(ctx, f.client, r); err != nil {
			return err
		}
		err = f.client.WithTransaction(ctx, &sql.TxOptions{ReadOnly: true}, func(tx *orm.Transaction) error {
			bound, e := r.InTransaction(tx)
			if e != nil {
				return e
			}
			_, e = bound.FindByID(tx.Context(), row.ID)
			return e
		})
		if !f.client.Capabilities().ReadOnlyTransactions {
			if err := expect(err, orm.ErrUnsupportedFeature); err != nil {
				return err
			}
			fmt.Println("  read-only transaction explicitly unsupported on this backend")
		} else if err != nil {
			return err
		}
		fmt.Printf("  commit/rollback/panic verified; isolation=%s; expired/nested/context guards verified\n", isolation)
		return nil
	})
}
func demonstratePanicRollback(ctx context.Context, c *orm.Client, r *orm.Repository[Patient]) (result error) {
	recovered := false
	func() {
		defer func() {
			if value := recover(); value != nil {
				message, ok := value.(string)
				if ok && message == "example panic" {
					recovered = true
				} else {
					result = fmt.Errorf("unexpected panic in transaction demo")
				}
			}
		}()
		result = c.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
			bound, err := r.InTransaction(tx)
			if err != nil {
				return err
			}
			record := patient("tx-panic", "one")
			if err := bound.Create(tx.Context(), &record); err != nil {
				return err
			}
			panic("example panic")
		})
	}()
	if result != nil {
		return result
	}
	if !recovered {
		return fmt.Errorf("panic did not propagate")
	}
	_, err := r.FindOne(ctx, byExternalID("tx-panic"))
	return expect(err, orm.ErrNotFound)
}

// A real commit acknowledgement can be lost after data persisted. This handler
// demonstrates the public contract without manufacturing network failures.
// Fault injection for that outcome belongs in db/orm's integration/test suite.
func describeError(err error) {
	var transaction *orm.TransactionError
	if errors.As(err, &transaction) {
		fmt.Printf("  transaction phase=%s outcome=%s rollback_error=%t\n", transaction.Phase, transaction.Outcome, transaction.RollbackError() != nil)
	}
	var database *orm.Error
	if errors.As(err, &database) {
		fmt.Printf("  operation=%s kind=%s sqlstate=%s driver_code=%s\n", database.Operation, database.Kind, database.SQLState, database.DriverCode)
	}
	switch {
	case errors.Is(err, orm.ErrCommitUnknown):
		fmt.Println("  reconcile by idempotency key/outbox before retrying; commit may have persisted")
	case errors.Is(err, orm.ErrDeadlock), errors.Is(err, orm.ErrSerialization):
		fmt.Println("  retry the complete transaction only under an explicit idempotent retry policy")
	case errors.Is(err, orm.ErrLockTimeout), errors.Is(err, orm.ErrStatementTimeout):
		fmt.Println("  server timeout: diagnose contention/query cost before retrying")
	}
	// errors.Is/As traverse wrapped causes; do not log errors.Unwrap(err) or native
	// Message/Detail fields. They may contain credentials or row values.
}
