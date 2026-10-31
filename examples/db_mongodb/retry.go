package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func (s *exampleState) retry(ctx context.Context) error {
	audit, err := s.manager.Lookup("audit")
	if err != nil {
		return err
	}
	if err := audit.CreateCollection(ctx, "retry_records"); err != nil {
		return err
	}
	attempts := 0
	before := s.manager.TransactionStats()
	err = s.manager.WithTransactionAcross(ctx, []string{"orders", "audit"}, func(tx *mongodb.Transaction) error {
		attempts++
		first, err := mongodb.NewTransactionRepository[Sample](tx, "orders", "samples")
		if err != nil {
			return err
		}
		second, err := mongodb.NewTransactionRepository[Sample](tx, "audit", "retry_records")
		if err != nil {
			return err
		}
		// Stable identifiers are chosen outside any random generation in the callback.
		doc := Sample{ID: 200, Code: "retry-record", Group: "retry"}
		if _, err := first.InsertOne(tx.Context(), &doc); err != nil {
			return err
		}
		if _, err := second.InsertOne(tx.Context(), &doc); err != nil {
			return err
		}
		if attempts == 1 {
			// Demo-only fault: trigger the driver's retry path after two real writes.
			// Do not fabricate error labels in production. No server-wide failpoint
			// or admin permission is needed to demonstrate callback re-execution.
			return mongo.CommandError{Code: 112, Message: "synthetic retry demonstration", Labels: []string{"TransientTransactionError"}}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if attempts != 2 {
		return errors.New("expected two callback attempts")
	}
	first, err := s.sampleRepository()
	if err != nil {
		return err
	}
	second := mongodb.NewRepository[Sample](audit, "retry_records")
	for _, r := range []*mongodb.Repository[Sample]{first, second} {
		count, err := r.Count(ctx, bson.M{"_id": 200})
		if err != nil {
			return err
		}
		if count != 1 {
			return errors.New("retry caused a missing or duplicate document")
		}
	}
	fmt.Fprintf(s.out, "  callback attempts=%d; first attempt rolled back; one record per database\n", attempts)
	after := s.manager.TransactionStats()
	if after.CallbackAttempts-before.CallbackAttempts != 2 || after.Retries-before.Retries != 1 || after.Active != 0 {
		return errors.New("transaction statistics did not capture retry")
	}
	fmt.Fprintf(s.out, "  stats: attempts=%d retries=%d active=%d\n", after.CallbackAttempts-before.CallbackAttempts, after.Retries-before.Retries, after.Active)
	return nil
}
