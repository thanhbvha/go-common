package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func (s *exampleState) guards(ctx context.Context) error {
	bad := s.configs["orders"]
	bad.MaxPoolSize++
	if err := s.manager.AddConnection(ctx, "conflicting-policy", bad, false); !errors.Is(err, mongodb.ErrClientGroupConflict) {
		return errors.New("group policy conflict accepted")
	}
	err := s.manager.WithTransactionAcross(ctx, []string{"orders", "isolated"}, func(*mongodb.Transaction) error { return errors.New("different-client callback must not run") })
	if !errors.Is(err, mongodb.ErrTransactionClientMismatch) {
		return errors.New("different-client transaction accepted")
	}
	var saved *mongodb.Repository[Sample]
	var savedContext context.Context
	err = s.manager.WithTransactionAcross(ctx, []string{"orders"}, func(tx *mongodb.Transaction) error {
		if _, err := tx.Database("audit"); !errors.Is(err, mongodb.ErrTransactionScope) {
			return errors.New("undeclared alias accepted")
		}
		if err := s.manager.WithTransaction(tx.Context(), "orders", func(context.Context) error { return errors.New("nested callback must not run") }); !errors.Is(err, mongodb.ErrNestedTransaction) {
			return errors.New("nested transaction accepted")
		}
		r, err := mongodb.NewTransactionRepository[Sample](tx, "orders", "samples")
		if err != nil {
			return err
		}
		saved, savedContext = r, tx.Context()
		if _, err := r.Count(ctx, bson.M{}); !errors.Is(err, mongodb.ErrTransactionContext) {
			return errors.New("original context accepted by scoped repository")
		}
		if _, err := r.Count(context.WithoutCancel(tx.Context()), bson.M{}); !errors.Is(err, mongodb.ErrTransactionContext) {
			return errors.New("transaction deadline bypass accepted")
		}
		derived, cancel := context.WithCancel(tx.Context())
		defer cancel()
		_, err = r.Count(derived, bson.M{"group": "fixture"})
		return err
	})
	if err != nil {
		return err
	}
	if _, err := saved.Count(savedContext, bson.M{}); !errors.Is(err, mongodb.ErrTransactionClosed) {
		return errors.New("ended callback repository remained usable")
	}
	fmt.Fprintln(s.out, "  rejected: group conflict, different client, nested transaction, undeclared alias, wrong context, expired scope")
	return nil
}
