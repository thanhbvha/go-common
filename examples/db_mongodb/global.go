package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// global is a compatibility example for applications deliberately choosing
// process-global wiring. Services and tests generally prefer an instance manager.
func (s *exampleState) global(ctx context.Context) (result error) {
	before := mongodb.GetTransactionStats()
	if err := mongodb.Init(ctx, s.configs, "orders"); err != nil {
		return err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		result = errors.Join(result, mongodb.DisconnectAll(shutdown))
	}()
	db, err := mongodb.Lookup("")
	if err != nil {
		return err
	}
	if mongodb.Get("orders").Client() != db.Client() {
		return errors.New("global default mismatch")
	}
	if caps, err := mongodb.GetCapabilities(ctx, "orders"); err != nil || !caps.Transactions {
		return errors.Join(errors.New("global capability check failed"), err)
	}
	if err := mongodb.WithTransaction(ctx, "orders", func(sessCtx context.Context) error {
		_, err := mongodb.NewRepository[Sample](db, "samples").Count(sessCtx, bson.M{})
		return err
	}); err != nil {
		return err
	}
	if err := mongodb.WithTransactionAcross(ctx, []string{"orders", "inventory"}, func(tx *mongodb.Transaction) error {
		orders, err := mongodb.NewTransactionRepository[Order](tx, "orders", "orders")
		if err != nil {
			return err
		}
		stock, err := mongodb.NewTransactionRepository[Stock](tx, "inventory", "stock")
		if err != nil {
			return err
		}
		if _, err := orders.Count(tx.Context(), bson.M{}); err != nil {
			return err
		}
		_, err = stock.FindByID(tx.Context(), "demo-sku")
		return err
	}, options.Transaction().SetWriteConcern(writeconcern.Majority())); err != nil {
		return err
	}
	stats := mongodb.GetTransactionStats()
	if stats.Active != 0 || stats.Committed-before.Committed != 2 {
		return errors.New("global transaction stats did not capture both commits")
	}
	fmt.Fprintln(s.out, "  global Init/Lookup/Get/transactions/capabilities/stats/DisconnectAll demonstrated")
	return nil
}
