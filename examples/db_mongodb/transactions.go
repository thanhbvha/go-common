package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

var (
	errInsufficientStock = errors.New("insufficient stock")
	errOrderConflict     = errors.New("idempotency key reused for another order")
)

// placeOrder is an illustrative trusted service operation, not an HTTP endpoint.
// Authenticate/authorize user and tenant access before calling it in a real service.
// The caller supplies a stable request ID outside the retryable callback.
func (s *exampleState) placeOrder(ctx context.Context, order Order) error {
	if strings.TrimSpace(order.ID) == "" || strings.TrimSpace(order.SKU) == "" || order.Quantity <= 0 {
		return errors.New("order ID, SKU and positive quantity are required")
	}
	err := s.manager.WithTransactionAcross(ctx, []string{"orders", "inventory", "audit"}, func(tx *mongodb.Transaction) error {
		orders, err := mongodb.NewTransactionRepository[Order](tx, "orders", "orders")
		if err != nil {
			return err
		}
		stock, err := mongodb.NewTransactionRepository[Stock](tx, "inventory", "stock")
		if err != nil {
			return err
		}
		outbox, err := mongodb.NewTransactionRepository[OutboxEvent](tx, "audit", "outbox")
		if err != nil {
			return err
		}
		existing, err := orders.FindByID(tx.Context(), order.ID)
		if err == nil {
			return sameOrder(existing, order)
		}
		if !errors.Is(err, mongodb.ErrNotFound) {
			return err
		}
		if _, err := orders.InsertOne(tx.Context(), &order); err != nil {
			return err
		}
		// The predicate and decrement are one atomic update. A separate read/check
		// followed by an unconditional decrement could oversell under concurrency.
		updated, err := stock.UpdateOne(tx.Context(), bson.M{"_id": order.SKU, "available": bson.M{"$gte": order.Quantity}}, bson.M{"$inc": bson.M{"available": -order.Quantity}})
		if err != nil {
			return err
		}
		if updated.MatchedCount != 1 {
			return errInsufficientStock
		}
		// Persist the event in the same transaction. An independent worker would
		// publish it after commit and handle duplicate delivery by this stable ID.
		_, err = outbox.InsertOne(tx.Context(), &OutboxEvent{ID: order.ID, Kind: "order.created", OrderID: order.ID})
		return err
	}, options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()))
	if mongo.IsDuplicateKeyError(err) {
		// Concurrent submissions of the same request can race on the unique ID.
		// Accept only a committed order with exactly the same intent; never retry
		// a non-idempotent business callback blindly after an ambiguous error.
		db, lookupErr := s.manager.Lookup("orders")
		if lookupErr != nil {
			return errors.Join(err, lookupErr)
		}
		existing, lookupErr := mongodb.NewRepository[Order](db, "orders").FindByID(ctx, order.ID)
		if lookupErr == nil {
			return sameOrder(existing, order)
		}
	}
	return err
}

func sameOrder(existing *Order, requested Order) error {
	if existing.SKU != requested.SKU || existing.Quantity != requested.Quantity {
		return errOrderConflict
	}
	return nil
}

func (s *exampleState) transactions(ctx context.Context) error {
	request := Order{ID: "request-commit", SKU: "demo-sku", Quantity: 3}
	if err := s.placeOrder(ctx, request); err != nil {
		return err
	}
	if err := s.placeOrder(ctx, request); err != nil {
		return err
	} // Idempotent replay.
	if err := s.verifyOrderState(ctx, "request-commit", true, 7); err != nil {
		return err
	}
	fmt.Fprintln(s.out, "  commit: order + stock decrement + outbox persisted; replay did not decrement twice")
	err := s.placeOrder(ctx, Order{ID: "request-rollback", SKU: "demo-sku", Quantity: 999})
	if !errors.Is(err, errInsufficientStock) {
		return errors.New("expected insufficient-stock rollback")
	}
	if err := s.verifyOrderState(ctx, "request-rollback", false, 7); err != nil {
		return err
	}
	fmt.Fprintln(s.out, "  rollback: rejected order and outbox absent; stock unchanged")
	if err := s.placeOrder(ctx, Order{ID: request.ID, SKU: request.SKU, Quantity: 1}); !errors.Is(err, errOrderConflict) {
		return errors.New("conflicting idempotency key accepted")
	}
	// Existing single-alias API: operations still require its session context.
	db, err := s.manager.Lookup("audit")
	if err != nil {
		return err
	}
	legacy := mongodb.NewRepository[OutboxEvent](db, "legacy_records")
	if err := db.CreateCollection(ctx, "legacy_records"); err != nil {
		return err
	}
	if err := s.manager.WithTransaction(ctx, "audit", func(sessCtx context.Context) error {
		_, err := legacy.InsertOne(sessCtx, &OutboxEvent{ID: "legacy-demo", Kind: "example.marker"})
		return err
	}, options.Transaction().SetWriteConcern(writeconcern.Majority())); err != nil {
		return err
	}
	if exists, err := legacy.Exists(ctx, bson.M{"_id": "legacy-demo"}); err != nil || !exists {
		return errors.Join(errors.New("legacy transaction marker missing"), err)
	}
	return nil
}

func (s *exampleState) verifyOrderState(ctx context.Context, id string, committed bool, available int) error {
	ordersDB, err := s.manager.Lookup("orders")
	if err != nil {
		return err
	}
	stockDB, err := s.manager.Lookup("inventory")
	if err != nil {
		return err
	}
	auditDB, err := s.manager.Lookup("audit")
	if err != nil {
		return err
	}
	orders := mongodb.NewRepository[Order](ordersDB, "orders")
	outbox := mongodb.NewRepository[OutboxEvent](auditDB, "outbox")
	stock := mongodb.NewRepository[Stock](stockDB, "stock")
	if exists, err := orders.Exists(ctx, bson.M{"_id": id}); err != nil || exists != committed {
		return errors.Join(errors.New("order atomicity check failed"), err)
	}
	if exists, err := outbox.Exists(ctx, bson.M{"_id": id}); err != nil || exists != committed {
		return errors.Join(errors.New("outbox atomicity check failed"), err)
	}
	item, err := stock.FindByID(ctx, "demo-sku")
	if err != nil {
		return err
	}
	if item.Available != available {
		return errors.New("inventory invariant failed")
	}
	return nil
}
