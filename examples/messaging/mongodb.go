package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	consumer "github.com/thanhbvha/go-common/v2/db/messaging/consumer/eventbus"
	mongomessaging "github.com/thanhbvha/go-common/v2/db/messaging/driver/mongodb"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	publisher "github.com/thanhbvha/go-common/v2/db/messaging/publisher/eventbus"
	dbmongodb "github.com/thanhbvha/go-common/v2/db/mongodb"
	bus "github.com/thanhbvha/go-common/v2/eventbus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const exampleAmount int64 = 9223372036854775807

type mongoOrder struct {
	ID     string `bson:"_id"`
	Amount int64  `bson:"amount"`
}
type mongoProjection struct {
	ID     string `bson:"_id"`
	Amount int64  `bson:"amount"`
}

// runMongoDBBroker owns two temporary databases bound to one managed client.
// Transactions span business state and audit messaging state; no broker calls
// occur inside a transaction callback.
func runMongoDBBroker(parent context.Context, settings exampleConfig, broker string, output io.Writer) (result error) {
	if err := settings.validateDriver("mongodb"); err != nil {
		return err
	}
	if err := settings.validateBroker(broker); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	namespace := "messaging_example_" + bson.NewObjectID().Hex()
	cfg := dbmongodb.Config{URI: settings.MongoDB.URI, RequireTransactions: true, ClientGroup: "messaging-example", AppName: "go-common-messaging-example", OperationTimeout: 5 * time.Second, TransactionTimeout: 15 * time.Second}
	businessCfg, auditCfg := cfg, cfg
	businessCfg.DBName = namespace + "_business"
	auditCfg.DBName = namespace + "_audit"
	manager := dbmongodb.NewManager()
	if err := manager.Init(ctx, map[string]dbmongodb.Config{"business": businessCfg, "audit": auditCfg}, "business"); err != nil {
		return &stepError{step: "mongodb.connect", err: err, hint: "check mongodb.uri, network/authentication/TLS and replicaSet name; use a replica set or sharded cluster, not standalone MongoDB"}
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		result = errors.Join(result, manager.DisconnectAll(cleanup))
	}()
	business, err := manager.Lookup("business")
	if err != nil {
		return err
	}
	audit, err := manager.Lookup("audit")
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		result = errors.Join(result, business.Drop(cleanup), audit.Drop(cleanup))
	}()

	// Deployment phase, on owned fixtures only. Provision is intentionally
	// separate from New, which performs read-only schema/index validation.
	storeConfig := mongomessaging.Config{Namespace: namespace, Alias: "audit", TransactionAliases: []string{"business"}}
	if err := mongomessaging.Provision(ctx, manager, storeConfig); err != nil {
		return &stepError{step: "mongodb.provision", err: err, hint: "check permission to create databases, collections, validators and indexes"}
	}
	for _, name := range []string{"orders", "effects"} {
		if err := business.CreateCollection(ctx, name); err != nil {
			return err
		}
	}
	store, err := mongomessaging.New(ctx, manager, storeConfig)
	if err != nil {
		return &stepError{step: "mongodb.store", err: err, hint: "check messaging schema version, validators and indexes"}
	}
	within := func(fn func(*dbmongodb.Transaction) error) error {
		return manager.WithTransactionAcross(ctx, []string{"business", "audit"}, fn, mongomessaging.TransactionOptions())
	}
	insertOrder := func(tx *dbmongodb.Transaction, id string) error {
		repo, err := dbmongodb.NewTransactionRepository[mongoOrder](tx, "business", "orders")
		if err != nil {
			return err
		}
		_, err = repo.InsertOne(tx.Context(), &mongoOrder{ID: id, Amount: exampleAmount})
		return err
	}
	project := func(tx *dbmongodb.Transaction, id string, amount int64) error {
		repo, err := dbmongodb.NewTransactionRepository[mongoProjection](tx, "business", "effects")
		if err != nil {
			return err
		}
		_, err = repo.InsertOne(tx.Context(), &mongoProjection{ID: id, Amount: amount})
		return err
	}

	tr, cleanupBroker, err := newBroker(ctx, settings, broker)
	if err != nil {
		return err
	}
	defer cleanupBroker()
	busConfig := bus.DefaultConfig()
	busConfig.Namespace = namespace
	b, err := bus.Open(busConfig, bus.WithTransport(tr))
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		result = errors.Join(result, b.Close(cleanup))
	}()
	handler, err := consumer.WrapInbox(store, consumer.Config{Namespace: namespace, Consumer: "orders-projector", Tenant: func(context.Context, bus.Job) (string, error) { return "demo", nil }}, func(_ context.Context, tx *dbmongodb.Transaction, job bus.Job) error {
		var payload struct {
			OrderID string `json:"order_id"`
			Amount  int64  `json:"amount"`
		}
		if err := job.Decode(&payload); err != nil {
			return err
		}
		if payload.Amount != exampleAmount {
			return errors.New("payload integer precision lost")
		}
		return project(tx, payload.OrderID, payload.Amount)
	})
	if err != nil {
		return err
	}
	if err := b.DeclareTask("order.created", bus.WithDelayDisabled()); err != nil {
		return err
	}
	if err := b.HandleTask("order.created", handler); err != nil {
		return err
	}
	if err := b.Start(ctx); err != nil {
		return err
	}
	newMessage := func(id string) messaging.Message {
		return messaging.Message{ID: id, Namespace: namespace, TenantID: "demo", Kind: messaging.Task, Type: "order.created", SchemaVersion: 1, Payload: fmt.Appendf(nil, `{"order_id":%q,"amount":%d}`, id, exampleAmount)}
	}

	// IDs are created outside retryable callbacks. A rollback must remove both
	// the business write and the outbox document across the two databases.
	rolledBack := newMessage(uuid.NewString())
	sentinel := errors.New("fixture rollback")
	if err := within(func(tx *dbmongodb.Transaction) error {
		if err := insertOrder(tx, rolledBack.ID); err != nil {
			return err
		}
		if err := store.Append(tx, rolledBack); err != nil {
			return err
		}
		return sentinel
	}); !errors.Is(err, sentinel) {
		return errors.New("MongoDB producer rollback failed")
	}
	if _, err := store.Get(ctx, "demo", rolledBack.ID); !errors.Is(err, messaging.ErrNotFound) {
		return errors.New("rolled-back MongoDB outbox persisted")
	}
	if n, err := business.Collection("orders").CountDocuments(ctx, bson.M{"_id": rolledBack.ID}); err != nil {
		return err
	} else if n != 0 {
		return errors.New("rolled-back MongoDB order persisted")
	}
	message := newMessage(uuid.NewString())
	if err := within(func(tx *dbmongodb.Transaction) error {
		if err := insertOrder(tx, message.ID); err != nil {
			return err
		}
		return store.Append(tx, message)
	}); err != nil {
		return err
	}
	// Equal append is safe; changing immutable intent for the same identity fails.
	if err := within(func(tx *dbmongodb.Transaction) error { return store.Append(tx, message) }); err != nil {
		return err
	}
	changed := message
	changed.Payload = []byte(`{"amount":0}`)
	if err := within(func(tx *dbmongodb.Transaction) error { return store.Append(tx, changed) }); !errors.Is(err, messaging.ErrIdentityConflict) {
		return errors.New("MongoDB identity conflict accepted")
	}

	claims, err := store.Claim(ctx, outbox.ClaimRequest{Limit: 1, MaxAttempts: 20, Lease: 10 * time.Millisecond})
	if err != nil || len(claims) != 1 {
		return errors.New("MongoDB claim fixture failed")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(20 * time.Millisecond):
	}
	pub, err := publisher.NewPublisher(b, publisher.Config{Namespace: namespace})
	if err != nil {
		return err
	}
	fault := &responseLoss{publisher: pub}
	fault.drop.Store(true)
	dcfg := outbox.DefaultConfig()
	dcfg.PollInterval = 10 * time.Millisecond
	dcfg.RetryMin = 10 * time.Millisecond
	dcfg.RetryMax = 20 * time.Millisecond
	dispatcher, err := outbox.NewDispatcher(store, fault, dcfg)
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- dispatcher.Run(ctx) }()
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		result = errors.Join(result, dispatcher.Drain(cleanup))
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			result = errors.Join(result, err)
		}
	}()
	for {
		record, err := store.Get(ctx, "demo", message.ID)
		if err != nil {
			return err
		}
		var effect mongoProjection
		err = business.Collection("effects").FindOne(ctx, bson.M{"_id": message.ID}).Decode(&effect)
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}
		if record.State == outbox.Published && err == nil {
			if effect.Amount != exampleAmount || record.Attempts != 3 || fault.calls.Load() != 2 {
				return errors.New("MongoDB committed recovery assertions failed")
			}
			fingerprint, err := message.Fingerprint()
			if err != nil {
				return err
			}
			replayed, err := store.Consume(ctx, inbox.Receipt{Identity: message.Identity(), Consumer: "orders-projector", Fingerprint: fingerprint}, func(*dbmongodb.Transaction) error { return errors.New("duplicate handler invoked") })
			if err != nil || replayed != inbox.Duplicate {
				return errors.New("MongoDB inbox replay failed")
			}
			orders, err := business.Collection("orders").CountDocuments(ctx, bson.M{})
			if err != nil {
				return err
			}
			effects, err := business.Collection("effects").CountDocuments(ctx, bson.M{})
			if err != nil {
				return err
			}
			if orders != 1 || effects != 1 {
				return errors.New("MongoDB business write applied more than once")
			}
			// This independent receipt shows a native callback retry. It writes its
			// own projection and never repeats an irreversible external operation.
			if err := verifyMongoCallbackRetry(ctx, namespace, store, business, project); err != nil {
				return err
			}
			_, err = fmt.Fprintf(output, "driver=mongodb; broker=%s; rollback=verified; order + outbox committed; recovered claim; claim attempts=%d; publish calls=%d; business effects=1; inbox replay=%s; identity conflict=verified; native callback attempts=2; retry business effects=1; cross-database transaction=verified; payload int64=verified\n", broker, record.Attempts, fault.calls.Load(), replayed)
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func verifyMongoCallbackRetry(ctx context.Context, namespace string, store *mongomessaging.Store, business *mongo.Database, project func(*dbmongodb.Transaction, string, int64) error) error {
	message := messaging.Message{ID: uuid.NewString(), Namespace: namespace, TenantID: "demo", Kind: messaging.Task, Type: "order.retry-check", SchemaVersion: 1, Payload: []byte(`{"retry":true}`)}
	fingerprint, err := message.Fingerprint()
	if err != nil {
		return err
	}
	receipt := inbox.Receipt{Identity: message.Identity(), Consumer: "retry-check", Fingerprint: fingerprint}
	attempts := 0
	result, err := store.Consume(ctx, receipt, func(tx *dbmongodb.Transaction) error {
		attempts++
		if err := project(tx, message.ID, exampleAmount); err != nil {
			return err
		}
		if attempts == 1 {
			return mongo.CommandError{Code: 112, Message: "fixture retryable transaction conflict", Labels: []string{"TransientTransactionError"}}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if result != inbox.Applied || attempts != 2 {
		return errors.New("native MongoDB callback did not retry safely")
	}
	if n, err := business.Collection("effects").CountDocuments(ctx, bson.M{"_id": message.ID, "amount": exampleAmount}); err != nil {
		return err
	} else if n != 1 {
		return errors.New("retried MongoDB callback applied business effect twice")
	}
	result, err = store.Consume(ctx, receipt, func(*dbmongodb.Transaction) error { return errors.New("retried receipt ran again") })
	if err != nil || result != inbox.Duplicate {
		return errors.New("retried MongoDB receipt was not deduplicated")
	}
	return nil
}
