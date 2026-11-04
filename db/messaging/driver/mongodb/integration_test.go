//go:build integration

package mongodb_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/contracttest"
	driver "github.com/thanhbvha/go-common/v2/db/messaging/driver/mongodb"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	database "github.com/thanhbvha/go-common/v2/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type fixture struct {
	contracttest.Fixture[*database.Transaction]
	manager *database.Manager
	db      *mongo.Database
	store   *driver.Store
	config  driver.Config
}

func newFixture(t *testing.T, configure ...func(*database.Config)) fixture {
	t.Helper()
	uri := os.Getenv("MONGODB_INTEGRATION_URI")
	if uri == "" {
		if os.Getenv("MESSAGING_MONGODB_REQUIRED") == "1" {
			t.Fatal("MONGODB_INTEGRATION_URI required")
		}
		t.Skip("set disposable MongoDB replica-set URI")
	}
	name := "messaging_it_" + bson.NewObjectID().Hex()
	cfg := database.Config{URI: uri, DBName: name, RequireTransactions: true, ClientGroup: "messaging-fixture", AppName: "go-common-messaging-integration"}
	for _, apply := range configure {
		apply(&cfg)
	}
	manager := database.NewManager()
	if err := manager.Init(t.Context(), map[string]database.Config{"main": cfg}); err != nil {
		t.Fatal("MongoDB fixture startup failed (URI omitted)")
	}
	db, err := manager.Lookup("main")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := db.Drop(ctx); err != nil {
			t.Error("temporary MongoDB cleanup failed")
		}
		if err := manager.DisconnectAll(ctx); err != nil {
			t.Error("fixture disconnect failed")
		}
	})
	config := driver.Config{Namespace: "test", Alias: "main", OperationTimeout: 30 * time.Second}
	if err := driver.Provision(t.Context(), manager, config); err != nil {
		t.Fatal(err)
	}
	store, err := driver.New(t.Context(), manager, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateCollection(t.Context(), "effects"); err != nil {
		t.Fatal(err)
	}
	f := contracttest.Fixture[*database.Transaction]{Namespace: "test", Store: store, Inbox: store, Within: func(ctx context.Context, fn func(*database.Transaction) error) error {
		return manager.WithTransactionAcross(ctx, []string{"main"}, fn, driver.TransactionOptions())
	}, Append: store.Append,
		Effect: func(tx *database.Transaction, id string) error {
			db, err := tx.Database("main")
			if err != nil {
				return err
			}
			_, err = db.Collection("effects").UpdateOne(tx.Context(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"count": 1}}, options.UpdateOne().SetUpsert(true))
			return err
		},
		Count: func(ctx context.Context, id string) int {
			var row struct{ Count int }
			err := db.Collection("effects").FindOne(ctx, bson.M{"_id": id}).Decode(&row)
			if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
				t.Error("fixture count failed")
			}
			return row.Count
		},
		Expire: func(ctx context.Context, id messaging.Identity) {
			_, err := db.Collection("messaging_outbox").UpdateOne(ctx, bson.M{"namespace": id.Namespace, "tenant_id": id.TenantID, "message_id": id.ID}, bson.M{"$set": bson.M{"lease_until": time.Now().Add(-time.Hour)}})
			if err != nil {
				t.Fatal(err)
			}
		},
	}
	return fixture{f, manager, db, store, config}
}
func TestMongoDBMessagingContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) contracttest.Fixture[*database.Transaction] { return newFixture(t).Fixture })
}
func message(f fixture, id string) messaging.Message {
	return messaging.Message{ID: id, Namespace: f.Namespace, TenantID: "a", Kind: messaging.Task, Type: "created", SchemaVersion: 1, Payload: []byte(`{"id":9223372036854775807}`), Headers: map[string]string{"$trace": "$literal-value"}}
}
func appendMessage(t *testing.T, f fixture, m messaging.Message) {
	t.Helper()
	if err := f.Within(t.Context(), func(tx *database.Transaction) error { return f.Append(tx, m) }); err != nil {
		t.Fatal(err)
	}
}

func TestMongoDBMessagingSchemaScopeAndAdmin(t *testing.T) {
	f := newFixture(t)
	ctx := t.Context()
	if err := driver.Provision(ctx, f.manager, f.config); err != nil {
		t.Fatal("provision not idempotent", err)
	}
	wrong := f.config
	wrong.Prefix = "missing"
	if _, err := driver.New(ctx, f.manager, wrong); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal("missing schema accepted", err)
	}
	var closed *database.Transaction
	if err := f.Within(ctx, func(tx *database.Transaction) error { closed = tx; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := f.Append(closed, message(f, "closed")); !errors.Is(err, database.ErrTransactionClosed) {
		t.Fatal(err)
	}
	if err := f.Append(nil, message(f, "nil")); !errors.Is(err, database.ErrTransactionClosed) {
		t.Fatal(err)
	}
	other := newFixture(t)
	if err := other.Within(ctx, func(tx *database.Transaction) error {
		if err := f.Append(tx, message(f, "cross")); !errors.Is(err, database.ErrTransactionClientMismatch) {
			t.Error(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"published", "dead", "processing", "pending", "future"} {
		m := message(f, id)
		if id == "future" {
			m.AvailableAt = time.Now().Add(time.Hour)
		}
		appendMessage(t, f, m)
	}
	page, err := f.store.List(ctx, outbox.ListRequest{Limit: 2})
	if err != nil || len(page.Records) != 2 || page.NextCursor == "" {
		t.Fatal(page, err)
	}
	tenant := "other"
	if _, err := f.store.List(ctx, outbox.ListRequest{TenantID: &tenant, Limit: 2, Cursor: page.NextCursor}); err == nil {
		t.Fatal("cursor filter changed")
	}
	page2, err := f.store.List(ctx, outbox.ListRequest{Limit: 10, Cursor: page.NextCursor})
	if err != nil || len(page2.Records) != 3 {
		t.Fatal(page2, err)
	}
	claims, err := f.store.Claim(ctx, outbox.ClaimRequest{Limit: 10, MaxAttempts: 20, Lease: time.Minute})
	if err != nil || len(claims) != 4 {
		t.Fatal(claims, err)
	}
	for _, c := range claims {
		switch c.Message.ID {
		case "published":
			err = f.store.MarkPublished(ctx, c)
		case "dead":
			err = f.store.Quarantine(ctx, c, "fixture_failure")
		case "pending":
			err = f.store.Retry(ctx, c, time.Hour, "fixture_retry")
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	m := message(f, "published")
	fp, _ := m.Fingerprint()
	r := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: fp}
	if _, err := f.store.Consume(ctx, r, func(*database.Transaction) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// No TTL/retention work occurs without explicit cutoffs.
	if _, err := f.store.Cleanup(ctx, outbox.CleanupRequest{Limit: 10}); err == nil {
		t.Fatal("implicit cleanup accepted")
	}
	result, err := f.store.Cleanup(ctx, outbox.CleanupRequest{PublishedBefore: time.Now(), InboxBefore: time.Now(), Limit: 1})
	if err != nil || result.Published != 1 || result.Inbox != 1 {
		t.Fatal(result, err)
	}
	stats, err := f.store.Stats(ctx, nil)
	if err != nil || stats.Pending != 2 || stats.Dead != 1 || stats.InFlight != 1 {
		t.Fatal(stats, err)
	}
	// Missing required indexes are detected without startup attempting a repair.
	if err := f.db.Collection("messaging_outbox").Indexes().DropOne(ctx, "identity"); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.New(ctx, f.manager, f.config); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal("missing index accepted", err)
	}
	if err := driver.Provision(ctx, f.manager, f.config); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Collection("messaging_inbox").Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "processed_at", Value: 1}}, Options: options.Index().SetName("unsafe_ttl").SetExpireAfterSeconds(3600)}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.New(ctx, f.manager, f.config); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal("automatic receipt expiration accepted", err)
	}
	if err := f.db.Collection("messaging_inbox").Indexes().DropOne(ctx, "unsafe_ttl"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.Collection("messaging_schema").UpdateOne(ctx, bson.M{"_id": "schema"}, bson.M{"$set": bson.M{"version": 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.New(ctx, f.manager, f.config); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal("future version accepted", err)
	}
}

func TestMongoDBMessagingBusinessConstraintIsNotDuplicate(t *testing.T) {
	f := newFixture(t)
	m := message(f, "constraint")
	fp, _ := m.Fingerprint()
	r := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: fp}
	if _, err := f.db.Collection("effects").InsertOne(t.Context(), bson.M{"_id": "unique", "count": 1}); err != nil {
		t.Fatal(err)
	}
	_, err := f.store.Consume(t.Context(), r, func(tx *database.Transaction) error {
		db, e := tx.Database("main")
		if e != nil {
			return e
		}
		_, e = db.Collection("effects").InsertOne(tx.Context(), bson.M{"_id": "unique", "count": 1})
		return e
	})
	if !mongo.IsDuplicateKeyError(err) {
		t.Fatal("business constraint masked", err)
	}
	result, err := f.store.Consume(t.Context(), r, func(tx *database.Transaction) error { return f.Effect(tx, "recovered") })
	if err != nil || result != inbox.Applied || f.Count(t.Context(), "recovered") != 1 {
		t.Fatal(result, err)
	}
}

func TestMongoDBMessagingCrossDatabaseScope(t *testing.T) {
	f := newFixture(t)
	ctx := t.Context()
	config := database.Config{URI: os.Getenv("MONGODB_INTEGRATION_URI"), DBName: f.db.Name() + "_business", RequireTransactions: true, ClientGroup: "messaging-fixture", AppName: "go-common-messaging-integration"}
	if err := f.manager.AddConnection(ctx, "business", config, false); err != nil {
		t.Fatal(err)
	}
	business, err := f.manager.Lookup("business")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = business.Drop(ctx)
	})
	if err := business.CreateCollection(ctx, "effects"); err != nil {
		t.Fatal(err)
	}
	cfg := f.config
	cfg.TransactionAliases = []string{"business"}
	store, err := driver.New(ctx, f.manager, cfg)
	if err != nil {
		t.Fatal(err)
	}
	m := message(f, "cross")
	fp, _ := m.Fingerprint()
	r := inbox.Receipt{Identity: m.Identity(), Consumer: "consumer", Fingerprint: fp}
	result, err := store.Consume(ctx, r, func(tx *database.Transaction) error {
		db, err := tx.Database("business")
		if err != nil {
			return err
		}
		if _, err := db.Collection("effects").InsertOne(tx.Context(), bson.M{"_id": "cross"}); err != nil {
			return err
		}
		return store.Append(tx, message(f, "next"))
	})
	if err != nil || result != inbox.Applied {
		t.Fatal(result, err)
	}
	if n, err := business.Collection("effects").CountDocuments(ctx, bson.M{}); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if err := f.manager.WithTransactionAcross(ctx, []string{"business"}, func(tx *database.Transaction) error {
		if err := store.Append(tx, message(f, "outside")); !errors.Is(err, database.ErrTransactionScope) {
			t.Error("undeclared alias accepted", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
