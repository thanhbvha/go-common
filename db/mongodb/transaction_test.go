package mongodb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestExplicitClientGroups(t *testing.T) {
	m := NewManager()
	calls := 0
	var sharedDeployment *testDeployment
	m.connect = func(_ context.Context, cfg Config) (*mongo.Client, error) {
		calls++
		client, deployment, _ := mockClient(t)
		if cfg.ClientGroup != "" {
			sharedDeployment = deployment
		}
		return client, nil
	}
	cfg := Config{URI: "mongodb://localhost", DBName: "orders", ClientGroup: "business"}
	other := cfg
	other.DBName = "inventory"
	private := cfg
	private.ClientGroup = ""
	private.DBName = "private"
	if err := m.Init(context.Background(), map[string]Config{"orders": cfg, "inventory": other, "business": private}, "orders"); err != nil {
		t.Fatal(err)
	}
	orders, _ := m.Lookup("orders")
	inventory, _ := m.Lookup("inventory")
	isolated, _ := m.Lookup("business")
	if calls != 2 || orders.Client() != inventory.Client() || orders.Client() == isolated.Client() || orders.Name() == inventory.Name() {
		t.Fatal("group/private client ownership incorrect")
	}
	extra := cfg
	extra.DBName = "billing"
	if err := m.AddConnection(context.Background(), "billing", extra, false); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("adding binding opened duplicate client")
	}
	extra.AppName = "different-policy"
	if err := m.AddConnection(context.Background(), "conflict", extra, false); !errors.Is(err, ErrClientGroupConflict) {
		t.Fatalf("policy conflict accepted: %v", err)
	}
	if calls != 2 {
		t.Fatal("conflict dialed a client")
	}
	if err := m.DisconnectAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sharedDeployment.disconnectCalls.Load() != 1 {
		t.Fatal("shared client disconnected more than once")
	}
	if len(m.clients) != 0 || len(m.bindings) != 0 {
		t.Fatal("shutdown retained ownership state")
	}
}

func TestGroupConflictsBeforeDial(t *testing.T) {
	for _, change := range []func(*Config){
		func(c *Config) { c.URI = "mongodb://user:private-password@localhost" },
		func(c *Config) { c.MaxPoolSize = 200 },
		func(c *Config) { c.OperationTimeout = time.Second },
		func(c *Config) { c.EnableTelemetry = true },
		func(c *Config) { c.TransactionTimeout = time.Second },
	} {
		m := NewManager()
		m.connect = func(context.Context, Config) (*mongo.Client, error) {
			t.Fatal("conflict must fail before dialing")
			return nil, nil
		}
		a := Config{URI: "mongodb://localhost", DBName: "a", ClientGroup: "shared"}
		b := a
		b.DBName = "b"
		change(&b)
		err := m.Init(context.Background(), map[string]Config{"a": a, "b": b})
		if !errors.Is(err, ErrClientGroupConflict) || strings.Contains(err.Error(), "private-password") {
			t.Fatalf("unsafe conflict error: %v", err)
		}
	}
}

func TestGroupedRollbackPreservesEarlierClients(t *testing.T) {
	m := NewManager()
	existing, previousDeployment, _ := mockClient(t)
	opened, openedDeployment, _ := mockClient(t)
	failure := errors.New("expected startup failure")
	calls := 0
	m.connect = func(_ context.Context, cfg Config) (*mongo.Client, error) {
		calls++
		if cfg.DBName == "previous" {
			return existing, nil
		}
		if cfg.DBName == "failure" {
			return nil, failure
		}
		return opened, nil
	}
	previous := Config{URI: "mongodb://localhost", DBName: "previous", ClientGroup: "previous"}
	if err := m.AddConnection(context.Background(), "previous", previous, true); err != nil {
		t.Fatal(err)
	}
	a := Config{URI: "mongodb://localhost", DBName: "a", ClientGroup: "shared"}
	b := a
	b.DBName = "b"
	joined := previous
	joined.DBName = "joined"
	err := m.Init(context.Background(), map[string]Config{"a": a, "b": b, "joined": joined, "z": {URI: a.URI, DBName: "failure"}})
	if !errors.Is(err, failure) || calls != 3 {
		t.Fatalf("unexpected rollback: calls=%d err=%v", calls, err)
	}
	if openedDeployment.disconnectCalls.Load() != 1 || previousDeployment.closed.Load() {
		t.Fatal("rollback closed wrong clients")
	}
	if _, err := m.Lookup("joined"); err == nil {
		t.Fatal("failed init published reused binding")
	}
	if db, err := m.Lookup(""); err != nil || db.Client() != existing {
		t.Fatal("rollback lost existing default")
	}
	if len(m.clients) != 1 || len(m.bindings) != 1 {
		t.Fatal("rollback published client ownership")
	}
}

func TestTransactionScopeAndContextGuards(t *testing.T) {
	ok := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}
	client, _, commands := mockClient(t, ok, ok, ok)
	m := NewManager()
	m.connect = func(context.Context, Config) (*mongo.Client, error) { return client, nil }
	cfg := Config{URI: "mongodb://localhost", DBName: "orders", ClientGroup: "business"}
	other := cfg
	other.DBName = "inventory"
	if err := m.Init(context.Background(), map[string]Config{"orders": cfg, "inventory": other}, "orders"); err != nil {
		t.Fatal(err)
	}
	var retained *Repository[bson.M]
	var retainedScope *Transaction
	err := m.WithTransactionAcross(context.Background(), []string{"", "inventory", "orders"}, func(tx *Transaction) error {
		retainedScope = tx
		if _, err := tx.Database("unlisted"); !errors.Is(err, ErrTransactionScope) {
			t.Fatalf("unlisted alias accepted: %v", err)
		}
		if err := m.WithTransaction(tx.Context(), "orders", func(context.Context) error { t.Fatal("nested callback invoked"); return nil }); !errors.Is(err, ErrNestedTransaction) {
			t.Fatalf("nested transaction accepted: %v", err)
		}
		for _, alias := range []string{"orders", "inventory"} {
			r, err := NewTransactionRepository[bson.M](tx, alias, "items")
			if err != nil {
				return err
			}
			retained = r
			doc := bson.M{"_id": 1}
			if _, err := r.InsertOne(context.Background(), &doc); !errors.Is(err, ErrTransactionContext) {
				t.Fatalf("wrong context accepted: %v", err)
			}
			// Even a manually transplanted session must retain the scope context.
			transplanted := mongo.NewSessionContext(context.Background(), mongo.SessionFromContext(tx.Context()))
			if _, err := r.InsertOne(transplanted, &doc); !errors.Is(err, ErrTransactionContext) {
				t.Fatalf("transplanted context accepted: %v", err)
			}
			if _, err := r.InsertOne(context.WithoutCancel(tx.Context()), &doc); !errors.Is(err, ErrTransactionContext) {
				t.Fatalf("transaction deadline bypass accepted: %v", err)
			}
			derived, cancel := context.WithCancel(tx.Context())
			_, err = r.InsertOne(derived, &doc)
			cancel()
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(*commands) != 3 {
		t.Fatalf("guards caused database access: %d commands", len(*commands))
	}
	if (*commands)[0].Lookup("$db").StringValue() != "orders" || (*commands)[1].Lookup("$db").StringValue() != "inventory" {
		t.Fatal("wrong database selected")
	}
	if _, err := retained.Count(retainedScope.Context(), bson.M{}); !errors.Is(err, ErrTransactionClosed) {
		t.Fatalf("expired repository accepted: %v", err)
	}
	if _, err := retainedScope.Database("orders"); !errors.Is(err, ErrTransactionClosed) {
		t.Fatal("expired scope accepted")
	}
}

func TestTransactionRejectsClientMismatchBeforeCallback(t *testing.T) {
	m := NewManager()
	m.connect = func(context.Context, Config) (*mongo.Client, error) {
		client, _, _ := mockClient(t)
		return client, nil
	}
	cfg := Config{URI: "mongodb://localhost", DBName: "test"} // Same URI is not an explicit shared group.
	if err := m.Init(context.Background(), map[string]Config{"a": cfg, "b": cfg}); err != nil {
		t.Fatal(err)
	}
	called := false
	err := m.WithTransactionAcross(context.Background(), []string{"a", "b"}, func(*Transaction) error { called = true; return nil })
	if !errors.Is(err, ErrTransactionClientMismatch) || called {
		t.Fatalf("client mismatch: callback=%v err=%v", called, err)
	}
	if err := m.WithTransactionAcross(context.Background(), []string{"missing"}, func(*Transaction) error { t.Fatal("missing alias callback invoked"); return nil }); err == nil {
		t.Fatal("missing alias accepted")
	}
	if err := m.WithTransactionAcross(context.Background(), nil, func(*Transaction) error { return nil }); err == nil {
		t.Fatal("empty scope accepted")
	}
}

func TestTransactionDeadlineAndRetryLifetime(t *testing.T) {
	t.Run("deadline fallback", func(t *testing.T) {
		m := NewManager()
		m.connect = func(context.Context, Config) (*mongo.Client, error) {
			client, _, _ := mockClient(t)
			return client, nil
		}
		cfg := Config{URI: "mongodb://localhost", DBName: "test", TransactionTimeout: 20 * time.Millisecond}
		if err := m.Init(context.Background(), map[string]Config{"main": cfg}); err != nil {
			t.Fatal(err)
		}
		err := m.WithTransaction(context.Background(), "main", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("missing whole-transaction deadline: %v", err)
		}
		parent, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		want, _ := parent.Deadline()
		if err := m.WithTransaction(parent, "main", func(ctx context.Context) error {
			got, _ := ctx.Deadline()
			if got != want {
				t.Fatal("caller deadline replaced")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("retry expires previous attempt", func(t *testing.T) {
		ok := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}
		transient := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 251}, {Key: "errmsg", Value: "retry"}, {Key: "errorLabels", Value: bson.A{"TransientTransactionError"}}}
		client, _, _ := mockClient(t, transient, ok, ok, ok)
		m := NewManager()
		m.connect = func(context.Context, Config) (*mongo.Client, error) { return client, nil }
		if err := m.Init(context.Background(), map[string]Config{"main": {URI: "mongodb://localhost", DBName: "test"}}); err != nil {
			t.Fatal(err)
		}
		var previous *Repository[bson.M]
		var previousContext context.Context
		calls := 0
		err := m.WithTransactionAcross(context.Background(), []string{"main"}, func(tx *Transaction) error {
			calls++
			if previous != nil {
				if _, err := previous.Count(previousContext, bson.M{}); !errors.Is(err, ErrTransactionClosed) {
					t.Fatal("prior attempt repository remained active")
				}
			}
			r, err := NewTransactionRepository[bson.M](tx, "main", "items")
			if err != nil {
				return err
			}
			previous, previousContext = r, tx.Context()
			doc := bson.M{"_id": 1}
			_, err = r.InsertOne(tx.Context(), &doc)
			return err
		}, options.Transaction())
		if err != nil || calls != 2 {
			t.Fatalf("retry calls=%d err=%v", calls, err)
		}
	})
}
