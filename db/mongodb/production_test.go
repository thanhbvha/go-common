package mongodb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestLifecycleWaitHonorsContext(t *testing.T) {
	m := NewManager()
	if err := m.opMu.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer m.opMu.Unlock()
	for _, fn := range []func(context.Context) error{
		func(ctx context.Context) error {
			return m.Init(ctx, map[string]Config{"a": {URI: "mongodb://localhost", DBName: "test"}})
		},
		func(ctx context.Context) error {
			return m.AddConnection(ctx, "a", Config{URI: "mongodb://localhost", DBName: "test"}, false)
		},
		m.DisconnectAll,
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
		err := fn(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lifecycle wait ignored deadline: %v", err)
		}
	}
}

func TestDrainTimeoutAndRetry(t *testing.T) {
	m := NewManager()
	client, deployment, _ := mockClient(t)
	m.connect = func(context.Context, Config) (*mongo.Client, error) { return client, nil }
	if err := m.Init(context.Background(), map[string]Config{"main": {URI: "mongodb://localhost", DBName: "test"}}); err != nil {
		t.Fatal(err)
	}
	entered, finish := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(finish) }) })
	done := make(chan error, 1)
	go func() {
		done <- m.WithTransaction(context.Background(), "main", func(context.Context) error { close(entered); <-finish; return nil })
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := m.DisconnectAll(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || deployment.closed.Load() {
		t.Fatalf("drain closed active client: %v", err)
	}
	if _, err := m.Lookup("main"); !errors.Is(err, ErrManagerClosing) {
		t.Fatalf("lookup during drain: %v", err)
	}
	if err := m.WithTransaction(context.Background(), "main", func(context.Context) error { t.Error("new transaction entered closing manager"); return nil }); !errors.Is(err, ErrManagerClosing) {
		t.Fatal(err)
	}
	if m.TransactionStats().Active != 1 {
		t.Fatal("active transaction not counted")
	}
	once.Do(func() { close(finish) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("callback did not finish")
	}
	if err := m.DisconnectAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !deployment.closed.Load() {
		t.Fatal("drained client remained open")
	}
	stats := m.TransactionStats()
	if stats.Active != 0 || stats.Started != 1 || stats.Committed != 1 {
		t.Fatalf("bad stats: %+v", stats)
	}
}

func TestCanceledStartupDoesNotPublishAndPanicDoesNotLeak(t *testing.T) {
	t.Run("canceled before publish", func(t *testing.T) {
		client, deployment, _ := mockClient(t)
		m := NewManager()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		m.connect = func(context.Context, Config) (*mongo.Client, error) { cancel(); return client, nil }
		err := m.Init(ctx, map[string]Config{"main": {URI: "mongodb://localhost", DBName: "test"}})
		if !errors.Is(err, context.Canceled) || !deployment.closed.Load() {
			t.Fatalf("canceled startup leaked client: %v", err)
		}
		if _, err := m.Lookup("main"); err == nil {
			t.Fatal("canceled startup published binding")
		}
	})
	t.Run("callback panic releases drain", func(t *testing.T) {
		client, _, _ := mockClient(t)
		m := NewManager()
		m.connect = func(context.Context, Config) (*mongo.Client, error) { return client, nil }
		if err := m.Init(context.Background(), map[string]Config{"main": {URI: "mongodb://localhost", DBName: "test"}}); err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Error("business panic was swallowed")
				}
			}()
			_ = m.WithTransaction(context.Background(), "main", func(context.Context) error { panic("business panic") })
		}()
		stats := m.TransactionStats()
		if stats.Failed != 1 || stats.Active != 0 {
			t.Fatalf("panic leaked transaction: %+v", stats)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := m.DisconnectAll(ctx); err != nil {
			t.Fatalf("panic blocked drain: %v", err)
		}
	})
}

func TestCapabilitiesAndRequiredTransactions(t *testing.T) {
	for _, topology := range []string{"standalone", "replica", "sharded"} {
		t.Run(topology, func(t *testing.T) {
			hello := bson.D{{Key: "ok", Value: 1}, {Key: "maxWireVersion", Value: 25}, {Key: "logicalSessionTimeoutMinutes", Value: 30}}
			if topology == "replica" {
				hello = append(hello, bson.E{Key: "setName", Value: "test"})
			}
			if topology == "sharded" {
				hello = append(hello, bson.E{Key: "msg", Value: "isdbgrid"})
			}
			client, _, _ := mockClient(t, hello)
			caps, err := DetectCapabilities(context.Background(), client)
			if err != nil || caps.Transactions != (topology != "standalone") {
				t.Fatalf("bad capabilities: %+v %v", caps, err)
			}
		})
	}
	client, deployment, _ := mockClient(t, bson.D{{Key: "ok", Value: 1}}, bson.D{{Key: "ok", Value: 1}, {Key: "maxWireVersion", Value: 25}})
	_ = client // connectAndPing creates its own client using the same deployment below.
	opts := options.Client()
	opts.Deployment = deployment
	_, err := connectAndPing(context.Background(), Config{RequireTransactions: true}, opts)
	if !errors.Is(err, ErrTransactionsUnsupported) || !deployment.closed.Load() {
		t.Fatalf("standalone accepted: %v", err)
	}
}

func TestSafeConfigurationDiagnostics(t *testing.T) {
	_, err := clientOptions(Config{URI: "mongodb://localhost", TransactionTimeout: -1})
	var configuration *ConfigError
	if !errors.Is(err, ErrInvalidConfig) || !errors.As(err, &configuration) || configuration.Field != "transaction_timeout" {
		t.Fatalf("missing typed error: %v", err)
	}
	conflict := groupConflict(Config{URI: "mongodb://secret-a@localhost"}, Config{URI: "mongodb://secret-b@localhost"})
	var group *ClientGroupError
	if !errors.As(conflict, &group) || len(group.Fields) != 1 || group.Fields[0] != "URI" {
		t.Fatal("group diagnosis missing field names")
	}
}

func TestTransactionMonitorUnknownCommitAndPanicIsolation(t *testing.T) {
	// Code 50 stops driver commit retry while preserving the unknown-result label.
	ok := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}
	unknown := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 50}, {Key: "errmsg", Value: "commit outcome unknown"}, {Key: "errorLabels", Value: bson.A{"UnknownTransactionCommitResult"}}}
	client, _, _ := mockClient(t, ok, unknown, ok) // Allow session cleanup's abort response.
	m := NewManager()
	m.connect = func(context.Context, Config) (*mongo.Client, error) { return client, nil }
	var events []TransactionEvent
	monitor := &TransactionMonitor{Event: func(e TransactionEvent) {
		events = append(events, e)
		if e.Phase == "started" {
			panic("observer must not break transaction")
		}
	}}
	if err := m.Init(context.Background(), map[string]Config{"main": {URI: "mongodb://localhost", DBName: "test", TransactionMonitor: monitor}}); err != nil {
		t.Fatal(err)
	}
	err := m.WithTransaction(context.Background(), "main", func(ctx context.Context) error {
		doc := bson.M{"_id": 1}
		_, err := NewRepository[bson.M](client.Database("test"), "items").InsertOne(ctx, &doc)
		return err
	})
	var serverError mongo.ServerError
	if !errors.As(err, &serverError) || !serverError.HasErrorLabel("UnknownTransactionCommitResult") {
		t.Fatalf("unknown commit lost: %v", err)
	}
	stats := m.TransactionStats()
	if stats.Unknown != 1 || stats.Active != 0 || stats.CallbackAttempts != 1 || stats.Committed != 0 {
		t.Fatalf("incorrect unknown classification: %+v", stats)
	}
	if len(events) != 3 || events[2].Outcome != "unknown" {
		t.Fatalf("bad transaction events: %+v", events)
	}
}

func TestCursorPaginationTokenAndOptions(t *testing.T) {
	client, _, commands := mockClient(t,
		cursorResponse("firstBatch", 0, bson.D{{Key: "_id", Value: int32(1)}}, bson.D{{Key: "_id", Value: int32(2)}}, bson.D{{Key: "_id", Value: int32(3)}}),
		cursorResponse("firstBatch", 0, bson.D{{Key: "_id", Value: int32(3)}}),
	)
	r := NewRepository[bson.M](client.Database("test"), "items")
	page, err := r.PaginateCursor(context.Background(), nil, CursorPageRequest{Size: 2}, options.Find().SetLimit(0).SetSkip(100))
	if err != nil || !page.HasMore || len(page.Items) != 2 || page.NextCursor == "" {
		t.Fatalf("bad cursor page: %+v %v", page, err)
	}
	next, err := r.PaginateCursor(context.Background(), bson.M{"active": true}, CursorPageRequest{Size: 2, After: page.NextCursor})
	if err != nil || next.HasMore || len(next.Items) != 1 || next.NextCursor != "" {
		t.Fatalf("bad final page: %+v %v", next, err)
	}
	if (*commands)[0].Lookup("limit").Int64() != 3 {
		t.Fatal("cursor bounds overridden")
	}
	for _, request := range []CursorPageRequest{{After: "bad%token"}, {After: page.NextCursor, Descending: true}} {
		if _, err := r.PaginateCursor(context.Background(), nil, request); !errors.Is(err, ErrInvalidCursor) {
			t.Fatal("invalid cursor accepted")
		}
	}
	if len(*commands) != 2 {
		t.Fatal("invalid cursor accessed server")
	}
}

func TestAggregateEachStreamsBatches(t *testing.T) {
	client, _, _ := mockClient(t, cursorResponse("firstBatch", 123, bson.D{{Key: "total", Value: 2}}), cursorResponse("nextBatch", 0, bson.D{{Key: "total", Value: 3}}))
	type output struct {
		Total int `bson:"total"`
	}
	sum := 0
	err := ExecuteAggregateEach(context.Background(), client.Database("test").Collection("items"), mongo.Pipeline{}, func(_ context.Context, row *output) error { sum += row.Total; return nil }, options.Aggregate().SetBatchSize(1))
	if err != nil || sum != 5 {
		t.Fatalf("aggregate stream sum=%d err=%v", sum, err)
	}
}
