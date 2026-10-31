package mongodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestClientConfiguration(t *testing.T) {
	opts, err := clientOptions(Config{URI: "mongodb://localhost"})
	if err != nil {
		t.Fatal(err)
	}
	if *opts.MaxPoolSize != 100 || *opts.ConnectTimeout != 10*time.Second || *opts.Timeout != 30*time.Second {
		t.Fatalf("missing bounded defaults: %+v", opts)
	}
	cfg := Config{URI: "mongodb://localhost/?maxPoolSize=7&minPoolSize=2&connectTimeoutMS=1200&timeoutMS=2500"}
	opts, err = clientOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if *opts.MaxPoolSize != 7 || *opts.MinPoolSize != 2 || *opts.ConnectTimeout != 1200*time.Millisecond || *opts.Timeout != 2500*time.Millisecond {
		t.Fatal("zero fields must preserve URI settings")
	}
	cfg.MaxPoolSize, cfg.ConnectTimeout, cfg.OperationTimeout = 8, time.Second, 2*time.Second
	opts, err = clientOptions(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if *opts.MaxPoolSize != 8 || *opts.ConnectTimeout != time.Second || *opts.Timeout != 2*time.Second {
		t.Fatal("explicit fields must override URI")
	}
	for _, cfg := range []Config{
		{}, {URI: "mongodb://localhost", PingTimeout: -1},
		{URI: "mongodb://localhost", ConnectTimeout: -1},
		{URI: "mongodb://localhost", OperationTimeout: -1},
		{URI: "mongodb://localhost", MinPoolSize: 3, MaxPoolSize: 2},
		{URI: "mongodb://user:super-secret@localhost/?maxPoolSize=invalid"},
	} {
		_, err := clientOptions(cfg)
		if err == nil {
			t.Fatal("expected invalid configuration error")
		}
		if strings.Contains(err.Error(), "super-secret") {
			t.Fatal("configuration error leaked credentials")
		}
	}
}

func TestTelemetryDoesNotCaptureDocuments(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
	opts, err := clientOptions(Config{URI: "mongodb://localhost", EnableTelemetry: true})
	if err != nil {
		t.Fatal(err)
	}
	cmd, _ := bson.Marshal(bson.D{{Key: "insert", Value: "patients"}, {Key: "documents", Value: bson.A{bson.D{{Key: "name", Value: "private-patient"}}}}})
	opts.Monitor.Started(context.Background(), &event.CommandStartedEvent{Command: cmd, DatabaseName: "test", CommandName: "insert", RequestID: 1, ConnectionID: "localhost:27017"})
	opts.Monitor.Succeeded(context.Background(), &event.CommandSucceededEvent{CommandFinishedEvent: event.CommandFinishedEvent{CommandName: "insert", RequestID: 1, ConnectionID: "localhost:27017"}})
	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected one span, got %d", len(spans))
	}
	for _, attr := range spans[0].Attributes {
		if attr.Key == "db.query.text" || attr.Key == "db.statement" || strings.Contains(attr.Value.AsString(), "private-patient") {
			t.Fatal("trace exposed command data")
		}
	}
}

func TestConnectAndPing(t *testing.T) {
	for _, fails := range []bool{false, true} {
		t.Run(fmt.Sprint(fails), func(t *testing.T) {
			cfg := Config{URI: "mongodb://localhost"} // PingTimeout=0 must use a positive default.
			opts, err := clientOptions(cfg)
			if err != nil {
				t.Fatal(err)
			}
			response := bson.D{{Key: "ok", Value: 1}}
			if fails {
				response = bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 2}, {Key: "errmsg", Value: "ping failed"}}
			}
			deployment := &testDeployment{MockDeployment: drivertest.NewMockDeployment(response)}
			opts.Deployment = deployment
			client, err := connectAndPing(context.Background(), cfg, opts)
			if fails {
				if err == nil || client != nil || !deployment.closed.Load() {
					t.Fatalf("failed ping must close client: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := client.Disconnect(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// The driver's wire-protocol mock exercises real command construction, decoding
// and cursor handling without requiring a live database or changing application data.
type testDeployment struct {
	*drivertest.MockDeployment
	closed          atomic.Bool
	disconnectCalls atomic.Int64
}

func (d *testDeployment) Disconnect(ctx context.Context) error {
	d.disconnectCalls.Add(1)
	if !d.closed.CompareAndSwap(false, true) {
		return nil
	}
	return d.MockDeployment.Disconnect(ctx)
}

func mockClient(t *testing.T, responses ...bson.D) (*mongo.Client, *testDeployment, *[]bson.Raw) {
	t.Helper()
	deployment := &testDeployment{MockDeployment: drivertest.NewMockDeployment(responses...)}
	commands := new([]bson.Raw)
	opts := options.Client().SetRetryReads(false).SetRetryWrites(false).SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, e *event.CommandStartedEvent) {
			*commands = append(*commands, append(bson.Raw(nil), e.Command...))
		},
	})
	opts.Deployment = deployment
	client, err := mongo.Connect(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client, deployment, commands
}

func cursorResponse(batch string, id int64, docs ...bson.D) bson.D {
	items := bson.A{}
	for _, doc := range docs {
		items = append(items, doc)
	}
	return bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{
		{Key: "id", Value: id}, {Key: "ns", Value: "test.items"}, {Key: batch, Value: items},
	}}}
}

func TestManagerAtomicInitialization(t *testing.T) {
	m := NewManager()
	first, deployment, _ := mockClient(t)
	existing, existingDeployment, _ := mockClient(t)
	failure := errors.New("connection unavailable")
	ctx, cancel := context.WithCancel(context.Background())
	m.connect = func(_ context.Context, cfg Config) (*mongo.Client, error) {
		if cfg.DBName == "existing" {
			return existing, nil
		}
		if cfg.DBName == "first" {
			return first, nil
		}
		if _, err := m.Lookup("a"); err == nil {
			t.Error("partially initialized connection became visible")
		}
		cancel()
		return nil, failure
	}
	if err := m.AddConnection(ctx, "existing", Config{URI: "mongodb://localhost", DBName: "existing"}, true); err != nil {
		t.Fatal(err)
	}
	err := m.Init(ctx, map[string]Config{
		"a": {URI: "mongodb://localhost", DBName: "first"},
		"b": {URI: "mongodb://localhost", DBName: "second"},
	}, "a")
	if !errors.Is(err, failure) {
		t.Fatalf("lost original failure: %v", err)
	}
	if _, err := m.Lookup("a"); err == nil {
		t.Fatal("failed initialization published connection")
	}
	if !deployment.closed.Load() {
		t.Fatal("rollback did not close client")
	}
	if db, err := m.Lookup(""); err != nil || db.Client() != existing || existingDeployment.closed.Load() {
		t.Fatal("rollback damaged preexisting default connection")
	}
	if m.initialized {
		t.Fatal("failed initialization marked ready")
	}
}

func TestManagerValidationBeforeDial(t *testing.T) {
	m := NewManager()
	m.connect = func(context.Context, Config) (*mongo.Client, error) {
		t.Fatal("must validate before dialing")
		return nil, nil
	}
	cfg := Config{URI: "mongodb://localhost", DBName: "test"}
	if err := m.Init(context.Background(), map[string]Config{"a": cfg}, "missing"); err == nil {
		t.Fatal("invalid default accepted")
	}
	for _, invalid := range []Config{{URI: cfg.URI}, {URI: cfg.URI, DBName: "bad.name"}, {URI: cfg.URI, DBName: "test", PingTimeout: -1}} {
		if err := m.Init(context.Background(), map[string]Config{"a": invalid}); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	if err := m.AddConnection(context.Background(), "", cfg, false); err == nil {
		t.Fatal("empty connection name accepted")
	}
}

func TestManagerLifecycle(t *testing.T) {
	m := NewManager()
	count := 0
	m.connect = func(context.Context, Config) (*mongo.Client, error) { count++; c, _, _ := mockClient(t); return c, nil }
	cfg := Config{URI: "mongodb://localhost", DBName: "test"}
	ctx := context.Background()
	if err := m.AddConnection(ctx, "existing", cfg, true); err != nil {
		t.Fatal(err)
	}
	before, _ := m.Lookup("")
	if err := m.Init(ctx, map[string]Config{"a": cfg}, "a"); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Lookup("existing"); got != before {
		t.Fatal("lost existing connection")
	}
	if got, _ := m.Lookup(""); got == before {
		t.Fatal("explicit default ignored")
	}
	if err := m.Init(ctx, map[string]Config{"ignored": cfg}); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatal("repeated init opened new connections")
	}
	if err := m.AddConnection(ctx, "a", cfg, false); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := m.DisconnectAll(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Lookup("a"); err == nil {
		t.Fatal("closed handle still published")
	}
	if err := m.DisconnectAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Init(ctx, map[string]Config{"fresh": cfg}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Lookup(""); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatal("reinitialization did not open fresh client")
	}
	if err := m.DisconnectAll(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestManagerConcurrentLifecycle(t *testing.T) {
	m := NewManager()
	m.connect = func(context.Context, Config) (*mongo.Client, error) { c, _, _ := mockClient(t); return c, nil }
	cfg := Config{URI: "mongodb://localhost", DBName: "test"}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx := context.Background()
			for n := 0; n < 5; n++ {
				_ = m.Init(ctx, map[string]Config{"main": cfg})
				_ = m.AddConnection(ctx, fmt.Sprint(i), cfg, false)
				_, _ = m.Lookup("")
				_ = m.DisconnectAll(ctx)
			}
		}(i)
	}
	wg.Wait()
	if err := m.DisconnectAll(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExists(t *testing.T) {
	for _, tc := range []struct {
		name          string
		response      bson.D
		exists, fails bool
	}{
		{"found", cursorResponse("firstBatch", 0, bson.D{{Key: "_id", Value: 1}}), true, false},
		{"missing", cursorResponse("firstBatch", 0), false, false},
		{"server error", bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 2}, {Key: "errmsg", Value: "bad query"}}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _, commands := mockClient(t, tc.response)
			repo := NewRepository[bson.M](client.Database("test"), "items")
			found, err := repo.Exists(context.Background(), nil)
			if found != tc.exists || (err != nil) != tc.fails {
				t.Fatalf("got %v, %v", found, err)
			}
			if (*commands)[0].Lookup("projection").Document().Lookup("_id").Int32() != 1 {
				t.Fatal("existence query must project only ID")
			}
		})
	}
}

func TestPagination(t *testing.T) {
	countResponse := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "test.items"}, {Key: "firstBatch", Value: bson.A{bson.D{{Key: "n", Value: int64(11)}}}}}}}
	client, _, commands := mockClient(t, countResponse, cursorResponse("firstBatch", 0, bson.D{{Key: "_id", Value: 3}}))
	repo := NewRepository[bson.M](client.Database("test"), "items")
	response, err := repo.Paginate(context.Background(), nil, PageRequest{Page: 2, Size: 2}, options.Find().SetSkip(99).SetLimit(0).SetProjection(bson.M{"_id": 1}))
	if err != nil {
		t.Fatal(err)
	}
	if response.TotalRows != 11 || response.TotalPages != 6 || response.Page != 2 || len(response.Items) != 1 {
		t.Fatalf("bad response: %+v", response)
	}
	find := (*commands)[1]
	if find.Lookup("skip").Int64() != 2 || find.Lookup("limit").Int64() != 2 {
		t.Fatal("caller bypassed pagination bounds")
	}
	if find.Lookup("sort").Document().Lookup("_id").Int32() != 1 {
		t.Fatal("missing stable default sort")
	}
	if find.Lookup("projection").Type == 0 {
		t.Fatal("caller projection lost")
	}
	if math.MaxInt > math.MaxInt32 {
		if _, err := repo.Paginate(context.Background(), nil, PageRequest{Page: math.MaxInt, Size: 1000}); err == nil {
			t.Fatal("overflow accepted")
		}
		if len(*commands) != 2 {
			t.Fatal("overflow must fail before database operations")
		}
	}
}

func TestSlicePagination(t *testing.T) {
	for _, length := range []int{0, 2, 3} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			docs := make([]bson.D, length)
			for i := range docs {
				docs[i] = bson.D{{Key: "_id", Value: i}}
			}
			client, _, commands := mockClient(t, cursorResponse("firstBatch", 0, docs...))
			repo := NewRepository[bson.M](client.Database("test"), "items")
			page, err := repo.PaginateSlice(context.Background(), nil, PageRequest{Size: 2}, options.Find().SetLimit(0))
			if err != nil {
				t.Fatal(err)
			}
			if page.HasMore != (length > 2) || len(page.Items) != min(length, 2) || page.Items == nil {
				t.Fatalf("bad slice page: %+v", page)
			}
			if len(*commands) != 1 || (*commands)[0].Lookup("limit").Int64() != 3 {
				t.Fatal("slice pagination should fetch size+1 without counting")
			}
		})
	}
}

func TestFindEach(t *testing.T) {
	stop := errors.New("stop processing")
	for _, tc := range []string{"success", "callback", "decode", "cursor", "cancel"} {
		t.Run(tc, func(t *testing.T) {
			doc := bson.D{{Key: "value", Value: 1}}
			if tc == "decode" {
				doc = bson.D{{Key: "value", Value: bson.D{{Key: "bad", Value: true}}}}
			}
			id := int64(0)
			if tc == "cursor" {
				id = 123
			}
			client, _, _ := mockClient(t, cursorResponse("firstBatch", id, doc), bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 2}, {Key: "errmsg", Value: "cursor failed"}})
			repo := NewRepository[struct {
				Value int `bson:"value"`
			}](client.Database("test"), "items")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc == "cancel" {
				cancel()
			}
			calls := 0
			err := repo.FindEach(ctx, nil, func(context.Context, *struct {
				Value int `bson:"value"`
			}) error {
				calls++
				if tc == "callback" {
					return stop
				}
				return nil
			}, options.Find().SetBatchSize(1))
			if tc == "success" && (err != nil || calls != 1) {
				t.Fatalf("got calls=%d, err=%v", calls, err)
			}
			if tc != "success" && err == nil {
				t.Fatal("expected error")
			}
			if tc == "callback" && !errors.Is(err, stop) {
				t.Fatal("callback error lost")
			}
			if tc == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
		})
	}
}

func TestFindEachBatchesAndCancellationCleanup(t *testing.T) {
	t.Run("multiple batches", func(t *testing.T) {
		client, _, commands := mockClient(t,
			cursorResponse("firstBatch", 123, bson.D{{Key: "_id", Value: 1}}),
			cursorResponse("nextBatch", 0, bson.D{{Key: "_id", Value: 2}}),
		)
		repo := NewRepository[bson.M](client.Database("test"), "items")
		calls := 0
		err := repo.FindEach(context.Background(), nil, func(context.Context, *bson.M) error { calls++; return nil }, options.Find().SetBatchSize(1))
		if err != nil || calls != 2 {
			t.Fatalf("streamed %d items: %v", calls, err)
		}
		if len(*commands) != 2 || (*commands)[1].Lookup("getMore").Int64() != 123 {
			t.Fatal("did not stream subsequent batch")
		}
	})
	t.Run("close after cancellation", func(t *testing.T) {
		client, _, commands := mockClient(t,
			cursorResponse("firstBatch", 123, bson.D{{Key: "_id", Value: 1}}),
			bson.D{{Key: "ok", Value: 1}, {Key: "cursorsKilled", Value: bson.A{int64(123)}}},
		)
		repo := NewRepository[bson.M](client.Database("test"), "items")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := repo.FindEach(ctx, nil, func(context.Context, *bson.M) error { cancel(); return context.Canceled })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
		if len(*commands) != 2 || (*commands)[1].Lookup("killCursors").StringValue() != "items" {
			t.Fatal("canceled context prevented cursor cleanup")
		}
	})
}

func TestTransactionSessionAndOptions(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprint(retry), func(t *testing.T) {
			ok := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}
			responses := []bson.D{ok, ok}
			if retry {
				transient := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 251}, {Key: "errmsg", Value: "retry transaction"}, {Key: "errorLabels", Value: bson.A{"TransientTransactionError"}}}
				responses = []bson.D{transient, ok, ok, ok} // failed insert, abort, insert, commit
			}
			client, _, commands := mockClient(t, responses...)
			m := NewManager()
			m.connect = func(context.Context, Config) (*mongo.Client, error) { return client, nil }
			if err := m.Init(context.Background(), map[string]Config{"main": {URI: "mongodb://localhost", DBName: "test"}}); err != nil {
				t.Fatal(err)
			}
			repo := NewRepository[bson.M](client.Database("test"), "items")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			calls := 0
			err := m.WithTransaction(ctx, "", func(ctx context.Context) error {
				calls++
				if mongo.SessionFromContext(ctx) == nil {
					t.Fatal("transaction context lost its session")
				}
				doc := bson.M{"value": 1}
				_, err := repo.InsertOne(ctx, &doc)
				return err
			}, options.Transaction().SetReadConcern(readconcern.Snapshot()).SetWriteConcern(writeconcern.Majority()))
			if err != nil {
				t.Fatal(err)
			}
			wantCalls := 1
			if retry {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("callback calls: got %d want %d", calls, wantCalls)
			}
			first, last := (*commands)[0], (*commands)[len(*commands)-1]
			if !first.Lookup("startTransaction").Boolean() || first.Lookup("autocommit").Boolean() {
				t.Fatal("insert not in transaction")
			}
			if first.Lookup("readConcern").Document().Lookup("level").StringValue() != "snapshot" {
				t.Fatal("read concern lost")
			}
			if last.Lookup("writeConcern").Document().Lookup("w").StringValue() != "majority" {
				t.Fatal("write concern lost")
			}
		})
	}
}
