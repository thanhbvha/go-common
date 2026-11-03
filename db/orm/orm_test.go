package orm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/postgres"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/sqlite"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type testUser struct {
	ID      uint   `gorm:"primaryKey;column:user_key"`
	Email   string `gorm:"uniqueIndex"`
	Name    string
	Age     int
	Active  bool
	Removed gorm.DeletedAt `gorm:"column:removed_at"`
}

var dbCounter atomic.Int64

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Backend = sqlite.New(fmt.Sprintf("file:orm_%d?mode=memory&cache=shared", dbCounter.Add(1)))
	cfg.MaxOpenConns = 1
	cfg.MaxIdleConns = 1
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return cfg
}
func openTest(t testing.TB) *Client {
	t.Helper()
	cfg := testConfig()
	cfg.Backend = sqlite.New(filepath.Join(t.TempDir(), "orm.db"))
	c, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := c.DB().AutoMigrate(&testUser{}); err != nil {
		t.Fatal(err)
	}
	return c
}
func userOptions() RepositoryOptions {
	return RepositoryOptions{WritableColumns: []string{"name", "age", "active"}, SortableColumns: []string{"age", "name"}, BatchSize: 2}
}
func userRepo(t testing.TB, c *Client) *Repository[testUser] {
	t.Helper()
	r, err := NewRepository[testUser](c, userOptions())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRepositorySafetyAndWriteContracts(t *testing.T) {
	ctx := context.Background()
	c := openTest(t)
	r := userRepo(t, c)
	users := []testUser{{Email: "a@example.invalid", Name: "a", Age: 10, Active: true}, {Email: "b@example.invalid", Name: "b", Age: 10}}
	if err := r.CreateMany(ctx, users); err != nil {
		t.Fatal(err)
	}
	if _, err := r.FindByID(ctx, "1 OR 1=1"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("string must be a bound value: %v", err)
	}
	if err := r.DeleteByID(ctx, "1 OR 1=1"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("delete must bind: %v", err)
	}
	if err := r.PatchByID(ctx, users[0].ID, map[string]any{"active": false, "age": 0, "name": ""}); err != nil {
		t.Fatal(err)
	}
	got, err := r.FindByID(ctx, users[0].ID)
	if err != nil || got.Active || got.Age != 0 || got.Name != "" {
		t.Fatalf("zero values lost: %+v %v", got, err)
	}
	if err := r.PatchByID(ctx, 999, map[string]any{"age": 3}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	for _, patch := range []map[string]any{nil, {"user_key": 1}, {"removed_at": nil}, {"age": gorm.Expr("age + 1")}, {"email": "other"}, {"name = NULL --": "bad"}} {
		if err := r.PatchByID(ctx, users[0].ID, patch); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("accepted unsafe patch: %v", err)
		}
	}
	for _, id := range []any{nil, 0, -1, []uint{1, 2}, gorm.Expr("1 OR 1=1"), map[string]any{"user_key": 1}} {
		if err := r.DeleteByID(ctx, id); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("accepted non-scalar id: %v", err)
		}
	}
	if err := r.DeleteByID(ctx, users[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := r.FindByID(ctx, users[0].ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := r.RestoreByID(ctx, users[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := r.RestoreByID(ctx, users[0].ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	page, err := r.Paginate(ctx, PageRequest{})
	if err != nil || page.TotalRows != 2 {
		t.Fatalf("unexpected insert/delete: %+v %v", page, err)
	}
}

func TestPaginationAndCursor(t *testing.T) {
	ctx := context.Background()
	c := openTest(t)
	r := userRepo(t, c)
	users := make([]testUser, 7)
	for i := range users {
		users[i] = testUser{Email: fmt.Sprintf("%d@example.invalid", i), Age: i % 2}
	}
	if err := r.CreateMany(ctx, users); err != nil {
		t.Fatal(err)
	}
	page, err := r.Paginate(ctx, PageRequest{Size: 2, Sorts: []Sort{{Field: "age", Direction: Descending}}})
	if err != nil || page.TotalRows != 7 || page.TotalPages != 4 || page.Items[0].ID != users[1].ID || page.Items[1].ID != users[3].ID {
		t.Fatalf("unstable ordering: %+v %v", page, err)
	}
	for _, req := range []PageRequest{{Page: math.MaxInt, Size: 100}, {Page: -1}, {Size: 1001}, {Sorts: []Sort{{Field: "age; DELETE FROM test_users", Direction: Ascending}}}, {Sorts: []Sort{{Field: "age", Direction: "desc;drop"}}}} {
		if _, err := r.Paginate(ctx, req); !errors.Is(err, ErrInvalidArgument) {
			t.Fatal(err)
		}
	}
	filter := func(db *gorm.DB) *gorm.DB { return db.Where("age = ?", 1) }
	filtered, err := r.Paginate(ctx, PageRequest{Page: 2, Size: 2}, filter)
	if err != nil || filtered.TotalRows != 3 || len(filtered.Items) != 1 {
		t.Fatalf("count/read contamination: %+v %v", filtered, err)
	}
	for _, direction := range []Direction{Ascending, Descending} {
		var ids []uint
		next := ""
		for {
			resp, err := r.CursorPage(ctx, CursorRequest{Size: 2, After: next, Direction: direction})
			if err != nil {
				t.Fatal(err)
			}
			for _, item := range resp.Items {
				ids = append(ids, item.ID)
			}
			next = resp.Next
			if next == "" {
				break
			}
		}
		if len(ids) != 7 {
			t.Fatalf("lost cursor rows: %v", ids)
		}
		for i := 1; i < len(ids); i++ {
			if direction == Ascending && ids[i] <= ids[i-1] || direction == Descending && ids[i] >= ids[i-1] {
				t.Fatalf("bad cursor order %v", ids)
			}
		}
	}
	if _, err := r.CursorPage(ctx, CursorRequest{After: "malformed"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
}

func TestBulkAtomicityAndExplicitUpsert(t *testing.T) {
	ctx := context.Background()
	c := openTest(t)
	r := userRepo(t, c)
	// Duplicate is in a later batch: earlier batches must roll back too.
	batch := []testUser{{Email: "a"}, {Email: "b"}, {Email: "a"}}
	if err := r.CreateMany(ctx, batch); err == nil {
		t.Fatal("duplicate accepted")
	}
	page, err := r.Paginate(ctx, PageRequest{})
	if err != nil || page.TotalRows != 0 {
		t.Fatalf("partial batch committed: %+v %v", page, err)
	}
	for _, policy := range []UpsertPolicy{{}, {ConflictColumns: []string{"email"}}, {ConflictColumns: []string{"email"}, UpdateColumns: []string{"user_key"}}, {ConflictColumns: []string{"email"}, UpdateColumns: []string{"name"}, DoNothing: true}} {
		if _, err := r.UpsertMany(ctx, []testUser{{Email: "a"}}, policy); !errors.Is(err, ErrInvalidArgument) {
			t.Fatal(err)
		}
	}
	policy := UpsertPolicy{ConflictColumns: []string{"email"}, UpdateColumns: []string{"name"}}
	for _, name := range []string{"first", "second"} {
		if _, err := r.UpsertMany(ctx, []testUser{{Email: "a", Name: name}}, policy); err != nil {
			t.Fatal(err)
		}
	}
	got, err := r.FindOne(ctx)
	if err != nil || got.Name != "second" {
		t.Fatalf("upsert: %+v %v", got, err)
	}
	rows, err := r.UpsertMany(ctx, []testUser{{Email: "a", Name: "ignored"}}, UpsertPolicy{ConflictColumns: []string{"email"}, DoNothing: true})
	if err != nil || rows != 0 {
		t.Fatalf("ignore: %d %v", rows, err)
	}
}

func TestTransactionScopeRollbackAndDrain(t *testing.T) {
	ctx := context.Background()
	c := openTest(t)
	root := userRepo(t, c)
	rollback := errors.New("application rollback")
	var escaped *Repository[testUser]
	var escapedTx *Transaction
	err := c.WithTransaction(ctx, nil, func(tx *Transaction) error {
		escapedTx = tx
		r, e := NewTransactionRepository[testUser](tx, userOptions())
		if e != nil {
			return e
		}
		escaped = r
		if _, e := r.FindOne(ctx); !errors.Is(e, ErrTransactionContext) {
			t.Fatalf("lost context: %v", e)
		}
		if _, e := root.FindOne(tx.Context()); !errors.Is(e, ErrTransactionContext) {
			t.Fatalf("root used in transaction: %v", e)
		}
		if e := c.WithTransaction(tx.Context(), nil, func(*Transaction) error { return nil }); !errors.Is(e, ErrNestedTransaction) {
			t.Fatal(e)
		}
		if e := r.Create(tx.Context(), &testUser{Email: "rollback"}); e != nil {
			return e
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if _, err := escaped.FindOne(escapedTx.Context()); !errors.Is(err, ErrTransactionClosed) {
		t.Fatal(err)
	}
	if _, err := root.FindOne(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- c.WithTransaction(ctx, nil, func(tx *Transaction) error {
			close(started)
			<-release
			r, e := NewTransactionRepository[testUser](tx, userOptions())
			if e != nil {
				return e
			}
			return r.Create(tx.Context(), &testUser{Email: "drained"})
		})
	}()
	<-started
	timeout, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := c.Close(timeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("did not drain: %v", err)
	}
	if err := c.Ping(ctx); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := c.WithTransaction(ctx, nil, func(*Transaction) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionDeadlineAndPanic(t *testing.T) {
	c := openTest(t)
	ctx := context.Background()
	c.cfg.TransactionTimeout = 10 * time.Millisecond
	if err := c.WithTransaction(ctx, nil, func(tx *Transaction) error { <-tx.Context().Done(); return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	// The panic scenario needs its normal transaction budget and an independent
	// pool; cancellation may discard a driver's connection asynchronously.
	c = openTest(t)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic swallowed")
			}
		}()
		_ = c.WithTransaction(ctx, nil, func(tx *Transaction) error {
			r, e := NewTransactionRepository[testUser](tx, userOptions())
			if e != nil {
				t.Fatal(e)
			}
			if e := r.Create(tx.Context(), &testUser{Email: "panic"}); e != nil {
				t.Fatal(e)
			}
			panic("test")
		})
	}()
	if _, err := userRepo(t, c).FindOne(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestManagerAtomicLifecycle(t *testing.T) {
	ctx := context.Background()
	m := NewManager()
	var opened *Client
	calls := 0
	m.open = func(ctx context.Context, cfg Config) (*Client, error) {
		calls++
		if calls == 2 {
			return nil, errors.New("connect failed")
		}
		var err error
		opened, err = Open(ctx, cfg)
		return opened, err
	}
	configs := map[string]Config{"a": testConfig(), "b": testConfig()}
	if err := m.Init(ctx, configs, "a"); err == nil {
		t.Fatal("expected failure")
	}
	if _, err := m.Lookup("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial publication: %v", err)
	}
	if err := opened.pool.Ping(); err == nil {
		t.Fatal("pool leaked")
	}
	m.open = nil
	if err := m.Init(ctx, configs, "a"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close(ctx) })
	if err := m.Init(ctx, configs, "a"); !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatal(err)
	}
	if err := m.Add(ctx, "a", testConfig()); !errors.Is(err, ErrDuplicateConnection) {
		t.Fatal(err)
	}
	// Slow Add must not block lookups; a second lifecycle operation can cancel.
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	m.open = func(ctx context.Context, cfg Config) (*Client, error) {
		close(started)
		<-release
		return Open(ctx, cfg)
	}
	go func() { done <- m.Add(ctx, "c", testConfig()) }()
	<-started
	lookup := make(chan error, 1)
	go func() { _, err := m.Lookup(""); lookup <- err }()
	select {
	case err := <-lookup:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("lookup blocked behind dial")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := m.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Lookup(""); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := m.Init(ctx, configs, "a"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestManagerConcurrentLookupAndClose(t *testing.T) {
	m := NewManager()
	ctx := context.Background()
	if err := m.Init(ctx, map[string]Config{"main": testConfig()}, ""); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, err := m.Lookup("")
				if err != nil && !errors.Is(err, ErrClosed) {
					t.Error(err)
				}
			}
		}()
	}
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestConfigAndContext(t *testing.T) {
	cfg := testConfig()
	bad := cfg
	bad.MaxOpenConns = 0
	if _, err := Open(context.Background(), bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(canceled, cfg); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	m := NewManager()
	calls := 0
	m.open = func(context.Context, Config) (*Client, error) { calls++; return nil, errors.New("unexpected dial") }
	if err := m.Init(context.Background(), map[string]Config{"main": cfg}, "missing"); !errors.Is(err, ErrInvalidConfig) || calls != 0 {
		t.Fatalf("validation after dial: %d %v", calls, err)
	}
	c := openTest(t)
	if _, err := NewRepository[*testUser](c, RepositoryOptions{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewRepository[testUser](c, RepositoryOptions{WritableColumns: []string{"user_key"}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := userRepo(t, c).Paginate(canceled, PageRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type stringKeyRow struct {
	Key   string `gorm:"primaryKey;column:record_key"`
	Value string
}
type scopedRow struct {
	ID        uint `gorm:"primaryKey"`
	Tenant    string
	Value     string
	DeletedAt gorm.DeletedAt
}

func TestStringKeysAndScopedWrites(t *testing.T) {
	c := openTest(t)
	ctx := context.Background()
	if err := c.DB().AutoMigrate(&stringKeyRow{}, &scopedRow{}); err != nil {
		t.Fatal(err)
	}
	stringsRepo, err := NewRepository[stringKeyRow](c, RepositoryOptions{WritableColumns: []string{"value"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := stringsRepo.Create(ctx, &stringKeyRow{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := stringsRepo.CreateMany(ctx, []stringKeyRow{{Key: "a"}, {Key: "b"}, {Key: "c"}}); err != nil {
		t.Fatal(err)
	}
	first, err := stringsRepo.CursorPage(ctx, CursorRequest{Size: 1})
	if err != nil || len(first.Items) != 1 || first.Items[0].Key != "a" {
		t.Fatalf("string cursor: %+v %v", first, err)
	}
	second, err := stringsRepo.CursorPage(ctx, CursorRequest{Size: 1, After: first.Next})
	if err != nil || second.Items[0].Key != "b" {
		t.Fatalf("string cursor next: %+v %v", second, err)
	}
	if _, err := stringsRepo.CursorPage(ctx, CursorRequest{Size: 1, After: first.Next, Direction: Descending}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	rows := []scopedRow{{Tenant: "one", Value: "one"}, {Tenant: "two", Value: "two"}}
	if err := c.DB().Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	opts := RepositoryOptions{WritableColumns: []string{"value"}, Scope: func(db *gorm.DB) *gorm.DB { return db.Where("tenant = ?", "one") }}
	r, err := NewRepository[scopedRow](c, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.PatchByID(ctx, rows[1].ID, map[string]any{"value": "changed"}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := r.DeleteByID(ctx, rows[1].ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := c.DB().Delete(&rows[1]).Error; err != nil {
		t.Fatal(err)
	}
	if err := r.RestoreByID(ctx, rows[1].ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := r.UpsertMany(ctx, rows, UpsertPolicy{ConflictColumns: []string{"id"}, UpdateColumns: []string{"value"}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	page, err := r.Paginate(ctx, PageRequest{})
	if err != nil || page.TotalRows != 1 || page.Items[0].Tenant != "one" {
		t.Fatalf("scope lost: %+v %v", page, err)
	}
}

func TestErrorSanitizesMixedChains(t *testing.T) {
	const secret = "private-driver-detail"
	err := normalize("transaction", errors.Join(normalize("create", errors.New("failed")), errors.New(secret)))
	if strings.Contains(err.Error(), secret) {
		t.Fatal("mixed error chain leaked")
	}
}

func TestPrivacyAndDriverErrorPreservation(t *testing.T) {
	const secret = "sensitive-marker-do-not-export"
	var logs bytes.Buffer
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer provider.Shutdown(context.Background())
	cfg := testConfig()
	cfg.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	cfg.SlowThreshold = time.Nanosecond
	cfg.EnableTelemetry = true
	cfg.TracerProvider = provider
	c, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if err := c.DB().AutoMigrate(&testUser{}); err != nil {
		t.Fatal(err)
	}
	r := userRepo(t, c)
	if err := r.Create(context.Background(), &testUser{Email: secret, Name: secret}); err != nil {
		t.Fatal(err)
	}
	_ = r.Create(context.Background(), &testUser{Email: secret})
	// Include raw SQL literals and a driver error containing the literal.
	_ = c.DB().Exec("SELECT '" + secret + "'").Error
	_ = c.DB().Exec("SELECT missing_column_" + secret).Error
	if strings.Contains(logs.String(), secret) {
		t.Fatalf("log privacy failure")
	}
	spans := recorder.Ended()
	if len(spans) == 0 {
		t.Fatal("no spans recorded")
	}
	for _, span := range spans {
		if strings.Contains(fmt.Sprint(span.Attributes(), span.Events(), span.Status(), span.Name()), secret) {
			t.Fatal("trace privacy failure")
		}
	}
	driverErr := &pgconn.PgError{Code: "23505", Message: secret, Detail: secret}
	safe := normalizeBackend("create", driverErr, postgres.New("unused-in-error-test"))
	var preserved *pgconn.PgError
	if strings.Contains(safe.Error(), secret) || !errors.Is(safe, ErrConflict) || !errors.As(safe, &preserved) || preserved != driverErr {
		t.Fatal("error contract failure")
	}
}

func FuzzPageValidation(f *testing.F) {
	f.Add(1, 10, "age", "asc")
	f.Add(math.MaxInt, 100, "age;drop table users", "desc")
	r := &Repository[testUser]{opts: RepositoryOptions{MaxPageSize: 100, MaxOffset: 100000}, sortable: map[string]bool{"age": true}}
	f.Fuzz(func(t *testing.T, page, size int, field, direction string) {
		req, offset, err := r.page(PageRequest{Page: page, Size: size, Sorts: []Sort{{Field: field, Direction: Direction(direction)}}})
		if err == nil && (offset < 0 || offset > 100000 || req.Size < 1 || req.Size > 100 || field != "age" || (direction != "asc" && direction != "desc")) {
			t.Fatal("invalid pagination accepted")
		}
	})
}
func FuzzCursorDecode(f *testing.F) {
	f.Add("")
	f.Add("garbage")
	f.Add(strings.Repeat("A", 8193))
	c := openTest(f)
	r := userRepo(f, c)
	f.Fuzz(func(t *testing.T, raw string) { _, _ = r.decodeCursor(raw, Ascending) })
}
func FuzzBoundID(f *testing.F) {
	f.Add("1")
	f.Add("1 OR 1=1")
	f.Add("' --")
	c := openTest(f)
	r := userRepo(f, c)
	f.Fuzz(func(t *testing.T, id string) {
		condition, err := r.identity(id)
		if err != nil {
			return
		}
		statement := c.DB().Session(&gorm.Session{DryRun: true}).Where(condition).Take(&testUser{}).Statement
		if len(statement.Vars) != 1 || statement.Vars[0] != condition.(clause.Eq).Value {
			t.Fatal("ID was not bound as a value")
		}
	})
}
