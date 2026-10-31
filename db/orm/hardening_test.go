package orm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/db/orm/backend/postgres"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"gorm.io/gorm"
)

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatal("condition did not become true")
		case <-tick.C:
		}
	}
}

func TestBindPreservesPolicyAndRejectsAnotherClient(t *testing.T) {
	c := openTest(t)
	other := openTest(t)
	ctx := context.Background()
	if err := c.DB().AutoMigrate(&scopedRow{}); err != nil {
		t.Fatal(err)
	}
	rows := []scopedRow{{Tenant: "one", Value: "original"}, {Tenant: "two", Value: "private"}}
	if err := c.DB().Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	opts := RepositoryOptions{WritableColumns: []string{"value"}, Scope: func(db *gorm.DB) *gorm.DB { return db.Where("tenant = ?", "one") }, MaxBulkRows: 2}
	repo, err := NewRepository[scopedRow](c, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.WithTransaction(ctx, nil, func(tx *Transaction) error {
		if _, e := repo.InTransaction(tx); !errors.Is(e, ErrTransactionClientMismatch) {
			t.Fatalf("cross-client bind accepted: %v", e)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var escaped *Repository[scopedRow]
	var txContext context.Context
	err = c.WithTransaction(ctx, nil, func(tx *Transaction) error {
		txContext = tx.Context()
		bound, e := repo.InTransaction(tx)
		if e != nil {
			return e
		}
		escaped = bound
		if bound.opts.MaxBulkRows != 2 {
			t.Fatal("lost limits")
		}
		if e := bound.PatchByID(tx.Context(), rows[1].ID, map[string]any{"value": "leaked"}); !errors.Is(e, ErrNotFound) {
			t.Fatal(e)
		}
		if e := bound.PatchByID(tx.Context(), rows[0].ID, map[string]any{"tenant": "two"}); !errors.Is(e, ErrInvalidArgument) {
			t.Fatal(e)
		}
		return bound.PatchByID(tx.Context(), rows[0].ID, map[string]any{"value": "updated"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := escaped.FindOne(txContext); !errors.Is(err, ErrTransactionClosed) {
		t.Fatal(err)
	}
	got, err := repo.FindByID(ctx, rows[0].ID)
	if err != nil || got.Value != "updated" {
		t.Fatalf("bound write: %+v %v", got, err)
	}
}

func TestAdmissionRejectCancelAndClose(t *testing.T) {
	c := openTest(t)
	c.cfg.MaxQueued = 1
	ctx := context.Background()
	release, err := c.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	queued := make(chan error, 1)
	go func() { _, e := userRepo(t, c).FindOne(canceled); queued <- e }()
	waitFor(t, func() bool { return c.AdmissionStats().Queued == 1 })
	called := false
	err = c.WithTransaction(ctx, nil, func(*Transaction) error { called = true; return nil })
	var txErr *TransactionError
	if !errors.Is(err, ErrBusy) || !errors.As(err, &txErr) || txErr.Phase != PhaseAdmission || called {
		t.Fatalf("admission: %v", err)
	}
	cancel()
	if e := <-queued; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	go func() { _, e := userRepo(t, c).FindOne(ctx); queued <- e }()
	waitFor(t, func() bool { return c.AdmissionStats().Queued == 1 })
	timeout, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	if err := c.Close(timeout); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if e := <-queued; !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	release()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	stats := c.AdmissionStats()
	if stats.InFlight != 0 || stats.Queued != 0 || stats.Rejected != 1 || stats.Canceled != 2 || stats.WaitDuration <= 0 {
		t.Fatalf("admission stats: %+v", stats)
	}
}

func TestTransactionUsesOnePermitAndQueuedWorkResumes(t *testing.T) {
	c := openTest(t)
	ctx := context.Background()
	repo := userRepo(t, c)
	started, finish, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- c.WithTransaction(ctx, nil, func(tx *Transaction) error {
			r, err := repo.InTransaction(tx)
			if err != nil {
				return err
			}
			if err := r.Create(tx.Context(), &testUser{Email: "single-permit"}); err != nil {
				return err
			}
			close(started)
			<-finish
			return nil
		})
	}()
	<-started
	read := make(chan error, 1)
	go func() { _, err := repo.FindOne(ctx); read <- err }()
	waitFor(t, func() bool { return c.AdmissionStats().Queued == 1 })
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if s := c.TransactionStats(); s.Committed != 1 || s.Active != 0 {
		t.Fatalf("transaction stats: %+v", s)
	}
}

type versionedRow struct {
	ID      uint `gorm:"primaryKey"`
	Tenant  string
	Value   string
	Version uint64
}

func versionRepo(t *testing.T, c *Client) *Repository[versionedRow] {
	t.Helper()
	r, err := NewRepository[versionedRow](c, RepositoryOptions{WritableColumns: []string{"value"}, VersionColumn: "version", Scope: func(db *gorm.DB) *gorm.DB { return db.Where("tenant = ?", "one") }})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestCompareAndSwapHasOneWinner(t *testing.T) {
	c := openTest(t)
	ctx := context.Background()
	if err := c.DB().AutoMigrate(&versionedRow{}); err != nil {
		t.Fatal(err)
	}
	r := versionRepo(t, c)
	row := versionedRow{Tenant: "one", Value: "initial"}
	if err := r.Create(ctx, &row); err != nil {
		t.Fatal(err)
	}
	if err := r.PatchByID(ctx, row.ID, map[string]any{"value": "unsafe"}); !errors.Is(err, ErrVersionRequired) {
		t.Fatal(err)
	}
	var wins atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := r.CompareAndSwapByID(ctx, row.ID, 0, map[string]any{"value": "won"})
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrVersionConflict) || !errors.Is(err, ErrConflict) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("winners: %d", wins.Load())
	}
	got, err := r.FindByID(ctx, row.ID)
	if err != nil || got.Version != 1 || got.Value != "won" {
		t.Fatalf("CAS: %+v %v", got, err)
	}
	private := versionedRow{Tenant: "two"}
	if err := c.DB().Create(&private).Error; err != nil {
		t.Fatal(err)
	}
	if err := r.CompareAndSwapByID(ctx, private.ID, 0, map[string]any{"value": "leaked"}); !errors.Is(err, ErrVersionConflict) {
		t.Fatal(err)
	}
	if err := r.CompareAndSwapByID(ctx, row.ID, 1, map[string]any{"version": 999}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if _, err := r.UpsertMany(ctx, []versionedRow{row}, UpsertPolicy{ConflictColumns: []string{"id"}, DoNothing: true}); !errors.Is(err, ErrVersionRequired) {
		t.Fatal(err)
	}
}

// Failure seam delegates real SQL operations, but simulates lost commit ACKs.
type failurePool struct {
	gorm.ConnPool
	pool                             *sql.DB
	beginErr, commitErr, rollbackErr error
}

func (p *failurePool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	if p.beginErr != nil {
		return nil, p.beginErr
	}
	tx, err := p.pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &failureTx{Tx: tx, commitErr: p.commitErr, rollbackErr: p.rollbackErr}, nil
}

type failureTx struct {
	*sql.Tx
	commitErr, rollbackErr error
}

func (tx *failureTx) Commit() error {
	err := tx.Tx.Commit()
	if err != nil {
		return err
	}
	return tx.commitErr
}
func (tx *failureTx) Rollback() error {
	err := tx.Tx.Rollback()
	if err != nil {
		return err
	}
	return tx.rollbackErr
}
func failSettlement(c *Client, begin, commit, rollback error) {
	p := &failurePool{ConnPool: c.pool, pool: c.pool, beginErr: begin, commitErr: commit, rollbackErr: rollback}
	c.db.ConnPool = p
	c.db.Statement.ConnPool = p
}
func TestTransactionFailurePhasesAndUnknownCommit(t *testing.T) {
	ctx := context.Background()
	secret := errors.New("sensitive-connection-detail")
	t.Run("setup", func(t *testing.T) {
		c := openTest(t)
		c.backend = postgres.New("unused-in-failure-fixture")
		err := c.WithTransaction(ctx, nil, func(*Transaction) error { t.Fatal("callback ran after failed setup"); return nil })
		var te *TransactionError
		if !errors.As(err, &te) || te.Phase != PhaseSetup || te.Outcome != OutcomeRolledBack {
			t.Fatalf("setup classification: %v", err)
		}
		if c.AdmissionStats().InFlight != 0 {
			t.Fatal("failed setup leaked permit")
		}
	})
	t.Run("begin", func(t *testing.T) {
		c := openTest(t)
		failSettlement(c, secret, nil, nil)
		err := c.WithTransaction(ctx, nil, func(*Transaction) error { t.Fatal("callback ran"); return nil })
		var te *TransactionError
		if !errors.As(err, &te) || te.Phase != PhaseBegin || te.Outcome != OutcomeNotStarted || !errors.Is(err, secret) || strings.Contains(err.Error(), secret.Error()) {
			t.Fatalf("begin classification: %v", err)
		}
	})
	t.Run("callback rollback failure", func(t *testing.T) {
		c := openTest(t)
		failSettlement(c, nil, nil, secret)
		callback := errors.New("callback failure")
		err := c.WithTransaction(ctx, nil, func(*Transaction) error { return callback })
		var te *TransactionError
		if !errors.As(err, &te) || te.Phase != PhaseCallback || te.Outcome != OutcomeUnknown || !errors.Is(te.RollbackError(), secret) || !errors.Is(err, callback) {
			t.Fatalf("rollback classification: %v", err)
		}
	})
	t.Run("commit ACK lost", func(t *testing.T) {
		c := openTest(t)
		r := userRepo(t, c)
		failSettlement(c, nil, secret, nil)
		err := c.WithTransaction(ctx, nil, func(tx *Transaction) error {
			bound, e := r.InTransaction(tx)
			if e != nil {
				return e
			}
			return bound.Create(tx.Context(), &testUser{Email: "committed-despite-error"})
		})
		var te *TransactionError
		if !errors.As(err, &te) || te.Phase != PhaseCommit || te.Outcome != OutcomeUnknown || !errors.Is(err, ErrCommitUnknown) || !errors.Is(err, secret) {
			t.Fatalf("commit classification: %v", err)
		}
		if _, e := r.FindOne(ctx); e != nil {
			t.Fatal("test must actually persist before losing ACK")
		}
		if c.TransactionStats().Unknown != 1 {
			t.Fatal("unknown commit metric lost")
		}
	})
}

type cancelKey struct {
	Text   string
	Cancel context.CancelFunc
}

func (k cancelKey) Value() (driver.Value, error) {
	if k.Cancel != nil {
		k.Cancel()
	}
	return k.Text, nil
}
func (cancelKey) GormDataType() string { return "string" }

type bulkCancelRow struct {
	Key cancelKey `gorm:"primaryKey"`
}

func TestBulkLimitsAndCancellationBeforeSQL(t *testing.T) {
	c := openTest(t)
	ctx := context.Background()
	r, err := NewRepository[testUser](c, RepositoryOptions{MaxBulkRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.CreateMany(ctx, make([]testUser, 3)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if _, err := r.UpsertMany(ctx, make([]testUser, 3), UpsertPolicy{ConflictColumns: []string{"email"}, DoNothing: true}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := c.DB().AutoMigrate(&bulkCancelRow{}); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	cr, err := NewRepository[bulkCancelRow](c, RepositoryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	creates := 0
	if err := c.db.Callback().Create().Before("*").Register("test:count_create", func(*gorm.DB) { creates++ }); err != nil {
		t.Fatal(err)
	}
	err = cr.CreateMany(canceled, []bulkCancelRow{{Key: cancelKey{Text: "one", Cancel: cancel}}, {Key: cancelKey{Text: "two"}}})
	if !errors.Is(err, context.Canceled) || creates != 0 {
		t.Fatalf("validation must cancel before SQL: %v creates=%d", err, creates)
	}
	if c.AdmissionStats().InFlight != 0 {
		t.Fatal("validation leaked permit")
	}
}

func TestMetricsMonitorAndCloseRegistration(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	cfg := testConfig()
	cfg.EnableTelemetry = true
	cfg.MeterProvider = provider
	cfg.Monitor = &Monitor{Event: func(Event) { panic("monitor failure") }}
	c, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if err := c.WithTransaction(context.Background(), nil, func(*Transaction) error { <-time.After(20 * time.Millisecond); return nil }); err != nil {
		t.Fatal(err)
	}
	_ = c.WithTransaction(context.Background(), nil, func(*Transaction) error { return errors.New("rollback") })
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			names[m.Name] = true
		}
	}
	for _, name := range []string{"orm.transactions", "orm.transaction.duration", "orm.pool.connections", "orm.pool.waits", "orm.pool.wait_time"} {
		if !names[name] {
			t.Errorf("metric missing: %s", name)
		}
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	rm = metricdata.ResourceMetrics{}
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if strings.HasPrefix(m.Name, "orm.pool.") {
				t.Errorf("pool callback survived close: %s", m.Name)
			}
		}
	}
	stats := c.TransactionStats()
	if stats.Started != 2 || stats.Committed != 1 || stats.RolledBack != 1 || stats.Failed != 1 || stats.Duration <= 0 {
		t.Fatalf("stats: %+v", stats)
	}
}
