//go:build integration

package orm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Uses a fresh schema, never public application tables. DSN is deployment-owned
// and is never printed. ORM_INTEGRATION_REQUIRED makes missing fixtures fail CI.
func postgresTest(t testing.TB) *Client {
	t.Helper()
	dsn := os.Getenv("ORM_POSTGRES_DSN")
	if dsn == "" {
		if os.Getenv("ORM_INTEGRATION_REQUIRED") == "1" {
			t.Fatal("ORM_POSTGRES_DSN is required")
		}
		t.Skip("set ORM_POSTGRES_DSN for disposable PostgreSQL tests")
	}
	cfg := DefaultConfig()
	cfg.Backend = postgres.New(dsn)
	cfg.MaxOpenConns = 2
	cfg.MaxIdleConns = 2
	c, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("orm_test_%d_%d", time.Now().UnixNano(), dbCounter.Add(1))
	if err := c.DB().Exec("CREATE SCHEMA " + name).Error; err != nil {
		_ = c.Close(context.Background())
		t.Fatal("create isolated test schema failed")
	}
	// Configure naming once, before any model is parsed or used concurrently.
	c.db.Config.NamingStrategy = schema.NamingStrategy{TablePrefix: name + "."}
	t.Cleanup(func() {
		if err := c.DB().Exec("DROP SCHEMA " + name + " CASCADE").Error; err != nil {
			t.Error("drop isolated test schema failed")
		}
		if err := c.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := c.DB().AutoMigrate(&testUser{}); err != nil {
		t.Fatal("create fixture tables failed")
	}
	return c
}

func TestPostgresProduction(t *testing.T) {
	ctx := context.Background()
	c := postgresTest(t)
	r := userRepo(t, c)
	user := testUser{Email: "integration@example.invalid", Age: 7, Active: true}
	if err := r.Create(ctx, &user); err != nil {
		t.Fatal(err)
	}
	t.Run("bound IDs and sorting", func(t *testing.T) {
		if _, err := r.FindByID(ctx, "1 OR 1=1"); err == nil {
			t.Fatal("SQL expression interpreted as an ID")
		}
		if err := r.DeleteByID(ctx, "1 OR 1=1"); err == nil {
			t.Fatal("unsafe deletion")
		}
		if _, err := r.Paginate(ctx, PageRequest{Sorts: []Sort{{Field: "age;SELECT pg_sleep(10)", Direction: Ascending}}}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatal(err)
		}
		if _, err := r.FindByID(ctx, user.ID); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("strict patch and soft delete", func(t *testing.T) {
		if err := r.PatchByID(ctx, user.ID, map[string]any{"active": false, "age": 0}); err != nil {
			t.Fatal(err)
		}
		if err := r.PatchByID(ctx, 999999, map[string]any{"age": 1}); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if err := r.DeleteByID(ctx, user.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := r.FindByID(ctx, user.ID); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if err := r.RestoreByID(ctx, user.ID); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("constraint classification preserves SQLSTATE", func(t *testing.T) {
		err := r.Create(ctx, &testUser{Email: user.Email})
		var pg *pgconn.PgError
		if !errors.Is(err, ErrConflict) || !errors.As(err, &pg) || pg.Code != "23505" {
			t.Fatal("unique constraint classification failed")
		}
		table := r.schema.Table
		// Fixture identifiers originate exclusively from the generated schema name.
		if e := c.DB().Exec("CREATE TABLE " + table + "_children (id bigint PRIMARY KEY, parent_key bigint REFERENCES " + table + "(user_key))").Error; e != nil {
			t.Fatal("create FK fixture failed")
		}
		err = c.normalize("foreign_key", c.DB().Exec("INSERT INTO "+table+"_children (id,parent_key) VALUES (?,?)", 1, 999999).Error)
		if !errors.Is(err, ErrConflict) || !errors.As(err, &pg) || pg.Code != "23503" {
			t.Fatal("foreign key classification failed")
		}
	})
	t.Run("explicit upsert and batch rollback", func(t *testing.T) {
		policy := UpsertPolicy{ConflictColumns: []string{"email"}, UpdateColumns: []string{"name"}}
		if _, err := r.UpsertMany(ctx, []testUser{{Email: user.Email, Name: "changed"}}, policy); err != nil {
			t.Fatal(err)
		}
		if err := r.CreateMany(ctx, []testUser{{Email: "batch-a"}, {Email: "batch-b"}, {Email: user.Email}}); err == nil {
			t.Fatal("duplicate accepted")
		}
		if _, err := r.FindOne(ctx, func(db *gorm.DB) *gorm.DB { return db.Where("email = ?", "batch-a") }); !errors.Is(err, ErrNotFound) {
			t.Fatal("partial batch persisted")
		}
	})
	t.Run("transaction rollback", func(t *testing.T) {
		sentinel := errors.New("rollback")
		err := c.WithTransaction(ctx, nil, func(tx *Transaction) error {
			tr, e := NewTransactionRepository[testUser](tx, userOptions())
			if e != nil {
				return e
			}
			if e = tr.Create(tx.Context(), &testUser{Email: "rolled-back"}); e != nil {
				return e
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
		if _, err := r.FindOne(ctx, func(db *gorm.DB) *gorm.DB { return db.Where("email = ?", "rolled-back") }); !errors.Is(err, ErrNotFound) {
			t.Fatal("rollback lost")
		}
	})
	t.Run("cancel and lock timeout", func(t *testing.T) {
		timeout, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		err := normalize("sleep", c.DB().WithContext(timeout).Exec("SELECT pg_sleep(5)").Error)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		err = c.WithTransaction(ctx, nil, func(tx *Transaction) error {
			db, e := tx.DB()
			if e != nil {
				return e
			}
			if e := db.Exec("SELECT user_key FROM "+r.schema.Table+" WHERE user_key = ? FOR UPDATE", user.ID).Error; e != nil {
				return normalize("lock", e)
			}
			blocked, stop := context.WithTimeout(ctx, 40*time.Millisecond)
			defer stop()
			e = r.PatchByID(blocked, user.ID, map[string]any{"age": 5})
			if !errors.Is(e, context.DeadlineExceeded) {
				return errors.New("lock wait did not honor deadline")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("repeatable read pagination snapshot", func(t *testing.T) {
		err := c.WithTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead}, func(tx *Transaction) error {
			tr, e := NewTransactionRepository[testUser](tx, userOptions())
			if e != nil {
				return e
			}
			first, e := tr.Paginate(tx.Context(), PageRequest{})
			if e != nil {
				return e
			}
			if e = r.Create(ctx, &testUser{Email: "outside-snapshot"}); e != nil {
				return e
			}
			second, e := tr.Paginate(tx.Context(), PageRequest{})
			if e != nil {
				return e
			}
			if first.TotalRows != second.TotalRows {
				return errors.New("snapshot changed")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("concurrent bounded pool workload", func(t *testing.T) {
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 20; j++ {
					if _, e := r.CursorPage(ctx, CursorRequest{Size: 10}); e != nil {
						errs <- e
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		stats := c.Stats()
		if stats.OpenConnections > 2 || stats.MaxOpenConnections != 2 || stats.InUse != 0 {
			t.Fatalf("pool bound/drain violated: %+v", stats)
		}
	})
}

func BenchmarkPostgresCursor(b *testing.B) {
	c := postgresTest(b)
	r := userRepo(b, c)
	models := make([]testUser, 1000)
	for i := range models {
		models[i].Email = fmt.Sprintf("bench-%d", i)
	}
	if err := r.CreateMany(context.Background(), models); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := r.CursorPage(context.Background(), CursorRequest{Size: 50}); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func TestPostgresPolicyBindingAndCAS(t *testing.T) {
	a, b := postgresTest(t), postgresTest(t)
	ctx := context.Background()
	for _, c := range []*Client{a, b} {
		if err := c.DB().AutoMigrate(&versionedRow{}); err != nil {
			t.Fatal("version fixture failed")
		}
	}
	r := versionRepo(t, a)
	row := versionedRow{Tenant: "one", Value: "initial"}
	if err := r.Create(ctx, &row); err != nil {
		t.Fatal(err)
	}
	if err := b.WithTransaction(ctx, nil, func(tx *Transaction) error {
		_, err := r.InTransaction(tx)
		if !errors.Is(err, ErrTransactionClientMismatch) {
			return errors.New("wrong client accepted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var wins atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := a.WithTransaction(ctx, nil, func(tx *Transaction) error {
				bound, err := r.InTransaction(tx)
				if err != nil {
					return err
				}
				return bound.CompareAndSwapByID(tx.Context(), row.ID, 0, map[string]any{"value": "winner"})
			})
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrVersionConflict) {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("CAS winners: %d", wins.Load())
	}
	got, err := r.FindByID(ctx, row.ID)
	if err != nil || got.Version != 1 {
		t.Fatalf("CAS persisted: %+v %v", got, err)
	}
}

func TestPostgresTransactionLocalTimeouts(t *testing.T) {
	c := postgresTest(t)
	ctx := context.Background()
	c.pool.SetMaxOpenConns(1)
	c.pool.SetMaxIdleConns(1)
	c.backend = c.backend.(*postgres.Driver).WithTimeouts(postgres.Timeouts{Statement: 40 * time.Millisecond, Lock: 20 * time.Millisecond, IdleInTransaction: time.Second})
	type settings struct {
		Statement string
		Lock      string
		Idle      string
	}
	read := func(db *gorm.DB) (settings, error) {
		var s settings
		err := db.Raw("SELECT current_setting('statement_timeout') AS statement, current_setting('lock_timeout') AS lock, current_setting('idle_in_transaction_session_timeout') AS idle").Scan(&s).Error
		return s, err
	}
	baseline, err := read(c.DB())
	if err != nil {
		t.Fatal("read baseline failed")
	}
	if err := c.DB().Exec("SELECT set_config('lock_timeout', ?, false)", "5ms").Error; err != nil {
		t.Fatal("configure inherited test limit failed")
	}
	err = c.WithTransaction(ctx, nil, func(tx *Transaction) error {
		db, e := tx.DB()
		if e != nil {
			return e
		}
		s, e := read(db)
		if e != nil {
			return normalize("read_settings", e)
		}
		if s.Lock != "5ms" {
			return errors.New("stricter inherited lock limit was weakened")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DB().Exec("SELECT set_config('lock_timeout', ?, false)", baseline.Lock).Error; err != nil {
		t.Fatal("restore inherited test limit failed")
	}
	err = c.WithTransaction(ctx, nil, func(tx *Transaction) error {
		db, err := tx.DB()
		if err != nil {
			return err
		}
		s, err := read(db)
		if err != nil {
			return normalize("read_settings", err)
		}
		if s.Statement != "40ms" || s.Lock != "20ms" || s.Idle != "1s" {
			return errors.New("transaction timeout settings not applied")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := read(c.DB())
	if err != nil || after != baseline {
		t.Fatalf("settings leaked on commit: %+v", after)
	}
	err = c.WithTransaction(ctx, nil, func(tx *Transaction) error {
		db, e := tx.DB()
		if e != nil {
			return e
		}
		return resultError("sleep", db.Exec("SELECT pg_sleep(1)"))
	})
	var te *TransactionError
	var pg *pgconn.PgError
	if !errors.As(err, &te) || te.Phase != PhaseCallback || te.Outcome != OutcomeRolledBack || !errors.As(err, &pg) || pg.Code != "57014" {
		t.Fatalf("server statement timeout classification: %v", err)
	}
	after, err = read(c.DB())
	if err != nil || after != baseline {
		t.Fatalf("settings leaked on rollback: %+v", after)
	}
	// Zero inherits the role/database policy rather than disabling server limits.
	c.backend = c.backend.(*postgres.Driver).WithTimeouts(postgres.Timeouts{})
	err = c.WithTransaction(ctx, nil, func(tx *Transaction) error {
		db, e := tx.DB()
		if e != nil {
			return e
		}
		s, e := read(db)
		if e != nil {
			return normalize("read_settings", e)
		}
		if s != baseline {
			return errors.New("zero timeout did not inherit")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPostgresServerLockAndIdleTimeout(t *testing.T) {
	c := postgresTest(t)
	ctx := context.Background()
	repo := userRepo(t, c)
	row := testUser{Email: "lock-target"}
	if err := repo.Create(ctx, &row); err != nil {
		t.Fatal(err)
	}
	c.backend = c.backend.(*postgres.Driver).WithTimeouts(postgres.Timeouts{Statement: time.Second, Lock: 30 * time.Millisecond, IdleInTransaction: time.Second})
	blocker := c.DB().Begin()
	if blocker.Error != nil {
		t.Fatal("begin blocker failed")
	}
	defer blocker.Rollback()
	if err := blocker.Exec("SELECT user_key FROM "+repo.schema.Table+" WHERE user_key = ? FOR UPDATE", row.ID).Error; err != nil {
		t.Fatal("lock blocker failed")
	}
	err := c.WithTransaction(ctx, nil, func(tx *Transaction) error {
		r, e := repo.InTransaction(tx)
		if e != nil {
			return e
		}
		return r.PatchByID(tx.Context(), row.ID, map[string]any{"age": 2})
	})
	var pg *pgconn.PgError
	if !errors.As(err, &pg) || pg.Code != "55P03" {
		t.Fatalf("server lock timeout not preserved: %v", err)
	}
	if err := blocker.Rollback().Error; err != nil {
		t.Fatal("release blocker failed")
	}
	c.backend = c.backend.(*postgres.Driver).WithTimeouts(postgres.Timeouts{Statement: time.Second, Lock: 30 * time.Millisecond, IdleInTransaction: 40 * time.Millisecond})
	err = c.WithTransaction(ctx, nil, func(tx *Transaction) error {
		<-time.After(150 * time.Millisecond)
		r, e := repo.InTransaction(tx)
		if e != nil {
			return e
		}
		_, e = r.FindByID(tx.Context(), row.ID)
		return e
	})
	if err == nil {
		t.Fatal("idle transaction unexpectedly survived server timeout")
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
