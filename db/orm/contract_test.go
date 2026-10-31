package orm

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

// The same behavioral contract runs against every supported adapter. Individual
// database tests add driver-specific assertions; this suite does not fake SQL.
type contractRow struct {
	ID      uint   `gorm:"primaryKey"`
	SKU     string `gorm:"size:64;uniqueIndex"`
	Alias   string `gorm:"size:64;uniqueIndex"`
	Tenant  string `gorm:"size:64"`
	Value   string `gorm:"type:text"`
	Number  int
	Version uint64
	Removed gorm.DeletedAt `gorm:"column:removed_at"`
}
type contractChild struct {
	ID       uint `gorm:"primaryKey"`
	ParentID uint
	Parent   contractRow `gorm:"foreignKey:ParentID;constraint:OnDelete:RESTRICT"`
}

func contractRepo(t *testing.T, c *Client, version bool) *Repository[contractRow] {
	t.Helper()
	opts := RepositoryOptions{WritableColumns: []string{"value", "number"}, SortableColumns: []string{"number"}, BatchSize: 2}
	if version {
		opts.VersionColumn = "version"
	}
	r, err := NewRepository[contractRow](c, opts)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func runContract(t *testing.T, open func(*testing.T) *Client) {
	ctx := context.Background()
	fresh := func(t *testing.T) (*Client, *Repository[contractRow]) {
		c := open(t)
		if err := c.DB().AutoMigrate(&contractRow{}, &contractChild{}); err != nil {
			t.Fatal("create contract schema failed")
		}
		t.Cleanup(func() {
			if err := c.DB().Migrator().DropTable(&contractChild{}, &contractRow{}); err != nil {
				t.Error("drop contract schema failed")
			}
		})
		return c, contractRepo(t, c, false)
	}
	t.Run("no_op_patch_and_bound_ID", func(t *testing.T) {
		_, r := fresh(t)
		row := contractRow{SKU: "one", Alias: "one", Value: "same"}
		if err := r.Create(ctx, &row); err != nil {
			t.Fatal(err)
		}
		if err := r.PatchByID(ctx, row.ID, map[string]any{"value": "same"}); err != nil {
			t.Fatalf("existing unchanged row reported missing: %v", err)
		}
		if err := r.PatchByID(ctx, 999999, map[string]any{"value": "same"}); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if err := r.DeleteByID(ctx, "1 OR 1=1"); err == nil {
			t.Fatal("ID expression accepted as SQL")
		}
		for _, bad := range []string{"1suffix", "1e0", " 1"} {
			if _, err := r.FindByID(ctx, bad); !errors.Is(err, ErrInvalidArgument) {
				t.Fatal("numeric ID coercion was accepted")
			}
		}
		if _, err := r.FindByID(ctx, row.ID); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("native_constraints_and_atomic_bulk", func(t *testing.T) {
		c, r := fresh(t)
		existing := contractRow{SKU: "exists", Alias: "exists"}
		if err := r.Create(ctx, &existing); err != nil {
			t.Fatal(err)
		}
		err := r.Create(ctx, &contractRow{SKU: "exists", Alias: "different"})
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("unique classification: %v", err)
		}
		child, err := NewRepository[contractChild](c, RepositoryOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := child.Create(ctx, &contractChild{ParentID: 999999}); !errors.Is(err, ErrConflict) {
			t.Fatalf("FK classification: %v", err)
		}
		err = r.CreateMany(ctx, []contractRow{{SKU: "batch-a", Alias: "batch-a"}, {SKU: "batch-b", Alias: "batch-b"}, {SKU: "exists", Alias: "batch-c"}})
		if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
		if _, err := r.FindOne(ctx, func(db *gorm.DB) *gorm.DB { return db.Where("sku = ?", "batch-a") }); !errors.Is(err, ErrNotFound) {
			t.Fatal("partial batch persisted")
		}
	})
	t.Run("explicit_upsert_capabilities", func(t *testing.T) {
		c, r := fresh(t)
		row := contractRow{SKU: "sku-a", Alias: "alias-a", Value: "original"}
		if err := r.Create(ctx, &row); err != nil {
			t.Fatal(err)
		}
		policy := UpsertPolicy{ConflictColumns: []string{"sku"}, UpdateColumns: []string{"value"}}
		if !c.Capabilities().StrictConflictTarget {
			if _, err := r.UpsertMany(ctx, []contractRow{{SKU: "sku-b", Alias: "alias-a", Value: "unsafe"}}, policy); !errors.Is(err, ErrUnsupportedFeature) {
				t.Fatal(err)
			}
			if _, err := r.UpsertMany(ctx, []contractRow{{SKU: "sku-b", Alias: "alias-a", Value: "explicit"}}, UpsertPolicy{AnyUniqueKey: true, UpdateColumns: []string{"value"}}); err != nil {
				t.Fatal(err)
			}
			got, err := r.FindByID(ctx, row.ID)
			if err != nil || got.SKU != "sku-a" || got.Value != "explicit" {
				t.Fatalf("any unique key contract: %+v %v", got, err)
			}
			if _, err := r.UpsertMany(ctx, []contractRow{{SKU: "sku-a", Alias: "alias-a"}}, UpsertPolicy{ConflictColumns: []string{"sku"}, DoNothing: true}); !errors.Is(err, ErrUnsupportedFeature) {
				t.Fatal(err)
			}
			return
		}
		if _, err := r.UpsertMany(ctx, []contractRow{{SKU: "sku-a", Alias: "alias-a", Value: "changed"}}, policy); err != nil {
			t.Fatal(err)
		}
		if _, err := r.UpsertMany(ctx, []contractRow{{SKU: "sku-b", Alias: "alias-a", Value: "wrong-target"}}, policy); !errors.Is(err, ErrConflict) {
			t.Fatalf("wrong unique constraint was silently targeted: %v", err)
		}
		if _, err := r.UpsertMany(ctx, []contractRow{{SKU: "sku-a", Alias: "alias-a", Value: "ignored"}}, UpsertPolicy{ConflictColumns: []string{"sku"}, DoNothing: true}); err != nil {
			t.Fatal(err)
		}
		got, err := r.FindByID(ctx, row.ID)
		if err != nil || got.Value != "changed" {
			t.Fatalf("do nothing: %+v %v", got, err)
		}
	})
	t.Run("soft_delete_and_stable_pagination", func(t *testing.T) {
		_, r := fresh(t)
		rows := []contractRow{{SKU: "a", Alias: "a", Number: 1}, {SKU: "b", Alias: "b", Number: 1}, {SKU: "c", Alias: "c", Number: 2}}
		if err := r.CreateMany(ctx, rows); err != nil {
			t.Fatal(err)
		}
		p, err := r.Paginate(ctx, PageRequest{Size: 2, Sorts: []Sort{{Field: "number", Direction: Ascending}}})
		if err != nil || p.TotalRows != 3 || len(p.Items) != 2 || p.Items[0].ID != rows[0].ID || p.Items[1].ID != rows[1].ID {
			t.Fatalf("pagination: %+v %v", p, err)
		}
		cursor, err := r.CursorPage(ctx, CursorRequest{Size: 2})
		if err != nil || cursor.Next == "" {
			t.Fatal(err)
		}
		next, err := r.CursorPage(ctx, CursorRequest{Size: 2, After: cursor.Next})
		if err != nil || len(next.Items) != 1 || next.Items[0].ID != rows[2].ID {
			t.Fatal("cursor gap")
		}
		if err := r.DeleteByID(ctx, rows[0].ID); err != nil {
			t.Fatal(err)
		}
		if _, err := r.FindByID(ctx, rows[0].ID); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		if err := r.RestoreByID(ctx, rows[0].ID); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("CAS_one_winner", func(t *testing.T) {
		c, _ := fresh(t)
		r := contractRepo(t, c, true)
		row := contractRow{SKU: "version", Alias: "version"}
		if err := r.Create(ctx, &row); err != nil {
			t.Fatal(err)
		}
		var wins atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := r.CompareAndSwapByID(ctx, row.ID, 0, map[string]any{"value": "winner"})
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
	})
	t.Run("transaction_binding_rollback_and_read_only", func(t *testing.T) {
		c, r := fresh(t)
		row := contractRow{SKU: "tx", Alias: "tx", Value: "before"}
		if err := r.Create(ctx, &row); err != nil {
			t.Fatal(err)
		}
		sentinel := errors.New("rollback requested")
		err := c.WithTransaction(ctx, nil, func(tx *Transaction) error {
			bound, e := r.InTransaction(tx)
			if e != nil {
				return e
			}
			if e = bound.PatchByID(tx.Context(), row.ID, map[string]any{"value": "after"}); e != nil {
				return e
			}
			return sentinel
		})
		if !errors.Is(err, sentinel) {
			t.Fatal(err)
		}
		got, err := r.FindByID(ctx, row.ID)
		if err != nil || got.Value != "before" {
			t.Fatal("rollback lost")
		}
		called := false
		err = c.WithTransaction(ctx, &sql.TxOptions{ReadOnly: true}, func(tx *Transaction) error {
			called = true
			bound, e := r.InTransaction(tx)
			if e != nil {
				return e
			}
			return bound.Create(tx.Context(), &contractRow{SKU: "read-only", Alias: "read-only"})
		})
		if c.Capabilities().ReadOnlyTransactions {
			if err == nil || !called {
				t.Fatal("read-only transaction allowed writing")
			}
		} else if !errors.Is(err, ErrUnsupportedFeature) || called {
			t.Fatal("unsupported read-only silently ignored")
		}
		if err := c.WithTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelLinearizable}, func(*Transaction) error { return nil }); !errors.Is(err, ErrUnsupportedFeature) {
			t.Fatal(err)
		}
	})
	t.Run("live_cancellation_and_pool_recovery", func(t *testing.T) {
		c, _ := fresh(t)
		query := "SELECT pg_sleep(2)"
		switch c.Capabilities().Name {
		case "mysql":
			query = "SELECT SLEEP(2)"
		case "sqlite":
			query = "WITH RECURSIVE t(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM t WHERE n<100000000) SELECT sum(n) FROM t"
		}
		timeout, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		err := c.resultError("cancel_contract", c.DB().WithContext(timeout).Exec(query))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("live cancellation: %v", err)
		}
		if err := c.Ping(ctx); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("payload_budget_splits_before_SQL", func(t *testing.T) {
		c, r := fresh(t)
		c.capabilities.MaxBatchBytes = 4096
		r.opts.BatchSize = 100
		creates := 0
		if err := c.db.Callback().Create().Before("*").Register("test:batch_count", func(*gorm.DB) { creates++ }); err != nil {
			t.Fatal(err)
		}
		rows := []contractRow{{SKU: "size-a", Alias: "size-a", Value: strings.Repeat("a", 700)}, {SKU: "size-b", Alias: "size-b", Value: strings.Repeat("b", 700)}, {SKU: "size-c", Alias: "size-c", Value: strings.Repeat("c", 700)}}
		if err := r.CreateMany(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if creates < 2 {
			t.Fatal("payload budget did not split batches")
		}
		creates = 0
		if err := r.CreateMany(ctx, []contractRow{{SKU: "large", Alias: "large", Value: strings.Repeat("x", 4096)}}); !errors.Is(err, ErrInvalidArgument) || creates != 0 {
			t.Fatal("oversized row reached SQL")
		}
	})
}
func TestSQLiteContract(t *testing.T) {
	runContract(t, func(t *testing.T) *Client { return openTest(t) })
}
