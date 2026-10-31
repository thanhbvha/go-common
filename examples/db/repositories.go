package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/thanhbvha/go-common/db/orm"
	"gorm.io/gorm"
)

func demoBackend(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		caps := f.client.Capabilities()
		fmt.Printf("  backend=%s version=%s bind_limit=%d batch_bytes=%d\n", caps.Name, caps.Version, caps.MaxBindParameters, caps.MaxBatchBytes)
		fmt.Printf("  strict_upsert=%t any_unique=%t read_only=%t server_timeouts=%t\n", caps.StrictConflictTarget, caps.AnyUniqueKeyUpsert, caps.ReadOnlyTransactions, caps.TransactionLocalTimeouts)
		if err := f.config.Require.Check(caps); err != nil {
			return err
		}
		fmt.Printf("  serializable=%t; factory=%s; profile validated before Open\n", caps.SupportsIsolation(sql.LevelSerializable), f.config.Backend.Name())
		// Unsupported mandatory features fail rather than being silently ignored.
		cfg := f.config
		cfg.Require.TransactionLocalTimeouts = true
		err := cfg.Validate()
		if !caps.TransactionLocalTimeouts {
			return expect(err, orm.ErrUnsupportedFeature)
		}
		return err
	})
}
func demoCRUD(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		r, err := f.repository()
		if err != nil {
			return err
		}
		one := patient("crud-one", "one")
		if err := r.Create(ctx, &one); err != nil {
			return err
		}
		many := []Patient{patient("crud-two", "one"), patient("crud-three", "one")}
		if err := r.CreateMany(ctx, many); err != nil {
			return err
		}
		found, err := r.FindByID(ctx, strconv.FormatUint(uint64(one.ID), 10))
		if err != nil {
			return err
		}
		if _, err := r.FindOne(ctx, func(db *gorm.DB) *gorm.DB { return db.Where("email = ?", one.Email) }); err != nil {
			return err
		}
		if err := r.PatchByID(ctx, found.ID, map[string]any{"name": "", "age": 0, "active": false}); err != nil {
			return err
		}
		found, err = r.FindByID(ctx, one.ID)
		if err != nil {
			return err
		}
		if err := require(found.Age == 0 && !found.Active && found.Name == "", "zero values were not patched"); err != nil {
			return err
		}
		if err := r.DeleteByID(ctx, one.ID); err != nil {
			return err
		}
		if _, err := r.FindByID(ctx, one.ID); !errors.Is(err, orm.ErrNotFound) {
			return fmt.Errorf("soft-deleted row remained visible")
		}
		if err := r.RestoreByID(ctx, one.ID); err != nil {
			return err
		}
		if _, err := r.FindByID(ctx, one.ID); err != nil {
			return err
		}
		fmt.Printf("  created=%d bulk=%d; zero-value patch and delete/restore verified\n", one.ID, len(many))
		return nil
	})
}
func demoPagination(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		r, err := f.repository()
		if err != nil {
			return err
		}
		rows := make([]Patient, 5)
		for i := range rows {
			rows[i] = patient(fmt.Sprintf("page-%d", i), "one")
			rows[i].Age = 20 + i/2
		}
		if err := r.CreateMany(ctx, rows); err != nil {
			return err
		}
		filter := func(db *gorm.DB) *gorm.DB { return db.Where("tenant_id = ?", "one") }
		page, err := r.Paginate(ctx, orm.PageRequest{Page: 2, Size: 2, Sorts: []orm.Sort{{Field: "age", Direction: orm.Descending}}}, filter)
		if err != nil {
			return err
		}
		fmt.Printf("  total=%d pages=%d page=%d size=%d items=%d\n", page.TotalRows, page.TotalPages, page.Page, page.Size, len(page.Items))
		for _, direction := range []orm.Direction{orm.Ascending, orm.Descending} {
			next := ""
			count := 0
			for {
				p, err := r.CursorPage(ctx, orm.CursorRequest{Size: 2, After: next, Direction: direction}, filter)
				if err != nil {
					return err
				}
				count += len(p.Items)
				next = p.Next
				if next == "" {
					break
				}
			}
			if err := require(count == 5, "cursor traversal lost rows"); err != nil {
				return err
			}
			fmt.Printf("  cursor=%s traversed=%d\n", direction, count)
		}
		return nil
	})
}
func demoScope(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		root, err := f.repository()
		if err != nil {
			return err
		}
		own, other := patient("tenant-one", "one"), patient("tenant-two", "two")
		if err := root.Create(ctx, &own); err != nil {
			return err
		}
		if err := root.Create(ctx, &other); err != nil {
			return err
		}
		opts := repositoryOptions()
		// A real service derives tenant from authenticated server-side identity.
		opts.Scope = func(db *gorm.DB) *gorm.DB { return db.Where("tenant_id = ?", "one") }
		scoped, err := orm.NewRepository[Patient](f.client, opts)
		if err != nil {
			return err
		}
		if _, err := scoped.FindByID(ctx, other.ID); !errors.Is(err, orm.ErrNotFound) {
			return fmt.Errorf("tenant query escaped scope")
		}
		if err := expect(scoped.PatchByID(ctx, other.ID, map[string]any{"status": "ready"}), orm.ErrNotFound); err != nil {
			return err
		}
		if err := root.DeleteByID(ctx, other.ID); err != nil {
			return err
		}
		if err := expect(scoped.RestoreByID(ctx, other.ID), orm.ErrNotFound); err != nil {
			return err
		}
		if err := f.client.WithTransaction(ctx, nil, func(tx *orm.Transaction) error {
			bound, err := scoped.InTransaction(tx)
			if err != nil {
				return err
			}
			return bound.PatchByID(tx.Context(), own.ID, map[string]any{"status": "ready"})
		}); err != nil {
			return err
		}
		fmt.Println("  tenant reads, writes, restore and transaction binding retain policy")
		fmt.Println("  inserts still require application validation of tenant/ownership fields")
		return nil
	})
}
func demoUpsert(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		r, err := f.repository()
		if err != nil {
			return err
		}
		row := patient("upsert", "one")
		if err := r.Create(ctx, &row); err != nil {
			return err
		}
		changed := patient("upsert", "one")
		changed.Name = "Changed"
		policy := orm.UpsertPolicy{ConflictColumns: []string{"external_id"}, UpdateColumns: []string{"name", "status"}}
		if !f.client.Capabilities().StrictConflictTarget {
			if _, err := r.UpsertMany(ctx, []Patient{changed}, policy); !errors.Is(err, orm.ErrUnsupportedFeature) {
				return fmt.Errorf("unsupported strict target accepted")
			}
			policy = orm.UpsertPolicy{AnyUniqueKey: true, UpdateColumns: []string{"name", "status"}}
		}
		affected, err := r.UpsertMany(ctx, []Patient{changed}, policy)
		if err != nil {
			return err
		}
		found, err := r.FindByID(ctx, row.ID)
		if err != nil {
			return err
		}
		if err := require(found.Name == "Changed", "upsert did not update selected columns"); err != nil {
			return err
		}
		if f.client.Capabilities().ConflictDoNothing {
			ignored := patient("upsert", "one")
			ignored.Name = "Must not replace"
			if _, err := r.UpsertMany(ctx, []Patient{ignored}, orm.UpsertPolicy{ConflictColumns: []string{"external_id"}, DoNothing: true}); err != nil {
				return err
			}
		} else {
			fmt.Println("  MySQL: explicit AnyUniqueKey; strict targets/DoNothing unsupported")
		}
		fmt.Printf("  upsert driver_rows_affected=%d (not a portable logical row count)\n", affected)
		return nil
	})
}
func demoCAS(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		opts := repositoryOptions()
		opts.VersionColumn = "version"
		r, err := orm.NewRepository[Patient](f.client, opts)
		if err != nil {
			return err
		}
		row := patient("cas", "one")
		if err := r.Create(ctx, &row); err != nil {
			return err
		}
		before, err := r.FindByID(ctx, row.ID)
		if err != nil {
			return err
		}
		if err := expect(r.PatchByID(ctx, row.ID, map[string]any{"status": "ready"}), orm.ErrVersionRequired); err != nil {
			return err
		}
		if err := r.CompareAndSwapByID(ctx, row.ID, before.Version, map[string]any{"status": "ready"}); err != nil {
			return err
		}
		stale := r.CompareAndSwapByID(ctx, row.ID, before.Version, map[string]any{"status": "overwritten"})
		if err := expect(stale, orm.ErrVersionConflict); err != nil {
			return err
		}
		if err := expect(stale, orm.ErrConflict); err != nil {
			return err
		}
		after, err := r.FindByID(ctx, row.ID)
		if err != nil {
			return err
		}
		fmt.Printf("  version %d -> %d; stale writer rejected\n", before.Version, after.Version)
		return nil
	})
}
func demoLimits(ctx context.Context, opt options) error {
	return withFixture(ctx, opt, nil, func(f *fixture) error {
		r, err := f.repository()
		if err != nil {
			return err
		}
		if err := expect(r.CreateMany(ctx, make([]Patient, 21)), orm.ErrInvalidArgument); err != nil {
			return err
		}
		_, err = r.Paginate(ctx, orm.PageRequest{Page: math.MaxInt, Size: 10})
		if err := expect(err, orm.ErrInvalidArgument); err != nil {
			return err
		}
		_, err = r.Paginate(ctx, orm.PageRequest{Size: 11})
		if err := expect(err, orm.ErrInvalidArgument); err != nil {
			return err
		}
		_, err = r.Paginate(ctx, orm.PageRequest{Sorts: []orm.Sort{{Field: "age; DROP TABLE patients", Direction: orm.Ascending}}})
		if err := expect(err, orm.ErrInvalidArgument); err != nil {
			return err
		}
		_, err = r.CursorPage(ctx, orm.CursorRequest{After: "malformed"})
		if err := expect(err, orm.ErrInvalidArgument); err != nil {
			return err
		}
		huge := patient("too-large", "one")
		huge.Name = strings.Repeat("x", f.client.Capabilities().MaxBatchBytes)
		if err := expect(r.CreateMany(ctx, []Patient{huge}), orm.ErrInvalidArgument); err != nil {
			return err
		}
		fmt.Println("  MaxBulkRows, MaxPageSize, MaxOffset, sort/cursor and byte limits enforced")
		fmt.Println("  BatchSize=2; MaxBulkRows=20; MaxPageSize=10; MaxOffset=100")
		return nil
	})
}
