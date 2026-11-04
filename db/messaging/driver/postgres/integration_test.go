//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/contracttest"
	"github.com/thanhbvha/go-common/v2/db/messaging/driver/postgres"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	"github.com/thanhbvha/go-common/v2/db/migration"
	"github.com/thanhbvha/go-common/v2/db/orm"
	backend "github.com/thanhbvha/go-common/v2/db/orm/backend/postgres"
)

type fixture struct {
	contracttest.Fixture[*orm.Transaction]
	client *orm.Client
	schema string
}

func newFixture(t *testing.T, configure ...func(*orm.Config)) fixture {
	t.Helper()
	dsn := os.Getenv("ORM_POSTGRES_DSN")
	if dsn == "" {
		if os.Getenv("MESSAGING_INTEGRATION_REQUIRED") == "1" {
			t.Fatal("ORM_POSTGRES_DSN required")
		}
		t.Skip("set ORM_POSTGRES_DSN for isolated PostgreSQL fixtures")
	}
	cfg := orm.DefaultConfig()
	cfg.Backend = backend.New(dsn)
	cfg.MaxOpenConns = 16
	cfg.MaxIdleConns = 16
	for _, fn := range configure {
		fn(&cfg)
	}
	c, err := orm.Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("msg_test_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.DB().WithContext(ctx).Exec(`DROP SCHEMA IF EXISTS "` + schema + `" CASCADE`).Error; err != nil {
			t.Error("fixture cleanup failed")
		}
		_ = c.Close(ctx)
	})
	if err := c.DB().Exec(`CREATE SCHEMA "` + schema + `"`).Error; err != nil {
		t.Fatal(err)
	}
	// A migration-only pool gives every fixture its own Goose metadata table.
	pgcfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal("fixture DSN invalid")
	}
	pgcfg.RuntimeParams["search_path"] = schema
	pool := stdlib.OpenDB(*pgcfg)
	defer pool.Close()
	files, err := postgres.MigrationFS(schema)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := migration.New(migration.Options{DB: pool, Dialect: "postgres", FS: files})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Up(t.Context()); err != nil {
		t.Fatal(err)
	}
	s, err := postgres.New(t.Context(), c, postgres.Config{Namespace: "test", Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.DB().Exec(`CREATE TABLE "` + schema + `".effects (id text PRIMARY KEY, count integer NOT NULL)`).Error; err != nil {
		t.Fatal(err)
	}
	f := contracttest.Fixture[*orm.Transaction]{Namespace: "test", Store: s, Inbox: s, Within: func(ctx context.Context, fn func(*orm.Transaction) error) error {
		return c.WithTransaction(ctx, nil, fn)
	}, Append: s.Append,
		Effect: func(tx *orm.Transaction, id string) error {
			db, e := tx.DB()
			if e != nil {
				return e
			}
			return db.Exec(`INSERT INTO "`+schema+`".effects VALUES (?,1) ON CONFLICT (id) DO UPDATE SET count=effects.count+1`, id).Error
		},
		Count: func(ctx context.Context, id string) int {
			var count int
			if e := c.DB().WithContext(ctx).Raw(`SELECT count FROM "`+schema+`".effects WHERE id=?`, id).Scan(&count).Error; e != nil {
				t.Error("count failed")
			}
			return count
		},
		Expire: func(ctx context.Context, id messaging.Identity) {
			if e := c.DB().WithContext(ctx).Exec(`UPDATE "`+schema+`".outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace=? AND tenant_id=? AND message_id=?`, id.Namespace, id.TenantID, id.ID).Error; e != nil {
				t.Fatal(e)
			}
		},
	}
	return fixture{f, c, schema}
}

func TestPostgresContract(t *testing.T) {
	contracttest.Run(t, func(t *testing.T) contracttest.Fixture[*orm.Transaction] { return newFixture(t).Fixture })
}

func TestTransactionScope(t *testing.T) {
	f := newFixture(t)
	other := newFixture(t)
	s := f.Store.(*postgres.Store)
	m := messaging.Message{ID: "scope", Namespace: f.Namespace, Kind: messaging.Task, Type: "test", SchemaVersion: 1, Payload: []byte(`{}`)}
	var closed *orm.Transaction
	if err := other.Within(t.Context(), func(tx *orm.Transaction) error {
		closed = tx
		if err := s.Append(tx, m); !errors.Is(err, orm.ErrTransactionClientMismatch) {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(closed, m); !errors.Is(err, orm.ErrTransactionClosed) {
		t.Fatal(err)
	}
	if err := s.Append(nil, m); !errors.Is(err, orm.ErrInvalidArgument) {
		t.Fatal(err)
	}
}

func TestAdminSchedulingCleanupAndIsolation(t *testing.T) {
	f := newFixture(t)
	ctx := t.Context()
	s := f.Store
	for i := 0; i < 5; i++ {
		m := messaging.Message{ID: fmt.Sprint(i), Namespace: f.Namespace, TenantID: "a", Kind: messaging.Task, Type: "test", SchemaVersion: 1, Payload: []byte(`{}`)}
		if i == 4 {
			m.AvailableAt = time.Now().Add(time.Hour)
		}
		if err := f.Within(ctx, func(tx *orm.Transaction) error { return f.Append(tx, m) }); err != nil {
			t.Fatal(err)
		}
	}
	tenant := "a"
	page, err := s.List(ctx, outbox.ListRequest{TenantID: &tenant, Limit: 2})
	if err != nil || len(page.Records) != 2 || page.NextCursor == "" {
		t.Fatal(page, err)
	}
	other := "b"
	if _, err := s.List(ctx, outbox.ListRequest{TenantID: &other, Limit: 2, Cursor: page.NextCursor}); err == nil {
		t.Fatal("cross-tenant cursor accepted")
	}
	next, err := s.List(ctx, outbox.ListRequest{TenantID: &tenant, Limit: 10, Cursor: page.NextCursor})
	if err != nil || len(next.Records) != 3 {
		t.Fatal(next, err)
	}
	claims, err := s.Claim(ctx, outbox.ClaimRequest{Limit: 10, MaxAttempts: 20, Lease: time.Minute})
	if err != nil || len(claims) != 4 {
		t.Fatal(claims, err)
	}
	if err := s.MarkPublished(ctx, claims[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Quarantine(ctx, claims[1], "fixture_failure"); err != nil {
		t.Fatal(err)
	}
	if err := s.Retry(ctx, claims[2], time.Hour, "fixture_retry"); err != nil {
		t.Fatal(err)
	}
	m := claims[0].Message
	hash, _ := m.Fingerprint()
	receipt := inbox.Receipt{Identity: m.Identity(), Consumer: "fixture", Fingerprint: hash}
	if _, err := f.Inbox.Consume(ctx, receipt, func(*orm.Transaction) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// A committed cutoff, explicitly selected by the application.
	cutoff := time.Now()
	result, err := s.Cleanup(ctx, outbox.CleanupRequest{TenantID: &tenant, PublishedBefore: cutoff, InboxBefore: cutoff, Limit: 1})
	if err != nil || result.Published != 1 || result.Inbox != 1 {
		t.Fatal(result, err)
	}
	stats, err := s.Stats(ctx, &tenant)
	if err != nil || stats.Pending != 2 || stats.Dead != 1 || stats.InFlight != 1 {
		t.Fatal(stats, err)
	}
	if _, err := s.Get(ctx, "a", claims[0].Message.ID); !errors.Is(err, messaging.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := s.Cleanup(ctx, outbox.CleanupRequest{Limit: 1}); err == nil {
		t.Fatal("implicit retention accepted")
	}
	if _, err := postgres.New(ctx, f.client, postgres.Config{Namespace: "other", Schema: "missing_schema"}); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal(err)
	}
}
