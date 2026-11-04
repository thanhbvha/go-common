//go:build integration

package postgres_test

import (
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/driver/postgres"
	"github.com/thanhbvha/go-common/v2/db/migration"
)

func TestMigrationLifecycle(t *testing.T) {
	f := newFixture(t)
	cfg, err := pgx.ParseConfig(os.Getenv("ORM_POSTGRES_DSN"))
	if err != nil {
		t.Fatal("fixture DSN invalid")
	}
	cfg.RuntimeParams["search_path"] = f.schema
	pool := stdlib.OpenDB(*cfg)
	defer pool.Close()
	files, err := postgres.MigrationFS(f.schema)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := migration.New(migration.Options{DB: pool, Dialect: "postgres", FS: files})
	if err != nil {
		t.Fatal(err)
	}
	results, err := mgr.Up(t.Context())
	if err != nil || len(results) != 0 {
		t.Fatal("migration not idempotent", err)
	}
	if _, err := mgr.Down(t.Context()); err != nil {
		t.Fatal("migration down failed", err)
	}
	if _, err := postgres.New(t.Context(), f.client, postgres.Config{Namespace: f.Namespace, Schema: f.schema}); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal("missing migration accepted", err)
	}
	if _, err := mgr.Up(t.Context()); err != nil {
		t.Fatal("migration reapply failed", err)
	}
	if err := f.client.DB().Exec(`UPDATE "` + f.schema + `".messaging_schema SET version=2`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := postgres.New(t.Context(), f.client, postgres.Config{Namespace: f.Namespace, Schema: f.schema}); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal("future schema version accepted", err)
	}
}
