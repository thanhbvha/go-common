//go:build integration

package mysql_test

import (
	"errors"
	"os"
	"testing"

	"database/sql"
	driver "github.com/go-sql-driver/mysql"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	"github.com/thanhbvha/go-common/v2/db/messaging/driver/mysql"
	"github.com/thanhbvha/go-common/v2/db/migration"
)

func TestMySQLMessagingMigrationLifecycle(t *testing.T) {
	f := newFixture(t)
	cfg, err := driver.ParseDSN(os.Getenv("ORM_MYSQL_DSN"))
	if err != nil {
		t.Fatal("fixture DSN invalid")
	}
	cfg.DBName = f.schema
	cfg.ParseTime = true
	pool, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal("migration pool failed")
	}
	defer pool.Close()
	files, err := mysql.MigrationFS(f.schema)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := migration.New(migration.Options{DB: pool, Dialect: "mysql", FS: files})
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
	if _, err := mysql.New(t.Context(), f.client, mysql.Config{Namespace: f.Namespace, Database: f.schema}); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal("missing migration accepted", err)
	}
	if _, err := mgr.Up(t.Context()); err != nil {
		t.Fatal("migration reapply failed", err)
	}
	if err := f.client.DB().Exec("UPDATE `" + f.schema + "`.messaging_schema SET version=2").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := mysql.New(t.Context(), f.client, mysql.Config{Namespace: f.Namespace, Database: f.schema}); !errors.Is(err, messaging.ErrSchema) {
		t.Fatal("future schema version accepted", err)
	}
}
