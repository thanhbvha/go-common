package migration

import (
	"context"
	"database/sql"
	"testing"
	"testing/fstest"

	_ "github.com/glebarez/go-sqlite"
)

func TestManagerIsolationContextAndBorrowedPool(t *testing.T) {
	ctx := context.Background()
	open := func() *sql.DB {
		t.Helper()
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { db.Close() })
		return db
	}
	dbA, dbB := open(), open()
	files := func(table string) fstest.MapFS {
		return fstest.MapFS{"migrations/001_create.sql": {Data: []byte("-- +goose Up\nCREATE TABLE " + table + " (id INTEGER PRIMARY KEY);\n-- +goose Down\nDROP TABLE " + table + ";\n")}}
	}
	a, err := New(Options{DB: dbA, Dialect: "sqlite3", Dir: "migrations", FS: files("a")})
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(Options{DB: dbB, Dialect: "sqlite3", Dir: "migrations", FS: files("b")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Up(ctx); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		db    *sql.DB
		table string
	}{{dbA, "a"}, {dbB, "b"}} {
		if _, err = entry.db.Exec("INSERT INTO " + entry.table + " VALUES (1)"); err != nil {
			t.Fatal(err)
		}
	}
	statuses, err := a.Status(ctx)
	if err != nil || len(statuses) != 1 {
		t.Fatalf("status=%v err=%v", statuses, err)
	}
	if _, err = a.Down(ctx); err != nil {
		t.Fatal(err)
	}
	if err = dbA.PingContext(ctx); err != nil {
		t.Fatal("borrowed pool was closed", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = a.Up(canceled); err == nil {
		t.Fatal("canceled migration accepted")
	}
}
