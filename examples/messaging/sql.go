package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"time"

	native "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/thanhbvha/go-common/v2/db/messaging"
	mysql "github.com/thanhbvha/go-common/v2/db/messaging/driver/mysql"
	"github.com/thanhbvha/go-common/v2/db/messaging/driver/postgres"
	"github.com/thanhbvha/go-common/v2/db/messaging/inbox"
	"github.com/thanhbvha/go-common/v2/db/messaging/outbox"
	"github.com/thanhbvha/go-common/v2/db/migration"
	"github.com/thanhbvha/go-common/v2/db/orm"
	mybackend "github.com/thanhbvha/go-common/v2/db/orm/backend/mysql"
	pgbackend "github.com/thanhbvha/go-common/v2/db/orm/backend/postgres"
)

type sqlStore interface {
	outbox.Store
	outbox.Admin
	inbox.Processor[*orm.Transaction]
	Append(*orm.Transaction, messaging.Message) error
}

type sqlFixture struct {
	client                  *orm.Client
	store                   sqlStore
	namespace, prefix, drop string
}

func (f *sqlFixture) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	if f.drop != "" {
		err = f.client.DB().WithContext(ctx).Exec(f.drop).Error
	}
	return errors.Join(err, f.client.Close(ctx))
}

// Only this fixture's unique schema/database is created and dropped. Both SQL
// drivers use the same producer/dispatcher/inbox demonstration in main.go.
func newSQLFixture(ctx context.Context, settings exampleConfig, driver string) (f *sqlFixture, result error) {
	cfg := orm.DefaultConfig()
	switch driver {
	case "postgres":
		cfg.Backend = pgbackend.New(settings.Postgres.DSN)
	case "mysql":
		cfg.Backend = mybackend.New(settings.MySQL.DSN)
	default:
		return nil, errors.New("unsupported SQL driver")
	}
	client, err := orm.Open(ctx, cfg)
	if err != nil {
		hint := "check selected database DSN, supported version and authentication"
		if driver == "mysql" {
			hint = "requires Oracle MySQL 8.4/InnoDB; check mysql.dsn, database existence and authentication"
		}
		return nil, &stepError{step: driver + ".connect", err: err, hint: hint}
	}
	f = &sqlFixture{client: client, namespace: fmt.Sprintf("messaging_example_%d", time.Now().UnixNano())}
	// Retain the owner in the closure even when an error return assigns f=nil.
	owner := f
	defer func() {
		if result != nil {
			result = errors.Join(result, owner.close())
		}
	}()
	var pool *sql.DB
	var files fs.FS
	columnType := "text"
	if driver == "postgres" {
		f.prefix = `"` + f.namespace + `".`
		if err := client.DB().WithContext(ctx).Exec("CREATE SCHEMA " + `"` + f.namespace + `"`).Error; err != nil {
			return nil, err
		}
		f.drop = "DROP SCHEMA " + `"` + f.namespace + `" CASCADE`
		native, err := pgx.ParseConfig(settings.Postgres.DSN)
		if err != nil {
			return nil, errors.New("invalid PostgreSQL DSN")
		}
		native.RuntimeParams["search_path"] = f.namespace
		pool = stdlib.OpenDB(*native)
		files, err = postgres.MigrationFS(f.namespace)
		if err != nil {
			_ = pool.Close()
			return nil, err
		}
	} else {
		f.prefix = "`" + f.namespace + "`."
		if err := client.DB().WithContext(ctx).Exec("CREATE DATABASE `" + f.namespace + "`").Error; err != nil {
			return nil, &stepError{step: "mysql.create_database", err: err, hint: "grant CREATE/DROP for the example's isolated temporary databases"}
		}
		f.drop = "DROP DATABASE `" + f.namespace + "`"
		native, err := native.ParseDSN(settings.MySQL.DSN)
		if err != nil {
			return nil, errors.New("invalid MySQL DSN")
		}
		native.DBName = f.namespace
		native.ParseTime = true
		pool, err = sql.Open("mysql", native.FormatDSN())
		if err != nil {
			return nil, err
		}
		files, err = mysql.MigrationFS(f.namespace)
		if err != nil {
			_ = pool.Close()
			return nil, err
		}
		columnType = "varbinary(256)"
	}
	defer pool.Close()
	mgr, err := migration.New(migration.Options{DB: pool, Dialect: driver, FS: files})
	if err != nil {
		return nil, err
	}
	if _, err := mgr.Up(ctx); err != nil {
		return nil, &stepError{step: driver + ".migrate", err: err, hint: "check migration and DDL permissions for the temporary fixture"}
	}
	for _, table := range []string{"orders", "effects"} {
		if err := client.DB().WithContext(ctx).Exec("CREATE TABLE " + f.prefix + table + " (id " + columnType + " PRIMARY KEY)").Error; err != nil {
			return nil, err
		}
	}
	if driver == "postgres" {
		f.store, err = postgres.New(ctx, client, postgres.Config{Namespace: f.namespace, Schema: f.namespace})
	} else {
		f.store, err = mysql.New(ctx, client, mysql.Config{Namespace: f.namespace, Database: f.namespace})
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}
