// Package postgres supports PostgreSQL 16–18 using the GORM pgx driver.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"time"

	"github.com/thanhbvha/go-common/v2/db/orm/backend"
	driver "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Timeouts struct {
	Statement         time.Duration
	Lock              time.Duration
	IdleInTransaction time.Duration
}

func DefaultTimeouts() Timeouts {
	return Timeouts{Statement: 10 * time.Second, Lock: time.Second, IdleInTransaction: 30 * time.Second}
}

type Driver struct {
	dsn      string
	timeouts Timeouts
}

func New(dsn string) *Driver { return &Driver{dsn: dsn, timeouts: DefaultTimeouts()} }

// WithTimeouts returns a copy. Zero inherits the server policy, never disables it.
func (d *Driver) WithTimeouts(t Timeouts) *Driver { copy := *d; copy.timeouts = t; return &copy }
func (*Driver) Name() string                      { return "postgres" }
func (d *Driver) Validate() error {
	if d == nil || d.dsn == "" {
		return backend.Invalid("postgres_dsn")
	}
	for _, v := range []time.Duration{d.timeouts.Statement, d.timeouts.Lock, d.timeouts.IdleInTransaction} {
		if v < 0 || v > 24*time.Hour || v > 0 && v < time.Millisecond {
			return backend.Invalid("postgres_timeouts")
		}
	}
	return nil
}
func (*Driver) Capabilities() backend.Capabilities {
	return backend.Capabilities{Name: "postgres", Transactions: true, Savepoints: true, MatchedRows: true, StrictConflictTarget: true, ConflictDoNothing: true, ReadOnlyTransactions: true, TransactionLocalTimeouts: true, IsolationLevels: []sql.IsolationLevel{sql.LevelDefault, sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead, sql.LevelSerializable}, MaxBindParameters: 60000, MaxBatchBytes: 4 << 20}
}
func (d *Driver) Prepare(ctx context.Context) (gorm.Dialector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return driver.Open(d.dsn), nil
}
func (d *Driver) Inspect(ctx context.Context, db *gorm.DB) (backend.Capabilities, error) {
	c := d.Capabilities()
	var version string
	if err := db.WithContext(ctx).Raw("SHOW server_version_num").Scan(&version).Error; err != nil {
		return c, err
	}
	number, err := strconv.Atoi(version)
	if err != nil || number/10000 < 16 || number/10000 > 18 {
		return c, backend.ErrUnsupportedVersion
	}
	c.Version = strconv.Itoa(number / 10000)
	return c, nil
}
func (d *Driver) SetupTransaction(ctx context.Context, db *gorm.DB) error {
	p := d.timeouts
	if p.Statement == 0 && p.Lock == 0 && p.IdleInTransaction == 0 {
		return nil
	}
	return db.WithContext(ctx).Exec(`SELECT set_config(s.name,
 CASE WHEN requested.ms = 0 THEN current_setting(s.name)
      WHEN s.setting::bigint = 0 THEN requested.ms::text
      ELSE LEAST(s.setting::bigint, requested.ms)::text END, true)
 FROM (VALUES ('statement_timeout', ?::bigint), ('lock_timeout', ?::bigint),
 ('idle_in_transaction_session_timeout', ?::bigint)) AS requested(name, ms)
 JOIN pg_settings AS s ON s.name = requested.name`, p.Statement.Milliseconds(), p.Lock.Milliseconds(), p.IdleInTransaction.Milliseconds()).Error
}
func (*Driver) Classify(err error) backend.ErrorInfo {
	var state interface{ SQLState() string }
	if !errors.As(err, &state) {
		return backend.ErrorInfo{}
	}
	code := state.SQLState()
	info := backend.ErrorInfo{SQLState: code, Code: code}
	switch code {
	case "23505", "23503", "23514", "23502":
		info.Kind = backend.Constraint
	case "40P01":
		info.Kind = backend.Deadlock
	case "40001":
		info.Kind = backend.Serialization
	case "55P03":
		info.Kind = backend.LockTimeout
	case "57014":
		info.Kind = backend.StatementTimeout
	}
	return info
}
