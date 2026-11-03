// Package sqlite supports SQLite via glebarez's pure-Go driver. Foreign keys and
// busy timeout are configured for every pooled connection through DSN pragmas.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	driver "github.com/glebarez/sqlite"
	"github.com/thanhbvha/go-common/v2/db/orm/backend"
	"gorm.io/gorm"
)

type Driver struct{ dsn string }

func New(dsn string) *Driver { return &Driver{dsn: dsn} }
func (*Driver) Name() string { return "sqlite" }
func (d *Driver) Validate() error {
	if d == nil || d.dsn == "" {
		return backend.Invalid("sqlite_dsn")
	}
	if strings.Contains(strings.ToLower(d.dsn), "_pragma") {
		return backend.Invalid("sqlite_pragmas_are_owned_by_adapter")
	}
	return nil
}
func (*Driver) Capabilities() backend.Capabilities {
	return backend.Capabilities{Name: "sqlite", Transactions: true, Savepoints: true, MatchedRows: true, StrictConflictTarget: true, ConflictDoNothing: true, IsolationLevels: []sql.IsolationLevel{sql.LevelDefault, sql.LevelSerializable}, MaxBindParameters: 900, MaxBatchBytes: 1 << 20}
}
func (d *Driver) Prepare(ctx context.Context) (gorm.Dialector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	separator := "?"
	if strings.Contains(d.dsn, "?") {
		separator = "&"
	}
	return driver.Open(d.dsn + separator + "_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"), nil
}
func (d *Driver) Inspect(ctx context.Context, db *gorm.DB) (backend.Capabilities, error) {
	c := d.Capabilities()
	var version string
	var fk int
	if err := db.WithContext(ctx).Raw("SELECT sqlite_version()").Scan(&version).Error; err != nil {
		return c, err
	}
	parts := strings.Split(version, ".")
	if len(parts) < 2 {
		return c, backend.ErrUnsupportedVersion
	}
	major, _ := strconv.Atoi(parts[0])
	minor, _ := strconv.Atoi(parts[1])
	if major != 3 || minor < 35 {
		return c, backend.ErrUnsupportedVersion
	}
	if err := db.WithContext(ctx).Raw("PRAGMA foreign_keys").Scan(&fk).Error; err != nil {
		return c, err
	}
	if fk != 1 {
		return c, backend.Unsupported("sqlite_foreign_keys")
	}
	c.Version = version
	return c, nil
}
func (*Driver) SetupTransaction(context.Context, *gorm.DB) error { return nil }
func (*Driver) Classify(err error) backend.ErrorInfo {
	var code interface{ Code() int }
	if !errors.As(err, &code) {
		return backend.ErrorInfo{}
	}
	n := code.Code()
	info := backend.ErrorInfo{Code: strconv.Itoa(n)}
	switch n & 255 {
	case 19:
		info.Kind = backend.Constraint
	case 5, 6:
		info.Kind = backend.LockTimeout
	}
	return info
}
