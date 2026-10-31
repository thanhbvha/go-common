// Package mysql supports Oracle MySQL 8.4 LTS with InnoDB tables. MariaDB/TiDB
// require separate adapters: wire compatibility does not imply identical semantics.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	driver "github.com/go-sql-driver/mysql"
	"github.com/thanhbvha/go-common/db/orm/backend"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Driver struct{ dsn string }

type ownedDialector struct {
	*gormmysql.Dialector
	pool *sql.DB
}

func (d *ownedDialector) PreparedPool() *sql.DB { return d.pool }

func New(dsn string) *Driver { return &Driver{dsn: dsn} }
func (*Driver) Name() string { return "mysql" }

type privateLogger struct{}

func (privateLogger) Print(...any) {}
func (d *Driver) config() (*driver.Config, error) {
	if d == nil || d.dsn == "" {
		return nil, backend.Invalid("mysql_dsn")
	}
	cfg, err := driver.ParseDSN(d.dsn)
	if err != nil {
		return nil, backend.Invalid("mysql_dsn")
	}
	if cfg.MultiStatements || cfg.InterpolateParams {
		return nil, backend.Invalid("mysql_multi_statements_or_interpolation")
	}
	if cfg.DBName == "" {
		return nil, backend.Invalid("mysql_database")
	}
	if cfg.Params != nil {
		for name := range cfg.Params {
			switch strings.ToLower(name) {
			case "sql_mode", "transaction_isolation", "transaction_read_only", "default_storage_engine", "autocommit":
				return nil, backend.Invalid("mysql_session_parameters")
			}
		}
	}
	cfg.ClientFoundRows = true
	cfg.ParseTime = true
	cfg.Logger = privateLogger{}
	return cfg, nil
}
func (d *Driver) Validate() error { _, err := d.config(); return err }
func (*Driver) Capabilities() backend.Capabilities {
	return backend.Capabilities{Name: "mysql", Transactions: true, Savepoints: true, MatchedRows: true, AnyUniqueKeyUpsert: true, ReadOnlyTransactions: true, IsolationLevels: []sql.IsolationLevel{sql.LevelDefault, sql.LevelReadUncommitted, sql.LevelReadCommitted, sql.LevelRepeatableRead, sql.LevelSerializable}, MaxBindParameters: 60000, MaxBatchBytes: 1 << 20}
}
func (d *Driver) Prepare(ctx context.Context) (gorm.Dialector, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, err := d.config()
	if err != nil {
		return nil, err
	}
	connector, err := driver.NewConnector(cfg)
	if err != nil {
		return nil, backend.Invalid("mysql_connector")
	}
	// OpenDB performs no network I/O. The core takes ownership of this pool.
	// A connector preserves the per-instance private logger; FormatDSN/sql.Open
	// would discard it and fall back to the driver's process-global logger.
	pool := sql.OpenDB(connector)
	// Disable GORM's context.Background SELECT VERSION(). Inspect probes explicitly
	// after the core's PingContext, within the same overall startup deadline.
	inner := gormmysql.New(gormmysql.Config{Conn: pool, DSNConfig: cfg, SkipInitializeWithVersion: true, DefaultStringSize: 191}).(*gormmysql.Dialector)
	return &ownedDialector{Dialector: inner, pool: pool}, nil
}
func (d *Driver) Inspect(ctx context.Context, db *gorm.DB) (backend.Capabilities, error) {
	c := d.Capabilities()
	var info struct {
		Version string
		Flavor  string
		Engine  string
		Mode    string
		Packet  int64
	}
	err := db.WithContext(ctx).Raw("SELECT VERSION() AS version, @@version_comment AS flavor, @@default_storage_engine AS engine, @@sql_mode AS mode, @@max_allowed_packet AS packet").Scan(&info).Error
	if err != nil {
		return c, err
	}
	if !strings.HasPrefix(info.Version, "8.4.") || !strings.Contains(strings.ToLower(info.Flavor), "mysql") || strings.Contains(strings.ToLower(info.Version+info.Flavor), "mariadb") || strings.Contains(strings.ToLower(info.Version+info.Flavor), "tidb") {
		return c, backend.ErrUnsupportedVersion
	}
	if !strings.EqualFold(info.Engine, "InnoDB") {
		return c, backend.Unsupported("mysql_innodb_default")
	}
	var nonTransactional int64
	if err := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' AND engine <> 'InnoDB'").Scan(&nonTransactional).Error; err != nil {
		return c, err
	}
	if nonTransactional != 0 {
		return c, backend.Unsupported("mysql_non_innodb_tables")
	}
	if !strings.Contains(info.Mode, "STRICT_TRANS_TABLES") && !strings.Contains(info.Mode, "STRICT_ALL_TABLES") {
		return c, backend.Unsupported("mysql_strict_sql_mode")
	}
	cfg, err := d.config()
	if err != nil {
		return c, err
	}
	packet := info.Packet
	if cfg.MaxAllowedPacket > 0 && int64(cfg.MaxAllowedPacket) < packet {
		packet = int64(cfg.MaxAllowedPacket)
	}
	if packet < 4096 {
		return c, backend.Unsupported("mysql_packet_size")
	}
	if packet/2 < int64(c.MaxBatchBytes) {
		c.MaxBatchBytes = int(packet / 2)
	}
	c.Version = "8.4"
	return c, nil
}

// MySQL server limits are deployment-owned. No SET SESSION settings are changed
// here; max_execution_time is not a general DML/transaction timeout.
func (*Driver) SetupTransaction(context.Context, *gorm.DB) error { return nil }
func (*Driver) Classify(err error) backend.ErrorInfo {
	var native *driver.MySQLError
	if !errors.As(err, &native) {
		return backend.ErrorInfo{}
	}
	info := backend.ErrorInfo{SQLState: string(native.SQLState[:]), Code: strconv.Itoa(int(native.Number))}
	switch native.Number {
	case 1062, 1451, 1452, 1048, 3819:
		info.Kind = backend.Constraint
	case 1213:
		info.Kind = backend.Deadlock
	case 1205:
		info.Kind = backend.LockTimeout
	case 3024:
		info.Kind = backend.StatementTimeout
	}
	return info
}
