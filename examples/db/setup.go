package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/thanhbvha/go-common/v2/db/orm"
	"github.com/thanhbvha/go-common/v2/db/orm/backend"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/mysql"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/postgres"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type Patient struct {
	ID         uint   `gorm:"primaryKey"`
	ExternalID string `gorm:"size:64;uniqueIndex"`
	Email      string `gorm:"size:191;uniqueIndex"`
	TenantID   string `gorm:"size:64;index"`
	Name       string `gorm:"size:191"`
	Age        int
	Active     bool
	Status     string `gorm:"size:32"`
	Version    uint64 `gorm:"not null;default:1"`
	DeletedAt  gorm.DeletedAt
}
type Visit struct {
	ID        uint `gorm:"primaryKey"`
	PatientID uint
	Patient   Patient `gorm:"foreignKey:PatientID;constraint:OnDelete:RESTRICT"`
	Note      string  `gorm:"size:191"`
}

// This decorator is demo isolation, not an alternative database driver. It
// forwards the backend contract and adds a generated per-run naming strategy.
// Real services normally use fixed table names with versioned migrations.
type namespacedBackend struct {
	backend.Backend
	prefix string
}
type namespacedDialector struct {
	gorm.Dialector
	prefix string
}

func (d namespacedDialector) Apply(cfg *gorm.Config) error {
	if inner, ok := d.Dialector.(interface{ Apply(*gorm.Config) error }); ok {
		if err := inner.Apply(cfg); err != nil {
			return err
		}
	}
	cfg.NamingStrategy = schema.NamingStrategy{TablePrefix: d.prefix}
	return nil
}
func (d namespacedDialector) PreparedPool() *sql.DB {
	if owned, ok := d.Dialector.(backend.PreparedPool); ok {
		return owned.PreparedPool()
	}
	return nil
}
func (b namespacedBackend) Prepare(ctx context.Context) (gorm.Dialector, error) {
	d, err := b.Backend.Prepare(ctx)
	if err != nil {
		return nil, err
	}
	return namespacedDialector{Dialector: d, prefix: b.prefix}, nil
}

func newConfig(opt options) (orm.Config, string, *localTelemetry, error) {
	random := make([]byte, 6)
	if _, err := rand.Read(random); err != nil {
		return orm.Config{}, "", nil, fmt.Errorf("cannot generate demo namespace")
	}
	prefix := "ormdemo_" + hex.EncodeToString(random) + "_"
	cfg := orm.DefaultConfig()
	// Every operational field can be tuned independently. These deliberately
	// small limits suit a tutorial; production budgets depend on replicas/load.
	cfg.MaxOpenConns = 2
	cfg.MaxIdleConns = 1
	cfg.ConnMaxLifetime = 30 * time.Minute
	cfg.ConnMaxIdleTime = 5 * time.Minute
	cfg.StartupTimeout = 5 * time.Second
	cfg.QueryTimeout = 5 * time.Second
	cfg.TransactionTimeout = 10 * time.Second
	cfg.MaxInFlight = 2
	cfg.MaxQueued = 8
	cfg.SlowThreshold = 200 * time.Millisecond
	cfg.Logger = slog.Default()
	var driver backend.Backend
	switch opt.backend {
	case "sqlite":
		driver = sqlite.New("file:" + prefix + "?mode=memory&cache=shared")
		cfg.MaxOpenConns = 1
		cfg.MaxInFlight = 1
	case "postgres":
		dsn := os.Getenv("ORM_POSTGRES_DSN")
		if dsn == "" {
			return cfg, "", nil, fmt.Errorf("set ORM_POSTGRES_DSN for -backend postgres")
		}
		driver = postgres.New(dsn).WithTimeouts(postgres.DefaultTimeouts())
	case "mysql":
		dsn := os.Getenv("ORM_MYSQL_DSN")
		if dsn == "" {
			return cfg, "", nil, fmt.Errorf("set ORM_MYSQL_DSN for -backend mysql")
		}
		driver = mysql.New(dsn)
	default:
		return cfg, "", nil, fmt.Errorf("unsupported demo backend")
	}
	// Checking the factory profile is pure configuration validation, without I/O.
	if err := driver.Validate(); err != nil {
		return cfg, "", nil, err
	}
	cfg.Backend = namespacedBackend{Backend: driver, prefix: prefix}
	cfg.Require = backend.Requirements{}
	var telemetry *localTelemetry
	if opt.otel {
		telemetry = newLocalTelemetry()
		cfg.EnableTelemetry = true
		cfg.TracerProvider = telemetry.traces
		cfg.MeterProvider = telemetry.metrics
		cfg.Monitor = &orm.Monitor{Event: telemetry.observe}
	}
	if err := cfg.Validate(); err != nil {
		if telemetry != nil {
			err = errors.Join(err, telemetry.close())
		}
		return cfg, "", nil, err
	}
	return cfg, prefix, telemetry, nil
}

type fixture struct {
	client    *orm.Client
	config    orm.Config
	prefix    string
	telemetry *localTelemetry
}

func openFixture(ctx context.Context, opt options, tune func(*orm.Config)) (*fixture, error) {
	cfg, prefix, telemetry, err := newConfig(opt)
	if err != nil {
		return nil, err
	}
	if tune != nil {
		tune(&cfg)
	}
	client, err := orm.Open(ctx, cfg)
	if err != nil {
		if telemetry != nil {
			err = errors.Join(err, telemetry.close())
		}
		return nil, err
	}
	f := &fixture{client: client, config: cfg, prefix: prefix, telemetry: telemetry}
	// Disposable demo tables only. Names are generated before this pool opens.
	if err := client.DB().WithContext(ctx).AutoMigrate(&Patient{}, &Visit{}); err != nil {
		return nil, errors.Join(nativeFailure("create scratch schema", err), f.close())
	}
	return f, nil
}
func (f *fixture) drop(ctx context.Context) error {
	return nativeFailure("drop owned scratch schema", f.client.DB().WithContext(ctx).Migrator().DropTable(&Visit{}, &Patient{}))
}
func (f *fixture) close() error {
	ctx, cancel := cleanupContext()
	defer cancel()
	err := errors.Join(f.drop(ctx), f.client.Close(ctx))
	if f.telemetry != nil {
		err = errors.Join(err, f.telemetry.close())
	}
	return err
}
func withFixture(ctx context.Context, opt options, tune func(*orm.Config), fn func(*fixture) error) (result error) {
	f, err := openFixture(ctx, opt, tune)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, f.close()) }()
	return fn(f)
}
func patient(key, tenant string) Patient {
	return Patient{ExternalID: key, Email: key + "@example.invalid", TenantID: tenant, Name: "Example", Age: 30, Active: true, Status: "waiting"}
}
func repositoryOptions() orm.RepositoryOptions {
	return orm.RepositoryOptions{WritableColumns: []string{"name", "age", "active", "status"}, SortableColumns: []string{"age", "name"}, BatchSize: 2, MaxBulkRows: 20, MaxPageSize: 10, MaxOffset: 100}
}
func (f *fixture) repository() (*orm.Repository[Patient], error) {
	return orm.NewRepository[Patient](f.client, repositoryOptions())
}
