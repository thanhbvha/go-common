package orm

import (
	"errors"
	"log"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"
)

// New initializes a new GORM database connection for PostgreSQL based on the config.
func New(cfg Config) (*gorm.DB, error) {
	if cfg.DBName == "" {
		return nil, errors.New("database name is required")
	}

	// Setup logger (uses standard logger, but configured for GORM)
	logLevel := gormlogger.Warn
	if cfg.Debug {
		logLevel = gormlogger.Info
	}

	newLogger := gormlogger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		gormlogger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logLevel,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)

	// Open connection
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  cfg.DSN(),
		PreferSimpleProtocol: true, // Disables implicit prepared statement usage to avoid cached plan errors
	}), &gorm.Config{
		Logger:                                   newLogger,
		DisableForeignKeyConstraintWhenMigrating: true, // Often preferred in microservices
		PrepareStmt:                              false, // Ensure GORM doesn't cache statements either
	})
	if err != nil {
		return nil, err
	}

	// Configure connection pool
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	if cfg.EnableTelemetry {
		if err := db.Use(tracing.NewPlugin()); err != nil {
			log.Printf("Warning: failed to initialize GORM telemetry: %v", err)
		}
	}

	return db, nil
}
