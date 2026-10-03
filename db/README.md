# db

Database abstraction wrappers for GORM (SQL) and MongoDB, with automatic OpenTelemetry instrumentation.

```go
import "github.com/thanhbvha/go-common/db/orm"
import "github.com/thanhbvha/go-common/db/mongodb"

// GORM (SQL) with Tracing
dbConn, _ := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
dbConn.Use(tracing.NewPlugin()) // Auto-trace all SQL queries

repo := orm.NewRepository[Patient](dbConn)
repo.InsertOne(ctx, &Patient{Name: "John Doe"})

// MongoDB with Tracing
cfg := mongodb.DefaultConfig()
cfg.URI = "mongodb://localhost:27017"
cfg.EnableTelemetry = true // Auto-trace BSON queries

client, _ := mongodb.NewClient(ctx, cfg)
mongoRepo := mongodb.NewRepository[Patient](client, "my_db", "patients")
mongoRepo.InsertOne(ctx, &Patient{Name: "Jane Doe"})
```

## Database Migrations

The `db/migration` package provides a robust wrapper around `goose` for handling schema migrations. It highly encourages using `embed.FS` to bundle SQL migrations directly into your application binary for safe, container-friendly deployments.

```go
import (
	"database/sql"
	"embed"
	"github.com/thanhbvha/go-common/db/migration"
)

//go:embed sql/*.sql
var migrationFS embed.FS

func migrateDatabase(db *sql.DB) error {
	// 1. Initialize migration manager
	migrator, err := migration.New(migration.Options{
		DB:      db,
		Dialect: "postgres",    // Supports: postgres, mysql, sqlite3, etc.
		Dir:     "sql",         // Directory inside the embedded FS
		FS:      migrationFS,   // Embedded files
	})
	if err != nil {
		return err
	}

	// 2. Apply all available migrations
	if err := migrator.Up(); err != nil {
		return err
	}
	
	// Other available methods:
	// migrator.Down()       // Rollback 1 version
	// migrator.Status()     // Print migration status
	// migrator.UpTo(ver)    // Migrate up to specific version
	// migrator.DownTo(ver)  // Rollback to specific version

	return nil
}
```
