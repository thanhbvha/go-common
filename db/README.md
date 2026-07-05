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
