# Database ORM Module

## Overview
The `db/orm` module provides a robust wrapper around GORM (PostgreSQL/SQLite) with advanced capabilities designed for microservices:
1. **Multi-Connection Pooling**: Safely manage multiple databases using a single singleton `orm.Manager`.
2. **Telemetry Integration**: Automatically traces slow queries using OpenTelemetry.
3. **Generic Repository Pattern**: Simplifies CRUD with `orm.NewRepository[T]()`.

## Key Structs & Configs
- `orm.Config`: Contains fields like `Host`, `Port`, `DBName`, `MaxOpenConns`, `MaxIdleConns`, etc.
- `orm.Manager`: Thread-safe manager that holds all `gorm.DB` connection pools.

## Generic Repository Operations
The `orm.Repository[T]` simplifies standard queries:
- `Insert(ctx, &entity)`: Inserts a single record.
- `InsertMany(ctx, []entity)`: Bulk inserts multiple records.
- `Paginate(ctx, req, condition...)`: Returns a paginated `orm.PageResponse[T]` based on an `orm.PageRequest` (Page, Size, Sorts).
- `Aggregate(ctx, &result, modifier)`: Used for complex queries like GROUP BY or JOINs. The `modifier` function allows you to build custom GORM chains.
- `RawQuery(ctx, sql, &result, args...)`: Executes raw SQL safely.
- `WithTx(tx *gorm.DB)`: Clones the repository to use an active transaction.

## 🚨 Best Practices for AI/Developers 
- **Initialization**: Always initialize using `err := orm.Init(configs, "default_db_name")`. The `Init` function is thread-safe and can merge configs if called multiple times.
- **Dynamic Connections**: Use `orm.AddConnection()` to add database connections at runtime without wiping existing ones.
- **Graceful Shutdown (CRITICAL)**: **ALWAYS** ensure `defer orm.Close()` is called in `main.go` after `orm.Init`. This prevents connection leaks and orphaned connections on the PostgreSQL server.
- **Retrieving Connections**: Use `orm.Get("db_name")` to fetch the specific connection pool, or just `orm.Get()` for the default pool.
- **Transactions (CRITICAL)**: When running transactions via `db.Transaction(func(tx *gorm.DB))`, you MUST clone the repository using `repo.WithTx(tx)` inside the callback. If you use the original `repo`, it will run outside the transaction!
