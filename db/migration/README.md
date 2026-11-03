# SQL migrations

`New(Options{DB, Dialect, FS, Dir})` constructs an instance-scoped Goose Provider.
Pass `context.Context` to Up, Down, UpTo, DownTo and Status. Methods return
structured Goose results/status; an empty migration filesystem is a constructor
error. FS is rooted at Dir with fs.Sub; without FS, Dir is a local directory.
The Go migration global registry is disabled. Migration metadata is isolated
between independent managers; the supplied SQL pool is always borrowed.

Run one dedicated deployment migration job per database before starting service
replicas. This wrapper serializes operations on one instance and does not install
cross-process locking. Give the job a deadline and keep backend-specific SQL in
separate migration sets. Go migration callbacks must honor their context. Do not
run production schema creation in service startup or an application transaction.
MySQL DDL can implicitly commit. Sanitize raw migration errors before logging.

The [database guide](../README.md#sql-migrations) includes an embedded-FS example.
