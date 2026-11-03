# config

Operational maturity, defaults and adoption gates are tracked in the [production matrix](../docs/PRODUCTION.md).

Instance-scoped configuration loading from files and environment variables using
Viper. Define exported fields with `mapstructure` tags. Start from
`DefaultOptions()` to enable environment binding; the literal zero Options value
leaves it disabled. A target must be a non-nil pointer to a struct.

```go
import "github.com/thanhbvha/go-common/v2/config"

type AppConfig struct {
    Server struct { Port int `mapstructure:"port"` } `mapstructure:"server"`
}
opts := config.DefaultOptions()
opts.Path = "config.yaml" // Empty means environment only.
opts.EnvPrefix = "APP"   // APP_SERVER_PORT maps to server.port.
opts.IgnoreFileNotFound = true
opts.Validate = func(value any) error {
    if value.(*AppConfig).Server.Port <= 0 { return errors.New("port required") }
    return nil
}
var cfg AppConfig
if err := config.Load(opts, &cfg); err != nil { return err }
```

Explicit empty environment values also override defaults/file values.
Environment variables override file values, including fields missing from the
file. Nested structs and pointer structs are bound recursively; `,squash` and
`-` tags are respected. Recursive type definitions are rejected. `LoadEnv(path,
&cfg)` accepts `.env` files and ignores only missing files; parse/permission errors
remain errors. Flat dotenv keys such as `SERVER_PORT` map to nested fields.
Decoding and validation use an independent deep copy of initialized target
defaults, so missing fields preserve defaults and failed decoding never mutates
caller-owned pointers, maps or slices. Excessively nested/cyclic defaults are
rejected. Unknown environment variables are ignored; malformed known values fail.

## Reloading

```go
watcher, err := config.Watch(ctx, opts,
    func() any { return &AppConfig{} },
    func(value any) { current.Store(value.(*AppConfig)) }, // atomic.Pointer[AppConfig]
    func(err error) { slog.Error("configuration reload rejected", "error", err) },
)
if err != nil { return err }
defer watcher.Close()
```

Watch requires a file path and publishes the initial validated snapshot before
returning. Directory notifications handle writes, atomic replacement and symlink
updates such as Kubernetes ConfigMaps. Reloads are debounced and serialized;
invalid snapshots retain the last published configuration. Callback panics become
errors. `onError` is optional; pass a handler to observe rejected reloads.

Treat published snapshots as immutable. Callbacks must return promptly and must
not call Close themselves. Canceling the context stops notifications; Close is
idempotent and waits for an already-running callback. Register Close with the
application lifecycle before shutting down dependencies used by callbacks.
