# config

A lightweight and robust configuration loader built on top of [Viper](https://github.com/spf13/viper). It simplifies loading configuration from files (YAML, JSON, `.env`) and automatically binds them to environment variables.

### Key Features
- **Automatic Env Binding:** Automatically overrides file configs with environment variables.
- **Nested Key Mapping:** Maps nested struct fields (e.g., `Database.Host`) to uppercase underscore environment variables (`DATABASE_HOST`).
- **Flexible File Loading:** Can read from any Viper-supported format or cleanly ignore missing `.env` files in production environments.

### Quick Start

#### 1. Define your Configuration Struct

Use `mapstructure` tags (used by Viper) to define how your config is unmarshaled.

```go
type AppConfig struct {
    Server struct {
        Port int    `mapstructure:"port"`
        Host string `mapstructure:"host"`
    } `mapstructure:"server"`
    
    Database struct {
        URL string `mapstructure:"url"`
    } `mapstructure:"database"`
}
```

#### 2. Load from a `.env` file (Convenience Method)

`LoadEnv` is a helper function specifically designed to load a `.env` file if it exists, and safely ignore it if it doesn't (useful for 12-factor apps running in production).

```go
import "github.com/thanhbvha/go-common/config"

var cfg AppConfig
// Looks for ".env" in the current directory. 
// If not found, it gracefully falls back to reading pure Environment Variables.
err := config.LoadEnv(".env", &cfg)
if err != nil {
    panic(err)
}

fmt.Printf("Server running on: %s:%d", cfg.Server.Host, cfg.Server.Port)
```

With this setup, you can configure your app via a `.env` file:
```env
SERVER_PORT=8080
DATABASE_URL=postgres://user:pass@localhost:5432/db
```

Or directly via standard environment variables:
```bash
export SERVER_PORT=8080
export DATABASE_URL=postgres://user:pass@localhost:5432/db
```

#### 3. Advanced Loading Options

If you need a prefix for your environment variables or want to load from a standard `config.yaml`, use the `Load` method with custom `Options`.

```go
opts := config.DefaultOptions()
opts.Path = "config.yaml"
opts.EnvPrefix = "MYAPP" // Looks for MYAPP_SERVER_PORT
opts.IgnoreFileNotFound = false // Fail if config.yaml is missing

var cfg AppConfig
err := config.Load(opts, &cfg)
```

### Key Types

| Symbol | Description |
|---|---|
| `Options` | Configuration settings for the loader (`Path`, `EnvPrefix`, `AutomaticEnv`, `IgnoreFileNotFound`) |
| `Load(opts, target)` | Core loading function. Takes custom options and unmarshals into the `target` struct pointer. |
| `LoadEnv(path, target)` | Convenience function tailored for loading `.env` files safely. |
