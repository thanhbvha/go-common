# Config Module

## Overview
The `config` module provides a standardized way to load application configurations. It wraps `spf13/viper` and uses `mapstructure` to unmarshal configurations directly into Go structs.

## Key Features
- **Flexible Loaders**: Choose between reading structured files (YAML/JSON) with `config.Load()` or simple flat environments with `config.LoadEnv()`.
- **Environment Overrides**: Fully complies with the 12-Factor App methodology by allowing environment variables to seamlessly override configuration file values.

## Available Methods
- `config.Load(opts, target)`: Loads a config based on `config.Options`. Best for nested YAML/JSON configurations. Supports `EnvPrefix` for overriding values.
- `config.LoadEnv(path, target)`: A specialized convenience function for reading `.env` files. It automatically handles "file not found" errors gracefully (useful when `.env` is absent in production).

## 🚨 Best Practices for AI/Developers
- **Struct Tags**: Use the `mapstructure:"field_name"` tag on your config structs, NOT `json` or `yaml` tags.
- **Environment Overrides (CRITICAL)**: If `EnvPrefix` is set to `APP` and your struct has a nested field `Server.Port`, the module will automatically look for an environment variable named `APP_SERVER_PORT`. Environment variables ALWAYS take precedence over values written in the YAML file.
- **Using `.env` files**: When using `config.LoadEnv()`, the environment variables are mapped directly to struct fields. E.g., `DATABASE_POOL_SIZE` in `.env` maps to the `mapstructure:"database_pool_size"` (or nested `Database.PoolSize`) field.
