# Config Module

## Overview
The `config` module provides a standardized way to load application configurations. It wraps `spf13/viper` and uses `mapstructure` to unmarshal configurations directly into Go structs.

## Key Features
- **File Parsing**: Supports YAML (default), JSON, TOML.
- **Environment Overrides**: Fully complies with the 12-Factor App methodology by allowing environment variables to seamlessly override configuration file values.

## 🚨 Best Practices for AI/Developers
- **Struct Tags**: Use the `mapstructure:"field_name"` tag on your config structs, NOT `json` or `yaml` tags.
- **Environment Overrides (CRITICAL)**: If `EnvPrefix` is set to `APP` and your struct has a nested field `Server.Port`, the module will automatically look for an environment variable named `APP_SERVER_PORT`. Environment variables ALWAYS take precedence over values written in the YAML file.
