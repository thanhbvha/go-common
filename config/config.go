package config

import (
	"strings"

	"github.com/spf13/viper"
)

// Options holds configuration for the config loader.
type Options struct {
	// Path to the config file (e.g. "config.yaml", ".env")
	// If empty, it will only read from environment variables.
	Path string

	// EnvPrefix is the prefix for environment variables.
	// For example, if EnvPrefix is "APP", it will look for APP_PORT instead of PORT.
	EnvPrefix string

	// AutomaticEnv determines whether to automatically load environment variables.
	// Default is true.
	AutomaticEnv bool

	// IgnoreFileNotFound determines whether to ignore errors when the config file is not found.
	// This is useful for .env files that might be present locally but not in production.
	IgnoreFileNotFound bool
}

// DefaultOptions returns standard options for loading configuration.
func DefaultOptions() Options {
	return Options{
		AutomaticEnv: true,
	}
}

// Load loads configuration from the given options into the target struct.
// target should be a pointer to a struct.
func Load(opts Options, target interface{}) error {
	v := viper.New()

	if opts.Path != "" {
		v.SetConfigFile(opts.Path)
		err := v.ReadInConfig()
		if err != nil {
			if !opts.IgnoreFileNotFound {
				return err
			}
			// If IgnoreFileNotFound is true, we only ignore "not found" errors.
			// Viper might return viper.ConfigFileNotFoundError or standard os.PathError.
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				if !strings.Contains(err.Error(), "no such file or directory") &&
					!strings.Contains(err.Error(), "The system cannot find the file specified") {
					return err
				}
			}
		}
	}

	if opts.AutomaticEnv {
		if opts.EnvPrefix != "" {
			v.SetEnvPrefix(opts.EnvPrefix)
		}
		// Replace dots and dashes with underscores for env variables
		// e.g., database.host -> DATABASE_HOST
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
		v.AutomaticEnv()
	}

	return v.Unmarshal(target)
}

// LoadEnv is a convenience function to load a .env file (if it exists) 
// and unmarshal environment variables into the target struct.
func LoadEnv(path string, target interface{}) error {
	opts := DefaultOptions()
	opts.Path = path
	opts.IgnoreFileNotFound = true // Usually .env is optional in production
	return Load(opts, target)
}
