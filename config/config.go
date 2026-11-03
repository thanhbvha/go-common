// Package config provides a simple, structured way to load application configuration
// from both files (like .yaml or .env) and environment variables.
//
// It acts as a lightweight wrapper around github.com/spf13/viper, focusing on the
// most common use-cases: loading configuration into a strongly typed Go struct and
// automatically mapping environment variables to struct fields.
//
// Basic usage:
//
//	type AppConfig struct {
//		Port int    `mapstructure:"port"`
//		DB   string `mapstructure:"db_url"`
//	}
//
//	var cfg AppConfig
//	// Loads from config.yaml and overrides with matching env variables
//	err := config.Load(config.Options{
//		Path:         "config.yaml",
//		AutomaticEnv: true,
//	}, &cfg)
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

// Options holds configuration for the config loader.
type Options struct {
	// Validate runs after decoding, before a configuration is accepted.
	Validate func(any) error

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
func Load(opts Options, target any) error {
	value := reflect.ValueOf(target)
	if value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Kind() != reflect.Struct {
		return errors.New("config: target must be a non-nil struct pointer")
	}
	v := viper.New()
	if opts.Path != "" {
		v.SetConfigFile(opts.Path)
		if strings.HasPrefix(filepath.Base(opts.Path), ".env") {
			v.SetConfigType("env")
		}
		if err := v.ReadInConfig(); err != nil {
			var notFound viper.ConfigFileNotFoundError
			if !opts.IgnoreFileNotFound || (!errors.Is(err, os.ErrNotExist) && !errors.As(err, &notFound)) {
				return err
			}
		}
	}
	if opts.AutomaticEnv {
		v.SetEnvPrefix(opts.EnvPrefix)
		v.AllowEmptyEnv(true)
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
		v.AutomaticEnv()
		if err := bindFields(v, value.Elem().Type(), "", map[reflect.Type]bool{}); err != nil {
			return err
		}
	}
	// Decode into a fresh value. Failed parsing or validation never partially
	// overwrites the caller's last accepted configuration.
	candidate, err := cloneDefaults(value.Elem(), 0)
	if err != nil {
		return err
	}
	fresh := reflect.New(value.Elem().Type())
	fresh.Elem().Set(candidate)
	if err := v.Unmarshal(fresh.Interface()); err != nil {
		return err
	}
	if opts.Validate != nil {
		if err := opts.Validate(fresh.Interface()); err != nil {
			return err
		}
	}
	value.Elem().Set(fresh.Elem())
	return nil
}

func bindFields(v *viper.Viper, t reflect.Type, prefix string, seen map[reflect.Type]bool) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	if seen[t] {
		return fmt.Errorf("config: recursive configuration type %s", t)
	}
	seen[t] = true
	defer delete(seen, t)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.PkgPath != "" {
			continue
		}
		tag := strings.Split(f.Tag.Get("mapstructure"), ",")
		name := tag[0]
		if name == "-" {
			continue
		}
		if name == "" {
			name = strings.ToLower(f.Name)
		}
		key := name
		if prefix != "" {
			key = prefix + "." + name
		}
		squash := false
		for _, option := range tag[1:] {
			squash = squash || option == "squash"
		}
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		// Struct values with scalar decoding (notably time.Time) remain leaves.
		if ft.Kind() == reflect.Struct && ft.PkgPath() != "time" {
			if squash {
				key = prefix
			}
			if err := bindFields(v, ft, key, seen); err != nil {
				return err
			}
		} else {
			// dotenv uses flat names; retain nested struct mapping without making file
			// values higher precedence than actual environment variables.
			flat := strings.NewReplacer(".", "_", "-", "_").Replace(key)
			if flat != key && v.IsSet(flat) {
				v.SetDefault(key, v.Get(flat))
			}
			if err := v.BindEnv(key); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadEnv is a convenience function to load a .env file (if it exists)
// and unmarshal environment variables into the target struct.
func LoadEnv(path string, target interface{}) error {
	opts := DefaultOptions()
	opts.Path = path
	opts.IgnoreFileNotFound = true // Usually .env is optional in production
	return Load(opts, target)
}

// cloneDefaults keeps decoding transactional even when mapstructure merges into
// caller-initialized pointer, map or slice defaults.
func cloneDefaults(value reflect.Value, depth int) (reflect.Value, error) {
	if depth > 128 {
		return reflect.Value{}, errors.New("config: defaults exceed maximum nesting depth")
	}
	out := reflect.New(value.Type()).Elem()
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return out, nil
		}
		out.Set(reflect.New(value.Type().Elem()))
		child, err := cloneDefaults(value.Elem(), depth+1)
		if err != nil {
			return out, err
		}
		out.Elem().Set(child)
	case reflect.Interface:
		if value.IsNil() {
			return out, nil
		}
		child, err := cloneDefaults(value.Elem(), depth+1)
		if err != nil {
			return out, err
		}
		out.Set(child)
	case reflect.Map:
		if value.IsNil() {
			return out, nil
		}
		out.Set(reflect.MakeMapWithSize(value.Type(), value.Len()))
		iter := value.MapRange()
		for iter.Next() {
			child, err := cloneDefaults(iter.Value(), depth+1)
			if err != nil {
				return out, err
			}
			out.SetMapIndex(iter.Key(), child)
		}
	case reflect.Slice:
		if value.IsNil() {
			return out, nil
		}
		out.Set(reflect.MakeSlice(value.Type(), value.Len(), value.Len()))
		for i := 0; i < value.Len(); i++ {
			child, err := cloneDefaults(value.Index(i), depth+1)
			if err != nil {
				return out, err
			}
			out.Index(i).Set(child)
		}
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			child, err := cloneDefaults(value.Index(i), depth+1)
			if err != nil {
				return out, err
			}
			out.Index(i).Set(child)
		}
	case reflect.Struct:
		out.Set(value)
		for i := 0; i < value.NumField(); i++ {
			if value.Type().Field(i).PkgPath != "" {
				continue
			}
			child, err := cloneDefaults(value.Field(i), depth+1)
			if err != nil {
				return out, err
			}
			out.Field(i).Set(child)
		}
	default:
		out.Set(value)
	}
	return out, nil
}
