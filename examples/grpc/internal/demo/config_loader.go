package demo

import (
	"errors"
	"flag"
	"os"
	"reflect"
	"strings"

	"github.com/spf13/viper"
	commonconfig "github.com/thanhbvha/go-common/config"
)

// parseConfiguration loads settings once at startup. Only flags explicitly passed
// on the command line override YAML/environment, including false, zero and "".
func parseConfiguration(fs *flag.FlagSet, args []string, prefix string, target any) error {
	path := os.Getenv(prefix + "_CONFIG")
	fs.StringVar(&path, "config", path, "optional YAML file; empty uses environment/defaults ("+prefix+"_CONFIG)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	type override struct{ name, value string }
	var overrides []override
	fs.Visit(func(f *flag.Flag) {
		if f.Name != "config" {
			overrides = append(overrides, override{f.Name, f.Value.String()})
		}
	})
	if err := loadOptions(path, prefix, target); err != nil {
		return err
	}
	for _, item := range overrides {
		// The same FlagSet already validated these values before loading configuration.
		if err := fs.Set(item.name, item.value); err != nil {
			return errors.New("invalid command-line override")
		}
	}
	return nil
}

func loadOptions(path, prefix string, target any) error {
	opts := commonconfig.DefaultOptions()
	opts.Path = path
	opts.EnvPrefix = prefix
	// An explicitly selected file is required; malformed/missing files fail closed.
	if err := commonconfig.Load(opts, target); err != nil {
		return errors.New("could not load gRPC configuration; check file and value types")
	}
	// Viper AutomaticEnv alone does not enumerate keys absent from YAML during
	// Unmarshal. Bind the example's tagged fields so env-only deployments work too.
	env := viper.New()
	env.SetEnvPrefix(prefix)
	env.AllowEmptyEnv(true)
	for _, key := range optionKeys(reflect.TypeOf(target).Elem()) {
		if err := env.BindEnv(key); err != nil {
			return errors.New("could not bind gRPC environment configuration")
		}
	}
	if err := env.Unmarshal(target); err != nil {
		return errors.New("invalid gRPC environment configuration; check value types")
	}
	return nil
}

// Options use flat YAML keys, with CommonOptions embedded via mapstructure squash.
func optionKeys(t reflect.Type) []string {
	var keys []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("mapstructure")
		if field.Anonymous && strings.Contains(tag, ",squash") {
			keys = append(keys, optionKeys(field.Type)...)
		} else if key := strings.Split(tag, ",")[0]; key != "" && key != "-" {
			keys = append(keys, key)
		}
	}
	return keys
}
