package demo

import (
	"errors"
	"flag"
	"os"

	commonconfig "github.com/thanhbvha/go-common/v2/config"
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
	return nil
}
