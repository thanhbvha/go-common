package config

import (
	"log"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// Watch loads the configuration and starts watching the file for changes.
// When a change is detected, it unmarshals the new config into a fresh struct
// created by targetFactory, and passes it to the onChange callback.
//
// This is useful for hot-reloading configurations in microservices without
// restarting the process (e.g., when a Kubernetes ConfigMap is updated).
//
// Example:
//
//	err := config.Watch(config.Options{Path: "config.yaml"},
//		func() interface{} { return &AppConfig{} },
//		func(newCfg interface{}) {
//			cfg := newCfg.(*AppConfig)
//			log.Println("Config updated:", cfg)
//			// update global state, reconfigure connections, etc.
//		})
func Watch(opts Options, targetFactory func() interface{}, onChange func(newTarget interface{})) error {
	v := viper.New()

	if opts.Path != "" {
		v.SetConfigFile(opts.Path)
		err := v.ReadInConfig()
		if err != nil {
			if !opts.IgnoreFileNotFound {
				return err
			}
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
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
		v.AutomaticEnv()
	}

	// Initial Load
	initialTarget := targetFactory()
	if err := v.Unmarshal(initialTarget); err != nil {
		return err
	}
	// Trigger first time
	onChange(initialTarget)

	// Setup watch if file path is provided
	if opts.Path != "" {
		v.OnConfigChange(func(e fsnotify.Event) {
			log.Printf("[Config] config file changed: %s", e.Name)
			newTarget := targetFactory()
			if err := v.Unmarshal(newTarget); err != nil {
				log.Printf("[Config] error unmarshaling new config: %v", err)
				return
			}
			onChange(newTarget)
		})
		v.WatchConfig()
	}

	return nil
}
