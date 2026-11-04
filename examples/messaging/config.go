package main

import (
	"errors"
	"flag"
	"io"
	"os"
	"strings"

	"github.com/thanhbvha/go-common/v2/config"
)

// Configuration belongs to the example, not the messaging library. Pass it
// explicitly so independent fixtures never change process-wide environment.
type exampleConfig struct {
	Driver   string `mapstructure:"driver"`
	Broker   string `mapstructure:"broker"`
	Postgres struct {
		DSN string `mapstructure:"dsn"`
	} `mapstructure:"postgres"`
	MySQL struct {
		DSN string `mapstructure:"dsn"`
	} `mapstructure:"mysql"`
	MongoDB struct {
		URI string `mapstructure:"uri"`
	} `mapstructure:"mongodb"`
	Redis struct {
		Addr     string `mapstructure:"addr"`
		Username string `mapstructure:"username"`
		Password string `mapstructure:"password"`
		DB       int    `mapstructure:"db"`
	} `mapstructure:"redis"`
	Kafka struct {
		Brokers []string `mapstructure:"brokers"`
	} `mapstructure:"kafka"`
}

func defaultConfigPath() string {
	for _, path := range []string{"config.yaml", "examples/messaging/config.yaml"} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return "config.yaml" // A missing file is an error; -config '' enables env-only.
}

func loadExampleConfig(path string, lookup func(string) (string, bool)) (exampleConfig, error) {
	cfg := exampleConfig{Driver: "postgres", Broker: "nats"}
	if err := config.Load(config.Options{Path: path}, &cfg); err != nil {
		// Decoder errors can include credential-bearing values. Do not expose them.
		return exampleConfig{}, errors.New("cannot read messaging YAML config; check -config path and field types")
	}
	for _, binding := range []struct {
		name   string
		target *string
	}{
		{"ORM_POSTGRES_DSN", &cfg.Postgres.DSN},
		{"ORM_MYSQL_DSN", &cfg.MySQL.DSN},
		{"MONGODB_INTEGRATION_URI", &cfg.MongoDB.URI},
		{"EVENTBUS_REDIS_ADDR", &cfg.Redis.Addr},
	} {
		if value, ok := lookup(binding.name); ok {
			*binding.target = value
		}
	}
	if value, ok := lookup("EVENTBUS_KAFKA_BROKERS"); ok {
		cfg.Kafka.Brokers = strings.Split(value, ",")
	}
	return cfg, nil
}

func parseConfig(args []string, output io.Writer) (exampleConfig, error) {
	flags := flag.NewFlagSet("messaging", flag.ContinueOnError)
	flags.SetOutput(output)
	path := flags.String("config", defaultConfigPath(), "YAML configuration file (empty for environment only)")
	driver := flags.String("driver", "", "Override driver: postgres, mysql, mongodb, or all")
	broker := flags.String("broker", "", "Override broker: redis, nats, kafka, or all")
	if err := flags.Parse(args); err != nil {
		return exampleConfig{}, err
	}
	if flags.NArg() != 0 {
		return exampleConfig{}, errors.New("unexpected positional arguments")
	}
	cfg, err := loadExampleConfig(*path, os.LookupEnv)
	if err != nil {
		return exampleConfig{}, err
	}
	flags.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "driver":
			cfg.Driver = *driver
		case "broker":
			cfg.Broker = *broker
		}
	})
	return cfg, cfg.validate()
}

func (cfg exampleConfig) validate() error {
	drivers, err := selectDrivers(cfg.Driver)
	if err != nil {
		return err
	}
	brokers, err := selectBrokers(cfg.Broker)
	if err != nil {
		return err
	}
	for _, driver := range drivers {
		if err := cfg.validateDriver(driver); err != nil {
			return err
		}
	}
	for _, broker := range brokers {
		if err := cfg.validateBroker(broker); err != nil {
			return err
		}
	}
	return nil
}
