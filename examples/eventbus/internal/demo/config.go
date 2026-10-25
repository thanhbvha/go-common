package demo

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	commonconfig "github.com/thanhbvha/go-common/config"
)

const (
	TaskName  = "send_welcome_email"
	EventName = "user_registered"
)

type Config struct {
	ConfigFile        string        `mapstructure:"-"`
	Driver            string        `mapstructure:"driver"`
	Namespace         string        `mapstructure:"namespace"`
	RedisAddr         string        `mapstructure:"redis_addr"`
	RedisPassword     string        `mapstructure:"redis_password"`
	NATSURLs          string        `mapstructure:"nats_urls"`
	NATSCredsFile     string        `mapstructure:"nats_creds_file"`
	NATSNKeyFile      string        `mapstructure:"nats_nkey_file"`
	KafkaBrokers      string        `mapstructure:"kafka_brokers"`
	KafkaTLS          bool          `mapstructure:"kafka_tls"`
	KafkaSASL         string        `mapstructure:"kafka_sasl_mechanism"`
	KafkaSASLUsername string        `mapstructure:"kafka_sasl_username"`
	KafkaSASLPassword string        `mapstructure:"kafka_sasl_password"`
	KafkaSASLToken    string        `mapstructure:"kafka_sasl_token"`
	Wait              time.Duration `mapstructure:"wait"`
}

func DefaultConfig() Config {
	return Config{
		Driver:       "memory",
		Namespace:    "example_eventbus",
		RedisAddr:    "127.0.0.1:6379",
		NATSURLs:     "nats://127.0.0.1:4222",
		KafkaBrokers: "127.0.0.1:9092",
	}
}

func LoadConfig(args []string) (Config, error) {
	cfg := DefaultConfig()
	path, err := configPath(args)
	if err != nil {
		return Config{}, err
	}
	if path == "" {
		path = os.Getenv("EVENTBUS_CONFIG_FILE")
	}
	opts := commonconfig.DefaultOptions()
	opts.Path = path
	opts.EnvPrefix = "EVENTBUS"
	if err := commonconfig.Load(opts, &cfg); err != nil {
		return Config{}, fmt.Errorf("load EventBus config %q: %w", path, err)
	}
	cfg.ConfigFile = path
	cfg.KafkaSASL = strings.ToLower(cfg.KafkaSASL)
	return cfg, nil
}

func configPath(args []string) (string, error) {
	for i, arg := range args {
		if arg == "--config" {
			if i+1 >= len(args) || strings.HasPrefix(args[i+1], "-") {
				return "", fmt.Errorf("--config requires a file path")
			}
			return filepath.Clean(args[i+1]), nil
		}
		if value, found := strings.CutPrefix(arg, "--config="); found {
			if strings.TrimSpace(value) == "" {
				return "", fmt.Errorf("--config requires a file path")
			}
			return filepath.Clean(value), nil
		}
	}
	return "", nil
}

func BindFlags(fs *flag.FlagSet, cfg *Config) {
	fs.StringVar(&cfg.ConfigFile, "config", cfg.ConfigFile, "optional .env or YAML configuration file")
	fs.StringVar(&cfg.Driver, "driver", cfg.Driver, "transport: memory, redis, nats, or kafka")
	fs.StringVar(&cfg.Namespace, "namespace", cfg.Namespace, "isolated EventBus namespace")
	fs.StringVar(&cfg.RedisAddr, "redis-addr", cfg.RedisAddr, "Redis host:port")
	fs.StringVar(&cfg.NATSURLs, "nats-urls", cfg.NATSURLs, "comma-separated NATS URLs")
	fs.StringVar(&cfg.KafkaBrokers, "kafka-brokers", cfg.KafkaBrokers, "comma-separated Kafka brokers")
	fs.BoolVar(&cfg.KafkaTLS, "kafka-tls", cfg.KafkaTLS, "enable Kafka TLS (credentials stay in environment variables)")
	fs.DurationVar(&cfg.Wait, "wait", cfg.Wait, "how long a finite demo waits; zero selects a driver-aware default")
}

func (c Config) Validate() error {
	switch c.Driver {
	case "memory", "redis", "nats", "kafka":
	default:
		return fmt.Errorf("unsupported driver %q", c.Driver)
	}
	if strings.TrimSpace(c.Namespace) == "" || c.Namespace != strings.TrimSpace(c.Namespace) {
		return fmt.Errorf("namespace must not be blank or have surrounding whitespace")
	}
	if c.Wait < 0 {
		return fmt.Errorf("wait must not be negative")
	}
	return nil
}

func (c Config) DemoWait(delay time.Duration) time.Duration {
	if c.Wait > 0 {
		return c.Wait
	}
	precision := time.Millisecond
	switch c.Driver {
	case "redis", "nats":
		precision = time.Second
	case "kafka":
		precision = 5 * time.Second
	}
	return delay + precision + 2*time.Second
}

func SplitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if item := strings.TrimSpace(part); item != "" {
			result = append(result, item)
		}
	}
	return result
}
