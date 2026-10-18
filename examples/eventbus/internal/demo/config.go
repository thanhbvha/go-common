package demo

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	TaskName  = "send_welcome_email"
	EventName = "user_registered"
)

type Config struct {
	Driver       string
	Namespace    string
	RedisAddr    string
	NATSURLs     string
	KafkaBrokers string
	KafkaTLS     bool
	KafkaSASL    string
	Wait         time.Duration
}

func DefaultConfig() Config {
	return Config{
		Driver:       envOr("EVENTBUS_DRIVER", "memory"),
		Namespace:    envOr("EVENTBUS_NAMESPACE", "example_eventbus"),
		RedisAddr:    envOr("EVENTBUS_REDIS_ADDR", "127.0.0.1:6379"),
		NATSURLs:     envOr("EVENTBUS_NATS_URLS", "nats://127.0.0.1:4222"),
		KafkaBrokers: envOr("EVENTBUS_KAFKA_BROKERS", "127.0.0.1:9092"),
		KafkaTLS:     envBool("EVENTBUS_KAFKA_TLS"),
		KafkaSASL:    strings.ToLower(os.Getenv("EVENTBUS_KAFKA_SASL_MECHANISM")),
	}
}

func BindFlags(fs *flag.FlagSet, cfg *Config) {
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

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
