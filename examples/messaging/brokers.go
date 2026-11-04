package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	goredis "github.com/redis/go-redis/v9"
	kafkadriver "github.com/thanhbvha/go-common/v2/eventbus/driver/kafka"
	natsdriver "github.com/thanhbvha/go-common/v2/eventbus/driver/nats"
	redisdriver "github.com/thanhbvha/go-common/v2/eventbus/driver/redis"
	"github.com/thanhbvha/go-common/v2/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/v2/kafka"
)

func selectBrokers(name string) ([]string, error) {
	switch name {
	case "all":
		return []string{"redis", "nats", "kafka"}, nil
	case "redis", "nats", "kafka":
		return []string{name}, nil
	default:
		return nil, errors.New("broker must be redis, nats, kafka, or all")
	}
}
func (cfg exampleConfig) validateBroker(name string) error {
	if name == "all" {
		return errors.New("select one broker per fixture")
	}
	if _, err := selectBrokers(name); err != nil {
		return err
	}
	if name == "redis" && strings.TrimSpace(cfg.Redis.Addr) == "" {
		return errors.New("redis.addr or EVENTBUS_REDIS_ADDR required for isolated Redis fixture")
	}
	if name == "kafka" && len(cfg.Kafka.Brokers) == 0 {
		return errors.New("kafka.brokers or EVENTBUS_KAFKA_BROKERS required for isolated Kafka fixture")
	}
	if name == "redis" && cfg.Redis.DB < 0 {
		return errors.New("redis.db must not be negative")
	}
	if name == "kafka" {
		for _, addr := range cfg.Kafka.Brokers {
			if strings.TrimSpace(addr) == "" {
				return errors.New("empty Kafka fixture address")
			}
		}
	}
	return nil
}

// newBroker only connects to explicitly supplied Redis/Kafka fixtures. NATS is
// an owned real JetStream server. The caller must close its bus before cleanup.
func newBroker(ctx context.Context, settings exampleConfig, name string) (transport.Transport, func(), error) {
	if err := settings.validateBroker(name); err != nil {
		return nil, nil, err
	}
	switch name {
	case "redis":
		client := goredis.NewClient(&goredis.Options{Addr: settings.Redis.Addr, Username: settings.Redis.Username, Password: settings.Redis.Password, DB: settings.Redis.DB})
		if err := client.Ping(ctx).Err(); err != nil {
			_ = client.Close()
			return nil, nil, &stepError{step: "redis.connect", err: err, hint: "check redis.addr, redis.username, redis.password, redis.db and network access"}
		}
		return redisdriver.New(client), func() { _ = client.Close() }, nil
	case "kafka":
		cfg := commonkafka.DefaultConfig()
		cfg.Brokers = append([]string(nil), settings.Kafka.Brokers...)
		for i := range cfg.Brokers {
			cfg.Brokers[i] = strings.TrimSpace(cfg.Brokers[i])
			if cfg.Brokers[i] == "" {
				return nil, nil, errors.New("empty Kafka fixture address")
			}
		}
		cfg.ProducerAcks = -1
		tr, err := kafkadriver.NewWithConfig(cfg)
		if err != nil {
			return nil, nil, err
		}
		cleanup := func() {
			stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tr.Close(stop)
		}
		if err := tr.Ping(ctx); err != nil {
			cleanup()
			return nil, nil, err
		}
		return tr, cleanup, nil
	case "nats":
		dir, err := os.MkdirTemp("", "go-common-messaging-")
		if err != nil {
			return nil, nil, err
		}
		s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: dir, NoLog: true, NoSigs: true})
		if err != nil {
			_ = os.RemoveAll(dir)
			return nil, nil, err
		}
		s.Start()
		if !s.ReadyForConnections(5 * time.Second) {
			s.Shutdown()
			_ = os.RemoveAll(dir)
			return nil, nil, errors.New("NATS fixture not ready")
		}
		conn, err := gonats.Connect(s.ClientURL())
		if err != nil {
			s.Shutdown()
			_ = os.RemoveAll(dir)
			return nil, nil, err
		}
		cleanup := func() { conn.Close(); s.Shutdown(); _ = os.RemoveAll(dir) }
		js, err := conn.JetStream()
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		return natsdriver.New(js), cleanup, nil
	}
	return nil, nil, errors.New("unknown broker")
}
