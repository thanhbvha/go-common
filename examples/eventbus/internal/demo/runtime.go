package demo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
	kafkadriver "github.com/thanhbvha/go-common/eventbus/driver/kafka"
	"github.com/thanhbvha/go-common/eventbus/driver/memory"
	natsdriver "github.com/thanhbvha/go-common/eventbus/driver/nats"
	redisdriver "github.com/thanhbvha/go-common/eventbus/driver/redis"
	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	commonnats "github.com/thanhbvha/go-common/nats"
	commonredis "github.com/thanhbvha/go-common/redis"
)

type Runtime struct {
	Transport transport.Transport
	close     func() error
}

func OpenRuntime(ctx context.Context, cfg Config, logger *Logger) (*Runtime, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	switch cfg.Driver {
	case "memory":
		return &Runtime{Transport: memory.New(), close: func() error { return nil }}, nil
	case "redis":
		host, port, err := splitHostPort(cfg.RedisAddr)
		if err != nil {
			return nil, fmt.Errorf("redis address: %w", err)
		}
		clientCfg := commonredis.DefaultConfig()
		clientCfg.Host, clientCfg.Port = host, port
		clientCfg.Password = os.Getenv("EVENTBUS_REDIS_PASSWORD")
		clientCfg.Logger = logger
		client := commonredis.New(clientCfg)
		if err := client.Connect(ctx); err != nil {
			return nil, fmt.Errorf("connect redis: %w", err)
		}
		return &Runtime{Transport: redisdriver.New(client.Native()), close: client.Close}, nil
	case "nats":
		clientCfg := commonnats.DefaultConfig()
		urls, err := safeNATSURLs(cfg.NATSURLs)
		if err != nil {
			return nil, err
		}
		clientCfg.URLs = urls
		clientCfg.CredFile = os.Getenv("EVENTBUS_NATS_CREDS_FILE")
		clientCfg.NKeyFile = os.Getenv("EVENTBUS_NATS_NKEY_FILE")
		clientCfg.Logger = logger
		client := commonnats.New(clientCfg)
		if err := client.Connect(ctx); err != nil {
			return nil, fmt.Errorf("connect nats: %w", err)
		}
		return &Runtime{Transport: natsdriver.New(client.JetStream()), close: client.Close}, nil
	case "kafka":
		clientCfg, err := kafkaConfig(cfg, logger)
		if err != nil {
			return nil, err
		}
		client := commonkafka.New(clientCfg)
		if err := client.Connect(ctx); err != nil {
			return nil, fmt.Errorf("connect kafka: %w", err)
		}
		tr, err := kafkadriver.NewWithClient(client)
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("create kafka transport: %w", err)
		}
		return &Runtime{Transport: tr, close: client.Close}, nil
	default:
		return nil, fmt.Errorf("unsupported driver %q", cfg.Driver)
	}
}

func safeNATSURLs(value string) ([]string, error) {
	addresses := SplitCSV(value)
	if len(addresses) == 0 {
		return nil, fmt.Errorf("at least one NATS URL is required")
	}
	for _, address := range addresses {
		parsed, err := url.Parse(address)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "nats" && parsed.Scheme != "tls") {
			return nil, fmt.Errorf("invalid NATS URL; expected nats://host:port or tls://host:port")
		}
		if parsed.User != nil {
			return nil, fmt.Errorf("NATS URL userinfo is not allowed; use a credentials or NKey file")
		}
	}
	return addresses, nil
}

func (r *Runtime) Close() error {
	if r == nil || r.close == nil {
		return nil
	}
	return r.close()
}

func NewBus(cfg Config, rt *Runtime, logger *Logger, metrics eventbus.Metrics, tracer eventbus.Tracer) (eventbus.Bus, error) {
	busCfg := eventbus.DefaultConfig()
	busCfg.Namespace = cfg.Namespace
	busCfg.Concurrency = 4
	busCfg.Prefetch = 8
	busCfg.MaxAttempts = 3
	busCfg.ProcessTimeout = 10 * time.Second
	busCfg.RetryPolicy = eventbus.ConstantBackoff{Delay: time.Second}
	busCfg.Logger = logger
	busCfg.Metrics = metrics
	busCfg.Tracer = tracer
	return eventbus.Open(busCfg, eventbus.WithTransport(rt.Transport))
}

func TopologyOptions(driver string) []eventbus.TopologyOption {
	base := []eventbus.TopologyOption{
		eventbus.WithConcurrency(4),
		eventbus.WithPrefetch(8),
		eventbus.WithMaxAttempts(3),
	}
	switch driver {
	case "redis":
		return append(base, redisdriver.WithBatchSize(12), redisdriver.WithBlock(time.Second), redisdriver.WithAckWait(30*time.Second), redisdriver.WithDelayBatchSize(100))
	case "nats":
		return append(base, natsdriver.WithAckWait(30*time.Second), natsdriver.WithBatchSize(12), natsdriver.WithFetchTimeout(time.Second), natsdriver.WithDelayBatchSize(100))
	case "kafka":
		return append(base, kafkadriver.WithPartitions(4), kafkadriver.WithReplicationFactor(1), kafkadriver.WithFetchMaxWait(500*time.Millisecond), kafkadriver.WithDelayBatchSize(100), kafkadriver.WithRetention(24*time.Hour))
	default:
		return base
	}
}

func EventTopologyOptions(driver string) []eventbus.TopologyOption {
	options := TopologyOptions(driver)
	switch driver {
	case "redis":
		options = append(options, redisdriver.WithEventLayoutTargeted())
	case "nats":
		options = append(options, natsdriver.WithEventLayoutTargeted())
	case "kafka":
		options = append(options, kafkadriver.WithEventLayoutTargeted())
	}
	return options
}

func SupportsDedupe(tr transport.Transport) bool {
	if _, ok := tr.(transport.TopologyDeduper); ok {
		return true
	}
	_, ok := tr.(transport.Deduper)
	return ok
}

func SupportsOrdering(tr transport.Transport) bool {
	return tr.Capabilities().SupportsBrokerOrdering()
}

func Start(ctx context.Context, bus eventbus.Bus) error {
	startCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := bus.Start(startCtx); err != nil {
		return fmt.Errorf("start eventbus: %w", err)
	}
	if err := bus.Ping(startCtx); err != nil {
		return fmt.Errorf("ping eventbus: %w", err)
	}
	return nil
}

func Drain(bus eventbus.Bus) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return bus.Drain(ctx)
}

func CloseTransport(tr transport.Transport) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return tr.Close(ctx)
}

func CloseAll(bus eventbus.Bus, rt *Runtime) error {
	var busErr error
	if bus != nil {
		busErr = Drain(bus)
	} else if rt != nil && rt.Transport != nil {
		busErr = CloseTransport(rt.Transport)
	}
	return errors.Join(busErr, rt.Close())
}

func splitHostPort(address string) (string, int, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid port %q", portText)
	}
	return host, port, nil
}

func kafkaConfig(cfg Config, logger *Logger) (commonkafka.Config, error) {
	result := commonkafka.DefaultConfig()
	result.Brokers = SplitCSV(cfg.KafkaBrokers)
	result.ClientID = "eventbus-example"
	result.Logger = logger
	if cfg.KafkaTLS {
		result.SecurityProtocol = commonkafka.SSL
		result.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	mechanism := cfg.KafkaSASL
	if mechanism == "" {
		return result, nil
	}
	if !cfg.KafkaTLS {
		return result, fmt.Errorf("Kafka SASL example requires TLS; set EVENTBUS_KAFKA_TLS=true")
	}
	result.SecurityProtocol = commonkafka.SASLSSL
	switch mechanism {
	case "plain":
		result.SASLMechanism = commonkafka.SASLPlain
	case "scram-sha-256":
		result.SASLMechanism = commonkafka.SASLSCRAMSHA256
	case "scram-sha-512":
		result.SASLMechanism = commonkafka.SASLSCRAMSHA512
	case "oauthbearer":
		result.SASLMechanism = commonkafka.SASLOAuthBearer
	default:
		return result, fmt.Errorf("unsupported Kafka SASL mechanism %q", mechanism)
	}
	result.SASLUsername = os.Getenv("EVENTBUS_KAFKA_SASL_USERNAME")
	result.SASLPassword = os.Getenv("EVENTBUS_KAFKA_SASL_PASSWORD")
	result.SASLToken = os.Getenv("EVENTBUS_KAFKA_SASL_TOKEN")
	return result, nil
}
