package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// NativeClientFactory creates role-specific franz-go clients from one immutable
// connection/security configuration. It is safe for concurrent use.
type NativeClientFactory struct {
	cfg      Config
	base     []kgo.Opt
	producer []kgo.Opt
	consumer []kgo.Opt
}

// NewNativeClientFactory snapshots cfg and validates the common TLS/SASL
// options shared by producer, consumer, delay, and administrative clients.
func NewNativeClientFactory(cfg Config) (*NativeClientFactory, error) {
	cfg = cfg.clone()
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{"localhost:9092"}
	}
	base, err := nativeConnectionOptions(cfg)
	if err != nil {
		return nil, err
	}
	producer, err := nativeProducerOptions(cfg)
	if err != nil {
		return nil, err
	}
	return &NativeClientFactory{
		cfg: cfg, base: base, producer: producer,
		consumer: nativeConsumerOptions(cfg),
	}, nil
}

// NewNativeClientFactoryFromClient snapshots an existing franz-go client's
// effective options. This keeps the deprecated raw-client adapter secure: TLS,
// SASL, client ID, timeouts, hooks, and broker discovery settings are copied
// instead of reconstructing a consumer from broker addresses alone.
func NewNativeClientFactoryFromClient(client *kgo.Client, overrides ...kgo.Opt) (*NativeClientFactory, error) {
	if client == nil {
		return nil, fmt.Errorf("kafka: native client is nil")
	}
	base := append(slices.Clone(client.Opts()), overrides...)
	return &NativeClientFactory{base: base}, nil
}

// New creates a client with common connection and security settings plus opts.
func (f *NativeClientFactory) New(opts ...kgo.Opt) (*kgo.Client, error) {
	if f == nil {
		return nil, fmt.Errorf("kafka: nil native client factory")
	}
	all := append(slices.Clone(f.base), opts...)
	return kgo.NewClient(all...)
}

// NewProducer creates a producer inheriting the factory's durability, retry,
// compression, batching, connection, and security configuration.
func (f *NativeClientFactory) NewProducer(opts ...kgo.Opt) (*kgo.Client, error) {
	producer := append(slices.Clone(f.producer), opts...)
	return f.New(producer...)
}

// NewConsumer creates a consumer inheriting connection, security, session,
// heartbeat, offset-reset, fetch, and commit configuration. Role-specific
// options are appended last so callers can supply the topology group/topics
// and a narrower fetch bound.
func (f *NativeClientFactory) NewConsumer(opts ...kgo.Opt) (*kgo.Client, error) {
	consumer := append(slices.Clone(f.consumer), opts...)
	return f.New(consumer...)
}

// LookupDefault returns the process-wide default without panicking. Libraries
// should prefer explicit clients and use this only for compatibility wiring.
func LookupDefault() (*Client, bool) {
	globalMu.RLock()
	c := globalClient
	globalMu.RUnlock()
	return c, c != nil
}

func nativeConnectionOptions(cfg Config) ([]kgo.Opt, error) {
	if cfg.SecurityProtocol < Plaintext || cfg.SecurityProtocol > SASLSSL {
		return nil, fmt.Errorf("kafka: unsupported security protocol %d", cfg.SecurityProtocol)
	}
	overhead := max(cfg.ReadTimeout, cfg.WriteTimeout)
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.DialTimeout),
		kgo.RequestTimeoutOverhead(overhead),
	}

	if cfg.SecurityProtocol == SSL || cfg.SecurityProtocol == SASLSSL {
		tlsCfg := cfg.TLSConfig
		if tlsCfg == nil {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12} //nolint:gosec
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	if cfg.SecurityProtocol != SASLPlaintext && cfg.SecurityProtocol != SASLSSL {
		return opts, nil
	}
	switch cfg.SASLMechanism {
	case SASLPlain:
		if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
			return nil, fmt.Errorf("kafka: SASL PLAIN requires username and password")
		}
		opts = append(opts, kgo.SASL(plain.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsMechanism()))
	case SASLSCRAMSHA256:
		if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
			return nil, fmt.Errorf("kafka: SASL SCRAM-SHA-256 requires username and password")
		}
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha256Mechanism()))
	case SASLSCRAMSHA512:
		if cfg.SASLUsername == "" || cfg.SASLPassword == "" {
			return nil, fmt.Errorf("kafka: SASL SCRAM-SHA-512 requires username and password")
		}
		opts = append(opts, kgo.SASL(scram.Auth{User: cfg.SASLUsername, Pass: cfg.SASLPassword}.AsSha512Mechanism()))
	case SASLOAuthBearer:
		if cfg.SASLToken == "" {
			return nil, fmt.Errorf("kafka: SASLOAuthBearer requires SASLToken; token URL exchange is not implemented")
		}
		staticToken := cfg.SASLToken
		opts = append(opts, kgo.SASL(oauth.Oauth(func(context.Context) (oauth.Auth, error) {
			return oauth.Auth{Token: staticToken}, nil
		})))
	case SASLNone:
		return nil, fmt.Errorf("kafka: SASL security protocol requires a SASL mechanism")
	default:
		return nil, fmt.Errorf("kafka: unsupported SASL mechanism %d", cfg.SASLMechanism)
	}
	return opts, nil
}

func nativeProducerOptions(cfg Config) ([]kgo.Opt, error) {
	if cfg.ProducerAcks != -1 && cfg.ProducerAcks != 0 && cfg.ProducerAcks != 1 {
		return nil, fmt.Errorf("kafka: ProducerAcks must be -1, 0, or 1")
	}
	if cfg.ProducerIdempotent && cfg.ProducerAcks != -1 {
		return nil, fmt.Errorf("kafka: ProducerIdempotent requires ProducerAcks=-1")
	}
	compression, err := buildCompressionOpt(cfg.ProducerCompression)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{
		compression,
		kgo.ProduceRequestTimeout(cfg.ProducerTimeout),
		kgo.RecordRetries(cfg.ProducerMaxRetries),
		kgo.RetryBackoffFn(exponentialBackoff(cfg.ProducerRetryBackoff, 30*time.Second)),
		kgo.MaxBufferedBytes(cfg.ProducerBatchSize),
		kgo.ProducerBatchMaxBytes(int32(cfg.ProducerBatchSize)),
		kgo.ProducerLinger(cfg.ProducerBatchMaxWait),
	}
	if cfg.ProducerIdempotent {
		return append(opts, kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil))), nil
	}
	opts = append(opts, kgo.DisableIdempotentWrite())
	switch cfg.ProducerAcks {
	case 0:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	case 1:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	return opts, nil
}

func nativeConsumerOptions(cfg Config) []kgo.Opt {
	resetOffset := kgo.NewOffset().AtStart()
	if cfg.ConsumerAutoOffsetReset == "latest" {
		resetOffset = kgo.NewOffset().AtEnd()
	}
	opts := []kgo.Opt{
		kgo.ConsumeResetOffset(resetOffset),
		kgo.SessionTimeout(cfg.ConsumerSessionTimeout),
		kgo.HeartbeatInterval(cfg.ConsumerHeartbeat),
		kgo.FetchMaxWait(cfg.ConsumerFetchMaxWait),
	}
	if cfg.ConsumerCommitInterval > 0 {
		return append(opts, kgo.AutoCommitInterval(cfg.ConsumerCommitInterval), kgo.AutoCommitMarks())
	}
	return append(opts, kgo.DisableAutoCommit())
}
