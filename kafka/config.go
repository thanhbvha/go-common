// Package kafka provides a production-ready Apache Kafka client wrapper
// built on top of github.com/twmb/franz-go. It supports TLS encryption,
// SASL authentication (PLAIN, SCRAM-SHA-256/512, OAuthBearer), idempotent
// producers, consumer groups with automatic offset management, and topic
// administration.
//
// The API mirrors the sibling nats package: a Config struct drives all options,
// a Client manages the lifecycle, and the SetDefault/Default pattern enables
// zero-argument usage across packages without a hidden global singleton.
//
// Basic usage:
//
//	cfg := kafka.DefaultConfig()
//	cfg.Brokers = []string{"kafka-1:9092", "kafka-2:9092"}
//	cfg.SecurityProtocol = kafka.SASLSSL
//	cfg.SASLMechanism = kafka.SASLSCRAMSHA256
//	cfg.SASLUsername = "app"
//	cfg.SASLPassword = os.Getenv("KAFKA_PASSWORD")
//
//	client := kafka.MustConnect(ctx, cfg)
//	kafka.SetDefault(client)
//	defer kafka.Close()
package kafka

import (
	"crypto/tls"
	"time"
)

// SecurityProtocol specifies the network security protocol used to
// communicate with Kafka brokers.
type SecurityProtocol int

const (
	// Plaintext uses no encryption and no authentication.
	// Suitable for local development only; never use in production.
	Plaintext SecurityProtocol = iota

	// SSL uses TLS encryption without SASL authentication.
	// Suitable when mutual TLS (mTLS) alone is sufficient.
	SSL

	// SASLPlaintext uses SASL authentication over an unencrypted connection.
	// Should only be used on trusted internal networks.
	SASLPlaintext

	// SASLSSL uses both SASL authentication and TLS encryption.
	// Recommended for all production deployments.
	SASLSSL
)

// SASLMechanism selects the SASL authentication mechanism.
type SASLMechanism int

const (
	// SASLNone disables SASL authentication.
	SASLNone SASLMechanism = iota

	// SASLPlain uses plain username/password authentication (RFC 4616).
	// Always pair with TLS (SASLSSL) to avoid credential exposure.
	SASLPlain

	// SASLSCRAMSHA256 uses SCRAM-SHA-256 (RFC 5802).
	// Recommended for internal services.
	SASLSCRAMSHA256

	// SASLSCRAMSHA512 uses SCRAM-SHA-512 (RFC 5802).
	// Provides the highest security among password-based mechanisms.
	SASLSCRAMSHA512

	// SASLOAuthBearer uses OAuth 2.0 bearer token authentication (RFC 7628).
	// Suitable for integration with SSO / OIDC providers.
	SASLOAuthBearer
)

// Logger is the logging interface consumed by Client.
// It is intentionally identical to the nats.Logger and redis.Logger interfaces
// so that the same adapter can satisfy all three without import cycles.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	// Async variants are used inside goroutines and callbacks to avoid
	// blocking the caller while the log entry is being written.
	InfoAsync(msg string, args ...any)
	WarnAsync(msg string, args ...any)
	ErrorAsync(msg string, args ...any)
}

// Config holds all tuneable parameters for a Kafka Client.
// Use DefaultConfig to obtain a value pre-filled with sensible defaults,
// then override only the fields you need.
type Config struct {
	// --- Connection ---

	// Brokers is the list of Kafka broker addresses (host:port).
	// Multiple addresses enable automatic failover across a cluster.
	// Default: ["localhost:9092"].
	Brokers []string

	// ClientID is the Kafka client identifier shown in broker metrics and logs.
	// Default: "go-common-kafka".
	ClientID string

	// --- Security ---

	// SecurityProtocol specifies the security protocol for broker connections.
	// Default: Plaintext.
	SecurityProtocol SecurityProtocol

	// TLSConfig is an optional custom TLS configuration.
	// When nil and SecurityProtocol is SSL or SASLSSL, the system default
	// TLS configuration is used.
	TLSConfig *tls.Config

	// SASLMechanism selects the SASL authentication mechanism.
	// Ignored when SecurityProtocol is Plaintext or SSL.
	// Default: SASLNone.
	SASLMechanism SASLMechanism

	// SASLUsername is the SASL username for PLAIN and SCRAM mechanisms.
	SASLUsername string

	// SASLPassword is the SASL password for PLAIN and SCRAM mechanisms.
	// Load from environment variables or a secrets manager; never hardcode.
	SASLPassword string

	// SASLToken is the static bearer token for OAuthBearer authentication.
	// Used when SASLMechanism is SASLOAuthBearer and no token URL is set.
	SASLToken string

	// SASLTokenURL is the OAuth 2.0 token endpoint URL.
	// When set, the client fetches and refreshes tokens automatically.
	SASLTokenURL string

	// --- Producer ---

	// ProducerAcks controls the durability guarantee for produced messages.
	//  -1 = wait for all in-sync replicas (strongest; default).
	//   0 = fire-and-forget (fastest; no durability guarantee).
	//   1 = wait for the partition leader only.
	ProducerAcks int

	// ProducerTimeout is the maximum time to wait for a produce request.
	// Default: 10s.
	ProducerTimeout time.Duration

	// ProducerBatchSize is the maximum number of bytes accumulated into a
	// single produce batch before sending.
	// Default: 1 048 576 (1 MB).
	ProducerBatchSize int

	// ProducerBatchMaxWait is the maximum time to accumulate records into a
	// batch before sending regardless of ProducerBatchSize.
	// Higher values trade latency for throughput.
	// Default: 5ms.
	ProducerBatchMaxWait time.Duration

	// ProducerMaxRetries is the number of times the producer retries a failed
	// request before returning an error to the caller.
	// Default: 5.
	ProducerMaxRetries int

	// ProducerRetryBackoff is the base duration between producer retry
	// attempts. The actual wait time grows exponentially.
	// Default: 100ms.
	ProducerRetryBackoff time.Duration

	// ProducerIdempotent enables idempotent producing, which guarantees that
	// messages are written to a partition exactly once even when retries occur.
	// Requires ProducerAcks == -1.
	// Default: true.
	ProducerIdempotent bool

	// ProducerCompression sets the compression codec applied to produce batches.
	// Accepted values: "none", "gzip", "snappy", "lz4", "zstd".
	// Default: "snappy".
	ProducerCompression string

	// --- Consumer ---

	// ConsumerGroup is the Kafka consumer group ID.
	// Required when calling Subscribe or SubscribeBatch.
	ConsumerGroup string

	// ConsumerSessionTimeout is the maximum time the broker waits for a
	// consumer heartbeat before declaring it failed and triggering a rebalance.
	// Default: 30s.
	ConsumerSessionTimeout time.Duration

	// ConsumerHeartbeat is the interval between heartbeats sent by the consumer
	// to the broker. Must be significantly lower than ConsumerSessionTimeout.
	// Default: 3s.
	ConsumerHeartbeat time.Duration

	// ConsumerAutoOffsetReset controls the offset used when no committed offset
	// exists for the consumer group.
	// Accepted values: "earliest", "latest".
	// Default: "earliest".
	ConsumerAutoOffsetReset string

	// ConsumerFetchMaxWait is the maximum time the broker waits to accumulate
	// records up to ConsumerFetchMin bytes before responding to a fetch request.
	// Default: 500ms.
	ConsumerFetchMaxWait time.Duration

	// ConsumerCommitInterval is the interval between automatic offset commits.
	// Set to 0 to disable auto-commit and manage offsets manually.
	// Default: 1s.
	ConsumerCommitInterval time.Duration

	// --- Connection timeouts ---

	// DialTimeout is the maximum time allowed to establish a TCP connection to
	// a broker. Default: 5s.
	DialTimeout time.Duration

	// WriteTimeout is the maximum time allowed for a single write request.
	// Default: 10s.
	WriteTimeout time.Duration

	// ReadTimeout is the maximum time allowed for a single read response.
	// Default: 10s.
	ReadTimeout time.Duration

	// MaxConnRetries is the number of connection attempts during Connect before
	// returning an error. Default: 3.
	MaxConnRetries int

	// --- Optional Logger ---

	// Logger receives connection, health-check, and error events.
	// Set to nil to suppress all internal logging.
	Logger Logger
}

// DefaultConfig returns a Config pre-populated with production-ready defaults.
// Override only the fields relevant to your deployment.
func DefaultConfig() Config {
	return Config{
		Brokers:  []string{"localhost:9092"},
		ClientID: "go-common-kafka",

		SecurityProtocol: Plaintext,
		SASLMechanism:    SASLNone,

		ProducerAcks:         -1,
		ProducerTimeout:      10 * time.Second,
		ProducerBatchSize:    1 << 20, // 1 MB
		ProducerBatchMaxWait: 5 * time.Millisecond,
		ProducerMaxRetries:   5,
		ProducerRetryBackoff: 100 * time.Millisecond,
		ProducerIdempotent:   true,
		ProducerCompression:  "snappy",

		ConsumerSessionTimeout:  30 * time.Second,
		ConsumerHeartbeat:       3 * time.Second,
		ConsumerAutoOffsetReset: "earliest",
		ConsumerFetchMaxWait:    500 * time.Millisecond,
		ConsumerCommitInterval:  1 * time.Second,

		DialTimeout:    5 * time.Second,
		WriteTimeout:   10 * time.Second,
		ReadTimeout:    10 * time.Second,
		MaxConnRetries: 3,
	}
}
