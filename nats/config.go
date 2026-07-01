// Package nats provides a production-ready NATS client wrapper built on top of
// github.com/nats-io/nats.go. It supports NATS Core (Pub/Sub, Request/Reply)
// and JetStream (persistent streams, durable consumers, KV store).
//
// The API mirrors the sibling redis package: a Config struct drives all options,
// a Client manages the lifecycle, and functional options customise JetStream
// streams and consumers.
//
// A process-wide default client can be registered with SetDefault and retrieved
// with Default, enabling zero-argument usage across packages without exposing a
// hidden global singleton.
//
// Basic usage:
//
//	cfg := nats.DefaultConfig()
//	cfg.URLs = []string{"nats://nats.example.com:4222"}
//
//	client := nats.New(cfg)
//	if err := client.Connect(context.Background()); err != nil {
//	    log.Fatal(err)
//	}
//	nats.SetDefault(client)
//	defer nats.Close()
package nats

import "time"

// StorageType selects where JetStream persists stream and KV data.
type StorageType int

const (
	// MemoryStorage stores messages in RAM. Fast but non-persistent (default).
	MemoryStorage StorageType = iota
	// FileStorage stores messages on disk. Survives server restarts.
	FileStorage
)

// RetentionPolicy controls when JetStream removes messages from a stream.
type RetentionPolicy int

const (
	// LimitsPolicy retains messages according to MaxMsgs/MaxBytes/MaxAge limits (default).
	LimitsPolicy RetentionPolicy = iota
	// WorkQueuePolicy removes a message once it has been ACK-ed by any consumer.
	// Mirrors Redis Stream behaviour when used as a task queue.
	WorkQueuePolicy
	// InterestPolicy removes a message once all active consumers have ACK-ed it.
	InterestPolicy
)

// Config holds all options needed to create and connect a NATS Client.
// Use DefaultConfig to obtain a value pre-filled with sensible defaults,
// then override only the fields you need.
type Config struct {
	// --- Connection ---

	// URLs is the list of NATS server addresses to connect to.
	// Supports plain NATS ("nats://host:4222") and TLS ("tls://host:4223").
	// Multiple addresses enable automatic failover across a cluster.
	// Default: ["nats://localhost:4222"].
	URLs []string

	// NKeyFile is the path to an NKey seed file used for NKey authentication.
	// Leave empty to skip NKey auth.
	NKeyFile string

	// CredFile is the path to a NATS credentials (.creds) file.
	// Leave empty to skip credentials-based auth.
	CredFile string

	// ConnectTimeout is the maximum time allowed to establish the initial connection.
	// Default: 5s.
	ConnectTimeout time.Duration

	// ReconnectWait is the time to wait between reconnection attempts.
	// Default: 2s.
	ReconnectWait time.Duration

	// MaxReconnects is the maximum number of reconnection attempts before giving up.
	// Set to -1 for unlimited reconnections.
	// Default: 60.
	MaxReconnects int

	// MaxConnRetries is the number of Ping/Status checks during Connect.
	// Default: 3.
	MaxConnRetries int

	// --- JetStream defaults ---

	// DefaultStorage is the storage backend used when AddStream or KVCreate
	// do not specify an explicit WithStorage/WithKVStorage option.
	// Default: MemoryStorage.
	DefaultStorage StorageType

	// DefaultReplicas is the replication factor used when AddStream or KVCreate
	// do not specify an explicit WithReplicas/WithKVReplicas option.
	// Set to 1 for single-server deployments.
	// Default: 1.
	DefaultReplicas int

	// --- Optional Logger ---

	// Logger receives connection, health-check, and error events.
	// Set to nil to suppress all internal logging.
	Logger Logger
}

// Logger is the logging interface accepted by nats.Client.
// It is intentionally identical to the redis.Logger interface so that the same
// adapter (e.g. the sibling logger package) can be used without import cycles.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	// Async variants are used inside goroutines (health-check, reconnect handlers)
	// to avoid blocking the caller while the log entry is enqueued.
	InfoAsync(msg string, args ...any)
	WarnAsync(msg string, args ...any)
	ErrorAsync(msg string, args ...any)
}

// DefaultConfig returns a Config pre-populated with sensible production defaults.
func DefaultConfig() Config {
	return Config{
		URLs:           []string{"nats://localhost:4222"},
		ConnectTimeout: 5 * time.Second,
		ReconnectWait:  2 * time.Second,
		MaxReconnects:  60,
		MaxConnRetries: 3,
		DefaultStorage: MemoryStorage,
		DefaultReplicas: 1,
	}
}
