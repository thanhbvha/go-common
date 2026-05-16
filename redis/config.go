// Package redis provides a production-ready Redis client wrapper built on top
// of github.com/redis/go-redis/v9. It supports three deployment topologies
// (standalone, Cluster, Sentinel), manages a connection pool, and runs a
// background health-check goroutine.
//
// A process-wide default client can be registered with SetDefault and
// retrieved with Default, enabling zero-argument usage across packages
// without exposing a hidden global singleton.
//
// Basic usage:
//
//	cfg := redis.DefaultConfig()
//	cfg.Host = "redis.example.com"
//	cfg.Prefix = "myapp:"
//
//	client := redis.New(cfg)
//	if err := client.Connect(context.Background()); err != nil {
//	    log.Fatal(err)
//	}
//	redis.SetDefault(client)
//	defer redis.Close()
package redis

import "time"

// ConnectionMode defines the Redis topology used when creating a client.
type ConnectionMode int

const (
	// ModeSingle connects to a single Redis server (default).
	ModeSingle ConnectionMode = iota
	// ModeCluster connects to a Redis Cluster via multiple seed addresses.
	ModeCluster
	// ModeSentinel connects via Redis Sentinel for automatic failover.
	ModeSentinel
)

// Config holds all options needed to create and connect a Redis Client.
// Use DefaultConfig to obtain a value pre-filled with sensible defaults,
// then override only the fields you need.
type Config struct {
	// --- Connection ---

	// Host is the Redis server hostname or IP (ModeSingle only). Default: "localhost".
	Host string
	// Port is the Redis server port (ModeSingle only). Default: 6379.
	Port int
	// Password is the AUTH password. Leave empty for no authentication.
	Password string
	// DB is the database index (ModeSingle and ModeSentinel only). Default: 0.
	DB int
	// Prefix is prepended to every key created with BuildKey. Default: "".
	Prefix string

	// --- Topology ---

	// Mode selects the connection topology. Default: ModeSingle.
	Mode ConnectionMode
	// ClusterAddrs is the list of seed addresses for ModeCluster.
	// Example: []string{"host1:6379", "host2:6379"}.
	ClusterAddrs []string
	// SentinelAddrs is the list of sentinel addresses for ModeSentinel.
	// Example: []string{"sentinel1:26379"}.
	SentinelAddrs []string
	// MasterName is the name of the monitored master for ModeSentinel.
	// Default: "mymaster".
	MasterName string

	// --- Connection Pool ---

	// PoolSize is the maximum number of socket connections. Default: 10.
	PoolSize int
	// MinIdleConns is the minimum number of idle connections to keep. Default: 2.
	MinIdleConns int
	// MaxIdleConns is the maximum number of idle connections. Default: 10.
	MaxIdleConns int
	// ConnMaxIdleTime is the maximum amount of time a connection may be idle.
	// Default: 5 minutes.
	ConnMaxIdleTime time.Duration
	// ConnMaxLifetime is the maximum lifetime of a connection. Default: 30 minutes.
	ConnMaxLifetime time.Duration

	// --- Timeouts ---

	// DialTimeout is the timeout for establishing new connections. Default: 5s.
	DialTimeout time.Duration
	// ReadTimeout is the timeout for socket reads. Default: 3s.
	ReadTimeout time.Duration
	// WriteTimeout is the timeout for socket writes. Default: 3s.
	WriteTimeout time.Duration

	// --- Retry ---

	// MaxRetries is the maximum number of retries before giving up. Default: 3.
	MaxRetries int
	// MinRetryBackoff is the minimum back-off between retries. Default: 100ms.
	MinRetryBackoff time.Duration
	// MaxRetryBackoff is the maximum back-off between retries. Default: 1s.
	MaxRetryBackoff time.Duration
	// MaxConnRetries is the number of Ping attempts during Connect. Default: 3.
	MaxConnRetries int

	// --- Optional Logger ---

	// Logger receives connection and health-check events.
	// Set to nil to suppress all internal logging.
	Logger Logger
}

// Logger is the logging interface accepted by redis.Client.
// It is intentionally minimal so that any structured logger
// (including the sibling logger package) can be adapted without import cycles.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// DefaultConfig returns a Config pre-populated with sensible production defaults.
func DefaultConfig() Config {
	return Config{
		Host:            "localhost",
		Port:            6379,
		DB:              0,
		Prefix:          "",
		Mode:            ModeSingle,
		MasterName:      "mymaster",
		PoolSize:        10,
		MinIdleConns:    2,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 5 * time.Minute,
		ConnMaxLifetime: 30 * time.Minute,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     3 * time.Second,
		WriteTimeout:    3 * time.Second,
		MaxRetries:      3,
		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 1000 * time.Millisecond,
		MaxConnRetries:  3,
	}
}
