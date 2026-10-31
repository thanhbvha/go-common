package mongodb

import (
	"time"

	"go.mongodb.org/mongo-driver/v2/event"
)

// Config holds the configuration for connecting to MongoDB
type Config struct {
	URI    string `mapstructure:"uri"`
	DBName string `mapstructure:"dbname"`
	// ClientGroup explicitly shares a client across aliases. Empty keeps a private client.
	// Configurations in a group must match exactly except for DBName.
	ClientGroup    string        `mapstructure:"client_group"`
	MaxPoolSize    uint64        `mapstructure:"max_pool_size"`
	MinPoolSize    uint64        `mapstructure:"min_pool_size"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
	PingTimeout    time.Duration `mapstructure:"ping_timeout"`
	// OperationTimeout is a client-level fallback. Request contexts should still have deadlines.
	OperationTimeout time.Duration `mapstructure:"operation_timeout"`
	// TransactionTimeout bounds the whole transaction when ctx has no deadline.
	TransactionTimeout time.Duration `mapstructure:"transaction_timeout"`
	// RequireTransactions fails startup if the deployment cannot run transactions.
	RequireTransactions bool   `mapstructure:"require_transactions"`
	AppName             string `mapstructure:"app_name"`
	EnableTelemetry     bool   `mapstructure:"enable_telemetry"`
	// PoolMonitor receives driver pool events; callbacks must be concurrency-safe and fast.
	PoolMonitor        *event.PoolMonitor  `mapstructure:"-"`
	TransactionMonitor *TransactionMonitor `mapstructure:"-"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() Config {
	return Config{
		URI:                "mongodb://localhost:27017",
		DBName:             "default_db",
		MaxPoolSize:        100,
		MinPoolSize:        0,
		ConnectTimeout:     10 * time.Second,
		PingTimeout:        5 * time.Second,
		OperationTimeout:   30 * time.Second,
		TransactionTimeout: 30 * time.Second,
	}
}
