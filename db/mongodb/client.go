package mongodb

import (
	"context"
	"errors"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/v2/mongo/otelmongo"
)

// New initializes a new MongoDB client connection.
func New(ctx context.Context, cfg Config) (*mongo.Client, error) {
	opts, err := clientOptions(cfg)
	if err != nil {
		return nil, err
	}
	return connectAndPing(ctx, cfg, opts)
}

// opts must already have passed clientOptions validation.
func connectAndPing(ctx context.Context, cfg Config, opts *options.ClientOptions) (*mongo.Client, error) {
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, err
	}
	pingTimeout := cfg.PingTimeout
	if pingTimeout == 0 {
		pingTimeout = DefaultConfig().PingTimeout
	}
	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		return nil, errors.Join(err, disconnectClient(client))
	}
	if cfg.RequireTransactions {
		capabilities, err := DetectCapabilities(pingCtx, client)
		if err != nil {
			return nil, errors.Join(err, disconnectClient(client))
		}
		if !capabilities.Transactions {
			return nil, errors.Join(ErrTransactionsUnsupported, disconnectClient(client))
		}
	}
	return client, nil
}

// clientOptions validates without dialing. Explicit nonzero fields override URI
// settings; zero fields preserve URI settings or receive bounded defaults.
func clientOptions(cfg Config) (*options.ClientOptions, error) {
	if strings.TrimSpace(cfg.URI) == "" {
		return nil, &ConfigError{Field: "uri", Code: "required"}
	}
	for field, value := range map[string]time.Duration{"connect_timeout": cfg.ConnectTimeout, "ping_timeout": cfg.PingTimeout, "operation_timeout": cfg.OperationTimeout, "transaction_timeout": cfg.TransactionTimeout} {
		if value < 0 {
			return nil, &ConfigError{Field: field, Code: "negative"}
		}
	}
	opts := options.Client().ApplyURI(cfg.URI)
	defaults := DefaultConfig()
	if cfg.MaxPoolSize != 0 {
		opts.SetMaxPoolSize(cfg.MaxPoolSize)
	} else if opts.MaxPoolSize == nil {
		opts.SetMaxPoolSize(defaults.MaxPoolSize)
	}
	if cfg.MinPoolSize != 0 {
		opts.SetMinPoolSize(cfg.MinPoolSize)
	}
	if cfg.ConnectTimeout != 0 {
		opts.SetConnectTimeout(cfg.ConnectTimeout)
	} else if opts.ConnectTimeout == nil {
		opts.SetConnectTimeout(defaults.ConnectTimeout)
	}
	if cfg.OperationTimeout != 0 {
		opts.SetTimeout(cfg.OperationTimeout)
	} else if opts.Timeout == nil {
		opts.SetTimeout(defaults.OperationTimeout)
	}
	if cfg.AppName != "" {
		opts.SetAppName(cfg.AppName)
	}
	if cfg.PoolMonitor != nil {
		opts.SetPoolMonitor(cfg.PoolMonitor)
	}
	if cfg.EnableTelemetry {
		opts.SetMonitor(otelmongo.NewMonitor(
			otelmongo.WithCommandAttributeDisabled(true),
		))
	}
	if err := opts.Validate(); err != nil {
		// URI parser errors can contain credentials. Never return the raw error.
		if opts.MinPoolSize != nil && opts.MaxPoolSize != nil && *opts.MaxPoolSize != 0 && *opts.MinPoolSize > *opts.MaxPoolSize {
			return nil, &ConfigError{Field: "min_pool_size", Code: "exceeds_max_pool_size"}
		}
		return nil, &ConfigError{Field: "uri_or_client_options", Code: "invalid"}
	}
	return opts, nil
}

// Cleanup must remain usable even when the startup/request context is canceled.
func disconnectClient(client *mongo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.Disconnect(ctx)
}

// GetDatabase returns a reference to the configured database
func GetDatabase(client *mongo.Client, dbName string) *mongo.Database {
	return client.Database(dbName)
}
