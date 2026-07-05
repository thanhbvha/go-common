package mongodb

import "time"

// Config holds the configuration for connecting to MongoDB
type Config struct {
	URI            string        `mapstructure:"uri"`
	DBName         string        `mapstructure:"dbname"`
	MaxPoolSize    uint64        `mapstructure:"max_pool_size"`
	MinPoolSize    uint64        `mapstructure:"min_pool_size"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
	PingTimeout    time.Duration `mapstructure:"ping_timeout"`
	EnableTelemetry bool         `mapstructure:"enable_telemetry"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() Config {
	return Config{
		URI:            "mongodb://localhost:27017",
		DBName:         "default_db",
		MaxPoolSize:    100,
		MinPoolSize:    10,
		ConnectTimeout: 10 * time.Second,
		PingTimeout:    5 * time.Second,
	}
}
