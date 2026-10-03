package orm

import (
	"time"

	"gorm.io/gorm"
)

// Config holds the configuration for connecting to the database
type Config struct {
	// Dialector defines the specific database driver (e.g., postgres.Open, mysql.Open, sqlite.Open).
	Dialector gorm.Dialector

	// Connection Pool Settings
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`

	// Print SQL logs (for debugging)
	Debug bool `mapstructure:"debug"`

	// Telemetry (OTel)
	EnableTelemetry bool `mapstructure:"enable_telemetry"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() Config {
	return Config{
		MaxIdleConns:    10,
		MaxOpenConns:    100,
		ConnMaxLifetime: time.Hour,
		Debug:           false,
	}
}
