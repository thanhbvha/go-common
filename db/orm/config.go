package orm

import (
	"fmt"
	"time"
)

// Config holds the configuration for connecting to the database
type Config struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	DBName   string `mapstructure:"dbname"`
	SSLMode  string `mapstructure:"sslmode"`
	TimeZone string `mapstructure:"timezone"`

	// Connection Pool Settings
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`

	// Print SQL logs (for debugging)
	Debug bool `mapstructure:"debug"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() Config {
	return Config{
		Host:            "127.0.0.1",
		Port:            5432,
		SSLMode:         "disable",
		TimeZone:        "Asia/Ho_Chi_Minh",
		MaxIdleConns:    10,
		MaxOpenConns:    100,
		ConnMaxLifetime: time.Hour,
		Debug:           false,
	}
}

// DSN returns the Data Source Name string for PostgreSQL
func (c Config) DSN() string {
	return fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=%s TimeZone=%s",
		c.Host, c.User, c.Password, c.DBName, c.Port, c.SSLMode, c.TimeZone)
}
