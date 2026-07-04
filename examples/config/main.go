package main

import (
	"fmt"
	"os"

	"github.com/thanhbvha/go-common/config"
)

type ServerConfig struct {
	Port int    `mapstructure:"port"`
	Host string `mapstructure:"host"`
}

type DatabaseConfig struct {
	DSN      string `mapstructure:"dsn"`
	PoolSize int    `mapstructure:"pool_size"`
}

type AppConfig struct {
	Name     string         `mapstructure:"name"`
	Mode     string         `mapstructure:"mode"`
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
}

func main() {
	// Create a dummy config file for the example
	createDummyConfig()
	defer os.Remove("example_config.yaml")

	// Set an environment variable to demonstrate override
	// Since EnvPrefix is "APP", this corresponds to the "Server.Port" field
	os.Setenv("APP_SERVER_PORT", "9999")
	defer os.Unsetenv("APP_SERVER_PORT")

	opts := config.DefaultOptions()
	opts.Path = "example_config.yaml"
	opts.EnvPrefix = "APP"

	var cfg AppConfig
	if err := config.Load(opts, &cfg); err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return
	}

	fmt.Println("=== Loaded Configuration ===")
	fmt.Printf("App Name: %s\n", cfg.Name)
	fmt.Printf("Mode: %s\n", cfg.Mode)
	fmt.Printf("Server Host: %s\n", cfg.Server.Host)
	fmt.Printf("Server Port: %d (Overridden by ENV APP_SERVER_PORT)\n", cfg.Server.Port)
	fmt.Printf("Database DSN: %s\n", cfg.Database.DSN)
	fmt.Printf("Database Pool Size: %d\n", cfg.Database.PoolSize)
}

func createDummyConfig() {
	content := []byte(`
name: "GoCommonExample"
mode: "development"
server:
  host: "127.0.0.1"
  port: 8080
database:
  dsn: "postgres://user:pass@localhost:5432/db"
  pool_size: 10
`)
	_ = os.WriteFile("example_config.yaml", content, 0644)
}
