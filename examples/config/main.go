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

// AppConfig defines the structured layout of the application configuration.
type AppConfig struct {
	Name     string         `mapstructure:"name"`
	Mode     string         `mapstructure:"mode"`
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
}

func main() {
	fmt.Println("=== Config Module Examples ===")
	fmt.Println("Uncomment one of the functions below to run a specific example.")

	RunYamlConfigExample()
	// RunEnvConfigExample()
}

// =====================================================================
// 1. YAML Config with Environment Variable Overrides
// =====================================================================
func RunYamlConfigExample() {
	fmt.Println("\n--- 1. Testing YAML Config (with ENV Overrides) ---")
	
	// Create a dummy config file for the example
	createDummyYamlConfig()
	defer os.Remove("example_config.yaml")

	// Set an environment variable to demonstrate override.
	// Since EnvPrefix is "APP", this corresponds to the "Server.Port" field
	os.Setenv("APP_SERVER_PORT", "9999")
	defer os.Unsetenv("APP_SERVER_PORT")

	opts := config.DefaultOptions()
	opts.Path = "example_config.yaml"
	opts.EnvPrefix = "APP"

	var cfg AppConfig
	// CRITICAL: config.Load reads the YAML file first, but ENV variables 
	// (prefixed with EnvPrefix) will ALWAYS override the file values.
	// This is the standard 12-Factor App methodology.
	if err := config.Load(opts, &cfg); err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		return
	}

	fmt.Println("Loaded Configuration:")
	fmt.Printf("- App Name: %s\n", cfg.Name)
	fmt.Printf("- Server Port: %d (Overridden by ENV APP_SERVER_PORT)\n", cfg.Server.Port)
	fmt.Printf("- Database DSN: %s\n", cfg.Database.DSN)
}

// =====================================================================
// 2. Load directly from a .env file
// =====================================================================
func RunEnvConfigExample() {
	fmt.Println("\n--- 2. Testing .env Config ---")
	
	// Create a dummy .env file
	createDummyDotEnv()
	defer os.Remove(".env.example")

	var cfg AppConfig
	
	// LoadEnv will automatically read the .env file and map it to the struct fields.
	// It internally ignores "file not found" errors so it's safe for production environments
	// where the .env file might be absent (and values are provided purely via system ENV).
	if err := config.LoadEnv(".env.example", &cfg); err != nil {
		fmt.Printf("Failed to load .env config: %v\n", err)
		return
	}

	fmt.Println("Loaded Configuration:")
	fmt.Printf("- App Name: %s\n", cfg.Name)
	fmt.Printf("- Mode: %s\n", cfg.Mode)
	fmt.Printf("- Server Port: %d\n", cfg.Server.Port)
}

// ---------------- Helpers to generate files for demo ----------------

func createDummyYamlConfig() {
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

func createDummyDotEnv() {
	content := []byte(`
NAME=DotEnvExample
MODE=production
SERVER_PORT=3000
SERVER_HOST=0.0.0.0
DATABASE_DSN=mysql://root:pass@localhost/db
DATABASE_POOL_SIZE=50
`)
	_ = os.WriteFile(".env.example", content, 0644)
}
