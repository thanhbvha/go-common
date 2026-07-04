package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/thanhbvha/go-common/config"
)

type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Password string `mapstructure:"password"`
}

type AppConfig struct {
	Name     string         `mapstructure:"name"`
	Database DatabaseConfig `mapstructure:"database"`
}

func TestLoadYAML(t *testing.T) {
	// Create a temp config file
	content := []byte(`
name: "MyApp"
database:
  host: "localhost"
  port: 5432
  password: "secret_yaml"
`)
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	opts := config.DefaultOptions()
	opts.Path = filePath

	var cfg AppConfig
	if err := config.Load(opts, &cfg); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "MyApp" {
		t.Errorf("expected Name='MyApp', got '%s'", cfg.Name)
	}
	if cfg.Database.Host != "localhost" {
		t.Errorf("expected Database.Host='localhost', got '%s'", cfg.Database.Host)
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("expected Database.Port=5432, got %d", cfg.Database.Port)
	}
	if cfg.Database.Password != "secret_yaml" {
		t.Errorf("expected Database.Password='secret_yaml', got '%s'", cfg.Database.Password)
	}
}

func TestLoadWithEnvOverride(t *testing.T) {
	// Setup env
	os.Setenv("TESTAPP_NAME", "EnvApp")
	os.Setenv("TESTAPP_DATABASE_PORT", "9999")
	defer os.Unsetenv("TESTAPP_NAME")
	defer os.Unsetenv("TESTAPP_DATABASE_PORT")

	// Create a temp config file
	content := []byte(`
name: "MyApp"
database:
  host: "localhost"
  port: 5432
`)
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(filePath, content, 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	opts := config.DefaultOptions()
	opts.Path = filePath
	opts.EnvPrefix = "TESTAPP"

	var cfg AppConfig
	if err := config.Load(opts, &cfg); err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Should be overridden by env
	if cfg.Name != "EnvApp" {
		t.Errorf("expected Name='EnvApp', got '%s'", cfg.Name)
	}
	if cfg.Database.Port != 9999 {
		t.Errorf("expected Database.Port=9999, got %d", cfg.Database.Port)
	}
	// Should remain from yaml
	if cfg.Database.Host != "localhost" {
		t.Errorf("expected Database.Host='localhost', got '%s'", cfg.Database.Host)
	}
}

func TestLoadEnvMissingFile(t *testing.T) {
	var cfg AppConfig
	// File doesn't exist, but it should not fail because IgnoreFileNotFound = true by default in LoadEnv
	err := config.LoadEnv("non_existent.env", &cfg)
	if err != nil {
		t.Fatalf("LoadEnv failed when file is missing: %v", err)
	}
}
