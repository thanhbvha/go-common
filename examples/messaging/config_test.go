package main

import (
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func envExampleConfig(t *testing.T) exampleConfig {
	t.Helper()
	cfg, err := loadExampleConfig("", os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func writeExampleConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestConfigFileAndEnvironmentPrecedence(t *testing.T) {
	path := writeExampleConfig(t, `driver: all
broker: all
postgres:
  dsn: file-postgres
mysql:
  dsn: file-mysql
mongodb:
  uri: file-mongo
redis:
  addr: file-redis
  username: user
  password: secret
  db: 2
kafka:
  brokers: [file-kafka-1, file-kafka-2]
`)
	env := map[string]string{}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	cfg, err := loadExampleConfig(path, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.MySQL.DSN != "file-mysql" || cfg.Postgres.DSN != "file-postgres" || cfg.MongoDB.URI != "file-mongo" || cfg.Redis.Addr != "file-redis" || cfg.Redis.Username != "user" || cfg.Redis.Password != "secret" || cfg.Redis.DB != 2 || len(cfg.Kafka.Brokers) != 2 {
		t.Fatal("YAML connection settings not decoded")
	}
	env["ORM_POSTGRES_DSN"] = "env-postgres"
	env["ORM_MYSQL_DSN"] = "env-mysql"
	env["MONGODB_INTEGRATION_URI"] = "mongodb://user:pass@host/?tls=true&replicaSet=rs0"
	env["EVENTBUS_REDIS_ADDR"] = "env-redis"
	env["EVENTBUS_KAFKA_BROKERS"] = " kafka-1 , kafka-2 "
	cfg, err = loadExampleConfig(path, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.MySQL.DSN != "env-mysql" || cfg.Postgres.DSN != env["ORM_POSTGRES_DSN"] || cfg.MongoDB.URI != env["MONGODB_INTEGRATION_URI"] || cfg.Redis.Addr != "env-redis" || len(cfg.Kafka.Brokers) != 2 || strings.TrimSpace(cfg.Kafka.Brokers[1]) != "kafka-2" {
		t.Fatal("environment did not override file")
	}
	// An explicitly empty override disables the endpoint instead of falling back
	// to local YAML and silently connecting to another database.
	env["ORM_POSTGRES_DSN"] = ""
	cfg, err = loadExampleConfig(path, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.validate() == nil {
		t.Fatal("empty explicit endpoint accepted")
	}
}

func TestConfigCLIOverridesAndValidation(t *testing.T) {
	for _, name := range []string{"ORM_POSTGRES_DSN", "ORM_MYSQL_DSN", "MONGODB_INTEGRATION_URI", "EVENTBUS_REDIS_ADDR", "EVENTBUS_KAFKA_BROKERS"} {
		t.Setenv(name, "")
	}
	// Only the selected combination needs endpoints. Environment wins over file.
	t.Setenv("MONGODB_INTEGRATION_URI", "fixture-mongo")
	path := writeExampleConfig(t, "driver: postgres\nbroker: kafka\n")
	cfg, err := parseConfig([]string{"-config", path, "-driver", "mongodb", "-broker", "nats"}, io.Discard)
	if err != nil || cfg.Driver != "mongodb" || cfg.Broker != "nats" {
		t.Fatal("CLI overrides failed", err)
	}
	if _, err := parseConfig([]string{"-config", path}, io.Discard); err == nil {
		t.Fatal("missing selected endpoints accepted")
	}
	for _, args := range [][]string{
		{"-config", path, "-driver", "unknown"},
		{"-config", path, "-driver", ""},
		{"-config", path, "extra"},
	} {
		if _, err := parseConfig(args, io.Discard); err == nil {
			t.Fatal("invalid CLI accepted")
		}
	}
	if _, err := parseConfig([]string{"-help"}, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Fatal("help should not load configuration", err)
	}
	if _, err := parseConfig([]string{"-config", "", "-driver", "mongodb"}, io.Discard); err != nil {
		t.Fatal("env-only mode failed", err)
	}
}

func TestConfigErrorsDoNotLeakValues(t *testing.T) {
	lookup := func(string) (string, bool) { return "", false }
	for _, path := range []string{
		filepath.Join(t.TempDir(), "missing.yaml"),
		writeExampleConfig(t, "redis:\n  db: private-credential\n"),
		writeExampleConfig(t, "postgres: [private-credential"),
	} {
		_, err := loadExampleConfig(path, lookup)
		if err == nil || strings.Contains(err.Error(), "private-credential") {
			t.Fatal("missing/invalid YAML not safely rejected")
		}
	}
}

func TestConfigSelectedBrokerValidation(t *testing.T) {
	cfg := exampleConfig{Driver: "postgres", Broker: "all"}
	cfg.Postgres.DSN = "fixture-postgres"
	cfg.Redis.Addr = "fixture-redis"
	for _, brokers := range [][]string{nil, {""}, {"kafka-1", " "}} {
		cfg.Kafka.Brokers = brokers
		if cfg.validate() == nil {
			t.Fatal("incomplete all-broker config accepted")
		}
	}
	cfg.Kafka.Brokers = []string{"fixture-kafka"}
	cfg.Redis.DB = -1
	if cfg.validate() == nil {
		t.Fatal("negative Redis database accepted")
	}
	cfg.Redis.DB = 0
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBundledConfigAndDefaultPaths(t *testing.T) {
	lookup := func(string) (string, bool) { return "", false }
	cfg, err := loadExampleConfig("config.yaml", lookup)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Driver, cfg.Broker = "all", "all"
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if got := defaultConfigPath(); got != "config.yaml" {
		t.Fatal(got)
	}
	t.Chdir("../..")
	if got := defaultConfigPath(); got != "examples/messaging/config.yaml" {
		t.Fatal(got)
	}
}
