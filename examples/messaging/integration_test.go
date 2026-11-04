//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	dbmongodb "github.com/thanhbvha/go-common/v2/db/mongodb"
)

// Load real fixture endpoints from a temporary YAML file with environment
// overrides disabled. This exercises file-only configuration against each DB
// and broker; the script also tests the CLI's file + environment precedence.
func fileFixtureConfig(t *testing.T) exampleConfig {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"postgres": map[string]string{"dsn": os.Getenv("ORM_POSTGRES_DSN")},
		"mysql":    map[string]string{"dsn": os.Getenv("ORM_MYSQL_DSN")},
		"mongodb":  map[string]string{"uri": os.Getenv("MONGODB_INTEGRATION_URI")},
		"redis":    map[string]string{"addr": os.Getenv("EVENTBUS_REDIS_ADDR")},
		"kafka":    map[string]any{"brokers": strings.Split(os.Getenv("EVENTBUS_KAFKA_BROKERS"), ",")},
	}) // JSON is valid YAML; marshaling preserves credentials/URI escaping.
	if err != nil {
		t.Fatal("cannot encode fixture configuration")
	}
	cfg, err := loadExampleConfig(writeExampleConfig(t, string(data)), func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// This runs the example itself, separately from the driver's contract suite.
func TestMessagingExample(t *testing.T) {
	if os.Getenv("ORM_POSTGRES_DSN") == "" {
		if os.Getenv("MESSAGING_INTEGRATION_REQUIRED") == "1" {
			t.Fatal("ORM_POSTGRES_DSN required")
		}
		t.Skip("set disposable PostgreSQL fixture address")
	}
	for _, broker := range []string{"redis", "nats", "kafka"} {
		t.Run(broker, func(t *testing.T) {
			if err := envExampleConfig(t).validateBroker(broker); err != nil {
				if os.Getenv("MESSAGING_BROKERS_REQUIRED") == "1" {
					t.Fatal(err)
				}
				t.Skip("set explicit Redis/Kafka fixture address")
			}
			var output bytes.Buffer
			if err := runBroker(t.Context(), fileFixtureConfig(t), broker, &output); err != nil {
				t.Fatalf("%s example failed: %v", broker, err)
			}
			text := output.String()
			for _, want := range []string{"broker=" + broker, "rollback=verified", "claim attempts=3", "publish calls=2", "business effects=1", "inbox replay=duplicate"} {
				if !strings.Contains(text, want) {
					t.Fatalf("missing assertion %q: %s", want, text)
				}
			}
			t.Log(strings.TrimSpace(text))
		})
	}
}

func TestMessagingExampleMongoDB(t *testing.T) {
	if os.Getenv("MONGODB_INTEGRATION_URI") == "" {
		if os.Getenv("MESSAGING_MONGODB_REQUIRED") == "1" {
			t.Fatal("MONGODB_INTEGRATION_URI required")
		}
		t.Skip("set disposable MongoDB replica-set URI")
	}
	brokers := []string{"nats"}
	if os.Getenv("MESSAGING_BROKERS_REQUIRED") == "1" {
		brokers = []string{"redis", "nats", "kafka"}
	}
	for _, broker := range brokers {
		t.Run(broker, func(t *testing.T) {
			if err := envExampleConfig(t).validateBroker(broker); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := runSelectedBroker(t.Context(), fileFixtureConfig(t), "mongodb", broker, &output); err != nil {
				t.Fatalf("MongoDB %s example failed: %v", broker, err)
			}
			text := output.String()
			for _, want := range []string{"driver=mongodb", "broker=" + broker, "rollback=verified", "claim attempts=3", "publish calls=2", "business effects=1", "inbox replay=duplicate", "identity conflict=verified", "native callback attempts=2", "retry business effects=1", "cross-database transaction=verified", "payload int64=verified"} {
				if !strings.Contains(text, want) {
					t.Fatalf("missing assertion %q: %s", want, text)
				}
			}
			t.Log(strings.TrimSpace(text))
		})
	}
}

func TestMessagingExampleMongoDBStandaloneRejected(t *testing.T) {
	uri := os.Getenv("MONGODB_STANDALONE_URI")
	if uri == "" {
		return
	} // Only the replica profile includes this negative fixture.
	t.Setenv("MONGODB_INTEGRATION_URI", uri)
	var output bytes.Buffer
	if err := runSelectedBroker(t.Context(), fileFixtureConfig(t), "mongodb", "nats", &output); !errors.Is(err, dbmongodb.ErrTransactionsUnsupported) {
		t.Fatal("standalone MongoDB accepted", err)
	}
}

func TestMessagingExampleMySQL(t *testing.T) {
	if os.Getenv("ORM_MYSQL_DSN") == "" {
		if os.Getenv("MESSAGING_MYSQL_REQUIRED") == "1" {
			t.Fatal("ORM_MYSQL_DSN required")
		}
		t.Skip("set disposable MySQL 8.4 fixture address")
	}
	brokers := []string{"nats"}
	if os.Getenv("MESSAGING_BROKERS_REQUIRED") == "1" {
		brokers = []string{"redis", "nats", "kafka"}
	}
	for _, broker := range brokers {
		t.Run(broker, func(t *testing.T) {
			cfg := fileFixtureConfig(t)
			if err := cfg.validateBroker(broker); err != nil {
				t.Fatal(err)
			}
			var output bytes.Buffer
			if err := runSelectedBroker(t.Context(), cfg, "mysql", broker, &output); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"driver=mysql", "broker=" + broker, "rollback=verified", "claim attempts=3", "publish calls=2", "business effects=1", "inbox replay=duplicate"} {
				if !strings.Contains(output.String(), want) {
					t.Fatalf("missing %q: %s", want, output.String())
				}
			}
			t.Log(strings.TrimSpace(output.String()))
		})
	}
}
