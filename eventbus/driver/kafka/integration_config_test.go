//go:build integration

package kafka

import (
	"fmt"
	"os"
	"strings"
	"testing"

	commonkafka "github.com/thanhbvha/go-common/v2/kafka"
)

// Required CI must supply the address of its disposable fixture. Local optional
// runs retain the historical default, without substituting it for a missing CI fixture.
func kafkaFixtureBrokers() ([]string, error) {
	raw := os.Getenv("EVENTBUS_KAFKA_BROKERS")
	if raw == "" {
		if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
			return nil, fmt.Errorf("EVENTBUS_KAFKA_BROKERS is required")
		}
		raw = "localhost:9092"
	}
	brokers := strings.Split(raw, ",")
	for i, address := range brokers {
		brokers[i] = strings.TrimSpace(address)
		if brokers[i] == "" {
			return nil, fmt.Errorf("EVENTBUS_KAFKA_BROKERS contains an empty address")
		}
	}
	return brokers, nil
}
func kafkaIntegrationBrokers(t *testing.T) []string {
	t.Helper()
	brokers, err := kafkaFixtureBrokers()
	if err != nil {
		t.Fatal(err)
	}
	return brokers
}
func kafkaIntegrationConfig(t *testing.T) commonkafka.Config {
	t.Helper()
	cfg := commonkafka.DefaultConfig()
	cfg.Brokers = kafkaIntegrationBrokers(t)
	return cfg
}

func kafkaFixtureUnavailable(t *testing.T, err error) {
	t.Helper()
	if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
		t.Fatalf("required Kafka fixture unavailable during test: %v", err)
	}
	t.Skipf("Kafka not available: %v", err)
}
