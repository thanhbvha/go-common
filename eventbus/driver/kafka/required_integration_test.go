//go:build integration

package kafka

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
		brokers, err := kafkaFixtureBrokers()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err = client.Ping(ctx)
			cancel()
			client.Close()
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "required disposable Kafka fixture unavailable:", err)
			os.Exit(1)
		}
	}
	os.Exit(m.Run())
}
