//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestKafkaSubscriptionFailureRedeliversFailedOffset(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprintf("batch=%v", batch), func(t *testing.T) {
			brokers := os.Getenv("WS_KAFKA_BROKERS")
			if brokers == "" {
				if os.Getenv("EVENTBUS_INTEGRATION_REQUIRED") == "1" {
					t.Fatal("WS_KAFKA_BROKERS required")
				}
				t.Skip("set WS_KAFKA_BROKERS for live Kafka wrapper tests")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cfg := DefaultConfig()
			cfg.Brokers = strings.Split(brokers, ",")
			cfg.ConsumerGroup = fmt.Sprintf("wrapper-group-%d", time.Now().UnixNano())
			cfg.MaxConnRetries = 1
			client := New(cfg)
			if err := client.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			topic := fmt.Sprintf("wrapper-failure-%d", time.Now().UnixNano())
			if err := client.CreateTopic(ctx, topic, 1, 1, nil); err != nil {
				t.Fatal(err)
			}
			defer client.DeleteTopic(context.Background(), topic)
			for _, value := range []string{"before", "failed", "after"} {
				if _, err := client.Produce(ctx, Message{Topic: topic, Key: []byte("same"), Value: []byte(value)}); err != nil {
					t.Fatal(err)
				}
			}
			failure := errors.New("handler failure")
			var seen []string
			var err error
			if batch {
				err = client.SubscribeBatch(ctx, []string{topic}, 1, func(_ context.Context, records []ConsumeRecord) error {
					seen = append(seen, string(records[0].Value))
					if string(records[0].Value) == "failed" {
						return failure
					}
					return nil
				})
			} else {
				err = client.Subscribe(ctx, []string{topic}, func(_ context.Context, record ConsumeRecord) error {
					seen = append(seen, string(record.Value))
					if string(record.Value) == "failed" {
						return failure
					}
					return nil
				})
			}
			if !errors.Is(err, failure) || len(seen) != 2 || seen[0] != "before" || seen[1] != "failed" {
				t.Fatalf("stop on failure: seen=%v err=%v", seen, err)
			}
			replay := errors.New("stop after observing replay")
			err = client.Subscribe(ctx, []string{topic}, func(_ context.Context, record ConsumeRecord) error {
				if string(record.Value) != "failed" {
					t.Errorf("restart got %q, want failed offset", record.Value)
				}
				return replay
			})
			if !errors.Is(err, replay) {
				t.Fatalf("replay error=%v", err)
			}
		})
	}
}
