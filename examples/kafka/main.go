// Package main demonstrates basic usage of the github.com/thanhbvha/go-common/kafka package.
// Run with: go run ./examples/kafka/
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/thanhbvha/go-common/kafka"
	"github.com/thanhbvha/go-common/logger"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ---- 1. Configure the client ----
	cfg := kafka.DefaultConfig()
	cfg.Brokers = []string{"localhost:9092"}
	cfg.ClientID = "kafka-example"

	// For SASL+TLS production setup, uncomment and configure:
	// cfg.SecurityProtocol = kafka.SASLSSL
	// cfg.SASLMechanism    = kafka.SASLSCRAMSHA256
	// cfg.SASLUsername     = os.Getenv("KAFKA_USER")
	// cfg.SASLPassword     = os.Getenv("KAFKA_PASSWORD")
	// cfg.ConsumerGroup    = "my-consumer-group"

	// Initialize logger
	l := logger.New(logger.DefaultOptions())
	logger.SetDefault(l)
	defer logger.Close()

	cfg.ConsumerGroup = "example-group"
	cfg.Logger = l

	// ---- 2. Connect ----
	client := kafka.MustConnect(ctx, cfg)
	kafka.SetDefault(client)
	defer func() {
		if err := kafka.Close(); err != nil {
			log.Printf("kafka: close error: %v", err)
		}
	}()

	// ---- 3. Ensure the example topic exists ----
	const topic = "example.events"
	if err := client.EnsureTopic(ctx, topic, 4, 1, map[string]string{
		"retention.ms": fmt.Sprintf("%d", 7*24*60*60*1000), // 7 days
	}); err != nil {
		log.Fatalf("kafka: EnsureTopic failed: %v", err)
	}

	// ---- 4. Produce ----
	type UserEvent struct {
		UserID string    `json:"user_id"`
		Action string    `json:"action"`
		At     time.Time `json:"at"`
	}

	result, err := client.ProduceJSON(ctx, topic, "user-42", UserEvent{
		UserID: "42",
		Action: "login",
		At:     time.Now(),
	})
	if err != nil {
		log.Fatalf("kafka: ProduceJSON failed: %v", err)
	}
	log.Printf("kafka: produced to partition=%d offset=%d", result.Partition, result.Offset)

	// ProduceBatch example
	batch := []kafka.Message{
		{Topic: topic, Key: []byte("user-10"), Value: []byte(`{"user_id":"10","action":"view"}`)},
		{Topic: topic, Key: []byte("user-11"), Value: []byte(`{"user_id":"11","action":"click"}`)},
	}
	if _, err := client.ProduceBatch(ctx, batch); err != nil {
		log.Printf("kafka: ProduceBatch warning: %v", err)
	}

	// ProduceAsync (fire-and-forget with callback)
	client.ProduceAsync(kafka.Message{
		Topic: topic,
		Key:   []byte("user-99"),
		Value: []byte(`{"user_id":"99","action":"logout"}`),
	}, func(r kafka.ProduceResult, err error) {
		if err != nil {
			log.Printf("kafka: async produce error: %v", err)
			return
		}
		log.Printf("kafka: async produced offset=%d", r.Offset)
	})

	// ---- 5. Consume ----
	go func() {
		log.Printf("kafka: starting consumer for topic %q", topic)
		if err := client.Subscribe(ctx, []string{topic}, func(ctx context.Context, r kafka.ConsumeRecord) error {
			log.Printf("kafka: received topic=%s partition=%d offset=%d key=%s value=%s",
				r.Topic, r.Partition, r.Offset, r.Key, r.Value)
			return nil
		}); err != nil {
			log.Printf("kafka: Subscribe stopped: %v", err)
		}
	}()

	// ---- 6. Admin operations ----
	topics, err := client.ListTopics(ctx)
	if err != nil {
		log.Printf("kafka: ListTopics error: %v", err)
	} else {
		log.Printf("kafka: visible topics: %v", topics)
	}

	info, err := client.DescribeTopic(ctx, topic)
	if err != nil {
		log.Printf("kafka: DescribeTopic error: %v", err)
	} else {
		log.Printf("kafka: topic=%q partitions=%d replicas=%d",
			info.Name, info.Partitions, info.ReplicationFactor)
	}

	// ---- 7. Wait for interrupt ----
	<-ctx.Done()
	log.Println("kafka: shutting down example")
}
