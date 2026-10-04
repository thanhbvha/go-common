// Package main demonstrates basic usage of the github.com/thanhbvha/go-common/queue_kafka package.
// Run with: go run ./examples/queue_kafka/
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
	queue_kafka "github.com/thanhbvha/go-common/queue_kafka"
	"github.com/thanhbvha/go-common/queue_kafka/registry"
)

// EmailPayload is an example job payload for sending email.
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// NotifyPayload is an example job payload for push notifications.
type NotifyPayload struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

// =====================================================================
// 1. TASK REGISTRATION (via init)
// =====================================================================

func init() {
	registry.Register("send-email", queue_kafka.JobTypeOptions{
		Concurrency: 4,
		MaxRetry:    5,
	}, handleSendEmail)

	registry.Register("push-notify", queue_kafka.JobTypeOptions{
		Concurrency: 8,
		MaxRetry:    3,
	}, handlePushNotify)
}

func handleSendEmail(job queue_kafka.Job) error {
	// In real code, unmarshal job.Data into your payload type, e.g.:
	//   var payload EmailPayload
	//   json.Unmarshal(mustJSON(job.Data), &payload)
	log.Printf("send-email: processing job_id=%s created_at=%s payload=%+v",
		job.ID, job.CreatedAt.Format(time.RFC3339), job.Data)

	// Simulate occasional failures to demonstrate retry behavior.
	if job.Retry < 2 {
		return fmt.Errorf("send-email: simulated transient error (retry %d)", job.Retry)
	}
	log.Printf("send-email: email sent successfully job_id=%s", job.ID)
	return nil
}

func handlePushNotify(job queue_kafka.Job) error {
	log.Printf("push-notify: processing job_id=%s payload=%+v", job.ID, job.Data)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize logger
	l := logger.New(logger.DefaultOptions())
	logger.SetDefault(l)
	defer logger.Close()

	// ---- 1. Set up the Kafka client ----
	kafkaCfg := kafka.DefaultConfig()
	kafkaCfg.Brokers = []string{"localhost:9092"}
	kafkaCfg.ClientID = "queue-kafka-example"
	kafkaCfg.ConsumerGroup = "queue-workers"
	kafkaCfg.Logger = l

	kc := kafka.MustConnect(ctx, kafkaCfg)
	kafka.SetDefault(kc)
	defer func() { _ = kafka.Close() }()

	// ---- 2. Create the queue ----
	qCfg := queue_kafka.DefaultConfig()
	qCfg.TopicPrefix = "myapp"
	qCfg.DefaultMaxRetry = 3
	qCfg.Logger = l

	q := queue_kafka.New(kc, qCfg)

	// ---- 3. Autoload Tasks ----
	// Apply all tasks registered via init() into our specific Queue instance.
	registry.ApplyToQueue(q)

	// ---- 4. Start the queue ----
	q.Start(ctx)
	defer q.Stop()
	log.Println("queue_kafka: all workers started")

	// ---- 6. Enqueue jobs ----

	// Immediate job
	if err := q.Enqueue(ctx, "send-email", EmailPayload{
		To:      "alice@example.com",
		Subject: "Welcome",
		Body:    "Hello from go-common queue_kafka!",
	}); err != nil {
		log.Fatalf("queue_kafka: Enqueue failed: %v", err)
	}

	// Delayed job — delivered after 30 seconds
	if err := q.EnqueueDelayed(ctx, "send-email", EmailPayload{
		To:      "bob@example.com",
		Subject: "Reminder",
		Body:    "This was delayed by 30 seconds.",
	}, 30*time.Second); err != nil {
		log.Printf("queue_kafka: EnqueueDelayed error: %v", err)
	}

	// Unique job — deduplicated by key within the producer session
	if err := q.EnqueueUnique(ctx, "push-notify", "notify-user-42-login",
		NotifyPayload{UserID: "42", Message: "You are now logged in"},
		1*time.Hour,
	); err != nil {
		log.Printf("queue_kafka: EnqueueUnique error: %v", err)
	}

	// Job with explicit partition key (all events for user-42 go to the same partition)
	if err := q.EnqueueWithKey(ctx, "push-notify", "user-42",
		NotifyPayload{UserID: "42", Message: "Your order has shipped"},
	); err != nil {
		log.Printf("queue_kafka: EnqueueWithKey error: %v", err)
	}

	log.Println("queue_kafka: all jobs enqueued — waiting for shutdown signal")

	// ---- 7. Wait for interrupt ----
	<-ctx.Done()
	log.Println("queue_kafka: shutting down example")
}
