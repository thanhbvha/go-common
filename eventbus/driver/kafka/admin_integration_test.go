//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaDLQAdminAndStatsIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cfg := commonkafka.DefaultConfig()
	created, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	backend := created.(*kafkaTransport)
	if err := backend.Ping(ctx); err != nil {
		_ = backend.Close(context.Background())
		t.Skipf("Kafka not available: %v", err)
	}
	topo := transport.Topology{Namespace: "p57", JobType: fmt.Sprintf("admin_%d", time.Now().UnixNano())}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
	topics = append(topics, backend.dlqAdminLockTopic(topo))
	defer func() {
		_ = backend.Close(context.Background())
		cleanup, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
		if err == nil {
			defer cleanup.Close()
			_, _ = kadm.NewClient(cleanup).DeleteTopics(context.Background(), topics...)
		}
	}()

	message := transport.Message{
		ID: "live-job", Type: topo.JobType, Payload: []byte("live-payload"),
		OrderingKey: "account-1", Headers: map[string]string{"correlation-id": "live-correlation"},
	}
	if err := backend.Publish(ctx, topo, message); err != nil {
		t.Fatal(err)
	}
	stats, err := backend.Stats(ctx, topo)
	if err != nil || stats.Pending != 1 || stats.Lag != 1 || !stats.LagKnown {
		t.Fatalf("published stats = %#v, %v", stats, err)
	}
	settleKafkaIntegrationDelivery(t, ctx, backend, topo, transport.Decision{
		Kind: transport.DecisionDeadLetter, Source: &message, Reason: "integration", Cause: errors.New("synthetic"), ErrorCode: "synthetic", Attempts: 2,
	})

	if count, err := backend.Count(ctx, topo); err != nil || count != 1 {
		t.Fatalf("DLQ count = %d, %v", count, err)
	}
	records, err := backend.List(ctx, topo, 0, 10)
	if err != nil || len(records) != 1 {
		t.Fatalf("DLQ list = %#v, %v", records, err)
	}
	record := records[0]
	if record.ID == "" || record.Message.ID != message.ID || record.Reason != "integration" || record.LastError != "synthetic" || record.Attempts != 2 || record.FailedAt.IsZero() || record.Source != backend.mainTopic(topo) || record.SourcePosition == "" {
		t.Fatalf("DLQ record = %#v", record)
	}
	stats, err = backend.Stats(ctx, topo)
	if err != nil || stats.Pending != 0 || stats.Delayed != 0 || stats.DLQ != 1 || stats.Lag != 0 {
		t.Fatalf("dead-letter stats = %#v, %v", stats, err)
	}
	if err := backend.Requeue(ctx, topo, []string{record.ID}); err != nil {
		t.Fatal(err)
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 0 {
		t.Fatalf("DLQ count after requeue = %d, %v", count, err)
	}
	stats, err = backend.Stats(ctx, topo)
	if err != nil || stats.Pending != 1 || stats.Lag != 1 || stats.DLQ != 0 {
		t.Fatalf("requeue stats = %#v, %v", stats, err)
	}
	settleKafkaIntegrationDelivery(t, ctx, backend, topo, transport.Decision{Kind: transport.DecisionAck})

	if err := backend.Publish(ctx, topo, message); err != nil {
		t.Fatal(err)
	}
	settleKafkaIntegrationDelivery(t, ctx, backend, topo, transport.Decision{Kind: transport.DecisionDeadLetter, Reason: "purge"})
	if err := backend.Purge(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 0 {
		t.Fatalf("DLQ count after purge = %d, %v", count, err)
	}
	if records, err := backend.List(ctx, topo, 0, 10); err != nil || len(records) != 0 {
		t.Fatalf("DLQ list after purge = %#v, %v", records, err)
	}
}

func settleKafkaIntegrationDelivery(t *testing.T, parent context.Context, backend *kafkaTransport, topo transport.Topology, decision transport.Decision) {
	t.Helper()
	consumeCtx, cancel := context.WithCancel(parent)
	defer cancel()
	deliveries := make(chan transport.Delivery, 1)
	done := make(chan error, 1)
	go func() {
		done <- backend.Consume(consumeCtx, transport.Subscription{Topology: topo, Concurrency: 1}, func(delivery transport.Delivery) {
			deliveries <- delivery
		})
	}()
	select {
	case delivery := <-deliveries:
		if err := delivery.Settle(parent, decision); err != nil {
			t.Fatal(err)
		}
		cancel()
	case err := <-done:
		t.Fatalf("consume ended before delivery: %v", err)
	case <-parent.Done():
		t.Fatal(parent.Err())
	}
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("consume shutdown: %v", err)
		}
	case <-parent.Done():
		t.Fatal(parent.Err())
	}
}
