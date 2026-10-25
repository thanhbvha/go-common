//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

func TestKafkaDLQRequeueOwnershipLossDoesNotCommitAfterSuccessor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	backend, successor, topo := kafkaDLQOwnershipFixture(t, ctx, "requeue_loss")
	appendKafkaDLQRecords(t, ctx, backend, topo, "zero", "one")
	topic := backend.dlqTopic(topo)
	backend.scanDLQ = func(context.Context, string, int32, int64, int64, int) ([]*kgo.Record, error) {
		return []*kgo.Record{kafkaAdminRecord(t, topic, 0, 0, "zero"), kafkaAdminRecord(t, topic, 0, 1, "one")}, nil
	}

	leaseReady := make(chan *dlqAdminLease, 1)
	backend.dlqLease = func(ctx context.Context, topology transport.Topology) (*dlqAdminLease, error) {
		lease, err := backend.acquireBrokerDLQAdminLease(ctx, topology)
		if err == nil {
			leaseReady <- lease
		}
		return lease, err
	}
	resume := make(chan struct{})
	lost := make(chan struct{})
	released := make(chan struct{})
	var once sync.Once
	backend.producer = settlementProducer(func(produceCtx context.Context, _ *kgo.Record) error {
		once.Do(func() {
			lease := <-leaseReady
			lease.lose()
			<-lease.ctx.Done()
			close(lost)
			<-lease.closed
			close(released)
			<-resume
		})
		return nil
	})

	staleDone := make(chan error, 1)
	go func() { staleDone <- backend.Requeue(ctx, topo, []string{"0:0", "0:1"}) }()
	select {
	case <-lost:
	case err := <-staleDone:
		t.Fatalf("requeue ended before ownership loss: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-released:
	case <-ctx.Done():
		t.Fatal("timed out waiting for stale lease to leave the consumer group")
	}
	appendKafkaDLQRecords(t, ctx, successor, topo, "three")
	if err := successor.Purge(ctx, topo); err != nil {
		t.Fatal(err)
	}
	close(resume)
	if err := <-staleDone; !errors.Is(err, errKafkaDLQAdminLeaseLost) {
		t.Fatalf("stale requeue error = %v, want lease loss", err)
	}
	if count, err := successor.Count(ctx, topo); err != nil || count != 0 {
		t.Fatalf("DLQ count after successor purge = %d, %v", count, err)
	}
}

func TestKafkaDLQPurgeOwnershipLossDoesNotCommitAfterSuccessor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	backend, successor, topo := kafkaDLQOwnershipFixture(t, ctx, "purge_loss")
	appendKafkaDLQRecords(t, ctx, backend, topo, "zero", "one")

	leaseReady := make(chan *dlqAdminLease, 1)
	backend.dlqLease = func(ctx context.Context, topology transport.Topology) (*dlqAdminLease, error) {
		lease, err := backend.acquireBrokerDLQAdminLease(ctx, topology)
		if err == nil {
			leaseReady <- lease
		}
		return lease, err
	}
	resume := make(chan struct{})
	lost := make(chan struct{})
	backend.adm = &leaseLossKafkaAdmin{
		kafkaAdmin: backend.adm,
		afterListEnd: func() {
			lease := <-leaseReady
			lease.lose()
			select {
			case <-lease.ctx.Done():
				close(lost)
			case <-ctx.Done():
			}
			<-resume
		},
	}

	staleDone := make(chan error, 1)
	go func() { staleDone <- backend.Purge(ctx, topo) }()
	select {
	case <-lost:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	appendKafkaDLQRecords(t, ctx, successor, topo, "three")
	if err := successor.Purge(ctx, topo); err != nil {
		t.Fatal(err)
	}
	close(resume)
	if err := <-staleDone; !errors.Is(err, errKafkaDLQAdminLeaseLost) {
		t.Fatalf("stale purge error = %v, want lease loss", err)
	}
	if count, err := successor.Count(ctx, topo); err != nil || count != 0 {
		t.Fatalf("DLQ count after successor purge = %d, %v", count, err)
	}
}

type leaseLossKafkaAdmin struct {
	kafkaAdmin
	once         sync.Once
	afterListEnd func()
}

func (a *leaseLossKafkaAdmin) ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error) {
	result, err := a.kafkaAdmin.ListEndOffsets(ctx, topics...)
	if err == nil {
		a.once.Do(a.afterListEnd)
	}
	return result, err
}

func kafkaDLQOwnershipFixture(t *testing.T, ctx context.Context, name string) (*kafkaTransport, *kafkaTransport, transport.Topology) {
	t.Helper()
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
	secondCreated, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	successor := secondCreated.(*kafkaTransport)
	topo := transport.Topology{Namespace: "p57", JobType: fmt.Sprintf("%s_%d", name, time.Now().UnixNano())}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
	topics = append(topics, backend.dlqAdminLockTopic(topo))
	t.Cleanup(func() {
		_ = successor.Close(context.Background())
		_ = backend.Close(context.Background())
		cleanup, cleanupErr := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
		if cleanupErr == nil {
			defer cleanup.Close()
			_, _ = kadm.NewClient(cleanup).DeleteTopics(context.Background(), topics...)
		}
	})
	return backend, successor, topo
}

func appendKafkaDLQRecords(t *testing.T, ctx context.Context, backend *kafkaTransport, topo transport.Topology, payloads ...string) {
	t.Helper()
	topic := backend.dlqTopic(topo)
	for _, payload := range payloads {
		record := kafkaAdminRecord(t, topic, 0, 0, payload)
		record.Offset = 0
		if err := backend.produceConfirmed(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
}

func TestKafkaEventStatsUseDurableSubscriberRegistryAfterGroupDeletion(t *testing.T) {
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
	topo := transport.Topology{
		Namespace: "p57", JobType: fmt.Sprintf("event_stats_%d", time.Now().UnixNano()), Kind: transport.EventTopology,
		Options: map[string]any{kafkaEventLayoutOption: kafkaTargetedLayoutV1},
	}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: "offline"}); err != nil {
		t.Fatal(err)
	}
	topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
	topics = append(topics, backend.targetedDelayTopics(topo)...)
	topics = append(topics, backend.privateReadyTopic(topo, "offline"), backend.subscriberMarkerTopic(topo, "offline"), backend.eventLayoutTopic(topo), backend.dlqAdminLockTopic(topo))
	defer func() {
		_ = backend.Close(context.Background())
		cleanup, cleanupErr := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
		if cleanupErr == nil {
			defer cleanup.Close()
			_, _ = kadm.NewClient(cleanup).DeleteTopics(context.Background(), topics...)
		}
	}()

	message := transport.Message{ID: "offline-event", Type: topo.JobType, Payload: []byte("payload")}
	if err := backend.Publish(ctx, topo, message); err != nil {
		t.Fatal(err)
	}
	group := kafkaConsumerGroup(topo, "offline")
	offsets := make(kadm.Offsets)
	offsets.AddOffset(backend.mainTopic(topo), 0, 0, -1)
	committed, err := backend.adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatal(err)
	}
	if err := committed.Error(); err != nil {
		t.Fatal(err)
	}
	adminClient := kadm.NewClient(backend.cl)
	responses, err := adminClient.DeleteGroups(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if response, ok := responses[group]; !ok || response.Err != nil {
		t.Fatalf("delete group response = %#v", response)
	}

	stats, err := backend.Stats(ctx, topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != 1 || stats.Lag != 1 {
		t.Fatalf("offline registered subscriber stats = %#v", stats)
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
