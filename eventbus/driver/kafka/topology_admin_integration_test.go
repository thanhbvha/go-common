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

func TestKafkaTopologyReconcileExpansionAndDLQLeaseIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	secondCreated, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	second := secondCreated.(*kafkaTransport)
	topo := transport.Topology{Namespace: "p675", JobType: fmt.Sprintf("topology_%d", time.Now().UnixNano()), Options: map[string]any{
		"kafka.partitions": int32(1), "kafka.replication_factor": int16(1),
		"kafka.retention_ms": 2 * time.Hour,
	}}
	topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
	topics = append(topics, backend.dlqAdminLockTopic(topo))
	t.Cleanup(func() {
		_ = second.Close(context.Background())
		_ = backend.Close(context.Background())
		client, clientErr := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
		if clientErr == nil {
			defer client.Close()
			_, _ = kadm.NewClient(client).DeleteTopics(context.Background(), topics...)
		}
	})
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	configs, err := backend.adm.DescribeTopicConfigs(ctx, backend.mainTopic(topo))
	if err != nil || len(configs) != 1 {
		t.Fatalf("configs = %#v, %v", configs, err)
	}
	foundRetention := false
	for _, config := range configs[0].Configs {
		if config.Key == "retention.ms" && config.Value != nil && *config.Value == fmt.Sprint((2*time.Hour).Milliseconds()) {
			foundRetention = true
		}
	}
	if !foundRetention {
		t.Fatal("retention.ms was not reconciled")
	}
	result, err := backend.ExpandPartitions(ctx, topo, 2)
	if err != nil || len(result.Topics) != len(topics)-1 {
		t.Fatalf("expansion = %#v, %v", result, err)
	}
	lease, err := backend.acquireDLQAdminLease(ctx, topo)
	if err != nil {
		t.Fatal(err)
	}
	blockedCtx, blockedCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer blockedCancel()
	if _, err := second.acquireDLQAdminLease(blockedCtx, topo); !errors.Is(err, errKafkaDLQAdminBusy) {
		lease.release()
		t.Fatalf("competing lease error = %v", err)
	}
	lease.release()
	acquireCtx, acquireCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer acquireCancel()
	next, err := second.acquireDLQAdminLease(acquireCtx, topo)
	if err != nil {
		t.Fatal(err)
	}
	next.release()
}

func TestKafkaEventPartitionExpansionIncludesTargetedAndPrivateTopics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	topo := transport.Topology{Namespace: "p675", JobType: fmt.Sprintf("event_expand_%d", time.Now().UnixNano()), Kind: transport.EventTopology, Options: map[string]any{kafkaEventLayoutOption: kafkaTargetedLayoutV1}}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha"}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	topics := append([]string{backend.mainTopic(topo), backend.dlqTopic(topo)}, backend.delayTopics(topo)...)
	topics = append(topics, backend.targetedDelayTopics(topo)...)
	topics = append(topics, backend.privateReadyTopic(topo, sub.Group), backend.subscriberMarkerTopic(topo, sub.Group), backend.eventLayoutTopic(topo), backend.dlqAdminLockTopic(topo))
	t.Cleanup(func() {
		_ = backend.Close(context.Background())
		cleanup, cleanupErr := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
		if cleanupErr == nil {
			defer cleanup.Close()
			_, _ = kadm.NewClient(cleanup).DeleteTopics(context.Background(), topics...)
		}
	})
	result, err := backend.ExpandPartitions(ctx, topo, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Topics) != len(topics)-3 {
		t.Fatalf("expanded topics = %d, want %d: %#v", len(result.Topics), len(topics)-3, result.Topics)
	}
	for _, topic := range topics[:len(topics)-3] {
		if !result.Topics[topic] {
			t.Fatalf("topic %q was not expanded", topic)
		}
	}
}

func TestKafkaEnsureEventTopologyCreatesSinglePartitionControlTopics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	topology := transport.Topology{
		Namespace: "p675",
		JobType:   fmt.Sprintf("event_provision_%d", time.Now().UnixNano()),
		Kind:      transport.EventTopology,
		Options: map[string]any{
			"kafka.partitions":     int32(4),
			"kafka.delay_disabled": true,
			kafkaEventLayoutOption: kafkaTargetedLayoutV1,
		},
	}
	dataTopics := []string{backend.mainTopic(topology), backend.dlqTopic(topology)}
	controlTopics := []string{backend.eventLayoutTopic(topology), backend.dlqAdminLockTopic(topology)}
	t.Cleanup(func() {
		_ = backend.Close(context.Background())
		cleanup, cleanupErr := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
		if cleanupErr == nil {
			defer cleanup.Close()
			_, _ = kadm.NewClient(cleanup).DeleteTopics(context.Background(), append(dataTopics, controlTopics...)...)
		}
	})
	if err := backend.EnsureTopology(ctx, topology); err != nil {
		t.Fatal(err)
	}
	metadata, err := backend.adm.Metadata(ctx, append(dataTopics, controlTopics...)...)
	if err != nil {
		t.Fatal(err)
	}
	for _, topic := range dataTopics {
		if got := len(metadata.Topics[topic].Partitions); got != 4 {
			t.Fatalf("data topic %q partitions = %d, want 4", topic, got)
		}
	}
	for _, topic := range controlTopics {
		if got := len(metadata.Topics[topic].Partitions); got != 1 {
			t.Fatalf("control topic %q partitions = %d, want 1", topic, got)
		}
	}
}
