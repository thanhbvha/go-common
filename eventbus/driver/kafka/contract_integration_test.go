//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	buscontract "github.com/thanhbvha/go-common/eventbus/contracttest"
	"github.com/thanhbvha/go-common/eventbus/transport"
	transportcontract "github.com/thanhbvha/go-common/eventbus/transport/contracttest"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaTransportContract(t *testing.T) {
	cfg := commonkafka.DefaultConfig()
	probe, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = probe.Ping(probeCtx)
	cancel()
	_ = probe.Close(context.Background())
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	namespace := fmt.Sprintf("kafka_contract_%d", time.Now().UnixNano())
	transportcontract.Run(t, transportcontract.Harness{
		New: func(t *testing.T) transport.Transport {
			created, err := NewWithConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			wrapped := &kafkaContractTransport{kafkaTransport: created.(*kafkaTransport)}
			t.Cleanup(func() { wrapped.cleanup(t) })
			return wrapped
		},
		Topology: func(name string, kind transport.TopologyKind) transport.Topology {
			name = strings.NewReplacer("/", "_", " ", "_").Replace(name)
			options := map[string]any{
				"kafka.partitions":       int32(1),
				"kafka.fetch_max_wait":   25 * time.Millisecond,
				"kafka.delay_batch_size": 10,
			}
			if kind == transport.EventTopology {
				options[kafkaEventLayoutOption] = kafkaTargetedLayoutV1
			}
			return transport.Topology{
				Namespace: namespace, JobType: name, Kind: kind,
				Options: options,
			}
		},
		DispatchDue: func(ctx context.Context, tr transport.Transport, topo transport.Topology, now time.Time) (int64, error) {
			return dispatchKafkaContractDue(ctx, tr.(*kafkaContractTransport).kafkaTransport, topo, now)
		},
		PendingIncludesInFlight:  true,
		SubscriberRetryIsolation: true,
		SubscriberDLQIsolation:   true,
		ReceiveWithin:            10 * time.Second,
	})
}

func TestKafkaBusContract(t *testing.T) {
	cfg := commonkafka.DefaultConfig()
	probe, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = probe.Ping(probeCtx)
	cancel()
	_ = probe.Close(context.Background())
	if err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	namespace := fmt.Sprintf("kafka_bus_contract_%d", time.Now().UnixNano())
	buscontract.Run(t, buscontract.Harness{
		New: func(t *testing.T) transport.Transport {
			created, err := NewWithConfig(cfg)
			if err != nil {
				t.Fatal(err)
			}
			wrapped := &kafkaContractTransport{kafkaTransport: created.(*kafkaTransport)}
			t.Cleanup(func() { wrapped.cleanup(t) })
			return wrapped
		},
		Topology: func(name string, kind transport.TopologyKind) transport.Topology {
			name = strings.NewReplacer("/", "_", " ", "_").Replace(name)
			return transport.Topology{Namespace: namespace, JobType: name, Kind: kind, Options: map[string]any{
				"kafka.partitions": int32(1), "kafka.fetch_max_wait": 25 * time.Millisecond, "kafka.delay_batch_size": 10,
			}}
		},
		DispatchDue: func(ctx context.Context, tr transport.Transport, topo transport.Topology, now time.Time) (int64, error) {
			return dispatchKafkaContractDue(ctx, tr.(*kafkaContractTransport).kafkaTransport, topo, now)
		},
		PrepareUnsettledRedelivery: func(*testing.T, transport.Topology) {},
		DurableRedelivery:          true,
		ReceiveWithin:              10 * time.Second,
	})
}

type kafkaContractTransport struct {
	*kafkaTransport
	topologiesMu sync.Mutex
	topologies   []transport.Topology
}

func (t *kafkaContractTransport) EnsureTopology(ctx context.Context, topo transport.Topology) error {
	if err := t.kafkaTransport.EnsureTopology(ctx, topo); err != nil {
		return err
	}
	t.mu.Lock()
	handle := t.sweepers[topo.Name()]
	delete(t.sweepers, topo.Name())
	t.mu.Unlock()
	if handle != nil {
		handle.cancel()
		select {
		case <-handle.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	t.topologiesMu.Lock()
	t.topologies = append(t.topologies, topo)
	t.topologiesMu.Unlock()
	return nil
}

func (t *kafkaContractTransport) cleanup(testingT *testing.T) {
	testingT.Helper()
	t.topologiesMu.Lock()
	topologies := append([]transport.Topology(nil), t.topologies...)
	t.topologiesMu.Unlock()
	var topics []string
	for _, topo := range topologies {
		topics = append(topics, t.mainTopic(topo), t.dlqTopic(topo))
		topics = append(topics, t.delayTopics(topo)...)
		if topo.Kind == transport.EventTopology {
			topics = append(topics, t.targetedDelayTopics(topo)...)
			privateTopics, _ := t.registeredSubscriberTopics(context.Background(), topo)
			topics = append(topics, privateTopics...)
			topics = append(topics, t.eventLayoutTopic(topo))
		}
		topics = append(topics, t.dlqAdminLockTopic(topo))
	}
	if err := t.Close(context.Background()); err != nil {
		testingT.Errorf("close Kafka contract transport: %v", err)
	}
	if len(topics) > 0 {
		topics = uniqueStrings(topics)
		client, err := t.factory.New()
		if err != nil {
			testingT.Errorf("create Kafka contract cleanup client: %v", err)
			return
		}
		defer client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		responses, err := kadm.NewClient(client).DeleteTopics(ctx, topics...)
		cancel()
		if err != nil {
			testingT.Errorf("delete Kafka contract topics: %v", err)
		} else {
			for _, response := range responses {
				if response.Err != nil && !errors.Is(response.Err, kerr.UnknownTopicOrPartition) {
					testingT.Errorf("delete Kafka contract topic %q: %v", response.Topic, response.Err)
				}
			}
		}
	}
}

func dispatchKafkaContractDue(ctx context.Context, backend *kafkaTransport, topo transport.Topology, now time.Time) (int64, error) {
	var moved int64
	for _, tier := range delayTiers {
		workers := []struct {
			topic, group string
			targeted     bool
		}{{backend.delayTopic(topo, tier), fmt.Sprintf("%s-delay-%s-group", topo.Name(), tier), false}}
		if topo.Kind == transport.EventTopology {
			workers = append(workers, struct {
				topic, group string
				targeted     bool
			}{backend.targetedDelayTopic(topo, tier), fmt.Sprintf("%s-target-delay-%s-group", topo.Name(), tier), true})
		}
		for _, worker := range workers {
			topic, group := worker.topic, worker.group
			lag, err := backend.groupLag(ctx, topic, group)
			if err != nil {
				return moved, err
			}
			if lag == 0 {
				continue
			}
			client, err := backend.newManualConsumer(
				kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				return moved, err
			}
			pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			fetches := client.PollRecords(pollCtx, delayBatchSize(topo))
			cancel()
			if err := fetches.Err(); err != nil {
				client.Close()
				return moved, err
			}
			records := fetches.Records()
			for _, record := range records {
				runAt, err := delayedRecordRunAt(record)
				if err != nil {
					client.Close()
					return moved, err
				}
				if backend.delayDestination(topo, runAt, now) == backend.mainTopic(topo) {
					moved++
				}
			}
			err = backend.processDelayBatchFor(ctx, topo, tier, worker.targeted, client, records, now)
			client.Close()
			if err != nil {
				return moved, err
			}
		}
	}
	return moved, nil
}
