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

var errKafkaContractResponseLoss = errors.New("synthetic response loss after durable Kafka produce")

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
		PrepareUnsettledRedelivery: func(*testing.T, transport.Transport, transport.Topology) {},
		PostCommitAmbiguity: func(t *testing.T, tr transport.Transport, topo transport.Topology, kind transport.DecisionKind, phase transportcontract.AmbiguityPhase) transportcontract.AmbiguityState {
			backend := tr.(*kafkaContractTransport).kafkaTransport
			switch phase {
			case transportcontract.AmbiguityArm:
				original := backend.producer
				var once sync.Once
				backend.producer = settlementProducer(func(ctx context.Context, record *kgo.Record) error {
					if err := original.ProduceSync(ctx, record).FirstErr(); err != nil {
						return err
					}
					injected := false
					once.Do(func() { injected = true })
					if injected {
						return errKafkaContractResponseLoss
					}
					return nil
				})
				return transportcontract.AmbiguityState{}
			case transportcontract.AmbiguityObserve:
				backend.producer = backend.cl
				return kafkaAmbiguityState(t, backend, topo, kind, 2)
			case transportcontract.AmbiguityFinal:
				return kafkaAmbiguityState(t, backend, topo, kind, 0)
			default:
				t.Fatalf("unknown ambiguity phase %d", phase)
				return transportcontract.AmbiguityState{}
			}
		},
		SchedulerOwnershipRecovery: func(t *testing.T, first, second transport.Transport, topo transport.Topology, now time.Time) int {
			firstBackend := first.(*kafkaContractTransport).kafkaTransport
			secondBackend := second.(*kafkaContractTransport).kafkaTransport
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var tier time.Duration
			var topic, group string
			for _, candidate := range delayTiers {
				candidateTopic := firstBackend.delayTopic(topo, candidate)
				candidateGroup := fmt.Sprintf("%s-delay-%s-group", topo.Name(), candidate)
				lag, err := firstBackend.groupLag(ctx, candidateTopic, candidateGroup)
				if err != nil {
					t.Fatal(err)
				}
				if lag > 0 {
					tier, topic, group = candidate, candidateTopic, candidateGroup
					break
				}
			}
			if topic == "" {
				t.Fatal("Kafka delayed source was not found in any tier")
			}
			owner, err := firstBackend.newManualConsumer(
				kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				t.Fatal(err)
			}
			fetched := owner.PollRecords(ctx, 1)
			if err := fetched.Err(); err != nil || len(fetched.Records()) != 1 {
				owner.CloseAllowingRebalance()
				t.Fatalf("Kafka owner poll = %d, %v", len(fetched.Records()), err)
			}
			// Owner exits without transfer or commit. The group successor must
			// receive the durable source from the same committed offset.
			owner.CloseAllowingRebalance()
			successor, err := secondBackend.newManualConsumer(
				kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer successor.CloseAllowingRebalance()
			fetched = successor.PollRecords(ctx, 1)
			if err := fetched.Err(); err != nil || len(fetched.Records()) != 1 {
				t.Fatalf("Kafka successor poll = %d, %v", len(fetched.Records()), err)
			}
			if err := secondBackend.processDelayBatch(ctx, topo, tier, successor, fetched.Records(), now); err != nil {
				t.Fatal(err)
			}
			return 1
		},
		PendingIncludesInFlight:  true,
		RedeliveryMayKeepAttempt: true,
		SubscriberRetryIsolation: true,
		SubscriberDLQIsolation:   true,
		ReceiveWithin:            10 * time.Second,
	})
}

func kafkaAmbiguityState(t *testing.T, backend *kafkaTransport, topo transport.Topology, kind transport.DecisionKind, expected int64) transportcontract.AmbiguityState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stats, err := backend.Stats(ctx, topo)
	if err != nil {
		t.Fatal(err)
	}
	destination := backend.dlqTopic(topo)
	if kind == transport.DecisionRetry {
		destination = backend.delayDestination(topo, time.Now().Add(time.Minute), time.Now())
	}
	ends, err := backend.adm.ListEndOffsets(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	var count int64
	for _, partition := range ends[destination] {
		if partition.Err != nil {
			t.Fatal(partition.Err)
		}
		count += partition.Offset
	}
	state := transportcontract.AmbiguityState{SourceOutstanding: stats.Pending > 0 || stats.InFlight > 0, DestinationCount: count}
	if expected > 0 {
		state.ExpectedFinalDestinations = expected
	}
	return state
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
		RepeatedReconnect:          true,
		ReconnectSessionLifetime:   250 * time.Millisecond,
		ReceiveWithin:              15 * time.Second,
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
