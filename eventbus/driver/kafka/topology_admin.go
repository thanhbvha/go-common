package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kgo"
)

// PartitionExpansionResult reports every related topic that was expanded before
// an error. Kafka cannot roll partition creation back.
type PartitionExpansionResult struct {
	Previous int32
	Target   int32
	Topics   map[string]bool
}

type PartitionExpansionError struct {
	Result PartitionExpansionResult
	Topic  string
	Err    error
}

func (e *PartitionExpansionError) Error() string {
	return fmt.Sprintf("kafka: expand related topic %q: %v", e.Topic, e.Err)
}

func (e *PartitionExpansionError) Unwrap() error { return e.Err }

// ExpandPartitions explicitly increases main, DLQ, and delay-tier partition
// counts. Startup never calls this method because key-to-partition mapping can
// change and break ordering across the transition.
func (t *kafkaTransport) ExpandPartitions(ctx context.Context, topology transport.Topology, target int32) (PartitionExpansionResult, error) {
	result := PartitionExpansionResult{Target: target, Topics: make(map[string]bool)}
	if err := t.ValidateTopology(topology); err != nil {
		return result, err
	}
	if target <= 0 {
		return result, fmt.Errorf("kafka: partition target must be positive")
	}
	topics := append([]string{t.mainTopic(topology), t.dlqTopic(topology)}, t.delayTopics(topology)...)
	metadata, err := t.adm.Metadata(ctx, topics...)
	if err != nil {
		return result, fmt.Errorf("kafka: inspect related partitions: %w", err)
	}
	for _, topic := range topics {
		detail, ok := metadata.Topics[topic]
		if !ok || detail.Err != nil {
			return result, fmt.Errorf("kafka: inspect related topic %q: %w", topic, detail.Err)
		}
		count := int32(len(detail.Partitions))
		if result.Previous == 0 {
			result.Previous = count
		} else if count != result.Previous {
			return result, fmt.Errorf("kafka: related topic %q has %d partitions, want %d", topic, count, result.Previous)
		}
	}
	if target < result.Previous {
		return result, fmt.Errorf("kafka: partition count cannot decrease from %d to %d", result.Previous, target)
	}
	if target == result.Previous {
		for _, topic := range topics {
			result.Topics[topic] = true
		}
		return result, nil
	}
	sort.Strings(topics)
	add := int(target - result.Previous)
	for _, topic := range topics {
		responses, err := t.adm.CreatePartitions(ctx, add, topic)
		if err != nil {
			return result, &PartitionExpansionError{Result: result, Topic: topic, Err: err}
		}
		response, ok := responses[topic]
		if !ok {
			return result, &PartitionExpansionError{Result: result, Topic: topic, Err: errors.New("missing broker response")}
		}
		if response.Err != nil {
			return result, &PartitionExpansionError{Result: result, Topic: topic, Err: response.Err}
		}
		result.Topics[topic] = true
	}
	return result, nil
}

type dlqAdminLease struct{ client *kgo.Client }

func (l *dlqAdminLease) release() {
	if l.client != nil {
		l.client.CloseAllowingRebalance()
	}
}

var errKafkaDLQAdminBusy = errors.New("kafka: DLQ administration is already active")

func (t *kafkaTransport) acquireDLQAdminLease(ctx context.Context, topology transport.Topology) (*dlqAdminLease, error) {
	// Unit tests construct the private transport directly without a native
	// factory. Supported constructors always provide one and therefore always
	// use the broker-backed lease below.
	if t.factory == nil {
		return &dlqAdminLease{}, nil
	}
	assigned := make(chan bool, 1)
	topic := t.dlqAdminLockTopic(topology)
	client, err := t.newManualConsumer(
		kgo.ConsumerGroup(topology.Name()+"-dlq-admin-lease"),
		kgo.ConsumeTopics(topic),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			select {
			case assigned <- len(partitions[topic]) == 1:
			default:
			}
		}),
		kgo.OnPartitionsRevoked(func(context.Context, *kgo.Client, map[string][]int32) {
			select {
			case assigned <- false:
			default:
			}
		}),
		kgo.OnPartitionsLost(func(context.Context, *kgo.Client, map[string][]int32) {
			select {
			case assigned <- false:
			default:
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: create DLQ admin lease consumer: %w", err)
	}
	for {
		select {
		case owns := <-assigned:
			if owns {
				return &dlqAdminLease{client: client}, nil
			}
			client.CloseAllowingRebalance()
			return nil, errKafkaDLQAdminBusy
		default:
		}
		pollCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		fetches := client.PollRecords(pollCtx, 1)
		cancel()
		if fetches.IsClientClosed() {
			client.CloseAllowingRebalance()
			return nil, fmt.Errorf("kafka: DLQ admin lease consumer closed")
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			client.CloseAllowingRebalance()
			return nil, fmt.Errorf("kafka: acquire DLQ admin lease: %w", err)
		}
		select {
		case owns := <-assigned:
			if owns {
				return &dlqAdminLease{client: client}, nil
			}
			client.CloseAllowingRebalance()
			return nil, errKafkaDLQAdminBusy
		case <-ctx.Done():
			client.CloseAllowingRebalance()
			return nil, ctx.Err()
		default:
		}
	}
}
