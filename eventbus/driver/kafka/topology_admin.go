package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
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
	if err := requireTargetedEventLayout(topology); err != nil {
		return result, err
	}
	if target <= 0 {
		return result, fmt.Errorf("kafka: partition target must be positive")
	}
	topics := []string{t.mainTopic(topology), t.dlqTopic(topology)}
	if topologyDelayEnabled(topology) {
		topics = append(topics, t.delayTopics(topology)...)
	}
	if topology.Kind == transport.EventTopology {
		if topologyDelayEnabled(topology) {
			topics = append(topics, t.targetedDelayTopics(topology)...)
		}
		privateTopics, err := t.registeredPrivateTopics(ctx, topology)
		if err != nil {
			return result, err
		}
		topics = append(topics, privateTopics...)
	}
	topics = uniqueStrings(topics)
	metadata, err := t.adm.Metadata(ctx, topics...)
	if err != nil {
		return result, fmt.Errorf("kafka: inspect related partitions: %w", err)
	}
	for _, topic := range topics {
		detail, ok := metadata.Topics[topic]
		if !ok {
			return result, fmt.Errorf("kafka: inspect related topic %q: missing broker metadata", topic)
		}
		if detail.Err != nil {
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

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

type dlqAdminLease struct {
	client *kgo.Client
	ctx    context.Context
	cancel context.CancelCauseFunc

	mu          sync.Mutex
	generation  uint64
	owned       bool
	closeOnce   sync.Once
	releaseOnce sync.Once
	done        chan struct{}
	closed      chan struct{}
}

func newDLQAdminLease(ctx context.Context, client *kgo.Client) *dlqAdminLease {
	leaseCtx, cancel := context.WithCancelCause(ctx)
	return &dlqAdminLease{
		client: client, ctx: leaseCtx, cancel: cancel,
		done: make(chan struct{}), closed: make(chan struct{}),
	}
}

func newLocalDLQAdminLease(ctx context.Context) *dlqAdminLease {
	lease := newDLQAdminLease(ctx, nil)
	lease.assign()
	close(lease.done)
	close(lease.closed)
	return lease
}

func (l *dlqAdminLease) assign() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.generation++
	l.owned = true
	return l.generation
}

func (l *dlqAdminLease) lose() {
	l.loseWithCause(errKafkaDLQAdminLeaseLost)
	if l.client != nil {
		// Closing from a partition callback can deadlock the group loop. Start
		// shutdown after the callback returns so a fenced member cannot rejoin.
		go l.guardedCloseClient()
	}
}

func (l *dlqAdminLease) loseWithCause(cause error) {
	l.mu.Lock()
	if l.owned {
		l.generation++
		l.owned = false
	}
	l.mu.Unlock()
	l.cancel(cause)
}

func (l *dlqAdminLease) operation() (context.Context, uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.owned {
		return nil, 0, errKafkaDLQAdminLeaseLost
	}
	return l.ctx, l.generation, nil
}

func (l *dlqAdminLease) check(generation uint64) error {
	if cause := context.Cause(l.ctx); errors.Is(cause, errKafkaDLQAdminLeaseLost) || errors.Is(cause, transport.ErrRecoveredPanic) {
		return cause
	}
	if err := l.ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.owned || l.generation != generation {
		return errKafkaDLQAdminLeaseLost
	}
	return nil
}

func (l *dlqAdminLease) monitor() {
	for {
		// LeaveGroupContext does not wake a parked poll. Bound each poll so a
		// queued revoke/lost callback is serviced without waiting for records.
		pollCtx, cancel := context.WithTimeout(l.ctx, 250*time.Millisecond)
		fetches := l.client.PollRecords(pollCtx, 1)
		cancel()
		if fetches.IsClientClosed() {
			l.lose()
			return
		}
		if err := fetches.Err(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			l.lose()
			return
		}
		if l.ctx.Err() != nil {
			return
		}
	}
}

func (l *dlqAdminLease) release() {
	l.releaseOnce.Do(func() {
		l.cancel(context.Canceled)
		if l.client == nil {
			return
		}
		l.guardedCloseClient()
		<-l.done
	})
}

func (l *dlqAdminLease) guardedCloseClient() {
	transport.Guard(func() { l.loseWithCause(transport.ErrRecoveredPanic) }, l.closeClient)
}

func (l *dlqAdminLease) closeClient() {
	l.closeOnce.Do(func() {
		defer close(l.closed)
		l.client.CloseAllowingRebalance()
	})
}

func (l *dlqAdminLease) startMonitor() {
	go func() {
		defer close(l.done)
		transport.Guard(func() {
			l.loseWithCause(transport.ErrRecoveredPanic)
			l.closeClient()
		}, l.monitor)
	}()
}

var (
	errKafkaDLQAdminBusy      = errors.New("kafka: DLQ administration is already active")
	errKafkaDLQAdminLeaseLost = errors.New("kafka: DLQ administration lease lost")
)

func (t *kafkaTransport) acquireDLQAdminLease(ctx context.Context, topology transport.Topology) (*dlqAdminLease, error) {
	if t.dlqLease != nil {
		return t.dlqLease(ctx, topology)
	}
	return t.acquireBrokerDLQAdminLease(ctx, topology)
}

func (t *kafkaTransport) acquireBrokerDLQAdminLease(ctx context.Context, topology transport.Topology) (*dlqAdminLease, error) {
	// Unit tests construct the private transport directly without a native
	// factory. Supported constructors always provide one and therefore always
	// use the broker-backed lease below.
	if t.factory == nil {
		return newLocalDLQAdminLease(ctx), nil
	}
	assigned := make(chan bool, 1)
	topic := t.dlqAdminLockTopic(topology)
	lease := newDLQAdminLease(ctx, nil)
	client, err := t.newManualConsumer(
		kgo.ConsumerGroup(t.dlqAdminGroup(topology)),
		kgo.ConsumeTopics(topic),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			owns := len(partitions[topic]) == 1
			if owns {
				lease.assign()
			}
			select {
			case assigned <- owns:
			default:
			}
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			if len(partitions[topic]) > 0 {
				lease.lose()
			}
			select {
			case assigned <- false:
			default:
			}
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			if len(partitions[topic]) > 0 {
				lease.lose()
			}
			select {
			case assigned <- false:
			default:
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: create DLQ admin lease consumer: %w", err)
	}
	lease.client = client
	for {
		select {
		case owns := <-assigned:
			if owns {
				lease.startMonitor()
				return lease, nil
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
				lease.startMonitor()
				return lease, nil
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
