package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	defaultConcurrency     = 10
	defaultPrefetch        = 20
	defaultDuplicateWindow = 2 * time.Minute
	retryLaneVersion       = "v2"
	subscriberLaneVersion  = 2
	subscriberLaneLease    = 30 * time.Second
	subscriberLanePoll     = 10 * time.Millisecond
)

type subscriberLaneState struct {
	Version       int    `json:"v"`
	State         string `json:"state"`
	Durable       string `json:"durable"`
	Generation    uint64 `json:"generation"`
	Owner         string `json:"owner,omitempty"`
	LeaseUntil    int64  `json:"lease_until,omitempty"`
	AckWaitNanos  int64  `json:"ack_wait_nanos,omitempty"`
	MaxAckPending int    `json:"max_ack_pending,omitempty"`
}

var (
	errSubscriberLaneUnavailable = errors.New("nats: subscriber lane unavailable")
	errSubscriberLaneLeaseLost   = errors.New("nats: subscriber lane lease lost")
)

type subscriberLaneLeaseHandle struct {
	t          *natsTransport
	kv         gonats.KeyValue
	key        string
	durable    string
	owner      string
	mu         sync.Mutex
	leaseUntil time.Time
	lostErr    error
	stop       chan struct{}
	done       chan struct{}
}

func (t *natsTransport) subscriberLaneLeaseDuration() time.Duration {
	if t.laneLeaseDuration > 0 {
		return t.laneLeaseDuration
	}
	return subscriberLaneLease
}

func (t *natsTransport) desiredStreamConfig(topo transport.Topology, name string) gonats.StreamConfig {
	var maxAge time.Duration
	if value, ok := topo.Options["nats.maxage"].(time.Duration); ok {
		maxAge = value
	}
	replicas := 1
	if value, ok := topo.Options["nats.replicas"].(int); ok {
		replicas = value
	}
	retention := gonats.LimitsPolicy
	if value, ok := topo.Options["nats.retention"].(gonats.RetentionPolicy); ok {
		retention = value
	}
	duplicates := defaultDuplicateWindow
	if value, ok := topo.Options["nats.duplicate_window"].(time.Duration); ok {
		duplicates = value
	}
	return gonats.StreamConfig{
		Name: name, Subjects: []string{name}, Storage: gonats.FileStorage,
		MaxAge: maxAge, Replicas: replicas, Retention: retention, Duplicates: duplicates,
	}
}

// reconcileStream creates a missing stream or updates only fields owned by
// EventBus. Unmanaged broker fields are preserved. JetStream rejects unsafe
// changes (for example storage transitions), and that error is returned.
func (t *natsTransport) reconcileStream(ctx context.Context, desired gonats.StreamConfig) error {
	return t.reconcileStreamWithLease(ctx, desired, nil)
}

func (t *natsTransport) reconcileStreamWithLease(ctx context.Context, desired gonats.StreamConfig, lease *subscriberLaneLeaseHandle) error {
	info, err := t.js.StreamInfo(desired.Name, gonats.Context(ctx))
	if err != nil {
		if !errors.Is(err, gonats.ErrStreamNotFound) {
			return fmt.Errorf("inspect stream %q: %w", desired.Name, err)
		}
		if lease != nil {
			if err := lease.checkOwnership(ctx); err != nil {
				return err
			}
		}
		if _, err := t.js.AddStream(&desired, gonats.Context(ctx)); err == nil {
			return nil
		} else if !errors.Is(err, gonats.ErrStreamNameAlreadyInUse) {
			return fmt.Errorf("create stream %q: %w", desired.Name, err)
		}
		// Another provisioner won the create race. Inspect its actual config
		// and reconcile it through the same managed-field path below.
		info, err = t.js.StreamInfo(desired.Name, gonats.Context(ctx))
		if err != nil {
			return fmt.Errorf("inspect concurrently created stream %q: %w", desired.Name, err)
		}
	}

	current := info.Config
	if slices.Equal(current.Subjects, desired.Subjects) &&
		current.Storage == desired.Storage && current.MaxAge == desired.MaxAge &&
		current.Replicas == desired.Replicas && current.Retention == desired.Retention &&
		current.Duplicates == desired.Duplicates {
		return nil
	}
	current.Subjects = slices.Clone(desired.Subjects)
	current.Storage = desired.Storage
	current.MaxAge = desired.MaxAge
	current.Replicas = desired.Replicas
	current.Retention = desired.Retention
	current.Duplicates = desired.Duplicates
	if lease != nil {
		if err := lease.checkOwnership(ctx); err != nil {
			return err
		}
	}
	if _, err := t.js.UpdateStream(&current, gonats.Context(ctx)); err != nil {
		return fmt.Errorf("update stream %q: %w", desired.Name, err)
	}
	return nil
}

func (t *natsTransport) consumerName(sub transport.Subscription) string {
	return sub.Topology.ConsumerNameWithSeparator(sub.Group, "_")
}

func (t *natsTransport) broadcastSubject(topo transport.Topology) string {
	return t.streamName(topo)
}

func (t *natsTransport) retryLaneID(topo transport.Topology, durable string) string {
	sum := sha256.Sum256([]byte(topo.Name() + "\x00" + durable))
	return hex.EncodeToString(sum[:16])
}

func (t *natsTransport) retryStreamName(topo transport.Topology, durable string) string {
	return "eventbus_retry_" + retryLaneVersionFor(topo) + "_" + t.retryLaneID(topo, durable)
}

func (t *natsTransport) retrySubject(topo transport.Topology, durable string) string {
	return "eventbus.retry." + retryLaneVersionFor(topo) + "." + t.retryLaneID(topo, durable)
}

func (t *natsTransport) retryConsumerName(topo transport.Topology, durable string) string {
	return "eventbus_retry_consumer_" + retryLaneVersionFor(topo) + "_" + t.retryLaneID(topo, durable)
}

func (t *natsTransport) subscriberLaneKey(topo transport.Topology, durable string) string {
	return "subscriber." + retryLaneVersionFor(topo) + "." + t.retryLaneID(topo, durable)
}

func retryLaneVersionFor(topo transport.Topology) string {
	if delayLayoutIndexed(topo) {
		return "v3"
	}
	return retryLaneVersion
}

func effectiveCapacity(sub transport.Subscription) (int, error) {
	concurrency := sub.Concurrency
	if concurrency < 0 || sub.Prefetch < 0 {
		return 0, fmt.Errorf("nats: invalid subscription capacity %d + %d", concurrency, sub.Prefetch)
	}
	if concurrency == 0 {
		concurrency = defaultConcurrency
		if value, ok := sub.Topology.Options["eventbus.concurrency"].(int); ok {
			concurrency = value
		}
	}
	prefetch := sub.Prefetch
	if prefetch == 0 {
		prefetch = defaultPrefetch
		if value, ok := sub.Topology.Options["eventbus.prefetch"].(int); ok {
			prefetch = value
		}
	}
	if concurrency <= 0 || prefetch <= 0 || concurrency > math.MaxInt-prefetch {
		return 0, fmt.Errorf("nats: invalid subscription capacity %d + %d", concurrency, prefetch)
	}
	return concurrency + prefetch, nil
}

func (t *natsTransport) desiredConsumerConfig(sub transport.Subscription) (gonats.ConsumerConfig, error) {
	ackWait := topologyAckWait(sub.Topology)
	maxAckPending, err := effectiveCapacity(sub)
	if err != nil {
		return gonats.ConsumerConfig{}, err
	}
	return gonats.ConsumerConfig{
		Durable: t.consumerName(sub), AckPolicy: gonats.AckExplicitPolicy,
		AckWait: ackWait, MaxAckPending: maxAckPending, FilterSubject: t.broadcastSubject(sub.Topology),
	}, nil
}

func (t *natsTransport) EnsureSubscription(ctx context.Context, sub transport.Subscription) error {
	if err := t.ValidateTopology(sub.Topology); err != nil {
		return err
	}
	if sub.Topology.Kind == transport.EventTopology && sub.Group == "" {
		return fmt.Errorf("nats: event subscription group must not be blank")
	}
	if sub.Topology.Kind == transport.TaskTopology && sub.Group != "" {
		return fmt.Errorf("nats: task subscription group must be empty")
	}
	if err := t.validateEventLayout(ctx, sub.Topology); err != nil {
		return err
	}
	var lease *subscriberLaneLeaseHandle
	if sub.Topology.Kind == transport.EventTopology {
		var err error
		lease, err = t.acquireSubscriberLane(ctx, sub.Topology, t.consumerName(sub), true, "provision")
		if err != nil {
			return err
		}
		defer lease.release(context.Background())
		if t.laneOperationHook != nil {
			t.laneOperationHook("provision")
		}
	}
	if err := t.reconcileConsumerWithLease(ctx, t.streamName(sub.Topology), sub, lease); err != nil {
		return err
	}
	if sub.Topology.Kind == transport.EventTopology {
		if err := t.ensureRetryLane(ctx, sub, lease); err != nil {
			return err
		}
		capacity, err := effectiveCapacity(sub)
		if err != nil {
			return err
		}
		return lease.finish(ctx, "active", topologyAckWait(sub.Topology), capacity)
	}
	return nil
}

func topologyAckWait(topo transport.Topology) time.Duration {
	if value, ok := topo.Options["nats.ack_wait"].(time.Duration); ok {
		return value
	}
	return 5 * time.Minute
}

func heartbeatInterval(ackWait time.Duration) time.Duration {
	interval := ackWait / 3
	if interval <= 0 {
		return ackWait
	}
	return interval
}

func (t *natsTransport) reconcileConsumer(ctx context.Context, stream string, sub transport.Subscription) error {
	return t.reconcileConsumerWithLease(ctx, stream, sub, nil)
}

func (t *natsTransport) reconcileConsumerWithLease(ctx context.Context, stream string, sub transport.Subscription, lease *subscriberLaneLeaseHandle) error {
	if err := t.ValidateTopology(sub.Topology); err != nil {
		return err
	}
	desired, err := t.desiredConsumerConfig(sub)
	if err != nil {
		return err
	}
	info, err := t.js.ConsumerInfo(stream, desired.Durable, gonats.Context(ctx))
	if err != nil {
		if !errors.Is(err, gonats.ErrConsumerNotFound) {
			return fmt.Errorf("inspect consumer %q: %w", desired.Durable, err)
		}
		if lease != nil {
			if err := lease.checkOwnership(ctx); err != nil {
				return err
			}
		}
		if _, err := t.js.AddConsumer(stream, &desired, gonats.Context(ctx)); err == nil {
			return nil
		} else if !errors.Is(err, gonats.ErrConsumerNameAlreadyInUse) {
			return fmt.Errorf("create consumer %q: %w", desired.Durable, err)
		}
		info, err = t.js.ConsumerInfo(stream, desired.Durable, gonats.Context(ctx))
		if err != nil {
			return fmt.Errorf("inspect concurrently created consumer %q: %w", desired.Durable, err)
		}
	}

	current := info.Config
	if current.AckPolicy != gonats.AckExplicitPolicy || current.DeliverSubject != "" {
		return fmt.Errorf("nats: consumer %q has incompatible acknowledgement or delivery mode", desired.Durable)
	}
	if current.AckWait == desired.AckWait && current.MaxAckPending == desired.MaxAckPending &&
		current.FilterSubject == desired.FilterSubject && len(current.FilterSubjects) == 0 {
		return nil
	}
	current.AckWait = desired.AckWait
	current.MaxAckPending = desired.MaxAckPending
	current.FilterSubject = desired.FilterSubject
	current.FilterSubjects = nil
	if lease != nil {
		if err := lease.checkOwnership(ctx); err != nil {
			return err
		}
	}
	if _, err := t.js.UpdateConsumer(stream, &current, gonats.Context(ctx)); err != nil {
		return fmt.Errorf("update consumer %q: %w", desired.Durable, err)
	}
	return nil
}

func (t *natsTransport) ensureRetryLane(ctx context.Context, sub transport.Subscription, lease *subscriberLaneLeaseHandle) error {
	durable := t.consumerName(sub)
	stream := t.retryStreamName(sub.Topology, durable)
	maxAge, replicas, duplicates := time.Duration(0), 1, defaultDuplicateWindow
	if value, ok := sub.Topology.Options["nats.maxage"].(time.Duration); ok {
		maxAge = value
	}
	if value, ok := sub.Topology.Options["nats.replicas"].(int); ok {
		replicas = value
	}
	if value, ok := sub.Topology.Options["nats.duplicate_window"].(time.Duration); ok {
		duplicates = value
	}
	desired := gonats.StreamConfig{
		Name: stream, Subjects: []string{t.retrySubject(sub.Topology, durable)}, Storage: gonats.FileStorage,
		MaxAge: maxAge, Replicas: replicas, Retention: gonats.LimitsPolicy, Duplicates: duplicates,
	}
	if delayLayoutIndexed(sub.Topology) {
		desired.Retention = gonats.WorkQueuePolicy
	}
	if err := t.reconcileStreamWithLease(ctx, desired, lease); err != nil {
		return fmt.Errorf("nats: provision subscriber retry stream: %w", err)
	}
	capacity, err := effectiveCapacity(sub)
	if err != nil {
		return err
	}
	name := t.retryConsumerName(sub.Topology, durable)
	config := gonats.ConsumerConfig{
		Durable: name, AckPolicy: gonats.AckExplicitPolicy, AckWait: topologyAckWait(sub.Topology),
		MaxAckPending: capacity, FilterSubject: t.retrySubject(sub.Topology, durable), DeliverPolicy: gonats.DeliverAllPolicy,
	}
	info, err := t.js.ConsumerInfo(stream, name, gonats.Context(ctx))
	if errors.Is(err, gonats.ErrConsumerNotFound) {
		if err := lease.checkOwnership(ctx); err != nil {
			return err
		}
		_, err = t.js.AddConsumer(stream, &config, gonats.Context(ctx))
		if err == nil || errors.Is(err, gonats.ErrConsumerNameAlreadyInUse) {
			return nil
		}
		return fmt.Errorf("nats: create subscriber retry consumer: %w", err)
	}
	if err != nil {
		return fmt.Errorf("nats: inspect subscriber retry consumer: %w", err)
	}
	current := info.Config
	if current.AckPolicy != gonats.AckExplicitPolicy || current.DeliverSubject != "" ||
		current.FilterSubject != config.FilterSubject || len(current.FilterSubjects) != 0 ||
		current.DeliverPolicy != gonats.DeliverAllPolicy {
		return fmt.Errorf("nats: subscriber retry consumer %q is incompatible", name)
	}
	if current.AckWait == config.AckWait && current.MaxAckPending == config.MaxAckPending {
		return nil
	}
	current.AckWait = config.AckWait
	current.MaxAckPending = config.MaxAckPending
	if err := lease.checkOwnership(ctx); err != nil {
		return err
	}
	if _, err := t.js.UpdateConsumer(stream, &current, gonats.Context(ctx)); err != nil {
		return fmt.Errorf("nats: update subscriber retry consumer: %w", err)
	}
	return nil
}

func (t *natsTransport) verifyRetryLane(ctx context.Context, topo transport.Topology, durable string) error {
	if topo.Kind != transport.EventTopology || durable == "" {
		return fmt.Errorf("%w: targeted event durable identity required", errSubscriberLaneUnavailable)
	}
	return t.withSubscriberLane(ctx, topo, durable, "verify", func(*subscriberLaneLeaseHandle, subscriberLaneState) error { return nil })
}

func (t *natsTransport) verifyRetryLaneConfig(ctx context.Context, topo transport.Topology, durable string, state subscriberLaneState) error {
	stream := t.retryStreamName(topo, durable)
	info, err := t.js.StreamInfo(stream, gonats.Context(ctx))
	if err != nil {
		return fmt.Errorf("%w: inspect subscriber retry stream %q: %v", errSubscriberLaneUnavailable, stream, err)
	}
	if !slices.Equal(info.Config.Subjects, []string{t.retrySubject(topo, durable)}) || info.Config.Retention != gonats.LimitsPolicy {
		return fmt.Errorf("%w: subscriber retry stream %q is incompatible", errSubscriberLaneUnavailable, stream)
	}
	consumer, err := t.js.ConsumerInfo(stream, t.retryConsumerName(topo, durable), gonats.Context(ctx))
	if err != nil {
		return fmt.Errorf("%w: inspect subscriber retry consumer: %v", errSubscriberLaneUnavailable, err)
	}
	config := consumer.Config
	if config.AckPolicy != gonats.AckExplicitPolicy || config.DeliverSubject != "" ||
		config.FilterSubject != t.retrySubject(topo, durable) || len(config.FilterSubjects) != 0 ||
		config.DeliverPolicy != gonats.DeliverAllPolicy || config.AckWait != time.Duration(state.AckWaitNanos) ||
		config.MaxAckPending != state.MaxAckPending {
		return fmt.Errorf("%w: subscriber retry consumer %q is incompatible", errSubscriberLaneUnavailable, config.Durable)
	}
	return nil
}

func (t *natsTransport) withSubscriberLane(ctx context.Context, topo transport.Topology, durable, operation string, fn func(*subscriberLaneLeaseHandle, subscriberLaneState) error) error {
	if err := t.validateEventLayout(ctx, topo); err != nil {
		return err
	}
	lease, err := t.acquireSubscriberLane(ctx, topo, durable, false, operation)
	if err != nil {
		return err
	}
	defer lease.release(context.Background())
	if t.laneOperationHook != nil {
		t.laneOperationHook(operation)
	}
	state, err := t.subscriberLaneState(topo, durable)
	if err != nil {
		return err
	}
	if err := t.verifyRetryLaneConfig(ctx, topo, durable, state); err != nil {
		return err
	}
	if err := lease.checkOwnership(ctx); err != nil {
		return err
	}
	return fn(lease, state)
}

// CleanupSubscription quarantines a subscriber identity. It prevents new
// targeted work, removes both delivery consumers, and deliberately retains the
// private retry stream so ready records remain durable and isolated for manual
// inspection instead of being deleted or exposed to another subscriber.
func (t *natsTransport) CleanupSubscription(ctx context.Context, sub transport.Subscription) error {
	if sub.Topology.Kind != transport.EventTopology || sub.Group == "" {
		return fmt.Errorf("nats: event subscriber required")
	}
	if err := t.validateEventLayout(ctx, sub.Topology); err != nil {
		return err
	}
	durable := t.consumerName(sub)
	lease, err := t.acquireSubscriberLane(ctx, sub.Topology, durable, false, "cleanup")
	if err != nil {
		return err
	}
	defer lease.release(context.Background())
	if t.laneOperationHook != nil {
		t.laneOperationHook("cleanup")
	}
	if err := t.ensureSubscriberLaneIdle(ctx, sub, durable); err != nil {
		return err
	}
	if err := lease.setState(ctx, "quarantined"); err != nil {
		return err
	}
	for _, consumer := range []struct{ stream, name string }{
		{t.streamName(sub.Topology), durable},
		{t.retryStreamName(sub.Topology, durable), t.retryConsumerName(sub.Topology, durable)},
	} {
		if err := lease.checkOwnership(ctx); err != nil {
			return err
		}
		if err := t.js.DeleteConsumer(consumer.stream, consumer.name, gonats.Context(ctx)); err != nil &&
			!errors.Is(err, gonats.ErrConsumerNotFound) && !errors.Is(err, gonats.ErrStreamNotFound) {
			return fmt.Errorf("nats: delete quarantined subscriber consumer: %w", err)
		}
	}
	return lease.finish(ctx, "quarantined", 0, 0)
}
