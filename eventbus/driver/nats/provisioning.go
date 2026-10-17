package nats

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	defaultConcurrency = 10
	defaultPrefetch    = 20
)

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
	return gonats.StreamConfig{
		Name: name, Subjects: []string{name}, Storage: gonats.FileStorage,
		MaxAge: maxAge, Replicas: replicas, Retention: retention,
	}
}

// reconcileStream creates a missing stream or updates only fields owned by
// EventBus. Unmanaged broker fields are preserved. JetStream rejects unsafe
// changes (for example storage transitions), and that error is returned.
func (t *natsTransport) reconcileStream(ctx context.Context, desired gonats.StreamConfig) error {
	info, err := t.js.StreamInfo(desired.Name, gonats.Context(ctx))
	if err != nil {
		if !errors.Is(err, gonats.ErrStreamNotFound) {
			return fmt.Errorf("inspect stream %q: %w", desired.Name, err)
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
		current.Replicas == desired.Replicas && current.Retention == desired.Retention {
		return nil
	}
	current.Subjects = slices.Clone(desired.Subjects)
	current.Storage = desired.Storage
	current.MaxAge = desired.MaxAge
	current.Replicas = desired.Replicas
	current.Retention = desired.Retention
	if _, err := t.js.UpdateStream(&current, gonats.Context(ctx)); err != nil {
		return fmt.Errorf("update stream %q: %w", desired.Name, err)
	}
	return nil
}

func (t *natsTransport) consumerName(sub transport.Subscription) string {
	name := sub.Topology.NameWithSeparator("_") + "_group"
	if sub.Group != "" {
		name = sub.Topology.NameWithSeparator("_") + "_" + sub.Group
	}
	return name
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
		AckWait: ackWait, MaxAckPending: maxAckPending,
	}, nil
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
	if current.AckWait == desired.AckWait && current.MaxAckPending == desired.MaxAckPending {
		return nil
	}
	current.AckWait = desired.AckWait
	current.MaxAckPending = desired.MaxAckPending
	if _, err := t.js.UpdateConsumer(stream, &current, gonats.Context(ctx)); err != nil {
		return fmt.Errorf("update consumer %q: %w", desired.Durable, err)
	}
	return nil
}
