package nats

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/goccy/go-json"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

// Stats returns delivery obligations, rather than retained stream history.
// JetStream limit-retention streams keep acknowledged messages, so consumer
// NumPending and NumAckPending are the authoritative lag inputs.
func (t *natsTransport) Stats(ctx context.Context, topology transport.Topology) (transport.TopologyStats, error) {
	if err := t.ValidateTopology(topology); err != nil {
		return transport.TopologyStats{}, err
	}
	if err := t.validateEventLayout(ctx, topology); err != nil {
		return transport.TopologyStats{}, err
	}
	stream := t.streamName(topology)
	streamInfo, err := t.js.StreamInfo(stream, gonats.Context(ctx))
	if err != nil {
		return transport.TopologyStats{}, fmt.Errorf("nats: inspect topology stream: %w", err)
	}
	delayed, err := t.delayedCount(ctx, topology)
	if err != nil {
		return transport.TopologyStats{}, err
	}
	dlq, err := t.Count(ctx, topology)
	if err != nil {
		return transport.TopologyStats{}, err
	}
	stats := transport.TopologyStats{Delayed: delayed, DLQ: dlq, LagKnown: true}

	consumers := make([]*gonats.ConsumerInfo, 0, streamInfo.State.Consumers)
	for info := range t.js.Consumers(stream, gonats.Context(ctx)) {
		if info != nil {
			consumers = append(consumers, info)
		}
	}
	if len(consumers) < streamInfo.State.Consumers {
		return transport.TopologyStats{}, fmt.Errorf("nats: incomplete consumer information: got %d of %d", len(consumers), streamInfo.State.Consumers)
	}
	selected := consumers
	if topology.Kind == transport.TaskTopology {
		selected = selected[:0]
		expected := t.consumerName(transport.Subscription{Topology: topology})
		for _, info := range consumers {
			if info.Name == expected {
				selected = append(selected, info)
			}
		}
	}
	if topology.Kind == transport.TaskTopology && len(selected) == 0 {
		stats.Pending, err = checkedNATSCount("pending", streamInfo.State.Msgs)
		if err != nil {
			return transport.TopologyStats{}, err
		}
		stats.Lag = stats.Pending
		if stats.Lag == 0 {
			stats.OldestAgeKnown = true
		} else if !streamInfo.State.FirstTime.IsZero() {
			stats.OldestAge = max(0, t.now().Sub(streamInfo.State.FirstTime))
			stats.OldestAgeKnown = true
		}
		return stats, nil
	}
	for _, info := range selected {
		if err := addNATSCount(&stats.Pending, info.NumPending); err != nil {
			return transport.TopologyStats{}, err
		}
		if info.NumAckPending < 0 || stats.InFlight > math.MaxInt64-int64(info.NumAckPending) {
			return transport.TopologyStats{}, fmt.Errorf("nats: invalid or overflowing in-flight count")
		}
		stats.InFlight += int64(info.NumAckPending)
	}
	if topology.Kind == transport.EventTopology {
		if err := t.addPrivateRetryStats(ctx, topology, &stats); err != nil {
			return transport.TopologyStats{}, err
		}
	}
	if stats.Pending > math.MaxInt64-stats.InFlight {
		return transport.TopologyStats{}, fmt.Errorf("nats: overflowing lag count")
	}
	stats.Lag = stats.Pending + stats.InFlight
	stats.OldestAgeKnown = stats.Lag == 0
	return stats, nil
}

func (t *natsTransport) addPrivateRetryStats(ctx context.Context, topology transport.Topology, stats *transport.TopologyStats) error {
	kv, err := t.getKV(t.kvBucketName(topology))
	if err != nil {
		return fmt.Errorf("nats: inspect subscriber lane registry: %w", err)
	}
	watcher, err := kv.Watch("subscriber."+retryLaneVersion+".>", gonats.IgnoreDeletes(), gonats.Context(ctx))
	if err != nil {
		return fmt.Errorf("nats: watch subscriber lane registry: %w", err)
	}
	defer watcher.Stop()
	seen := make(map[string]struct{})
	errorsCh := watcher.Error()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case watchErr, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if watchErr != nil {
				return fmt.Errorf("nats: watch subscriber lane registry: %w", watchErr)
			}
		case entry, ok := <-watcher.Updates():
			if !ok {
				return fmt.Errorf("nats: subscriber lane watcher closed before initial snapshot completed")
			}
			if entry == nil {
				return nil
			}
			var state subscriberLaneState
			if err := json.Unmarshal(entry.Value(), &state); err != nil || state.Version != subscriberLaneVersion || state.Durable == "" {
				return fmt.Errorf("nats: invalid subscriber lane registry entry %q", entry.Key())
			}
			if _, ok := seen[state.Durable]; ok {
				continue
			}
			seen[state.Durable] = struct{}{}
			info, err := t.js.ConsumerInfo(t.retryStreamName(topology, state.Durable), t.retryConsumerName(topology, state.Durable), gonats.Context(ctx))
			if errors.Is(err, gonats.ErrConsumerNotFound) || errors.Is(err, gonats.ErrStreamNotFound) {
				continue
			}
			if err != nil {
				return fmt.Errorf("nats: inspect private retry consumer: %w", err)
			}
			if err := addNATSCount(&stats.Pending, info.NumPending); err != nil {
				return err
			}
			if info.NumAckPending < 0 || stats.InFlight > math.MaxInt64-int64(info.NumAckPending) {
				return fmt.Errorf("nats: invalid or overflowing private retry in-flight count")
			}
			stats.InFlight += int64(info.NumAckPending)
		}
	}
}

func (t *natsTransport) SubscriptionStats(ctx context.Context, sub transport.Subscription) (transport.TopologyStats, error) {
	if sub.Topology.Kind != transport.EventTopology || sub.Group == "" {
		return transport.TopologyStats{}, fmt.Errorf("nats: event subscriber required")
	}
	if err := t.validateEventLayout(ctx, sub.Topology); err != nil {
		return transport.TopologyStats{}, err
	}
	info, err := t.js.ConsumerInfo(t.streamName(sub.Topology), t.consumerName(sub), gonats.Context(ctx))
	if err != nil {
		return transport.TopologyStats{}, fmt.Errorf("nats: inspect subscriber: %w", err)
	}
	pending, err := checkedNATSCount("pending", info.NumPending)
	if err != nil {
		return transport.TopologyStats{}, err
	}
	inFlight := int64(info.NumAckPending)
	retryInfo, err := t.js.ConsumerInfo(
		t.retryStreamName(sub.Topology, t.consumerName(sub)),
		t.retryConsumerName(sub.Topology, t.consumerName(sub)),
		gonats.Context(ctx),
	)
	if err != nil {
		return transport.TopologyStats{}, fmt.Errorf("nats: inspect subscriber retry lane: %w", err)
	}
	if err := addNATSCount(&pending, retryInfo.NumPending); err != nil {
		return transport.TopologyStats{}, err
	}
	if retryInfo.NumAckPending < 0 || inFlight > math.MaxInt64-int64(retryInfo.NumAckPending) {
		return transport.TopologyStats{}, fmt.Errorf("nats: invalid or overflowing subscriber in-flight count")
	}
	inFlight += int64(retryInfo.NumAckPending)
	delayed, err := t.subscriberDelayedCount(ctx, sub.Topology, t.consumerName(sub))
	if err != nil {
		return transport.TopologyStats{}, err
	}
	lag := pending + inFlight
	return transport.TopologyStats{Pending: pending, Delayed: delayed, InFlight: inFlight, Lag: lag, LagKnown: true, OldestAgeKnown: lag == 0}, nil
}

func (t *natsTransport) subscriberDelayedCount(ctx context.Context, topology transport.Topology, durable string) (int64, error) {
	kv, err := t.getKV(t.kvBucketName(topology))
	if err != nil {
		return 0, fmt.Errorf("nats: inspect subscriber delayed records: %w", err)
	}
	watcher, err := kv.Watch("v2.>", gonats.IgnoreDeletes(), gonats.Context(ctx))
	if err != nil {
		return 0, fmt.Errorf("nats: watch subscriber delayed records: %w", err)
	}
	defer watcher.Stop()
	var count int64
	errorsCh := watcher.Error()
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case watchErr, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if watchErr != nil {
				return 0, fmt.Errorf("nats: watch subscriber delayed records: %w", watchErr)
			}
		case entry, ok := <-watcher.Updates():
			if !ok {
				return 0, fmt.Errorf("nats: subscriber delayed watcher closed before initial snapshot completed")
			}
			if entry == nil {
				return count, nil
			}
			var record delayRecord
			if err := json.Unmarshal(entry.Value(), &record); err != nil {
				return 0, fmt.Errorf("nats: decode subscriber delayed record: %w", err)
			}
			if record.Target == durable {
				if count == math.MaxInt64 {
					return 0, fmt.Errorf("nats: overflowing subscriber delayed count")
				}
				count++
			}
		}
	}
}

func (t *natsTransport) delayedCount(ctx context.Context, topology transport.Topology) (int64, error) {
	kv, err := t.getKV(t.kvBucketName(topology))
	if err != nil {
		return 0, fmt.Errorf("nats: inspect delayed records: %w", err)
	}
	lister, err := kv.ListKeys(gonats.Context(ctx))
	if err != nil {
		return 0, fmt.Errorf("nats: list delayed records: %w", err)
	}
	defer lister.Stop()
	var count int64
	errorsCh := lister.Error()
	keysCh := lister.Keys()
	for keysCh != nil || errorsCh != nil {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case key, ok := <-keysCh:
			if !ok {
				keysCh = nil
				continue
			}
			if strings.HasPrefix(key, "v2.") {
				if count == math.MaxInt64 {
					return 0, fmt.Errorf("nats: overflowing delayed count")
				}
				count++
			}
		case listErr, ok := <-errorsCh:
			if !ok {
				errorsCh = nil
				continue
			}
			if listErr != nil {
				return 0, fmt.Errorf("nats: list delayed records: %w", listErr)
			}
		}
	}
	return count, nil
}

func checkedNATSCount(name string, value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("nats: overflowing %s count", name)
	}
	return int64(value), nil
}

func addNATSCount(total *int64, value uint64) error {
	converted, err := checkedNATSCount("pending", value)
	if err != nil || *total > math.MaxInt64-converted {
		return fmt.Errorf("nats: invalid or overflowing pending count")
	}
	*total += converted
	return nil
}
