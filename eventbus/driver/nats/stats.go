package nats

import (
	"context"
	"fmt"
	"math"
	"strings"

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
	if stats.Pending > math.MaxInt64-stats.InFlight {
		return transport.TopologyStats{}, fmt.Errorf("nats: overflowing lag count")
	}
	stats.Lag = stats.Pending + stats.InFlight
	stats.OldestAgeKnown = stats.Lag == 0
	return stats, nil
}

func (t *natsTransport) SubscriptionStats(ctx context.Context, sub transport.Subscription) (transport.TopologyStats, error) {
	if sub.Topology.Kind != transport.EventTopology || sub.Group == "" {
		return transport.TopologyStats{}, fmt.Errorf("nats: event subscriber required")
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
	lag := pending + inFlight
	return transport.TopologyStats{Pending: pending, InFlight: inFlight, Lag: lag, LagKnown: true, OldestAgeKnown: lag == 0}, nil
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
