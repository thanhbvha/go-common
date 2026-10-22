package nats

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/goccy/go-json"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

var errStopKVSnapshot = errors.New("nats: stop KV snapshot")

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
	seen := make(map[string]struct{})
	err := t.walkKVSnapshot(ctx, topology, "subscriber."+retryLaneVersion+".>", func(entry gonats.KeyValueEntry) error {
		var state subscriberLaneState
		if err := json.Unmarshal(entry.Value(), &state); err != nil || state.Version != subscriberLaneVersion || state.Durable == "" {
			return fmt.Errorf("nats: invalid subscriber lane registry entry")
		}
		if _, ok := seen[state.Durable]; ok {
			return nil
		}
		seen[state.Durable] = struct{}{}
		info, err := t.js.ConsumerInfo(t.retryStreamName(topology, state.Durable), t.retryConsumerName(topology, state.Durable), gonats.Context(ctx))
		if errors.Is(err, gonats.ErrConsumerNotFound) || errors.Is(err, gonats.ErrStreamNotFound) {
			return nil
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
		return nil
	})
	if err != nil {
		return fmt.Errorf("nats: snapshot subscriber lane registry: %w", err)
	}
	return nil
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
	var count int64
	err := t.walkKVSnapshot(ctx, topology, "v2.>", func(entry gonats.KeyValueEntry) error {
		var record delayRecord
		if err := json.Unmarshal(entry.Value(), &record); err != nil {
			return fmt.Errorf("nats: decode subscriber delayed record: %w", err)
		}
		if record.Target == durable {
			if count == math.MaxInt64 {
				return fmt.Errorf("nats: overflowing subscriber delayed count")
			}
			count++
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("nats: snapshot subscriber delayed records: %w", err)
	}
	return count, nil
}

func (t *natsTransport) delayedCount(ctx context.Context, topology transport.Topology) (int64, error) {
	var count int64
	err := t.walkKVSnapshot(ctx, topology, "v2.>", func(gonats.KeyValueEntry) error {
		if count == math.MaxInt64 {
			return fmt.Errorf("nats: overflowing delayed count")
		}
		count++
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("nats: snapshot delayed records: %w", err)
	}
	return count, nil
}

func (t *natsTransport) walkKVSnapshot(ctx context.Context, topology transport.Topology, pattern string, visit func(gonats.KeyValueEntry) error) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	bucket := t.kvBucketName(topology)
	prefix := "$KV." + bucket + "."
	sub, err := t.js.PullSubscribe(prefix+pattern, "",
		gonats.BindStream("KV_"+bucket),
		gonats.DeliverLastPerSubject(),
		gonats.AckNone(),
		gonats.InactiveThreshold(time.Minute),
		gonats.Context(ctx),
	)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()
	info, err := sub.ConsumerInfo()
	if err != nil {
		return err
	}
	remaining := info.NumPending
	for remaining > 0 {
		batch := min(remaining, 1000)
		messages, err := sub.Fetch(int(batch), gonats.Context(ctx))
		if err != nil {
			return err
		}
		if len(messages) == 0 {
			return fmt.Errorf("snapshot returned no messages with %d pending", remaining)
		}
		for _, message := range messages {
			if op := message.Header.Get("KV-Operation"); op == "DEL" || op == "PURGE" {
				continue
			}
			metadata, err := message.Metadata()
			if err != nil {
				return err
			}
			entry := &snapshotKVEntry{
				bucket: bucket, key: message.Subject[len(prefix):], value: message.Data,
				revision: metadata.Sequence.Stream, created: metadata.Timestamp, delta: metadata.NumPending,
			}
			if err := visit(entry); errors.Is(err, errStopKVSnapshot) {
				return nil
			} else if err != nil {
				return err
			}
		}
		remaining -= uint64(len(messages))
	}
	return nil
}

type snapshotKVEntry struct {
	bucket   string
	key      string
	value    []byte
	revision uint64
	created  time.Time
	delta    uint64
}

func (e *snapshotKVEntry) Bucket() string               { return e.bucket }
func (e *snapshotKVEntry) Key() string                  { return e.key }
func (e *snapshotKVEntry) Value() []byte                { return e.value }
func (e *snapshotKVEntry) Revision() uint64             { return e.revision }
func (e *snapshotKVEntry) Created() time.Time           { return e.created }
func (e *snapshotKVEntry) Delta() uint64                { return e.delta }
func (e *snapshotKVEntry) Operation() gonats.KeyValueOp { return gonats.KeyValuePut }

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
