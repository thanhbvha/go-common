package redis

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

// Stats returns a near-point-in-time view assembled from Redis stream, group,
// sorted-set, and DLQ primitives. Lag counts ready plus in-flight delivery
// obligations; delayed and dead-letter records are reported separately.
func (t *redisTransport) Stats(ctx context.Context, topology transport.Topology) (transport.TopologyStats, error) {
	if err := t.ValidateTopology(topology); err != nil {
		return transport.TopologyStats{}, err
	}

	stream := t.streamKey(topology)
	typeName, err := t.rc.Type(ctx, stream).Result()
	if err != nil {
		return transport.TopologyStats{}, fmt.Errorf("redis: stats stream type: %w", err)
	}
	if typeName != "none" && typeName != "stream" {
		return transport.TopologyStats{}, fmt.Errorf("redis: stats source must be a stream")
	}

	pipe := t.rc.Pipeline()
	delayedCmd := pipe.ZCard(ctx, t.delayedKey(topology))
	dlqCmd := pipe.XLen(ctx, t.dlqKey(topology))
	streamLenCmd := pipe.XLen(ctx, stream)
	if _, err := pipe.Exec(ctx); err != nil {
		return transport.TopologyStats{}, fmt.Errorf("redis: stats counts: %w", err)
	}
	stats := transport.TopologyStats{
		Delayed:  delayedCmd.Val(),
		DLQ:      dlqCmd.Val(),
		LagKnown: true,
	}
	streamLen := streamLenCmd.Val()
	if typeName == "none" && streamLen > 0 {
		// A concurrent publisher created the stream between TYPE and the count
		// pipeline. Continue with group inspection instead of reporting zero.
		typeName = "stream"
	}

	if typeName == "none" {
		stats.OldestAgeKnown = true
		return stats, nil
	}
	groups, err := t.rc.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return transport.TopologyStats{}, fmt.Errorf("redis: stats groups: %w", err)
	}
	groups = statsGroups(topology.Kind, groups)

	// A producer-only task has no consumer group yet. When its first consumer
	// creates workers at 0, every retained stream entry becomes ready work.
	if topology.Kind == transport.TaskTopology && len(groups) == 0 {
		stats.Pending = streamLen
		stats.Lag = streamLen
		if streamLen == 0 {
			stats.OldestAgeKnown = true
			return stats, nil
		}
		messages, err := t.rc.XRangeN(ctx, stream, "-", "+", 1).Result()
		if err != nil {
			return transport.TopologyStats{}, fmt.Errorf("redis: stats oldest ready: %w", err)
		}
		if len(messages) == 1 {
			stats.OldestAge, stats.OldestAgeKnown = messageAge(messages[0], time.Now())
		}
		return stats, nil
	}

	oldestKnown := true
	var oldest time.Time
	for _, group := range groups {
		ready, err := t.readyCount(ctx, stream, group)
		if err != nil {
			return transport.TopologyStats{}, err
		}
		if err := addStatCount(&stats.Pending, ready); err != nil {
			return transport.TopologyStats{}, err
		}
		pending, err := t.rc.XPending(ctx, stream, group.Name).Result()
		if err != nil {
			return transport.TopologyStats{}, fmt.Errorf("redis: stats pending for group %q: %w", group.Name, err)
		}
		if err := addStatCount(&stats.InFlight, pending.Count); err != nil {
			return transport.TopologyStats{}, err
		}

		candidate, known, err := t.oldestGroupEntry(ctx, stream, group, ready, pending)
		if err != nil {
			return transport.TopologyStats{}, err
		}
		if !known {
			oldestKnown = false
		} else if !candidate.IsZero() && (oldest.IsZero() || candidate.Before(oldest)) {
			oldest = candidate
		}
	}
	if err := addStatCount(&stats.Lag, stats.Pending); err != nil {
		return transport.TopologyStats{}, err
	}
	if err := addStatCount(&stats.Lag, stats.InFlight); err != nil {
		return transport.TopologyStats{}, err
	}
	if stats.Lag == 0 {
		stats.OldestAgeKnown = true
		return stats, nil
	}
	if oldestKnown && !oldest.IsZero() {
		stats.OldestAge = max(0, time.Since(oldest))
		stats.OldestAgeKnown = true
	}
	return stats, nil
}

func (t *redisTransport) SubscriptionStats(ctx context.Context, sub transport.Subscription) (transport.TopologyStats, error) {
	if sub.Topology.Kind != transport.EventTopology || sub.Group == "" {
		return transport.TopologyStats{}, fmt.Errorf("redis: event subscriber required")
	}
	stream := t.streamKey(sub.Topology)
	groups, err := t.rc.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return transport.TopologyStats{}, fmt.Errorf("redis: subscriber stats groups: %w", err)
	}
	wanted := "subscriber:" + sub.Group
	for _, group := range groups {
		if group.Name != wanted {
			continue
		}
		ready, err := t.readyCount(ctx, stream, group)
		if err != nil {
			return transport.TopologyStats{}, err
		}
		pending, err := t.rc.XPending(ctx, stream, group.Name).Result()
		if err != nil {
			return transport.TopologyStats{}, err
		}
		lag := ready + pending.Count
		return transport.TopologyStats{Pending: ready, InFlight: pending.Count, Lag: lag, LagKnown: true, OldestAgeKnown: lag == 0}, nil
	}
	return transport.TopologyStats{}, fmt.Errorf("redis: subscriber group %q is not provisioned", sub.Group)
}

// Redis 7 reports EntriesRead and Lag in XINFO GROUPS. Redis 6 and miniredis
// omit EntriesRead, so fall back to bounded XRANGE pages instead of inventing a
// value. The fallback is exact but O(ready backlog); callers can cancel it.
func (t *redisTransport) readyCount(ctx context.Context, stream string, group goredis.XInfoGroup) (int64, error) {
	if group.EntriesRead > 0 {
		if group.Lag < 0 {
			return 0, fmt.Errorf("redis: stats lag unavailable for group %q", group.Name)
		}
		return group.Lag, nil
	}

	const pageSize = int64(1000)
	cursor := exclusiveStreamID(group.LastDeliveredID)
	var total int64
	for {
		messages, err := t.rc.XRangeN(ctx, stream, cursor, "+", pageSize).Result()
		if err != nil {
			return 0, fmt.Errorf("redis: stats ready fallback for group %q: %w", group.Name, err)
		}
		if err := addStatCount(&total, int64(len(messages))); err != nil {
			return 0, err
		}
		if int64(len(messages)) < pageSize {
			return total, nil
		}
		cursor = exclusiveStreamID(messages[len(messages)-1].ID)
	}
}

func statsGroups(kind transport.TopologyKind, groups []goredis.XInfoGroup) []goredis.XInfoGroup {
	selected := groups[:0]
	for _, group := range groups {
		if kind == transport.TaskTopology && group.Name == "workers" {
			return []goredis.XInfoGroup{group}
		}
		if kind == transport.EventTopology && strings.HasPrefix(group.Name, "subscriber:") {
			selected = append(selected, group)
		}
	}
	return selected
}

func exclusiveStreamID(id string) string {
	if id == "" {
		id = "0-0"
	}
	return "(" + id
}

func addStatCount(total *int64, value int64) error {
	if value < 0 || *total > math.MaxInt64-value {
		return fmt.Errorf("redis: invalid or overflowing stats count")
	}
	*total += value
	return nil
}

func (t *redisTransport) oldestGroupEntry(ctx context.Context, stream string, group goredis.XInfoGroup, ready int64, pending *goredis.XPending) (time.Time, bool, error) {
	var oldest time.Time
	known := true
	if pending.Count > 0 {
		if pending.Lower == "" {
			known = false
		} else {
			message, found, err := t.streamMessage(ctx, stream, pending.Lower)
			if err != nil {
				return time.Time{}, false, err
			}
			if !found {
				known = false
			} else {
				oldest = messageReadyAt(message)
			}
		}
	}
	if ready > 0 {
		messages, err := t.rc.XRangeN(ctx, stream, exclusiveStreamID(group.LastDeliveredID), "+", 1).Result()
		if err != nil {
			return time.Time{}, false, fmt.Errorf("redis: stats oldest ready for group %q: %w", group.Name, err)
		}
		if len(messages) == 0 {
			known = false
		} else {
			candidate := messageReadyAt(messages[0])
			if oldest.IsZero() || candidate.Before(oldest) {
				oldest = candidate
			}
		}
	}
	return oldest, known, nil
}

func (t *redisTransport) streamMessage(ctx context.Context, stream, id string) (goredis.XMessage, bool, error) {
	messages, err := t.rc.XRangeN(ctx, stream, id, id, 1).Result()
	if err != nil {
		return goredis.XMessage{}, false, fmt.Errorf("redis: stats stream entry %s: %w", id, err)
	}
	if len(messages) == 0 {
		return goredis.XMessage{}, false, nil
	}
	return messages[0], true, nil
}

func messageAge(message goredis.XMessage, now time.Time) (time.Duration, bool) {
	readyAt := messageReadyAt(message)
	if readyAt.IsZero() {
		return 0, false
	}
	return max(0, now.Sub(readyAt)), true
}

func messageReadyAt(message goredis.XMessage) time.Time {
	var payload []byte
	switch value := message.Values["payload"].(type) {
	case string:
		payload = []byte(value)
	case []byte:
		payload = value
	}
	var envelope struct {
		CreatedAt int64 `json:"created_at"`
		RunAt     int64 `json:"run_at"`
	}
	if len(payload) > 0 && json.Unmarshal(payload, &envelope) == nil {
		readyAt := max(envelope.CreatedAt, envelope.RunAt)
		if readyAt > 0 {
			return time.UnixMilli(readyAt)
		}
	}
	millis, _, ok := strings.Cut(message.ID, "-")
	if !ok {
		return time.Time{}
	}
	value, err := strconv.ParseInt(millis, 10, 64)
	if err != nil || value <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(value)
}
