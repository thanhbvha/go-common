package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/registry"
	"github.com/thanhbvha/go-common/eventbus/transport"
	commonredis "github.com/thanhbvha/go-common/redis"
)

func init() {
	registry.Register("redis", func() (transport.Transport, error) {
		c := commonredis.Default()
		if c == nil {
			return nil, fmt.Errorf("redis default client not set")
		}
		return New(c.Native()), nil
	})
}

const (
	// Lua script to move delayed jobs to the stream atomically
	luaMoveDelayed = `
local zset = KEYS[1]
local stream = KEYS[2]
local max_score = ARGV[1]
local limit = ARGV[2]
limit = tonumber(limit)
if not limit or limit < 1 or limit > 1000 or limit ~= math.floor(limit) then
    return redis.error_reply("invalid delayed dispatch batch limit")
end
local source_type = redis.call("TYPE", zset).ok
local target_type = redis.call("TYPE", stream).ok
if source_type ~= "none" and source_type ~= "zset" then
    return redis.error_reply("delayed source must be a sorted set")
end
if target_type ~= "none" and target_type ~= "stream" then
    return redis.error_reply("delayed destination must be a stream")
end
local jobs = redis.call("ZRANGEBYSCORE", zset, "-inf", max_score, "LIMIT", 0, limit)
for _, job in ipairs(jobs) do
    redis.call("XADD", stream, "*", "payload", job)
    redis.call("ZREM", zset, job)
end
return #jobs
`
)

type redisTransport struct {
	rc goredis.UniversalClient

	mu       sync.Mutex
	sweepers map[string]context.CancelFunc
}

var (
	_ transport.Transport         = (*redisTransport)(nil)
	_ transport.Deduper           = (*redisTransport)(nil)
	_ transport.TopologyDeduper   = (*redisTransport)(nil)
	_ transport.DLQAdmin          = (*redisTransport)(nil)
	_ transport.CursorDLQAdmin    = (*redisTransport)(nil)
	_ transport.StatsReader       = (*redisTransport)(nil)
	_ transport.TopologyValidator = (*redisTransport)(nil)
	_ transport.Delivery          = (*redisDelivery)(nil)
)

func New(rc goredis.UniversalClient) transport.Transport {
	return &redisTransport{
		rc:       rc,
		sweepers: make(map[string]context.CancelFunc),
	}
}

func (t *redisTransport) Name() string { return "redis" }

func (t *redisTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		DelayPrecision: 1 * time.Second, // Sweeper runs every second
		Ordering:       false,           // Redis streams don't have per-message ordering keys
	}
}

func (t *redisTransport) groupName(sub transport.Subscription) string {
	if sub.Group != "" {
		return "subscriber:" + sub.Group
	}
	return "workers"
}

func (t *redisTransport) EnsureTopology(ctx context.Context, topo transport.Topology) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	if err := t.rc.HSet(ctx, t.metadataKey(topo), map[string]any{
		"layout_version": "1", "namespace": topo.Namespace,
		"kind": topo.Kind.String(), "name": topo.JobType,
	}).Err(); err != nil {
		return fmt.Errorf("redis EnsureTopology metadata: %w", err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	topoName := topologyPrefix(topo)
	if _, ok := t.sweepers[topoName]; !ok {
		sweepCtx, cancel := context.WithCancel(context.Background())
		t.sweepers[topoName] = cancel
		go t.delaySweeper(sweepCtx, topo)
	}

	return nil
}

func (t *redisTransport) Publish(ctx context.Context, topo transport.Topology, msg transport.Message) error {
	stream := t.streamKey(topo)

	maxLen := int64(100000)
	if val, ok := topo.Options["redis.maxlen"].(int64); ok && val > 0 {
		maxLen = val
	}

	// Add directly to stream
	return t.rc.XAdd(ctx, &goredis.XAddArgs{
		Stream: stream,
		MaxLen: maxLen,
		Approx: true,
		Values: map[string]interface{}{"payload": msg.Payload},
	}).Err()
}

func (t *redisTransport) PublishAt(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	delayed := t.delayedKey(topo)
	score := float64(runAt.UnixMilli())

	return t.rc.ZAdd(ctx, delayed, goredis.Z{
		Score:  score,
		Member: msg.Payload,
	}).Err()
}

func (t *redisTransport) delaySweeper(ctx context.Context, topo transport.Topology) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := t.dispatchDue(ctx, topo, time.Now())
			if err != nil && !errors.Is(err, context.Canceled) {
				// We don't have logger here, just silently retry on next tick
				// In a real production transport, we should have a logger injected
			}
		}
	}
}

var moveDelayedScript = goredis.NewScript(luaMoveDelayed)

// dispatchDue performs one bounded broker-side move. Lua errors do not roll
// back prior writes: validate types first and remove each source only after
// its destination write, so partial failure cannot silently lose a job.
func (t *redisTransport) dispatchDue(ctx context.Context, topo transport.Topology, now time.Time) (int64, error) {
	if err := t.ValidateTopology(topo); err != nil {
		return 0, err
	}
	limit := 100
	if value, ok := topo.Options["redis.delay_batch_size"].(int); ok {
		limit = value
	}
	return moveDelayedScript.Run(ctx, t.rc, []string{t.delayedKey(topo), t.streamKey(topo)}, now.UnixMilli(), limit).Int64()
}

func (t *redisTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := t.streamKey(sub.Topology)
	group := t.groupName(sub)
	consumer := "consumer-" + generateID() // need a unique consumer name

	// Ensure group exists
	err := t.rc.XGroupCreateMkStream(ctx, stream, group, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("redis Consume XGROUP CREATE: %w", err)
	}

	batchSize := consumerBatchSize(sub)

	blockTime := 2 * time.Second
	if val, ok := sub.Topology.Options["redis.block"].(time.Duration); ok && val > 0 {
		blockTime = val
	}

	// Serialize reclaim and fresh intake through the same callback. Each pass
	// scans one bounded page; preserve the cursor even when no entries qualify.
	cursor := "0-0"
	nextReclaim := time.Time{}
	for ctx.Err() == nil {
		if !time.Now().Before(nextReclaim) {
			next, err := t.reclaimPage(ctx, sub, consumer, cursor, deliver)
			if err != nil {
				return fmt.Errorf("redis reclaim: %w", err)
			}
			cursor = next
			if cursor == "0-0" {
				nextReclaim = time.Now().Add(30 * time.Second)
			}
		}
		// Read from Stream
		res, err := t.rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    batchSize,
			Block:    min(blockTime, 2*time.Second),
		}).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			if err == goredis.Nil {
				// Block timeout
				continue
			}
			return err
		}

		for _, streamMsgs := range res {
			for _, xmsg := range streamMsgs.Messages {
				if err := t.deliverMessage(ctx, sub, xmsg, deliver, consumer); err != nil {
					return err
				}
			}
		}
	}
	return ctx.Err()
}

func consumerBatchSize(sub transport.Subscription) int64 {
	batchSize := int64(1)
	if val, ok := sub.Topology.Options["redis.batch_size"].(int64); ok && val > 0 {
		batchSize = val
	}
	if capacity := int64(sub.Concurrency + sub.Prefetch); capacity > 0 {
		batchSize = min(batchSize, capacity)
	}
	return batchSize
}

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (t *redisTransport) Reserve(ctx context.Context, dedupeKey string, ttl time.Duration) (bool, error) {
	// Legacy direct calls have no topology information. Isolate their storage
	// from topology-scoped engine reservations.
	return t.rc.SetNX(ctx, "eventbus:dedupe:"+dedupeKey, "1", ttl).Result()
}

func (t *redisTransport) Release(ctx context.Context, dedupeKey string) error {
	key := "eventbus:dedupe:" + dedupeKey
	return t.rc.Del(ctx, key).Err()
}

func (t *redisTransport) ReserveTopology(ctx context.Context, topo transport.Topology, key string, ttl time.Duration) (bool, error) {
	if err := t.ValidateTopology(topo); err != nil {
		return false, err
	}
	if key == "" || ttl <= 0 {
		return false, fmt.Errorf("redis: dedupe key and positive TTL required")
	}
	return t.rc.SetNX(ctx, t.dedupeKey(topo, key), "1", ttl).Result()
}

func (t *redisTransport) ReleaseTopology(ctx context.Context, topo transport.Topology, key string) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("redis: dedupe key required")
	}
	return t.rc.Del(ctx, t.dedupeKey(topo, key)).Err()
}

func (t *redisTransport) Ping(ctx context.Context) error {
	return t.rc.Ping(ctx).Err()
}

func (t *redisTransport) Close(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, cancel := range t.sweepers {
		cancel()
	}
	t.sweepers = make(map[string]context.CancelFunc)
	return nil
}

// redisDelivery implements Delivery
type redisDelivery struct {
	transport *redisTransport
	topo      transport.Topology
	stream    string
	group     string
	consumer  string
	xmsgID    string
	msg       transport.Message
	count     int
	settled   transport.SettlementGuard
}

func (d *redisDelivery) Message() transport.Message { return d.msg }
func (d *redisDelivery) DeliveryAttempt() int       { return d.count }

func (d *redisDelivery) ack(ctx context.Context) error {
	if d.consumer != "" {
		n, err := ackOwnedScript.Run(ctx, d.transport.rc, []string{d.stream}, d.group, d.xmsgID, d.consumer).Int64()
		if err != nil {
			return err
		}
		if n != 1 {
			return transport.ErrLeaseLost
		}
		return nil
	}
	return d.transport.rc.XAck(ctx, d.stream, d.group, d.xmsgID).Err()
}

func (d *redisDelivery) Settle(ctx context.Context, decision transport.Decision) error {
	return d.settled.Settle(decision, func() error {
		switch decision.Kind {
		case transport.DecisionAck:
			return d.ack(ctx)
		case transport.DecisionRetry:
		case transport.DecisionDeadLetter:
			// Both transfer outcomes share the broker-side pending check/write/ACK.
		default:
			return transport.ErrInvalidDecision
		}
		return d.transfer(ctx, decision)
	})
}

func decisionError(decision transport.Decision) string {
	if decision.Cause == nil {
		return ""
	}
	return decision.Cause.Error()
}
