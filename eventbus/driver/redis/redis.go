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
	luaPublishReady = `
local stream = KEYS[1]
local limit = tonumber(ARGV[1])
local current = redis.call("XLEN", stream)
if current >= limit then return -(current + 1) end
redis.call("XADD", stream, "*", "payload", ARGV[2])
return current + 1
`
	luaPublishDelayed = `
local delayed = KEYS[1]
local limit = tonumber(ARGV[1])
local current = redis.call("ZCARD", delayed)
if not redis.call("ZSCORE", delayed, ARGV[3]) and current >= limit then
    return -(current + 1)
end
redis.call("ZADD", delayed, ARGV[2], ARGV[3])
return current + 1
`
	// Lua script to move delayed jobs to the stream atomically
	luaMoveDelayed = `
local zset = KEYS[1]
local stream = KEYS[2]
local max_score = ARGV[1]
local limit = ARGV[2]
local ready_limit = tonumber(ARGV[3])
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
local current = redis.call("XLEN", stream)
if current + #jobs > ready_limit then return -(current + 1) end
for _, job in ipairs(jobs) do
    redis.call("XADD", stream, "*", "payload", job)
    redis.call("ZREM", zset, job)
end
return #jobs
`
	luaBeginPublication = `
local current = redis.call("GET", KEYS[1])
if not current then
    local created = redis.call("SET", KEYS[1], "r:" .. ARGV[1], "PX", ARGV[2], "NX")
    if created then return 1 end
    current = redis.call("GET", KEYS[1])
end
if current and string.sub(current, 1, 2) == "p:" then return 2 end
return 3
`
	luaConfirmPublication = `
local current = redis.call("GET", KEYS[1])
if current == "p:" .. ARGV[1] then return 1 end
if current ~= "r:" .. ARGV[1] then return 0 end
redis.call("SET", KEYS[1], "p:" .. ARGV[1], "KEEPTTL")
return 1
`
	luaAbortPublication = `
local current = redis.call("GET", KEYS[1])
if not current then return 1 end
if current ~= "r:" .. ARGV[1] then return 0 end
redis.call("DEL", KEYS[1])
return 1
`
)

type redisTransport struct {
	rc goredis.UniversalClient

	mu                  sync.Mutex
	sweepers            map[string]context.CancelFunc
	sweeperWG           sync.WaitGroup
	schedulerStatuses   map[string]transport.SchedulerStatus
	sweepDelay          func(context.Context, transport.Topology, time.Time) (int64, error)
	closed              bool
	retentionTopologies map[string]transport.Topology
	retentionStatuses   map[string]RetentionStatus
	capacityRejections  map[string]map[string]uint64
	retentionCancel     context.CancelFunc
	retentionDone       chan struct{}
	retentionWake       chan struct{}
}

var (
	_ transport.Transport               = (*redisTransport)(nil)
	_ transport.Deduper                 = (*redisTransport)(nil)
	_ transport.TopologyDeduper         = (*redisTransport)(nil)
	_ transport.PublicationDeduper      = (*redisTransport)(nil)
	_ transport.DLQAdmin                = (*redisTransport)(nil)
	_ transport.CursorDLQAdmin          = (*redisTransport)(nil)
	_ transport.StatsReader             = (*redisTransport)(nil)
	_ transport.SubscriptionStatsReader = (*redisTransport)(nil)
	_ transport.TopologyValidator       = (*redisTransport)(nil)
	_ transport.SubscriptionProvisioner = (*redisTransport)(nil)
	_ transport.SchedulerHealthReader   = (*redisTransport)(nil)
	_ RetentionAdmin                    = (*redisTransport)(nil)
	_ transport.Delivery                = (*redisDelivery)(nil)
)

func New(rc goredis.UniversalClient) transport.Transport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &redisTransport{
		rc: rc, sweepers: make(map[string]context.CancelFunc),
		schedulerStatuses:   make(map[string]transport.SchedulerStatus),
		retentionTopologies: make(map[string]transport.Topology),
		retentionStatuses:   make(map[string]RetentionStatus),
		capacityRejections:  make(map[string]map[string]uint64),
		retentionCancel:     cancel, retentionDone: make(chan struct{}), retentionWake: make(chan struct{}, 1),
	}
	t.sweepDelay = t.dispatchDue
	go t.retentionLoop(ctx)
	return t
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
	if t.closed {
		return fmt.Errorf("redis: transport closed")
	}
	topoName := topologyPrefix(topo)
	if _, ok := t.sweepers[topoName]; !ok {
		sweepCtx, cancel := context.WithCancel(context.Background())
		t.sweepers[topoName] = cancel
		t.sweeperWG.Add(1)
		go func() {
			defer t.sweeperWG.Done()
			t.delaySweeper(sweepCtx, topo)
		}()
	}
	t.retentionTopologies[topoName] = topo
	select {
	case t.retentionWake <- struct{}{}:
	default:
	}

	return nil
}

func (t *redisTransport) Publish(ctx context.Context, topo transport.Topology, msg transport.Message) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	result, err := publishReadyScript.Run(ctx, t.rc, []string{t.streamKey(topo)}, maxReadyEntries(topo), msg.Payload).Int64()
	if err != nil {
		return fmt.Errorf("redis: publish ready: %w", err)
	}
	if result < 0 {
		return t.capacityError(topo, "ready", -result-1, maxReadyEntries(topo))
	}
	return nil
}

func (t *redisTransport) PublishAt(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	delayed := t.delayedKey(topo)
	score := float64(runAt.UnixMilli())

	result, err := publishDelayedScript.Run(ctx, t.rc, []string{delayed}, maxDelayedEntries(topo), score, msg.Payload).Int64()
	if err != nil {
		return fmt.Errorf("redis: publish delayed: %w", err)
	}
	if result < 0 {
		return t.capacityError(topo, "delayed", -result-1, maxDelayedEntries(topo))
	}
	return nil
}

func (t *redisTransport) delaySweeper(ctx context.Context, topo transport.Topology) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = t.sweepDelay(ctx, topo, time.Now())
		}
	}
}

var (
	moveDelayedScript    = goredis.NewScript(luaMoveDelayed)
	publishReadyScript   = goredis.NewScript(luaPublishReady)
	publishDelayedScript = goredis.NewScript(luaPublishDelayed)
)

// dispatchDue performs one bounded broker-side move. Lua errors do not roll
// back prior writes: validate types first and remove each source only after
// its destination write, so partial failure cannot silently lose a job.
func (t *redisTransport) dispatchDue(ctx context.Context, topo transport.Topology, now time.Time) (int64, error) {
	t.recordSchedulerPoll(topo, now)
	if err := t.ValidateTopology(topo); err != nil {
		t.recordSchedulerError(topo, "poll", err, now)
		return 0, err
	}
	limit := 100
	if value, ok := topo.Options["redis.delay_batch_size"].(int); ok {
		limit = value
	}
	moved, err := moveDelayedScript.Run(ctx, t.rc, []string{t.delayedKey(topo), t.streamKey(topo)}, now.UnixMilli(), limit, maxReadyEntries(topo)).Int64()
	if err != nil {
		t.recordSchedulerError(topo, "dispatch", err, now)
		return 0, err
	}
	if moved < 0 {
		err := t.capacityError(topo, "ready", -moved-1, maxReadyEntries(topo))
		t.recordSchedulerError(topo, "transfer", err, now)
		return 0, err
	}
	t.recordSchedulerSuccess(topo, moved, now)
	return moved, nil
}

func (t *redisTransport) recordSchedulerPoll(topo transport.Topology, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := topologyPrefix(topo)
	status := t.schedulerStatuses[key]
	status.Transport, status.Topology, status.Worker = "redis", topo.Name(), "delayed"
	status.LastPoll = now
	status.Polls++
	t.schedulerStatuses[key] = status
}

func (t *redisTransport) recordSchedulerSuccess(topo transport.Topology, moved int64, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := topologyPrefix(topo)
	status := t.schedulerStatuses[key]
	status.LastSuccess = now
	status.Dispatches++
	status.Transfers += uint64(moved)
	status.Commits += uint64(moved)
	status.ConsecutiveErrors = 0
	status.Degraded = false
	t.schedulerStatuses[key] = status
}

func (t *redisTransport) recordSchedulerError(topo transport.Topology, operation string, err error, now time.Time) {
	if errors.Is(err, context.Canceled) {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	key := topologyPrefix(topo)
	status := t.schedulerStatuses[key]
	status.Transport, status.Topology, status.Worker = "redis", topo.Name(), "delayed"
	status.LastErrorAt = now
	status.LastErrorOperation = operation
	status.LastError = boundedSchedulerError(err)
	status.Retries++
	status.ConsecutiveErrors++
	status.Degraded = true
	t.schedulerStatuses[key] = status
}

func (t *redisTransport) SchedulerHealth(topo transport.Topology) []transport.SchedulerStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	status, ok := t.schedulerStatuses[topologyPrefix(topo)]
	if !ok {
		return nil
	}
	return []transport.SchedulerStatus{status}
}

func boundedSchedulerError(err error) string {
	if err == nil {
		return ""
	}
	text := fmt.Sprintf("%T", err)
	if len(text) > transport.MaxSchedulerErrorBytes {
		text = text[:transport.MaxSchedulerErrorBytes]
	}
	return text
}

func (t *redisTransport) capacityError(topo transport.Topology, resource string, current, limit int64) error {
	t.mu.Lock()
	key := topologyPrefix(topo)
	if t.capacityRejections[key] == nil {
		t.capacityRejections[key] = make(map[string]uint64)
	}
	t.capacityRejections[key][resource]++
	t.mu.Unlock()
	select {
	case t.retentionWake <- struct{}{}:
	default:
	}
	return &transport.CapacityError{Transport: "redis", Topology: topologyPrefix(topo), Resource: resource, Current: current, Limit: limit}
}

func (t *redisTransport) Consume(ctx context.Context, sub transport.Subscription, deliver func(transport.Delivery)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream := t.streamKey(sub.Topology)
	group := t.groupName(sub)
	consumer := "consumer-" + generateID() // need a unique consumer name

	if err := t.EnsureSubscription(ctx, sub); err != nil {
		return err
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

var (
	beginPublicationScript   = goredis.NewScript(luaBeginPublication)
	confirmPublicationScript = goredis.NewScript(luaConfirmPublication)
	abortPublicationScript   = goredis.NewScript(luaAbortPublication)
)

func (t *redisTransport) BeginPublication(ctx context.Context, topo transport.Topology, key, reservationID string, ttl time.Duration) (transport.DedupeReservationState, error) {
	if err := t.ValidateTopology(topo); err != nil {
		return 0, err
	}
	if key == "" || reservationID == "" || ttl <= 0 {
		return 0, fmt.Errorf("redis: dedupe key, reservation ID, and positive TTL required")
	}
	ttlMillis := max(int64(1), ttl.Milliseconds())
	state, err := beginPublicationScript.Run(ctx, t.rc, []string{t.dedupeKey(topo, key)}, reservationID, ttlMillis).Int()
	if err != nil {
		return 0, fmt.Errorf("redis dedupe begin: %w", err)
	}
	return transport.DedupeReservationState(state), nil
}

func (t *redisTransport) ConfirmPublication(ctx context.Context, topo transport.Topology, key, reservationID string) error {
	confirmed, err := confirmPublicationScript.Run(ctx, t.rc, []string{t.dedupeKey(topo, key)}, reservationID).Int()
	if err != nil {
		return fmt.Errorf("redis dedupe confirm: %w", err)
	}
	if confirmed != 1 {
		return transport.ErrDedupeAmbiguous
	}
	return nil
}

func (t *redisTransport) AbortPublication(ctx context.Context, topo transport.Topology, key, reservationID string) error {
	aborted, err := abortPublicationScript.Run(ctx, t.rc, []string{t.dedupeKey(topo, key)}, reservationID).Int()
	if err != nil {
		return fmt.Errorf("redis dedupe abort: %w", err)
	}
	if aborted != 1 {
		return transport.ErrDedupeAmbiguous
	}
	return nil
}

func (t *redisTransport) Ping(ctx context.Context) error {
	return t.rc.Ping(ctx).Err()
}

func (t *redisTransport) Close(ctx context.Context) error {
	t.mu.Lock()
	t.closed = true
	for _, cancel := range t.sweepers {
		cancel()
	}
	t.sweepers = make(map[string]context.CancelFunc)
	retentionCancel := t.retentionCancel
	retentionDone := t.retentionDone
	t.mu.Unlock()
	retentionCancel()
	done := make(chan struct{})
	go func() {
		t.sweeperWG.Wait()
		<-retentionDone
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
