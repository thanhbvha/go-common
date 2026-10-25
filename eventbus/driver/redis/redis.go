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
	ensureTopologyMetadataLua = `
local metadata = KEYS[1]
local expected = {"layout_version", ARGV[1], "namespace", ARGV[2], "kind", ARGV[3], "name", ARGV[4]}
for index = 1, #expected, 2 do
    local current = redis.call("HGET", metadata, expected[index])
    if current and current ~= expected[index + 1] then return -1 end
end
if ARGV[5] ~= "" then
    local current = redis.call("HGET", metadata, "event_layout")
    if current and current ~= ARGV[5] then return -2 end
end
redis.call("HSET", metadata, unpack(expected))
if ARGV[5] ~= "" then redis.call("HSET", metadata, "event_layout", ARGV[5]) end
return 1
`
	luaPublishReady = `
local stream = KEYS[1]
local metadata = KEYS[2]
local limit = tonumber(ARGV[1])
if redis.call("HGET", metadata, "capacity_state") ~= "ready" then return redis.error_reply("capacity counters are initializing") end
local current = tonumber(redis.call("HGET", metadata, "ready_entries"))
if not current then return redis.error_reply("ready capacity counter is missing") end
if current >= limit then return -(current + 1) end
redis.call("XADD", stream, "*", "payload", ARGV[2])
redis.call("HSET", metadata, "ready_entries", current + 1)
return current + 1
`
	luaPublishDelayed = `
local delayed = KEYS[1]
local metadata = KEYS[2]
local limit = tonumber(ARGV[1])
if redis.call("HGET", metadata, "capacity_state") ~= "ready" then return redis.error_reply("capacity counters are initializing") end
local current = tonumber(redis.call("HGET", metadata, "delayed_entries"))
if not current then return redis.error_reply("delayed capacity counter is missing") end
local exists = redis.call("ZSCORE", delayed, ARGV[3])
if not exists and current >= limit then
    return -(current + 1)
end
redis.call("ZADD", delayed, ARGV[2], ARGV[3])
if not exists then
    current = current + 1
    redis.call("HSET", metadata, "delayed_entries", current)
end
return current
`
	// Lua script to move delayed jobs to the stream atomically
	luaMoveDelayed = `
local zset = KEYS[1]
local stream = KEYS[2]
local metadata = KEYS[3]
local max_score = ARGV[1]
local limit = ARGV[2]
local ready_limit = tonumber(ARGV[3])
if redis.call("HGET", metadata, "capacity_state") ~= "ready" then return redis.error_reply("capacity counters are initializing") end
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
local current = tonumber(redis.call("HGET", metadata, "ready_entries"))
if not current then return redis.error_reply("ready capacity counter is missing") end
if current + #jobs > ready_limit then return -(current + 1) end
for _, job in ipairs(jobs) do
    redis.call("XADD", stream, "*", "payload", job)
    redis.call("ZREM", zset, job)
end
if #jobs > 0 then
    redis.call("HSET", metadata, "ready_entries", current + #jobs)
    local delayed = tonumber(redis.call("HGET", metadata, "delayed_entries")) or #jobs
    redis.call("HSET", metadata, "delayed_entries", math.max(0, delayed - #jobs))
end
return #jobs
`
	luaBeginPublication = `
local current = redis.call("GET", KEYS[1])
if not current then
    local created = redis.call("SET", KEYS[1], "r:" .. ARGV[1], "PX", ARGV[2], "NX")
    if created then return {1, ARGV[1]} end
    current = redis.call("GET", KEYS[1])
end
if current and string.sub(current, 1, 2) == "p:" then return {2, string.sub(current, 3)} end
if current and string.sub(current, 1, 2) == "r:" then return {3, string.sub(current, 3)} end
return {3, ""}
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

const maxEventLaneQuantum = 16

var ensureTopologyMetadataScript = goredis.NewScript(ensureTopologyMetadataLua)

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
	dispatchCursors     map[string]string
	dispatchShared      map[string]bool
	capacityReady       map[string]bool
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
		dispatchCursors:     make(map[string]string),
		dispatchShared:      make(map[string]bool),
		capacityReady:       make(map[string]bool),
		retentionCancel:     cancel, retentionDone: make(chan struct{}), retentionWake: make(chan struct{}, 1),
	}
	t.sweepDelay = t.dispatchDue
	go t.retentionLoop(ctx)
	return t
}

func (t *redisTransport) Name() string { return "redis" }

func (t *redisTransport) Capabilities() transport.Capabilities {
	return transport.Capabilities{
		Durability:     transport.DurabilityBroker,
		Delay:          transport.DelayCapabilities{Supported: true, Precision: time.Second, Provisioning: transport.DelayPerTopologyScheduler, Optional: true},
		Settlement:     transport.SettlementCapabilities{Retry: transport.SettlementAtomic, DLQ: transport.SettlementAtomic},
		Dedupe:         transport.DedupeBrokerConfirmed,
		Heartbeat:      transport.ScopeBroker,
		Admin:          transport.AdminCapabilities{DLQ: true, CursorDLQ: true, Stats: true, SubscriptionStats: true, Pause: transport.ScopeProcess},
		Scheduler:      transport.SchedulerCapabilities{Health: true, WorkerScope: transport.ScopeBroker},
		Payload:        transport.PayloadCapabilities{CoreLimitSupported: true},
		DelayPrecision: time.Second,
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
	eventLayout := ""
	if topo.Kind == transport.EventTopology {
		eventLayout = redisTargetedLayoutV1
	}
	result, err := ensureTopologyMetadataScript.Run(ctx, t.rc, []string{t.metadataKey(topo)},
		"1", topo.Namespace, topo.Kind.String(), topo.JobType, eventLayout).Int64()
	if err != nil {
		return fmt.Errorf("redis EnsureTopology metadata: %w", err)
	}
	if result == -1 {
		return fmt.Errorf("redis: topology metadata identity mismatch")
	}
	if result == -2 {
		return fmt.Errorf("redis: event layout marker mismatch")
	}
	if err := t.ensureCapacityCounters(ctx, topo); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return fmt.Errorf("redis: transport closed")
	}
	topoName := topologyPrefix(topo)
	delayDisabled, _ := topo.Options["eventbus.delay_disabled"].(bool)
	if !delayDisabled {
		if _, ok := t.sweepers[topoName]; !ok {
			sweepCtx, cancel := context.WithCancel(context.Background())
			t.sweepers[topoName] = cancel
			t.sweeperWG.Add(1)
			go func() {
				defer t.sweeperWG.Done()
				t.delaySweeper(sweepCtx, topo)
			}()
		}
	}
	t.retentionTopologies[topoName] = topo
	select {
	case t.retentionWake <- struct{}{}:
	default:
	}

	return nil
}

func (t *redisTransport) Publish(ctx context.Context, topo transport.Topology, msg transport.Message) error {
	return t.publishReady(ctx, topo, msg, true)
}

func (t *redisTransport) publishReady(ctx context.Context, topo transport.Topology, msg transport.Message, retryCapacity bool) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	if err := t.ensureCapacityCounters(ctx, topo); err != nil {
		return err
	}
	result, err := publishReadyScript.Run(ctx, t.rc, []string{t.streamKey(topo), t.metadataKey(topo)}, maxReadyEntries(topo), msg.Payload).Int64()
	if err != nil {
		t.invalidateCapacityOnScriptError(topo, err)
		if retryCapacity && isCapacityNotReadyError(err) {
			if initErr := t.ensureCapacityCounters(ctx, topo); initErr != nil {
				return fmt.Errorf("redis: capacity counters are initializing: %w", initErr)
			}
			return t.publishReady(ctx, topo, msg, false)
		}
		return fmt.Errorf("redis: publish ready: %w", err)
	}
	if result < 0 {
		return t.capacityError(topo, "ready", -result-1, maxReadyEntries(topo))
	}
	return nil
}

func (t *redisTransport) PublishAt(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time) error {
	return t.publishDelayed(ctx, topo, msg, runAt, true)
}

func (t *redisTransport) publishDelayed(ctx context.Context, topo transport.Topology, msg transport.Message, runAt time.Time, retryCapacity bool) error {
	if err := t.ValidateTopology(topo); err != nil {
		return err
	}
	if disabled, _ := topo.Options["eventbus.delay_disabled"].(bool); disabled {
		if runAt.After(time.Now()) {
			return transport.ErrNotSupported
		}
		return t.publishReady(ctx, topo, msg, retryCapacity)
	}
	if err := t.ensureCapacityCounters(ctx, topo); err != nil {
		return err
	}
	delayed := t.delayedKey(topo)
	score := float64(runAt.UnixMilli())

	result, err := publishDelayedScript.Run(ctx, t.rc, []string{delayed, t.metadataKey(topo)}, maxDelayedEntries(topo), score, msg.Payload).Int64()
	if err != nil {
		t.invalidateCapacityOnScriptError(topo, err)
		if retryCapacity && isCapacityNotReadyError(err) {
			if initErr := t.ensureCapacityCounters(ctx, topo); initErr != nil {
				return fmt.Errorf("redis: capacity counters are initializing: %w", initErr)
			}
			return t.publishDelayed(ctx, topo, msg, runAt, false)
		}
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
	if err := t.ensureCapacityCounters(ctx, topo); err != nil {
		t.recordSchedulerError(topo, "capacity-init", err, now)
		return 0, err
	}
	limit := 100
	if value, ok := topo.Options["redis.delay_batch_size"].(int); ok {
		limit = value
	}
	type lane struct{ delayed, ready string }
	lanes := make([]lane, 0, schedulerLaneScanMax+1)
	key := topologyPrefix(topo)
	t.mu.Lock()
	cursor, shared := t.dispatchCursors[key], t.dispatchShared[key]
	t.mu.Unlock()
	if topo.Kind != transport.EventTopology || shared {
		lanes = append(lanes, lane{t.delayedKey(topo), t.streamKey(topo)})
	}
	if topo.Kind == transport.EventTopology {
		minimum := "-"
		if cursor != "" {
			minimum = "(" + cursor
		}
		groups, err := t.rc.ZRangeByLex(ctx, t.groupIndexKey(topo), &goredis.ZRangeBy{Min: minimum, Max: "+", Offset: 0, Count: schedulerLaneScanMax}).Result()
		if err != nil {
			t.recordSchedulerError(topo, "dispatch", err, now)
			return 0, fmt.Errorf("redis: scan subscriber retry lanes: %w", err)
		}
		if len(groups) == 0 && cursor != "" {
			groups, err = t.rc.ZRangeByLex(ctx, t.groupIndexKey(topo), &goredis.ZRangeBy{Min: "-", Max: "+", Offset: 0, Count: schedulerLaneScanMax}).Result()
			if err != nil {
				t.recordSchedulerError(topo, "dispatch", err, now)
				return 0, fmt.Errorf("redis: wrap subscriber retry lanes: %w", err)
			}
		}
		for _, group := range groups {
			lanes = append(lanes, lane{t.subscriberRetryDelayedKey(topo, group), t.subscriberRetryStreamKey(topo, group)})
		}
		t.mu.Lock()
		if len(groups) > 0 {
			t.dispatchCursors[key] = groups[len(groups)-1]
		} else {
			t.dispatchCursors[key] = ""
		}
		t.dispatchShared[key] = !shared
		t.mu.Unlock()
	}
	var moved int64
	for index, current := range lanes {
		if moved >= int64(limit) {
			break
		}
		batch := limit - int(moved)
		if topo.Kind == transport.EventTopology {
			remainingLanes := len(lanes) - index
			batch = max(1, batch/remainingLanes)
			batch = min(batch, maxEventLaneQuantum)
		}
		count, err := moveDelayedScript.Run(ctx, t.rc, []string{current.delayed, current.ready, t.metadataKey(topo)}, now.UnixMilli(), batch, maxReadyEntries(topo)).Int64()
		if err != nil {
			t.invalidateCapacityOnScriptError(topo, err)
			t.recordSchedulerError(topo, "dispatch", err, now)
			return moved, err
		}
		if count < 0 {
			err := t.capacityError(topo, "ready", -count-1, maxReadyEntries(topo))
			t.recordSchedulerError(topo, "transfer", err, now)
			return moved, err
		}
		moved += count
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
	streams := []string{t.streamKey(sub.Topology)}
	group := t.groupName(sub)
	if sub.Topology.Kind == transport.EventTopology {
		streams = append(streams, t.subscriberRetryStreamKey(sub.Topology, group))
	}
	consumer := "consumer-" + generateID() // need a unique consumer name

	if err := t.EnsureSubscription(ctx, sub); err != nil {
		return err
	}

	capacity := sub.Concurrency + sub.Prefetch
	if capacity <= 0 {
		capacity = 1
	}
	permits := make(chan struct{}, capacity)
	acquire := func() error {
		select {
		case permits <- struct{}{}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	release := func() { <-permits }
	reserveBatch := func(limit int) (int, error) {
		if err := acquire(); err != nil {
			return 0, err
		}
		reserved := 1
		for reserved < limit {
			select {
			case permits <- struct{}{}:
				reserved++
			default:
				return reserved, nil
			}
		}
		return reserved, nil
	}
	releaseN := func(count int) {
		for range count {
			release()
		}
	}

	blockTime := 2 * time.Second
	if val, ok := sub.Topology.Options["redis.block"].(time.Duration); ok && val > 0 {
		blockTime = val
	}

	// Serialize reclaim and fresh intake through the same callback. Each pass
	// scans one bounded page; preserve the cursor even when no entries qualify.
	cursors := make([]string, len(streams))
	for i := range cursors {
		cursors[i] = "0-0"
	}
	nextReclaim := time.Time{}
	preferred := 0
	for ctx.Err() == nil {
		if !time.Now().Before(nextReclaim) {
			complete := true
			for offset := range len(streams) {
				index := (preferred + offset) % len(streams)
				if err := acquire(); err != nil {
					return err
				}
				next, delivered, err := t.reclaimStreamPageWithRelease(ctx, sub, streams[index], consumer, cursors[index], deliver, 1, release)
				if err != nil {
					if !delivered {
						release()
					}
					return fmt.Errorf("redis reclaim: %w", err)
				}
				if !delivered {
					release()
				}
				cursors[index] = next
				complete = complete && next == "0-0"
			}
			if complete {
				interval := 30 * time.Second
				if ackWait, ok := sub.Topology.Options["redis.ack_wait"].(time.Duration); ok {
					interval = min(interval, ackWait)
				}
				nextReclaim = time.Now().Add(interval)
			}
		}
		reserved, err := reserveBatch(int(consumerBatchSize(sub)))
		if err != nil {
			return err
		}
		stream := streams[preferred]
		res, err := t.rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    int64(reserved),
			Block:    min(blockTime, 2*time.Second),
		}).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) {
				releaseN(reserved)
				return ctx.Err()
			}
			if err == goredis.Nil {
				releaseN(reserved)
				// Block timeout
				preferred = (preferred + 1) % len(streams)
				continue
			}
			releaseN(reserved)
			return err
		}

		deliveredCount := 0
		for _, streamMsgs := range res {
			for _, xmsg := range streamMsgs.Messages {
				delivered, deliverErr := t.deliverStreamMessageWithRelease(ctx, sub, streamMsgs.Stream, xmsg, deliver, release, consumer)
				if delivered {
					deliveredCount++
				} else {
					release()
				}
				if deliverErr != nil {
					remaining := reserved - deliveredCount
					if !delivered {
						remaining--
					}
					releaseN(remaining)
					return deliverErr
				}
			}
		}
		if unused := reserved - deliveredCount; unused > 0 {
			releaseN(unused)
		}
		preferred = (preferred + 1) % len(streams)
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
	if err := t.validateTopologyOptions(topo, false); err != nil {
		return false, err
	}
	if key == "" || ttl <= 0 {
		return false, fmt.Errorf("redis: dedupe key and positive TTL required")
	}
	return t.rc.SetNX(ctx, t.dedupeKey(topo, key), "1", ttl).Result()
}

func (t *redisTransport) ReleaseTopology(ctx context.Context, topo transport.Topology, key string) error {
	if err := t.validateTopologyOptions(topo, false); err != nil {
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
	reservation, err := t.BeginPublicationWithResult(ctx, topo, key, reservationID, ttl)
	return reservation.State, err
}

func (t *redisTransport) BeginPublicationWithResult(ctx context.Context, topo transport.Topology, key, reservationID string, ttl time.Duration) (transport.PublicationReservation, error) {
	if err := t.validateTopologyOptions(topo, false); err != nil {
		return transport.PublicationReservation{}, err
	}
	if key == "" || reservationID == "" || ttl <= 0 {
		return transport.PublicationReservation{}, fmt.Errorf("redis: dedupe key, reservation ID, and positive TTL required")
	}
	ttlMillis := max(int64(1), ttl.Milliseconds())
	values, err := beginPublicationScript.Run(ctx, t.rc, []string{t.dedupeKey(topo, key)}, reservationID, ttlMillis).Slice()
	if err != nil {
		return transport.PublicationReservation{}, fmt.Errorf("redis dedupe begin: %w", err)
	}
	if len(values) != 2 {
		return transport.PublicationReservation{}, fmt.Errorf("redis dedupe begin returned %d values", len(values))
	}
	state, ok := values[0].(int64)
	if !ok {
		return transport.PublicationReservation{}, fmt.Errorf("redis dedupe begin returned invalid state %T", values[0])
	}
	owner, ok := values[1].(string)
	if !ok {
		return transport.PublicationReservation{}, fmt.Errorf("redis dedupe begin returned invalid owner %T", values[1])
	}
	return transport.PublicationReservation{State: transport.DedupeReservationState(state), MessageID: owner}, nil
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
	transport   *redisTransport
	topo        transport.Topology
	stream      string
	group       string
	consumer    string
	xmsgID      string
	msg         transport.Message
	count       int
	settled     transport.SettlementGuard
	release     func()
	releaseOnce sync.Once
}

func (d *redisDelivery) Message() transport.Message { return d.msg }
func (d *redisDelivery) DeliveryAttempt() int       { return d.count }

func (d *redisDelivery) ack(ctx context.Context) error {
	if d.consumer != "" {
		deleteSource := d.topo.Kind == transport.EventTopology && d.stream != d.transport.streamKey(d.topo)
		if deleteSource {
			if err := d.transport.ensureCapacityCounters(ctx, d.topo); err != nil {
				return err
			}
		}
		if err := d.transport.validateEventLayout(ctx, d.topo); err != nil {
			return err
		}
		n, err := ackOwnedScript.Run(ctx, d.transport.rc, []string{d.stream, d.transport.metadataKey(d.topo)}, d.group, d.xmsgID, d.consumer, boolInt(deleteSource)).Int64()
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
	err := d.settled.Settle(decision, func() error {
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
	if err == nil {
		d.releaseOnce.Do(func() {
			if d.release != nil {
				d.release()
			}
		})
	}
	return err
}
