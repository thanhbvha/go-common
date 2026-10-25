package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	groupStateActive   = "active"
	groupStateRetiring = "retiring:"
	retentionLeaseTTL  = 15 * time.Second
	retirementScanPage = 1000
	retentionCycleMax  = 64
)

var (
	// ErrSubscriptionRetiring prevents a consumer restart from silently
	// cancelling an explicit administrative retirement.
	ErrSubscriptionRetiring = errors.New("redis: subscription is retiring")
	// ErrSubscriptionNotDrained means safe retirement has started, but the
	// group still owns pending or unread work up to its retirement barrier.
	ErrSubscriptionNotDrained = errors.New("redis: subscription is not drained")
)

var (
	ensureSubscriptionScript = goredis.NewScript(`
local stream = KEYS[1]
local groups = KEYS[2]
local metadata = KEYS[3]
local private = KEYS[4]
local group_index = KEYS[5]
local group = ARGV[1]
local stream_type = redis.call("TYPE", stream).ok
local private_type = redis.call("TYPE", private).ok
if stream_type ~= "none" and stream_type ~= "stream" then return redis.error_reply("subscription source must be a stream") end
if private_type ~= "none" and private_type ~= "stream" then return redis.error_reply("subscription retry source must be a stream") end
local created = redis.pcall("XGROUP", "CREATE", stream, group, "0", "MKSTREAM")
if type(created) == "table" and created.err and not string.find(created.err, "BUSYGROUP", 1, true) then
    return redis.error_reply(created.err)
end
local state = redis.call("HGET", groups, group)
if state and string.sub(state, 1, 9) == "retiring:" then return -1 end
created = redis.pcall("XGROUP", "CREATE", private, group, "0", "MKSTREAM")
if type(created) == "table" and created.err and not string.find(created.err, "BUSYGROUP", 1, true) then
    return redis.error_reply(created.err)
end
if state ~= "active" then
    redis.call("HSET", groups, group, "active")
    redis.call("ZADD", group_index, 0, group)
    return redis.call("HINCRBY", metadata, "retention_epoch", 1)
end
redis.call("ZADD", group_index, 0, group)
local epoch = redis.call("HGET", metadata, "retention_epoch")
if not epoch then return 0 end
return tonumber(epoch)
`)
	trimAtEpochScript = goredis.NewScript(`
if redis.call("HGET", KEYS[2], "capacity_state") ~= "ready" then return redis.error_reply("capacity counters are initializing") end
local epoch = redis.call("HGET", KEYS[2], "retention_epoch")
if not epoch then epoch = "0" end
if epoch ~= ARGV[1] then return -1 end
local manifest = redis.call("HGETALL", KEYS[3])
local managed = {}
for index = 1, #manifest, 2 do managed[manifest[index]] = true end
local groups = redis.call("XINFO", "GROUPS", KEYS[1])
for _, group in ipairs(groups) do
    local name = nil
    for index = 1, #group, 2 do
        if group[index] == "name" then name = group[index + 1] end
    end
    local relevant = name == "workers" or string.sub(name or "", 1, 11) == "subscriber:"
    if relevant and not managed[name] then return -2 end
end
local batch = tonumber(ARGV[3])
if not batch or batch < 1 then return redis.error_reply("invalid retention batch") end
local candidates = redis.call("XRANGE", KEYS[1], "-", "(" .. ARGV[2], "COUNT", batch + 1)
if #candidates == 0 then return 0 end
local threshold = ARGV[2]
if #candidates > batch then threshold = candidates[batch + 1][1] end
local trimmed = redis.call("XTRIM", KEYS[1], "MINID", threshold)
if trimmed > 0 then
    local ready = tonumber(redis.call("HGET", KEYS[2], "ready_entries")) or trimmed
    redis.call("HSET", KEYS[2], "ready_entries", math.max(0, ready - trimmed))
end
return trimmed
`)
	releaseRetentionLeaseScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call("DEL", KEYS[1])
`)
	beginRetirementScript = goredis.NewScript(`
local state = redis.call("HGET", KEYS[2], ARGV[1])
if not state then return redis.error_reply("subscription is not provisioned") end
if string.sub(state, 1, 9) == "retiring:" then
    return string.sub(state, 10)
end
local info = redis.call("XINFO", "STREAM", KEYS[1])
local barrier = "0-0"
for index = 1, #info, 2 do
    if info[index] == "last-generated-id" then barrier = info[index + 1] end
end
redis.call("HSET", KEYS[2], ARGV[1], "retiring:" .. barrier)
redis.call("HINCRBY", KEYS[3], "retention_epoch", 1)
return barrier
`)
	finishRetirementScript = goredis.NewScript(`
local expected = "retiring:" .. ARGV[2]
if redis.call("HGET", KEYS[2], ARGV[1]) ~= expected then return -3 end
local function id_ge(left, right)
    local left_dash = string.find(left, "-", 1, true)
    local right_dash = string.find(right, "-", 1, true)
    if not left_dash or not right_dash then return false end
    local left_ms = tonumber(string.sub(left, 1, left_dash - 1))
    local left_seq = tonumber(string.sub(left, left_dash + 1))
    local right_ms = tonumber(string.sub(right, 1, right_dash - 1))
    local right_seq = tonumber(string.sub(right, right_dash + 1))
    if not left_ms or not left_seq or not right_ms or not right_seq then return false end
    return left_ms > right_ms or (left_ms == right_ms and left_seq >= right_seq)
end
local groups = redis.call("XINFO", "GROUPS", KEYS[1])
local delivered = nil
for _, group in ipairs(groups) do
    local name = nil
    local last = nil
    for index = 1, #group, 2 do
        if group[index] == "name" then name = group[index + 1] end
        if group[index] == "last-delivered-id" then last = group[index + 1] end
    end
    if name == ARGV[1] then delivered = last end
end
if not delivered then return -4 end
local pending = redis.call("XPENDING", KEYS[1], ARGV[1])[1]
local private_pending = redis.call("XPENDING", KEYS[4], ARGV[1])[1]
local private_ready = redis.call("XLEN", KEYS[4])
local private_delayed = redis.call("ZCARD", KEYS[5])
if ARGV[3] ~= "1" and (pending > 0 or not id_ge(delivered, ARGV[2]) or private_pending > 0 or private_ready > 0 or private_delayed > 0) then return -2 end
redis.call("XGROUP", "DESTROY", KEYS[1], ARGV[1])
redis.call("XGROUP", "DESTROY", KEYS[4], ARGV[1])
redis.call("DEL", KEYS[4], KEYS[5])
if private_ready > 0 then
    local ready = tonumber(redis.call("HGET", KEYS[3], "ready_entries")) or private_ready
    redis.call("HSET", KEYS[3], "ready_entries", math.max(0, ready - private_ready))
end
if private_delayed > 0 then
    local delayed = tonumber(redis.call("HGET", KEYS[3], "delayed_entries")) or private_delayed
    redis.call("HSET", KEYS[3], "delayed_entries", math.max(0, delayed - private_delayed))
end
redis.call("HDEL", KEYS[2], ARGV[1])
redis.call("ZREM", KEYS[6], ARGV[1])
redis.call("HINCRBY", KEYS[3], "retention_epoch", 1)
return pending
`)
)

// RetentionStatus is a point-in-time operational view of one topology's
// compactor. Blocked is a normal safe state when a group still needs history.
type RetentionStatus struct {
	LastRun          time.Time
	LastSuccess      time.Time
	SafeWatermark    string
	Trimmed          int64
	Blocked          string
	LastError        string
	TotalTrimmed     int64
	EpochConflicts   uint64
	LeaseContentions uint64
}

// SubscriptionRetirement reports the work observed when an explicit
// subscriber retirement was attempted. Retired is false with
// ErrSubscriptionNotDrained until a safe retirement reaches its barrier.
type SubscriptionRetirement struct {
	Group   string
	Barrier string
	Pending int64
	Unread  int64
	Forced  bool
	Retired bool
}

// RetentionReport is a payload-free operational snapshot. Ready is degraded
// by broker errors, manifest conflicts, or exhausted admission capacity; an
// intentionally offline group alone is not a liveness failure.
type RetentionReport struct {
	RetentionStatus
	ReadyEntries, ReadyLimit       int64
	DelayedEntries, DelayedLimit   int64
	DLQEntries, DLQLimit           int64
	ActiveGroups, RetiringGroups   int
	MissingGroups, UnmanagedGroups int
	ManifestEpoch                  string
	CapacityRejections             map[string]uint64
	Ready                          bool
}

// RetentionAdmin is the Redis-specific optional administration surface.
type RetentionAdmin interface {
	RetireSubscription(context.Context, transport.Subscription, bool) (SubscriptionRetirement, error)
	RetentionReport(context.Context, transport.Topology) (RetentionReport, error)
}

func (t *redisTransport) EnsureSubscription(ctx context.Context, sub transport.Subscription) error {
	if err := t.ValidateTopology(sub.Topology); err != nil {
		return err
	}
	if sub.Topology.Kind == transport.EventTopology && strings.TrimSpace(sub.Group) == "" {
		return fmt.Errorf("redis: event subscription group must not be blank")
	}
	if sub.Topology.Kind == transport.TaskTopology && sub.Group != "" {
		return fmt.Errorf("redis: task subscription group must be empty")
	}
	if err := t.validateEventLayout(ctx, sub.Topology); err != nil {
		return err
	}
	group := t.groupName(sub)
	privateStream := t.streamKey(sub.Topology)
	if sub.Topology.Kind == transport.EventTopology {
		privateStream = t.subscriberRetryStreamKey(sub.Topology, group)
	}
	epoch, err := ensureSubscriptionScript.Run(ctx, t.rc, []string{
		t.streamKey(sub.Topology), t.groupManifestKey(sub.Topology), t.metadataKey(sub.Topology),
		privateStream, t.groupIndexKey(sub.Topology),
	}, group).Int64()
	if err != nil {
		return fmt.Errorf("redis: ensure subscription %q: %w", group, err)
	}
	if epoch == -1 {
		return fmt.Errorf("%w: %s", ErrSubscriptionRetiring, group)
	}
	t.registerRetentionTopology(sub.Topology)
	return nil
}

func (t *redisTransport) registerRetentionTopology(topo transport.Topology) {
	t.mu.Lock()
	t.retentionTopologies[topologyPrefix(topo)] = topo
	t.mu.Unlock()
	select {
	case t.retentionWake <- struct{}{}:
	default:
	}
}

func (t *redisTransport) retentionLoop(ctx context.Context) {
	defer close(t.retentionDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	last := make(map[string]time.Time)
	cursor := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.retentionWake:
		case <-ticker.C:
		}
		now := time.Now()
		t.mu.Lock()
		topologies := make(map[string]transport.Topology, len(t.retentionTopologies))
		for key, topo := range t.retentionTopologies {
			topologies[key] = topo
		}
		t.mu.Unlock()
		keys := make([]string, 0, len(topologies))
		for key := range topologies {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		if len(keys) == 0 {
			continue
		}
		if cursor >= len(keys) {
			cursor = 0
		}
		cycleSize := min(len(keys), retentionCycleMax)
		for offset := range cycleSize {
			key := keys[(cursor+offset)%len(keys)]
			topo := topologies[key]
			interval := defaultRetentionInterval
			if value, ok := topo.Options["redis.retention_interval"].(time.Duration); ok {
				interval = value
			}
			if previous := last[key]; !previous.IsZero() && now.Sub(previous) < interval {
				continue
			}
			last[key] = now
			passCtx, cancel := context.WithTimeout(ctx, min(interval, 10*time.Second))
			_, _ = t.compactReadyStream(passCtx, topo)
			cancel()
		}
		cursor = (cursor + cycleSize) % len(keys)
	}
}

func (t *redisTransport) compactReadyStream(ctx context.Context, topo transport.Topology) (int64, error) {
	if err := t.ValidateTopology(topo); err != nil {
		return 0, err
	}
	if err := t.validateEventLayout(ctx, topo); err != nil {
		return 0, err
	}
	now := time.Now()
	status := RetentionStatus{LastRun: now}
	defer func() { t.setRetentionStatus(topo, status) }()

	token := generateID()
	acquired, err := t.rc.SetNX(ctx, t.retentionLeaseKey(topo), token, retentionLeaseTTL).Result()
	if err != nil {
		status.LastError = err.Error()
		return 0, fmt.Errorf("redis: acquire retention lease: %w", err)
	}
	if !acquired {
		status.Blocked = "lease-held"
		status.LeaseContentions = 1
		return 0, nil
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = releaseRetentionLeaseScript.Run(releaseCtx, t.rc, []string{t.retentionLeaseKey(topo)}, token).Result()
	}()

	// Read the epoch before the manifest. A provisioner that commits after this
	// read changes the epoch and invalidates the final trim; one that commits
	// before it is necessarily present in the following manifest snapshot.
	epoch, err := t.rc.HGet(ctx, t.metadataKey(topo), "retention_epoch").Result()
	if errors.Is(err, goredis.Nil) {
		epoch = "0"
	} else if err != nil {
		status.LastError = err.Error()
		return 0, fmt.Errorf("redis: read retention epoch: %w", err)
	}
	manifest, err := t.rc.HGetAll(ctx, t.groupManifestKey(topo)).Result()
	if err != nil {
		status.LastError = err.Error()
		return 0, fmt.Errorf("redis: read retention group manifest: %w", err)
	}
	if len(manifest) == 0 {
		status.Blocked = "no-provisioned-groups"
		return 0, nil
	}
	groups, err := t.rc.XInfoGroups(ctx, t.streamKey(topo)).Result()
	if err != nil {
		status.LastError = err.Error()
		return 0, fmt.Errorf("redis: inspect retention groups: %w", err)
	}
	actual := make(map[string]goredis.XInfoGroup, len(groups))
	for _, group := range groups {
		actual[group.Name] = group
		if relevantRedisGroup(topo.Kind, group.Name) {
			if _, ok := manifest[group.Name]; !ok {
				status.Blocked = "unmanaged-group:" + group.Name
				return 0, nil
			}
		}
	}

	watermark := ""
	for name, state := range manifest {
		if state != groupStateActive && !strings.HasPrefix(state, groupStateRetiring) {
			status.Blocked = "invalid-group-state:" + name
			return 0, nil
		}
		group, ok := actual[name]
		if !ok {
			status.Blocked = "missing-group:" + name
			return 0, nil
		}
		protected := group.LastDeliveredID
		if protected == "" || protected == "0-0" {
			status.Blocked = "unread-group:" + name
			return 0, nil
		}
		pending, err := t.rc.XPending(ctx, t.streamKey(topo), name).Result()
		if err != nil {
			status.LastError = err.Error()
			return 0, fmt.Errorf("redis: inspect pending group %q: %w", name, err)
		}
		if pending.Count > 0 {
			if pending.Lower == "" {
				status.Blocked = "unknown-pending:" + name
				return 0, nil
			}
			protected = pending.Lower
		}
		if watermark == "" || compareStreamIDs(protected, watermark) < 0 {
			watermark = protected
		}
	}
	if minimumAge, ok := topo.Options["redis.retention_min_age"].(time.Duration); ok && minimumAge > 0 {
		ageFloor := fmt.Sprintf("%d-0", now.Add(-minimumAge).UnixMilli())
		if compareStreamIDs(ageFloor, watermark) < 0 {
			watermark = ageFloor
		}
	}
	if watermark == "" || watermark == "0-0" {
		status.Blocked = "no-safe-watermark"
		return 0, nil
	}
	batch := redisInt64Option(topo, "redis.retention_batch_size", defaultRetentionBatch)
	trimmed, err := trimAtEpochScript.Run(ctx, t.rc, []string{
		t.streamKey(topo), t.metadataKey(topo), t.groupManifestKey(topo),
	}, epoch, watermark, batch).Int64()
	if err != nil {
		status.LastError = err.Error()
		return 0, fmt.Errorf("redis: trim ready stream: %w", err)
	}
	if trimmed == -1 {
		status.Blocked = "manifest-changed"
		status.EpochConflicts++
		return 0, nil
	}
	if trimmed == -2 {
		status.Blocked = "unmanaged-group-race"
		return 0, nil
	}
	status.SafeWatermark = watermark
	status.Trimmed = trimmed
	status.LastSuccess = now
	return trimmed, nil
}

// RetireSubscription starts (or resumes) a barrier-based retirement. Safe
// retirement returns ErrSubscriptionNotDrained while the group still owes
// work. Forced retirement destroys the group and explicitly reports the
// pending/unread snapshot it abandons.
func (t *redisTransport) RetireSubscription(ctx context.Context, sub transport.Subscription, force bool) (SubscriptionRetirement, error) {
	if err := t.ValidateTopology(sub.Topology); err != nil {
		return SubscriptionRetirement{}, err
	}
	if sub.Topology.Kind != transport.EventTopology || strings.TrimSpace(sub.Group) == "" {
		return SubscriptionRetirement{}, fmt.Errorf("redis: event subscriber required")
	}
	if err := t.validateEventLayout(ctx, sub.Topology); err != nil {
		return SubscriptionRetirement{}, err
	}
	group := t.groupName(sub)
	privateStream := t.subscriberRetryStreamKey(sub.Topology, group)
	privateDelayed := t.subscriberRetryDelayedKey(sub.Topology, group)
	barrier, err := beginRetirementScript.Run(ctx, t.rc, []string{
		t.streamKey(sub.Topology), t.groupManifestKey(sub.Topology), t.metadataKey(sub.Topology),
	}, group).Text()
	if err != nil {
		return SubscriptionRetirement{}, fmt.Errorf("redis: begin retirement %q: %w", group, err)
	}
	result := SubscriptionRetirement{Group: group, Barrier: barrier, Forced: force}
	groups, err := t.rc.XInfoGroups(ctx, t.streamKey(sub.Topology)).Result()
	if err != nil {
		return result, fmt.Errorf("redis: inspect retiring group: %w", err)
	}
	delivered := ""
	for _, candidate := range groups {
		if candidate.Name == group {
			delivered = candidate.LastDeliveredID
			break
		}
	}
	if delivered == "" {
		return result, fmt.Errorf("redis: retiring group %q is missing", group)
	}
	pending, err := t.rc.XPending(ctx, t.streamKey(sub.Topology), group).Result()
	if err != nil {
		return result, fmt.Errorf("redis: inspect retiring pending work: %w", err)
	}
	result.Pending = pending.Count
	result.Unread, err = t.countUnreadThrough(ctx, sub.Topology, delivered, barrier)
	if err != nil {
		return result, err
	}
	if sub.Topology.Kind == transport.EventTopology {
		privatePending, err := t.rc.XPending(ctx, privateStream, group).Result()
		if err != nil {
			return result, fmt.Errorf("redis: inspect retiring retry pending work: %w", err)
		}
		privateEntries, err := t.rc.XLen(ctx, privateStream).Result()
		if err != nil {
			return result, fmt.Errorf("redis: inspect retiring retry ready work: %w", err)
		}
		privateDelayedCount, err := t.rc.ZCard(ctx, privateDelayed).Result()
		if err != nil {
			return result, fmt.Errorf("redis: inspect retiring retry delayed work: %w", err)
		}
		result.Pending += privatePending.Count
		result.Unread += privateEntries - privatePending.Count + privateDelayedCount
	}
	finished, err := finishRetirementScript.Run(ctx, t.rc, []string{
		t.streamKey(sub.Topology), t.groupManifestKey(sub.Topology), t.metadataKey(sub.Topology), privateStream, privateDelayed,
		t.groupIndexKey(sub.Topology),
	}, group, barrier, boolInt(force)).Int64()
	if err != nil {
		return result, fmt.Errorf("redis: finish retirement %q: %w", group, err)
	}
	switch finished {
	case -2:
		return result, fmt.Errorf("%w: group=%s pending=%d unread=%d", ErrSubscriptionNotDrained, group, result.Pending, result.Unread)
	case -3:
		return result, fmt.Errorf("redis: retirement manifest changed for %q", group)
	case -4:
		return result, fmt.Errorf("redis: retiring group %q disappeared", group)
	}
	result.Retired = true
	return result, nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (t *redisTransport) countUnreadThrough(ctx context.Context, topo transport.Topology, after, barrier string) (int64, error) {
	if compareStreamIDs(after, barrier) >= 0 {
		return 0, nil
	}
	start := "(" + after
	var count int64
	for {
		entries, err := t.rc.XRangeN(ctx, t.streamKey(topo), start, barrier, retirementScanPage).Result()
		if err != nil {
			return 0, fmt.Errorf("redis: count retiring unread work: %w", err)
		}
		count += int64(len(entries))
		if len(entries) < retirementScanPage {
			return count, nil
		}
		start = "(" + entries[len(entries)-1].ID
	}
}

func relevantRedisGroup(kind transport.TopologyKind, name string) bool {
	if kind == transport.TaskTopology {
		return name == "workers"
	}
	return strings.HasPrefix(name, "subscriber:")
}

func compareStreamIDs(left, right string) int {
	lms, lseq, lok := parseStreamID(left)
	rms, rseq, rok := parseStreamID(right)
	if !lok || !rok {
		return strings.Compare(left, right)
	}
	if lms < rms || lms == rms && lseq < rseq {
		return -1
	}
	if lms == rms && lseq == rseq {
		return 0
	}
	return 1
}

func parseStreamID(id string) (uint64, uint64, bool) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	return ms, seq, err == nil
}

func (t *redisTransport) setRetentionStatus(topo transport.Topology, status RetentionStatus) {
	t.mu.Lock()
	previous := t.retentionStatuses[topologyPrefix(topo)]
	if status.LastSuccess.IsZero() {
		status.LastSuccess = previous.LastSuccess
	}
	if status.SafeWatermark == "" {
		status.SafeWatermark = previous.SafeWatermark
	}
	status.TotalTrimmed += previous.TotalTrimmed + status.Trimmed
	status.EpochConflicts += previous.EpochConflicts
	status.LeaseContentions += previous.LeaseContentions
	t.retentionStatuses[topologyPrefix(topo)] = status
	t.mu.Unlock()
}

func (t *redisTransport) RetentionStatus(topo transport.Topology) RetentionStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.retentionStatuses[topologyPrefix(topo)]
}

// RetentionReport reads broker usage and combines it with coordinator-local
// progress/counters. It contains no payload or credential data.
func (t *redisTransport) RetentionReport(ctx context.Context, topo transport.Topology) (RetentionReport, error) {
	if err := t.ValidateTopology(topo); err != nil {
		return RetentionReport{}, err
	}
	if err := t.validateEventLayout(ctx, topo); err != nil {
		return RetentionReport{}, err
	}
	report := RetentionReport{
		RetentionStatus: t.RetentionStatus(topo),
		ReadyLimit:      maxReadyEntries(topo),
		DelayedLimit:    maxDelayedEntries(topo),
		DLQLimit:        maxDLQEntries(topo),
		Ready:           true,
	}
	pipe := t.rc.Pipeline()
	ready := pipe.XLen(ctx, t.streamKey(topo))
	delayed := pipe.ZCard(ctx, t.delayedKey(topo))
	dlq := pipe.XLen(ctx, t.dlqKey(topo))
	manifestCommand := pipe.HGetAll(ctx, t.groupManifestKey(topo))
	epochCommand := pipe.HGet(ctx, t.metadataKey(topo), "retention_epoch")
	_, pipelineErr := pipe.Exec(ctx)
	if pipelineErr != nil && !errors.Is(pipelineErr, goredis.Nil) {
		return report, fmt.Errorf("redis: retention usage report: %w", pipelineErr)
	}
	var err error
	if report.ReadyEntries, err = ready.Result(); err != nil {
		return report, fmt.Errorf("redis: ready usage: %w", err)
	}
	if report.DelayedEntries, err = delayed.Result(); err != nil {
		return report, fmt.Errorf("redis: delayed usage: %w", err)
	}
	if report.DLQEntries, err = dlq.Result(); err != nil {
		return report, fmt.Errorf("redis: DLQ usage: %w", err)
	}
	manifest, err := manifestCommand.Result()
	if err != nil {
		return report, fmt.Errorf("redis: retention manifest report: %w", err)
	}
	report.ManifestEpoch, err = epochCommand.Result()
	if errors.Is(err, goredis.Nil) {
		report.ManifestEpoch = "0"
	} else if err != nil {
		return report, fmt.Errorf("redis: retention epoch report: %w", err)
	}
	if topo.Kind == transport.EventTopology {
		for group := range manifest {
			privateReady, err := t.rc.XLen(ctx, t.subscriberRetryStreamKey(topo, group)).Result()
			if err != nil {
				return report, fmt.Errorf("redis: subscriber retry ready usage: %w", err)
			}
			privateDelayed, err := t.rc.ZCard(ctx, t.subscriberRetryDelayedKey(topo, group)).Result()
			if err != nil {
				return report, fmt.Errorf("redis: subscriber retry delayed usage: %w", err)
			}
			if report.ReadyEntries > math.MaxInt64-privateReady || report.DelayedEntries > math.MaxInt64-privateDelayed {
				return report, fmt.Errorf("redis: retention usage count overflow")
			}
			report.ReadyEntries += privateReady
			report.DelayedEntries += privateDelayed
		}
	}

	groups, err := t.rc.XInfoGroups(ctx, t.streamKey(topo)).Result()
	if err != nil {
		return report, fmt.Errorf("redis: retention group report: %w", err)
	}
	actual := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		actual[group.Name] = struct{}{}
		if relevantRedisGroup(topo.Kind, group.Name) {
			if _, ok := manifest[group.Name]; !ok {
				report.UnmanagedGroups++
			}
		}
	}
	for group, state := range manifest {
		if state == groupStateActive {
			report.ActiveGroups++
		}
		if strings.HasPrefix(state, groupStateRetiring) {
			report.RetiringGroups++
		}
		if _, ok := actual[group]; !ok {
			report.MissingGroups++
		}
	}
	t.mu.Lock()
	rejections := t.capacityRejections[topologyPrefix(topo)]
	report.CapacityRejections = make(map[string]uint64, len(rejections))
	for resource, count := range rejections {
		report.CapacityRejections[resource] = count
	}
	t.mu.Unlock()
	report.Ready = report.LastError == "" && !retentionBlockDegradesReadiness(report.Blocked) &&
		report.MissingGroups == 0 && report.UnmanagedGroups == 0 &&
		report.ReadyEntries < report.ReadyLimit && report.DelayedEntries < report.DelayedLimit && report.DLQEntries < report.DLQLimit
	return report, nil
}

func retentionBlockDegradesReadiness(blocked string) bool {
	return blocked == "manifest-changed" || blocked == "no-safe-watermark" ||
		blocked == "unmanaged-group-race" ||
		strings.HasPrefix(blocked, "invalid-group-state:") ||
		strings.HasPrefix(blocked, "missing-group:") ||
		strings.HasPrefix(blocked, "unmanaged-group:") ||
		strings.HasPrefix(blocked, "unknown-pending:")
}
