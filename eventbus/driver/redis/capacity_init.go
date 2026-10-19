package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const (
	capacityCounterVersion = "1"
	capacityInitPage       = 100
	capacityInitLeaseTTL   = 15 * time.Second
	capacityInitRetry      = 10 * time.Millisecond
)

var (
	beginCapacityInitScript = goredis.NewScript(`
local metadata = KEYS[1]
local lease = KEYS[2]
if redis.call("HGET", metadata, "capacity_counter_version") == ARGV[1]
    and redis.call("HGET", metadata, "capacity_state") == "ready"
    and redis.call("HEXISTS", metadata, "ready_entries") == 1
    and redis.call("HEXISTS", metadata, "delayed_entries") == 1 then
    return 0
end
if not redis.call("SET", lease, ARGV[2], "PX", ARGV[3], "NX") then return 2 end
redis.call("HSET", metadata, "capacity_state", "initializing:" .. ARGV[2])
return 1
`)
	renewCapacityInitScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call("PEXPIRE", KEYS[1], ARGV[2])
`)
	commitCapacityInitScript = goredis.NewScript(`
local metadata = KEYS[1]
local lease = KEYS[2]
if redis.call("GET", lease) ~= ARGV[1] then return -2 end
if redis.call("HGET", metadata, "capacity_state") ~= "initializing:" .. ARGV[1] then return -2 end
local epoch = redis.call("HGET", metadata, "retention_epoch") or "0"
if epoch ~= ARGV[2] then return -1 end
redis.call("HSET", metadata,
    "ready_entries", ARGV[3],
    "delayed_entries", ARGV[4],
    "capacity_counter_version", ARGV[5],
    "capacity_state", "ready")
redis.call("DEL", lease)
return 1
`)
	releaseCapacityInitScript = goredis.NewScript(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call("DEL", KEYS[1])
`)
)

func (t *redisTransport) capacityInitLeaseKey(topo transport.Topology) string {
	return topologyPrefix(topo) + ":capacity-init"
}

func (t *redisTransport) ensureCapacityCounters(ctx context.Context, topo transport.Topology) error {
	if err := t.validateEventLayout(ctx, topo); err != nil {
		return err
	}
	metadata := t.metadataKey(topo)
	lease := t.capacityInitLeaseKey(topo)
	for {
		ready, err := t.capacityCountersReady(ctx, metadata)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		token := generateID()
		status, err := beginCapacityInitScript.Run(ctx, t.rc, []string{metadata, lease},
			capacityCounterVersion, token, capacityInitLeaseTTL.Milliseconds()).Int64()
		if err != nil {
			return fmt.Errorf("redis: begin capacity counter initialization: %w", err)
		}
		switch status {
		case 0:
			return nil
		case 1:
			err = t.reconcileCapacityCounters(ctx, topo, token)
			releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, _ = releaseCapacityInitScript.Run(releaseCtx, t.rc, []string{lease}, token).Result()
			cancel()
			if err == nil {
				return nil
			}
			if !errors.Is(err, errCapacityManifestChanged) && !errors.Is(err, errCapacityLeaseLost) {
				return err
			}
		case 2:
		default:
			return fmt.Errorf("redis: invalid capacity initialization status %d", status)
		}

		timer := time.NewTimer(capacityInitRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("redis: capacity counters are initializing: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func (t *redisTransport) capacityCountersReady(ctx context.Context, metadata string) (bool, error) {
	values, err := t.rc.HMGet(ctx, metadata, "capacity_counter_version", "capacity_state", "ready_entries", "delayed_entries").Result()
	if err != nil {
		return false, fmt.Errorf("redis: inspect capacity counters: %w", err)
	}
	return len(values) == 4 && values[0] == capacityCounterVersion && values[1] == "ready" && values[2] != nil && values[3] != nil, nil
}

var (
	errCapacityManifestChanged = errors.New("redis: capacity manifest changed")
	errCapacityLeaseLost       = errors.New("redis: capacity initialization lease lost")
)

func (t *redisTransport) reconcileCapacityCounters(ctx context.Context, topo transport.Topology, token string) error {
	epoch, err := t.rc.HGet(ctx, t.metadataKey(topo), "retention_epoch").Result()
	if errors.Is(err, goredis.Nil) {
		epoch = "0"
	} else if err != nil {
		return fmt.Errorf("redis: read capacity manifest epoch: %w", err)
	}

	ready, delayed, err := t.countCapacityLanes(ctx, topo, token)
	if err != nil {
		return err
	}
	result, err := commitCapacityInitScript.Run(ctx, t.rc,
		[]string{t.metadataKey(topo), t.capacityInitLeaseKey(topo)},
		token, epoch, ready, delayed, capacityCounterVersion).Int64()
	if err != nil {
		return fmt.Errorf("redis: commit capacity counters: %w", err)
	}
	switch result {
	case 1:
		return nil
	case -1:
		return errCapacityManifestChanged
	case -2:
		return errCapacityLeaseLost
	default:
		return fmt.Errorf("redis: invalid capacity counter commit status %d", result)
	}
}

func (t *redisTransport) countCapacityLanes(ctx context.Context, topo transport.Topology, token string) (int64, int64, error) {
	ready, err := t.rc.XLen(ctx, t.streamKey(topo)).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("redis: count shared ready capacity: %w", err)
	}
	delayed, err := t.rc.ZCard(ctx, t.delayedKey(topo)).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("redis: count shared delayed capacity: %w", err)
	}
	if topo.Kind != transport.EventTopology {
		return ready, delayed, nil
	}

	var cursor uint64
	seen := make(map[string]struct{})
	for {
		groups, next, err := t.rc.HScan(ctx, t.groupManifestKey(topo), cursor, "*", capacityInitPage).Result()
		if err != nil {
			return 0, 0, fmt.Errorf("redis: scan capacity manifest: %w", err)
		}
		lanes := make([]string, 0, len(groups)/2)
		for index := 0; index < len(groups); index += 2 {
			group := groups[index]
			if _, exists := seen[group]; exists {
				continue
			}
			seen[group] = struct{}{}
			lanes = append(lanes, group)
		}
		if len(lanes) > 0 {
			pipe := t.rc.Pipeline()
			readyCommands := make([]*goredis.IntCmd, 0, len(lanes))
			delayedCommands := make([]*goredis.IntCmd, 0, len(lanes))
			for _, group := range lanes {
				readyCommands = append(readyCommands, pipe.XLen(ctx, t.subscriberRetryStreamKey(topo, group)))
				delayedCommands = append(delayedCommands, pipe.ZCard(ctx, t.subscriberRetryDelayedKey(topo, group)))
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return 0, 0, fmt.Errorf("redis: count private capacity lanes: %w", err)
			}
			for index := range lanes {
				privateReady, err := readyCommands[index].Result()
				if err != nil {
					return 0, 0, fmt.Errorf("redis: count private ready capacity: %w", err)
				}
				privateDelayed, err := delayedCommands[index].Result()
				if err != nil {
					return 0, 0, fmt.Errorf("redis: count private delayed capacity: %w", err)
				}
				if ready > math.MaxInt64-privateReady || delayed > math.MaxInt64-privateDelayed {
					return 0, 0, fmt.Errorf("redis: capacity counter overflow")
				}
				ready += privateReady
				delayed += privateDelayed
			}
		}
		renewed, err := renewCapacityInitScript.Run(ctx, t.rc, []string{t.capacityInitLeaseKey(topo)},
			token, capacityInitLeaseTTL.Milliseconds()).Int64()
		if err != nil {
			return 0, 0, fmt.Errorf("redis: renew capacity initialization lease: %w", err)
		}
		if renewed != 1 {
			return 0, 0, errCapacityLeaseLost
		}
		if next == 0 {
			return ready, delayed, nil
		}
		cursor = next
	}
}
