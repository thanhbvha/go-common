package redis

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

const maxDLQAdminBatch = 1000

var (
	redisStreamID = regexp.MustCompile(`^[0-9]+-[0-9]+$`)
	requeueScript = goredis.NewScript(`
local dlq = KEYS[1]
local metadata = KEYS[2]
local groups = KEYS[3]
local capacity = tonumber(ARGV[1])
if redis.call("HGET", metadata, "capacity_state") ~= "ready" then return redis.error_reply("capacity counters are initializing") end
local dlq_type = redis.call("TYPE", dlq).ok
if dlq_type ~= "none" and dlq_type ~= "stream" then
    return redis.error_reply("DLQ source must be a stream")
end
if dlq_type == "none" then return 0 end

local selected = {}
local additions = {}
for index = 2, #ARGV, 3 do
    local id = ARGV[index]
    local destination = tonumber(ARGV[index + 1])
    local expected_group = ARGV[index + 2]
    if not destination or destination < 4 or destination > #KEYS then
        return redis.error_reply("invalid requeue destination")
    end
    local records = redis.call("XRANGE", dlq, id, id, "COUNT", 1)
    if #records > 0 then
        local fields = records[1][2]
        local payload = nil
        local source_group = nil
        for field = 1, #fields, 2 do
            if fields[field] == "payload" then
                payload = fields[field + 1]
            elseif fields[field] == "source_group" then
                source_group = fields[field + 1]
            end
        end
        if not payload then
            return redis.error_reply("DLQ record has no payload")
        end
        if expected_group ~= "" and source_group ~= expected_group then
            return redis.error_reply("DLQ source group changed")
        end
        if expected_group ~= "" and redis.call("HGET", groups, expected_group) ~= "active" then
            return -1000000001
        end
        table.insert(selected, {id, payload, destination})
        additions[destination] = (additions[destination] or 0) + 1
    end
end

local current = tonumber(redis.call("HGET", metadata, "ready_entries"))
if not current then return redis.error_reply("ready capacity counter is missing") end
local total = 0
for destination, count in pairs(additions) do
    local ready_type = redis.call("TYPE", KEYS[destination]).ok
    if ready_type ~= "none" and ready_type ~= "stream" then
        return redis.error_reply("requeue destination must be a stream")
    end
    total = total + count
end
if current + total > capacity then return -(current + 1) end

for _, record in ipairs(selected) do
    redis.call("XADD", KEYS[record[3]], "*", "payload", record[2])
    redis.call("XDEL", dlq, record[1])
end
if total > 0 then redis.call("HSET", metadata, "ready_entries", current + total) end
return #selected
`)
)

// ListPage returns oldest-first records after cursor. Redis stream IDs remain
// stable when earlier records are requeued/deleted, so pages do not shift.
func (t *redisTransport) ListPage(ctx context.Context, topology transport.Topology, cursor string, limit int) (transport.DLQPage, error) {
	if err := t.ValidateTopology(topology); err != nil {
		return transport.DLQPage{}, err
	}
	if err := t.validateEventLayout(ctx, topology); err != nil {
		return transport.DLQPage{}, err
	}
	if limit < 1 || limit > maxDLQAdminBatch {
		return transport.DLQPage{}, fmt.Errorf("redis: DLQ page limit must be between 1 and %d", maxDLQAdminBatch)
	}
	min := "-"
	if cursor != "" {
		if !redisStreamID.MatchString(cursor) {
			return transport.DLQPage{}, fmt.Errorf("redis: invalid DLQ cursor")
		}
		min = "(" + cursor
	}

	records, err := t.rc.XRangeN(ctx, t.dlqKey(topology), min, "+", int64(limit+1)).Result()
	if err != nil {
		return transport.DLQPage{}, fmt.Errorf("redis: list DLQ: %w", err)
	}
	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
	}

	page := transport.DLQPage{Records: make([]transport.DeadLetter, 0, len(records))}
	for _, record := range records {
		deadLetter, err := decodeDeadLetter(record)
		if err != nil {
			return transport.DLQPage{}, err
		}
		page.Records = append(page.Records, deadLetter)
	}
	if hasMore {
		page.NextCursor = records[len(records)-1].ID
	}
	return page, nil
}

// List retains the positional DLQAdmin contract. It walks bounded cursor pages
// internally; callers that need deletion-stable pagination should use ListPage.
func (t *redisTransport) List(ctx context.Context, topology transport.Topology, offset, limit int) ([]transport.DeadLetter, error) {
	if offset < 0 {
		return nil, fmt.Errorf("redis: DLQ offset must not be negative")
	}
	if limit < 1 || limit > maxDLQAdminBatch {
		return nil, fmt.Errorf("redis: DLQ page limit must be between 1 and %d", maxDLQAdminBatch)
	}

	cursor := ""
	remaining := offset
	for remaining > 0 {
		step := min(remaining, maxDLQAdminBatch)
		page, err := t.ListPage(ctx, topology, cursor, step)
		if err != nil {
			return nil, err
		}
		if len(page.Records) < step {
			return nil, nil
		}
		remaining -= len(page.Records)
		cursor = page.Records[len(page.Records)-1].ID
	}
	page, err := t.ListPage(ctx, topology, cursor, limit)
	if err != nil {
		return nil, err
	}
	return page.Records, nil
}

// Requeue atomically publishes each selected DLQ record back to the ready
// stream and removes it from the DLQ. Missing IDs are successful no-ops, so a
// repeated request cannot publish the same record twice.
func (t *redisTransport) Requeue(ctx context.Context, topology transport.Topology, messageIDs []string) error {
	if err := t.ValidateTopology(topology); err != nil {
		return err
	}
	if err := t.ensureCapacityCounters(ctx, topology); err != nil {
		return err
	}
	if len(messageIDs) > maxDLQAdminBatch {
		return fmt.Errorf("redis: DLQ requeue batch exceeds %d records", maxDLQAdminBatch)
	}
	unique := make([]string, 0, len(messageIDs))
	seen := make(map[string]struct{}, len(messageIDs))
	for _, id := range messageIDs {
		if !redisStreamID.MatchString(id) {
			return fmt.Errorf("redis: invalid DLQ record ID")
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return nil
	}
	dlq := t.dlqKey(topology)
	keys := []string{dlq, t.metadataKey(topology), t.groupManifestKey(topology), t.streamKey(topology)}
	keyIndexes := map[string]int{keys[3]: 4}
	args := []any{maxReadyEntries(topology)}
	for _, id := range unique {
		destination := keys[3]
		expectedGroup := ""
		if topology.Kind == transport.EventTopology {
			records, err := t.rc.XRangeN(ctx, dlq, id, id, 1).Result()
			if err != nil {
				return fmt.Errorf("redis: inspect DLQ requeue source: %w", err)
			}
			if len(records) == 0 {
				continue
			}
			group, err := requiredDLQString(records[0], "source_group")
			if err != nil {
				return err
			}
			if !strings.HasPrefix(group, "subscriber:") {
				return fmt.Errorf("redis: event DLQ record %s has invalid source group", id)
			}
			expectedGroup = group
			destination = t.subscriberRetryStreamKey(topology, group)
		}
		index, ok := keyIndexes[destination]
		if !ok {
			keys = append(keys, destination)
			index = len(keys)
			keyIndexes[destination] = index
		}
		args = append(args, id, index, expectedGroup)
	}
	if len(args) == 1 {
		return nil
	}
	result, err := requeueScript.Run(ctx, t.rc, keys, args...).Int64()
	if err != nil {
		return fmt.Errorf("redis: requeue DLQ: %w", err)
	}
	if result < 0 {
		if result == -1000000001 {
			return fmt.Errorf("%w: DLQ target is not active", ErrSubscriptionRetiring)
		}
		return t.capacityError(topology, "ready", -result-1, maxReadyEntries(topology))
	}
	return nil
}

func (t *redisTransport) Purge(ctx context.Context, topology transport.Topology) error {
	if err := t.ValidateTopology(topology); err != nil {
		return err
	}
	if err := t.validateEventLayout(ctx, topology); err != nil {
		return err
	}
	if err := t.rc.Del(ctx, t.dlqKey(topology)).Err(); err != nil {
		return fmt.Errorf("redis: purge DLQ: %w", err)
	}
	return nil
}

func (t *redisTransport) Count(ctx context.Context, topology transport.Topology) (int64, error) {
	if err := t.ValidateTopology(topology); err != nil {
		return 0, err
	}
	if err := t.validateEventLayout(ctx, topology); err != nil {
		return 0, err
	}
	count, err := t.rc.XLen(ctx, t.dlqKey(topology)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: count DLQ: %w", err)
	}
	return count, nil
}

func decodeDeadLetter(record goredis.XMessage) (transport.DeadLetter, error) {
	payload, err := requiredDLQString(record, "payload")
	if err != nil {
		return transport.DeadLetter{}, err
	}
	reason, err := requiredDLQString(record, "reason")
	if err != nil {
		return transport.DeadLetter{}, err
	}
	lastError, err := requiredDLQString(record, "error")
	if err != nil {
		return transport.DeadLetter{}, err
	}
	attemptText, err := requiredDLQString(record, "attempts")
	if err != nil {
		return transport.DeadLetter{}, err
	}
	attempts, err := strconv.Atoi(attemptText)
	if err != nil || attempts < 0 {
		return transport.DeadLetter{}, fmt.Errorf("redis: DLQ record %s has invalid attempts", record.ID)
	}
	failedText, err := requiredDLQString(record, "failed_at")
	if err != nil {
		return transport.DeadLetter{}, err
	}
	failedMillis, err := strconv.ParseInt(failedText, 10, 64)
	if err != nil || failedMillis < 0 {
		return transport.DeadLetter{}, fmt.Errorf("redis: DLQ record %s has invalid failed_at", record.ID)
	}
	source, err := requiredDLQString(record, "source_stream")
	if err != nil {
		return transport.DeadLetter{}, err
	}
	sourceGroup, err := requiredDLQString(record, "source_group")
	if err != nil {
		return transport.DeadLetter{}, err
	}
	sourcePosition, err := requiredDLQString(record, "source_id")
	if err != nil {
		return transport.DeadLetter{}, err
	}

	message := transport.Message{Payload: []byte(payload)}
	var envelope struct {
		ID          string            `json:"id"`
		Type        string            `json:"type"`
		OrderingKey string            `json:"ordering_key"`
		DedupeKey   string            `json:"dedupe_key"`
		Headers     map[string]string `json:"headers"`
	}
	if json.Unmarshal(message.Payload, &envelope) == nil {
		message.ID = envelope.ID
		message.Type = envelope.Type
		message.OrderingKey = envelope.OrderingKey
		message.DedupeKey = envelope.DedupeKey
		message.Headers = envelope.Headers
	}

	return transport.DeadLetter{
		ID:             record.ID,
		Message:        message,
		Reason:         reason,
		LastError:      lastError,
		Attempts:       attempts,
		FailedAt:       time.UnixMilli(failedMillis),
		Source:         source,
		SourceGroup:    sourceGroup,
		SourcePosition: sourcePosition,
	}, nil
}

func requiredDLQString(record goredis.XMessage, field string) (string, error) {
	value, exists := record.Values[field]
	if !exists {
		return "", fmt.Errorf("redis: DLQ record %s is missing %s", record.ID, field)
	}
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	default:
		return "", fmt.Errorf("redis: DLQ record %s has invalid %s", record.ID, field)
	}
}
