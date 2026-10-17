package redis

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
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
local ready = KEYS[2]
local dlq_type = redis.call("TYPE", dlq).ok
local ready_type = redis.call("TYPE", ready).ok
if dlq_type ~= "none" and dlq_type ~= "stream" then
    return redis.error_reply("DLQ source must be a stream")
end
if ready_type ~= "none" and ready_type ~= "stream" then
    return redis.error_reply("requeue destination must be a stream")
end
if dlq_type == "none" then return 0 end

local selected = {}
for _, id in ipairs(ARGV) do
    local records = redis.call("XRANGE", dlq, id, id, "COUNT", 1)
    if #records > 0 then
        local fields = records[1][2]
        local payload = nil
        for index = 1, #fields, 2 do
            if fields[index] == "payload" then
                payload = fields[index + 1]
                break
            end
        end
        if not payload then
            return redis.error_reply("DLQ record has no payload")
        end
        table.insert(selected, {id, payload})
    end
end

for _, record in ipairs(selected) do
    redis.call("XADD", ready, "*", "payload", record[2])
    redis.call("XDEL", dlq, record[1])
end
return #selected
`)
)

// ListPage returns oldest-first records after cursor. Redis stream IDs remain
// stable when earlier records are requeued/deleted, so pages do not shift.
func (t *redisTransport) ListPage(ctx context.Context, topology transport.Topology, cursor string, limit int) (transport.DLQPage, error) {
	if err := t.ValidateTopology(topology); err != nil {
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
	if len(messageIDs) > maxDLQAdminBatch {
		return fmt.Errorf("redis: DLQ requeue batch exceeds %d records", maxDLQAdminBatch)
	}
	unique := make([]any, 0, len(messageIDs))
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
	if _, err := requeueScript.Run(ctx, t.rc, []string{t.dlqKey(topology), t.streamKey(topology)}, unique...).Int64(); err != nil {
		return fmt.Errorf("redis: requeue DLQ: %w", err)
	}
	return nil
}

func (t *redisTransport) Purge(ctx context.Context, topology transport.Topology) error {
	if err := t.ValidateTopology(topology); err != nil {
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
