package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// streamIntake owns no deliveries. The caller reserves the total delivery
// budget before read and releases it only after settlement or an unused read.
type streamIntake struct {
	client          goredis.UniversalClient
	streams         []string
	group, consumer string
	block           time.Duration
	preferred       int
	waitStreams     []string
}

func (r *streamIntake) read(ctx context.Context, count int64) ([]goredis.XStream, error) {
	if len(r.streams) == 1 {
		return r.readGroup(ctx, r.streams[0], count, r.block)
	}
	if r.waitStreams != nil {
		return nil, r.wait(ctx)
	}
	if result, err := r.poll(ctx, count); err != goredis.Nil {
		return result, err
	}

	// Observe tails BEFORE the second empty sweep. An append between that
	// sweep and XREAD is then visible to the waiter, avoiding a lost wake-up.
	pipe := r.client.Pipeline()
	tails := make([]*goredis.Cmd, len(r.streams))
	for i, stream := range r.streams {
		// Read only the cursor we need. The pinned client's typed XINFO
		// decoder rejects metadata fields added by newer Redis servers.
		tails[i] = pipe.Do(ctx, "XINFO", "STREAM", stream)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	streams := make([]string, 0, len(r.streams)*2)
	streams = append(streams, r.streams...)
	for _, tail := range tails {
		id, err := streamTailID(tail.Val())
		if err != nil {
			return nil, err
		}
		streams = append(streams, id)
	}
	if result, err := r.poll(ctx, count); err != goredis.Nil {
		return result, err
	}
	// This is a read-only wake-up, NOT XREADGROUP: COUNT is per stream, so
	// a multi-stream group read could acquire more PEL entries than permits
	// (especially when capacity=1). At most one record per lane is observed
	// and discarded; only the next single-lane poll acquires deliveries.
	r.waitStreams = streams
	return nil, r.wait(ctx)
}

// XINFO replies can use RESP2 alternating fields or a RESP3 map. Ignore
// unrelated metadata, but fail closed if the required cursor is absent/invalid.
func streamTailID(value any) (string, error) {
	var id any
	switch fields := value.(type) {
	case []interface{}:
		if len(fields)%2 != 0 {
			return "", fmt.Errorf("redis: invalid XINFO STREAM field pairs")
		}
		for i := 0; i < len(fields); i += 2 {
			if fields[i] == "last-generated-id" {
				id = fields[i+1]
				break
			}
		}
	case map[interface{}]interface{}:
		id = fields["last-generated-id"]
	case map[string]interface{}:
		id = fields["last-generated-id"]
	}
	if cursor, ok := id.(string); ok {
		if cursor == "" {
			cursor = "0-0"
		} // Empty Redis-compatible test streams.
		return cursor, nil
	}
	return "", fmt.Errorf("redis: missing or invalid XINFO STREAM last-generated-id")
}

func (r *streamIntake) wait(ctx context.Context) error {
	_, err := r.client.XRead(ctx, &goredis.XReadArgs{Streams: r.waitStreams, Count: 1, Block: r.block}).Result()
	// Keep the verified empty tails across idle timeouts. Steady idle needs
	// only one blocking command per interval, with no repeated polling/XINFO.
	if err != goredis.Nil {
		r.waitStreams = nil
	}
	if err != nil {
		return err
	}
	return goredis.Nil
}

func (r *streamIntake) poll(ctx context.Context, count int64) ([]goredis.XStream, error) {
	for range r.streams {
		stream := r.streams[r.preferred]
		r.preferred = (r.preferred + 1) % len(r.streams)
		result, err := r.readGroup(ctx, stream, count, -1)
		if err != goredis.Nil {
			return result, err
		}
	}
	return nil, goredis.Nil
}

func (r *streamIntake) readGroup(ctx context.Context, stream string, count int64, block time.Duration) ([]goredis.XStream, error) {
	return r.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group: r.group, Consumer: r.consumer, Streams: []string{stream, ">"}, Count: count, Block: block,
	}).Result()
}
