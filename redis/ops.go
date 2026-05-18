package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// ---- key helper ----

// BuildKey prepends the configured Prefix to key.
// Use this consistently so all keys share the same namespace.
func (c *Client) BuildKey(key string) string {
	c.mu.RLock()
	prefix := c.cfg.Prefix
	c.mu.RUnlock()
	return prefix + key
}

// ---- guard helper ----

// requireConnected returns ErrClosed when the client is not connected.
func (c *Client) requireConnected() (goredis.UniversalClient, error) {
	c.mu.RLock()
	rc := c.rc
	ok := c.connected
	c.mu.RUnlock()
	if !ok || rc == nil {
		return nil, goredis.ErrClosed
	}
	return rc, nil
}

// ---- String / counter operations ----

// Set stores value at key with an optional expiration (0 = no expiry).
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.Set(ctx, key, value, expiration).Err()
}

// Get retrieves the string value stored at key.
// Returns ("", nil) when the key does not exist.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	val, err := rc.Get(ctx, key).Result()
	if err == goredis.Nil {
		return "", nil
	}
	return val, err
}

// SetNX sets key to value only if it does not already exist.
// Returns true when the key was set, false when it already existed.
func (c *Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return false, err
	}
	return rc.SetNX(ctx, key, value, expiration).Result()
}

// Delete removes one or more keys. Missing keys are silently ignored.
func (c *Client) Delete(ctx context.Context, keys ...string) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.Del(ctx, keys...).Err()
}

// Exists returns the number of the given keys that exist in Redis.
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.Exists(ctx, keys...).Result()
}

// Expire sets the TTL on key. Returns an error if key does not exist.
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.Expire(ctx, key, expiration).Err()
}

// TTL returns the remaining time-to-live of key.
// Returns -1 when key has no expiry, -2 when key does not exist.
func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.TTL(ctx, key).Result()
}

// Type returns the Redis data type stored at key (string, list, set, zset, hash, stream).
func (c *Client) Type(ctx context.Context, key string) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	return rc.Type(ctx, key).Result()
}

// Incr atomically increments the integer stored at key by 1.
// When key does not exist it is initialised to 0 before incrementing.
// If expiration > 0 and the key was just created (val == 1), the TTL is set.
func (c *Client) Incr(ctx context.Context, key string, expiration time.Duration) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	val, err := rc.Incr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if val == 1 && expiration > 0 {
		rc.Expire(ctx, key, expiration)
	}
	return val, nil
}

// IncrBy atomically increments the integer stored at key by increment.
func (c *Client) IncrBy(ctx context.Context, key string, increment int64, expiration time.Duration) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	val, err := rc.IncrBy(ctx, key, increment).Result()
	if err != nil {
		return 0, err
	}
	if val == increment && expiration > 0 {
		rc.Expire(ctx, key, expiration)
	}
	return val, nil
}

// IncrByFloat atomically increments the float stored at key by increment.
func (c *Client) IncrByFloat(ctx context.Context, key string, increment float64, expiration time.Duration) (float64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	val, err := rc.IncrByFloat(ctx, key, increment).Result()
	if err != nil {
		return 0, err
	}
	if val == increment && expiration > 0 {
		rc.Expire(ctx, key, expiration)
	}
	return val, nil
}

// Decr atomically decrements the integer stored at key by 1.
func (c *Client) Decr(ctx context.Context, key string, expiration time.Duration) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	val, err := rc.Decr(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	if val == -1 && expiration > 0 {
		rc.Expire(ctx, key, expiration)
	}
	return val, nil
}

// DecrBy atomically decrements the integer stored at key by decrement.
func (c *Client) DecrBy(ctx context.Context, key string, decrement int64, expiration time.Duration) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	val, err := rc.DecrBy(ctx, key, decrement).Result()
	if err != nil {
		return 0, err
	}
	if val == -decrement && expiration > 0 {
		rc.Expire(ctx, key, expiration)
	}
	return val, nil
}

// ---- Hash operations ----

// HSet sets one or more field-value pairs on the hash stored at key.
func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.HSet(ctx, key, values...).Err()
}

// HGet returns the value associated with field in the hash at key.
func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	return rc.HGet(ctx, key, field).Result()
}

// HGetAll returns all fields and values of the hash stored at key.
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.HGetAll(ctx, key).Result()
}

// HDel removes one or more fields from the hash stored at key.
func (c *Client) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.HDel(ctx, key, fields...).Result()
}

// HExists returns whether field is an existing field in the hash stored at key.
func (c *Client) HExists(ctx context.Context, key, field string) (bool, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return false, err
	}
	return rc.HExists(ctx, key, field).Result()
}

// HIncrBy increments the number stored at field in the hash stored at key by increment.
func (c *Client) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.HIncrBy(ctx, key, field, incr).Result()
}

// HIncrByFloat increments the float value of a hash field by the given amount.
func (c *Client) HIncrByFloat(ctx context.Context, key, field string, incr float64) (float64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.HIncrByFloat(ctx, key, field, incr).Result()
}

// HKeys returns all field names in the hash stored at key.
func (c *Client) HKeys(ctx context.Context, key string) ([]string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.HKeys(ctx, key).Result()
}

// HVals returns all values in the hash stored at key.
func (c *Client) HVals(ctx context.Context, key string) ([]string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.HVals(ctx, key).Result()
}

// HLen returns the number of fields contained in the hash stored at key.
func (c *Client) HLen(ctx context.Context, key string) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.HLen(ctx, key).Result()
}

// HMGet returns the values associated with the specified fields in the hash stored at key.
func (c *Client) HMGet(ctx context.Context, key string, fields ...string) ([]interface{}, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.HMGet(ctx, key, fields...).Result()
}

// HSetNX sets field in the hash stored at key to value, only if field does not yet exist.
func (c *Client) HSetNX(ctx context.Context, key, field string, value interface{}) (bool, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return false, err
	}
	return rc.HSetNX(ctx, key, field, value).Result()
}

// ---- List operations ----

// LPush prepends one or more values to the list stored at key.
func (c *Client) LPush(ctx context.Context, key string, values ...interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.LPush(ctx, key, values...).Err()
}

// LPushX prepends one or more values to the list stored at key, only if the list exists.
func (c *Client) LPushX(ctx context.Context, key string, values ...interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.LPushX(ctx, key, values...).Err()
}

// RPush appends one or more values to the list stored at key.
func (c *Client) RPush(ctx context.Context, key string, values ...interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.RPush(ctx, key, values...).Err()
}

// RPushX appends one or more values to the list stored at key, only if the list exists.
func (c *Client) RPushX(ctx context.Context, key string, values ...interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.RPushX(ctx, key, values...).Err()
}

// LPop removes and returns the first element of the list at key.
func (c *Client) LPop(ctx context.Context, key string) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	return rc.LPop(ctx, key).Result()
}

// LLen returns the length of the list stored at key.
func (c *Client) LLen(ctx context.Context, key string) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.LLen(ctx, key).Result()
}

// LIndex returns the element at index in the list stored at key.
func (c *Client) LIndex(ctx context.Context, key string, index int64) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	return rc.LIndex(ctx, key, index).Result()
}

// LSet sets the list element at index to value.
func (c *Client) LSet(ctx context.Context, key string, index int64, value interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.LSet(ctx, key, index, value).Err()
}

// LInsert inserts value in the list stored at key either before or after the reference value pivot.
func (c *Client) LInsert(ctx context.Context, key, op string, pivot, value interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.LInsert(ctx, key, op, pivot, value).Err()
}

// LRem removes the first count occurrences of elements equal to value from the list stored at key.
func (c *Client) LRem(ctx context.Context, key string, count int64, value interface{}) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.LRem(ctx, key, count, value).Result()
}

// LTrim trims an existing list so that it will contain only the specified range of elements specified.
func (c *Client) LTrim(ctx context.Context, key string, start, stop int64) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.LTrim(ctx, key, start, stop).Err()
}

// RPopLPush atomically returns and removes the last element (tail) of the list stored at source, and pushes the element at the first element (head) of the list stored at destination.
func (c *Client) RPopLPush(ctx context.Context, source, destination string) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	return rc.RPopLPush(ctx, source, destination).Result()
}

// BLPop is a blocking list pop primitive.
func (c *Client) BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.BLPop(ctx, timeout, keys...).Result()
}

// BRPop is a blocking list pop primitive.
func (c *Client) BRPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.BRPop(ctx, timeout, keys...).Result()
}

// RPop removes and returns the last element of the list at key.
func (c *Client) RPop(ctx context.Context, key string) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	return rc.RPop(ctx, key).Result()
}

// ---- Set operations ----

// SAdd adds one or more members to the set stored at key.
func (c *Client) SAdd(ctx context.Context, key string, members ...interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.SAdd(ctx, key, members...).Err()
}

// SMembers returns all members of the set stored at key.
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.SMembers(ctx, key).Result()
}

// SRem removes one or more members from the set stored at key.
func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.SRem(ctx, key, members...).Err()
}

// SIsMember returns true if member is a member of the set stored at key.
func (c *Client) SIsMember(ctx context.Context, key, member string) (bool, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return false, err
	}
	return rc.SIsMember(ctx, key, member).Result()
}

// SPop removes and returns one or more members from the set stored at key.
func (c *Client) SPop(ctx context.Context, key string) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	return rc.SPop(ctx, key).Result()
}

// ---- Sorted Set operations ----

// ZAdd adds one or more members with scores to the sorted set at key.
func (c *Client) ZAdd(ctx context.Context, key string, members ...goredis.Z) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.ZAdd(ctx, key, members...).Err()
}

// ZRange returns a range of members from the sorted set at key by rank.
func (c *Client) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.ZRange(ctx, key, start, stop).Result()
}

// ZRem removes one or more members from the sorted set at key.
func (c *Client) ZRem(ctx context.Context, key string, members ...interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.ZRem(ctx, key, members...).Err()
}

// ZCard returns the number of elements in the sorted set at key.
func (c *Client) ZCard(ctx context.Context, key string) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.ZCard(ctx, key).Result()
}

// ZScore returns the score of element in the sorted set at key.
func (c *Client) ZScore(ctx context.Context, key, member string) (float64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.ZScore(ctx, key, member).Result()
}

// ZRank returns the rank of element in the sorted set at key.
func (c *Client) ZRank(ctx context.Context, key, member string) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.ZRank(ctx, key, member).Result()
}

// ---- Pipeline / transaction ----

// Pipeline returns a non-atomic pipeline that batches commands.
func (c *Client) Pipeline() (goredis.Pipeliner, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.Pipeline(), nil
}

// TxPipeline returns an atomic transaction pipeline (MULTI/EXEC).
func (c *Client) TxPipeline() (goredis.Pipeliner, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.TxPipeline(), nil
}

// ---- Pub/Sub ----

// Publish sends message to the given channel.
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.Publish(ctx, channel, message).Err()
}

// Subscribe returns a PubSub handle subscribed to the given channels.
func (c *Client) Subscribe(ctx context.Context, channels ...string) (*goredis.PubSub, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.Subscribe(ctx, channels...), nil
}

// ---- Stream operations ----

// XAdd appends a message to the Redis Stream specified by args.Stream.
func (c *Client) XAdd(ctx context.Context, args *goredis.XAddArgs) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	return rc.XAdd(ctx, args).Result()
}

// XGroupCreateMkStream creates a consumer group on stream, creating the stream
// if it does not exist. start is typically "$" (new messages only) or "0"
// (all existing messages). Returns nil when the group already exists.
func (c *Client) XGroupCreateMkStream(ctx context.Context, stream, group, start string) error {
	rc, err := c.requireConnected()
	if err != nil {
		return err
	}
	return rc.XGroupCreateMkStream(ctx, stream, group, start).Err()
}

// XReadGroup reads up to count messages from stream for the given consumer group.
// block is the maximum time to wait for new messages (0 = no blocking).
func (c *Client) XReadGroup(ctx context.Context, group, consumer string, streams []string, count int64, block time.Duration) ([]goredis.XStream, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	if block <= 0 {
		block = 5 * time.Second
	}
	if count <= 0 {
		count = 1
	}
	return rc.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streams,
		Count:    count,
		Block:    block,
	}).Result()
}

// XAck acknowledges one or more message IDs within a consumer group.
// Returns the number of messages actually acknowledged.
func (c *Client) XAck(ctx context.Context, stream, group string, ids ...string) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.XAck(ctx, stream, group, ids...).Result()
}

// XDel removes one or more messages from a stream by ID.
func (c *Client) XDel(ctx context.Context, stream string, ids ...string) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.XDel(ctx, stream, ids...).Result()
}

// XRange returns messages from stream in the range [start, stop].
func (c *Client) XRange(ctx context.Context, stream, start, stop string) ([]goredis.XMessage, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.XRange(ctx, stream, start, stop).Result()
}

// XRangeN returns up to count messages from stream in the range [start, stop].
func (c *Client) XRangeN(ctx context.Context, stream, start, stop string, count int64) ([]goredis.XMessage, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.XRangeN(ctx, stream, start, stop, count).Result()
}

// XLen returns the number of entries in the stream.
func (c *Client) XLen(ctx context.Context, stream string) (int64, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return 0, err
	}
	return rc.XLen(ctx, stream).Result()
}

// XPending returns a summary of pending (unacknowledged) messages in a group.
func (c *Client) XPending(ctx context.Context, stream, group string) (*goredis.XPending, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.XPending(ctx, stream, group).Result()
}

// XPendingExt returns detailed information about pending messages filtered by
// idle time. Pass idle = 0 to skip the idle filter.
func (c *Client) XPendingExt(ctx context.Context, stream, group, start, end string, count int64, idle time.Duration) ([]goredis.XPendingExt, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	args := &goredis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  start,
		End:    end,
		Count:  count,
	}
	if idle > 0 {
		args.Idle = idle
	}
	return rc.XPendingExt(ctx, args).Result()
}

// XClaim transfers ownership of pending messages to consumer.
func (c *Client) XClaim(ctx context.Context, args *goredis.XClaimArgs) ([]goredis.XMessage, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.XClaim(ctx, args).Result()
}

// XAutoClaim atomically scans and claims idle pending messages (Redis >= 6.2).
// Returns the claimed messages, the next cursor for pagination, and any error.
func (c *Client) XAutoClaim(ctx context.Context, args *goredis.XAutoClaimArgs) ([]goredis.XMessage, string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, "", err
	}
	return rc.XAutoClaim(ctx, args).Result()
}

// XInfoGroups returns metadata about all consumer groups on stream.
func (c *Client) XInfoGroups(ctx context.Context, stream string) ([]goredis.XInfoGroup, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.XInfoGroups(ctx, stream).Result()
}

// XInfoConsumers returns metadata about all consumers in a group.
func (c *Client) XInfoConsumers(ctx context.Context, stream, group string) ([]goredis.XInfoConsumer, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return nil, err
	}
	return rc.XInfoConsumers(ctx, stream, group).Result()
}

// ---- Server info ----

// Info returns the Redis server INFO output for the requested sections.
// Pass no arguments to retrieve all sections.
func (c *Client) Info(ctx context.Context, section ...string) (string, error) {
	rc, err := c.requireConnected()
	if err != nil {
		return "", err
	}
	return rc.Info(ctx, section...).Result()
}

// ServerVersion parses and returns the redis_version field from INFO server.
// Example return value: "7.2.4".
func (c *Client) ServerVersion(ctx context.Context) (string, error) {
	info, err := c.Info(ctx, "server")
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "redis_version:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "redis_version:")), nil
		}
	}
	return "", fmt.Errorf("redis: redis_version not found in INFO server output")
}
