// Package redis provides Redis session leases and targeted Pub/Sub delivery.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

type Registry struct {
	client        goredis.UniversalClient
	prefix        string
	maxSessions   int
	roomNodeIndex bool
}

func NewRegistry(client goredis.UniversalClient, prefix string, options ...RegistryOption) *Registry {
	if prefix == "" {
		prefix = "websocket"
	}
	r := &Registry{client: client, prefix: prefix, maxSessions: 1000}
	for _, opt := range options {
		opt(r)
	}
	return r
}
func digest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func (r *Registry) key(tenant, kind, id string) string {
	return r.prefix + ":{" + digest(tenant) + "}:" + kind + ":" + digest(id)
}
func valid(l routing.Location, ttl time.Duration) error {
	if !l.Valid() || ttl < time.Millisecond {
		return routing.ErrInvalid
	}
	return nil
}

type RegistryOption func(*Registry)

func WithMaxSessionsPerUser(n int) RegistryOption {
	return func(r *Registry) {
		if n > 0 {
			r.maxSessions = n
		}
	}
}

// WithRoomNodeIndex enables O(hosting nodes) room resolution. All writers in
// this namespace must first support the node index; see the migration guidance.
// Writers maintain both formats even when reads use the compatibility path.
func WithRoomNodeIndex() RegistryOption {
	return func(r *Registry) { r.roomNodeIndex = true }
}

func (r *Registry) roomKeys(l routing.Location, room string) []string {
	pair, _ := json.Marshal([2]string{room, l.NodeID})
	return []string{r.key(l.TenantID, "room", room), r.key(l.TenantID, "room_nodes", room), r.key(l.TenantID, "room_node", string(pair))}
}

type storedLocation struct {
	routing.Location
	LeaseUntil int64 `json:"lease_until"`
}

func encode(l routing.Location) []byte {
	b, _ := json.Marshal(storedLocation{Location: l, LeaseUntil: l.ExpiresAt.UnixMilli()})
	return b
}

// Replacing a live session does not consume a slot. A new session only needs
// expiry pruning when the physical hash is full; every mutation stays atomic.
// Keeping the existing hash format also supports writers from older versions.
const registerScript = `
local now = tonumber(ARGV[4])
local limit = tonumber(ARGV[5])
local old = redis.call('HGET', KEYS[1], ARGV[1])
local replacing = false
if old then
    if cjson.decode(old).lease_until > now then
        replacing = true
    else
        redis.call('HDEL', KEYS[1], ARGV[1])
    end
end
if not replacing and redis.call('HLEN', KEYS[1]) >= limit then
    local all = redis.call('HGETALL', KEYS[1])
    for i = 1, #all, 2 do
        if cjson.decode(all[i+1]).lease_until <= now then
            redis.call('HDEL', KEYS[1], all[i])
        end
    end
    if redis.call('HLEN', KEYS[1]) >= limit then return 0 end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
if redis.call('PTTL', KEYS[1]) < tonumber(ARGV[3]) then
    redis.call('PEXPIRE', KEYS[1], ARGV[3])
end
return 1
`

func (r *Registry) Register(ctx context.Context, l routing.Location, ttl time.Duration) (routing.Location, error) {
	if err := valid(l, ttl); err != nil {
		return l, err
	}
	l.ExpiresAt = time.Now().Add(ttl)
	n, err := r.client.Eval(ctx, registerScript, []string{r.key(l.TenantID, "user", l.UserID)}, l.SessionID, encode(l), ttl.Milliseconds(), time.Now().UnixMilli(), r.maxSessions).Int()
	if err != nil {
		return l, err
	}
	if n != 1 {
		return l, routing.ErrCapacity
	}
	return l, nil
}

const renewScript = `
local old = redis.call('HGET', KEYS[1], ARGV[1])
if old and cjson.decode(old).generation ~= ARGV[2] then return 0 end
if not old and redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[5]) then return -1 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
if redis.call('PTTL', KEYS[1]) < tonumber(ARGV[4]) then
    redis.call('PEXPIRE', KEYS[1], ARGV[4])
end
return 1
`

func (r *Registry) Renew(ctx context.Context, ls []routing.Location, ttl time.Duration) error {
	if ttl < time.Millisecond {
		return routing.ErrInvalid
	}
	if len(ls) == 0 {
		return r.client.Ping(ctx).Err()
	}
	var err error
	for _, result := range r.RenewBatch(ctx, ls, ttl) {
		err = errors.Join(err, result.Err)
	}
	return err
}

// RenewBatch executes every bounded chunk, preserving a result for each input.
func (r *Registry) RenewBatch(ctx context.Context, ls []routing.Location, ttl time.Duration) []routing.RenewResult {
	results := make([]routing.RenewResult, len(ls))
	for i, l := range ls {
		results[i] = routing.RenewResult{Location: l, Err: valid(l, ttl)}
	}
	for start := 0; start < len(ls); start += 256 {
		end := min(start+256, len(ls))
		pipe := r.client.Pipeline()
		cmds := make([]*goredis.Cmd, end-start)
		for i := start; i < end; i++ {
			if results[i].Err != nil {
				continue
			}
			l := ls[i]
			l.ExpiresAt = time.Now().Add(ttl)
			cmds[i-start] = pipe.Eval(ctx, renewScript, []string{r.key(l.TenantID, "user", l.UserID)}, l.SessionID, l.Generation, encode(l), ttl.Milliseconds(), r.maxSessions)
		}
		_, _ = pipe.Exec(ctx) // Each command carries its own transport/script error.
		for offset, cmd := range cmds {
			if cmd == nil {
				continue
			}
			n, err := cmd.Int()
			if err == nil {
				switch n {
				case 1:
				case -1:
					err = routing.ErrCapacity
				default:
					err = routing.ErrStale
				}
			}
			results[start+offset].Err = err
		}
	}
	return results
}

const removeScript = `
local old = redis.call('HGET', KEYS[1], ARGV[1])
if old and cjson.decode(old).generation == ARGV[2] then
    redis.call('HDEL', KEYS[1], ARGV[1])
end
return 1
`

func (r *Registry) Remove(ctx context.Context, l routing.Location) error {
	if !l.Valid() {
		return routing.ErrInvalid
	}
	return r.client.Eval(ctx, removeScript, []string{r.key(l.TenantID, "user", l.UserID)}, l.SessionID, l.Generation).Err()
}

// All keys share the tenant hash slot. Room members include the generation,
// so delayed cleanup never deletes membership belonging to a replacement.
const removeSessionScript = `
local old = redis.call('HGET', KEYS[1], ARGV[1])
if old and cjson.decode(old).generation == ARGV[2] then
 redis.call('HDEL', KEYS[1], ARGV[1])
end
for i = 2, #KEYS, 3 do
 redis.call('ZREM', KEYS[i], ARGV[3])
 redis.call('ZREM', KEYS[i+2], ARGV[3])
 redis.call('ZREMRANGEBYSCORE', KEYS[i+2], '-inf', ARGV[5])
 local last = redis.call('ZREVRANGE', KEYS[i+2], 0, 0, 'WITHSCORES')
 if #last == 0 then
  redis.call('ZREM', KEYS[i+1], ARGV[4])
 else
  redis.call('ZADD', KEYS[i+1], last[2], ARGV[4])
 end
end
return 1
`

func (r *Registry) RemoveSession(ctx context.Context, l routing.Location, rooms []string) error {
	if !l.Valid() {
		return routing.ErrInvalid
	}
	keys := []string{r.key(l.TenantID, "user", l.UserID)}
	for _, room := range rooms {
		if room == "" {
			return routing.ErrInvalid
		}
		keys = append(keys, r.roomKeys(l, room)...)
	}
	return r.client.Eval(ctx, removeSessionScript, keys, l.SessionID, l.Generation, member(l), l.NodeID, time.Now().UnixMilli()).Err()
}

// Single-session routing inspects and prunes only that field. User-wide routing
// still scans because it must return all live locations.
const resolveScript = `
if ARGV[2] ~= '' then
    local value = redis.call('HGET', KEYS[1], ARGV[2])
    if not value then return {} end
    if cjson.decode(value).lease_until <= tonumber(ARGV[1]) then
        redis.call('HDEL', KEYS[1], ARGV[2])
        return {}
    end
    return {value}
end
local all = redis.call('HGETALL', KEYS[1])
local result = {}
for i = 1, #all, 2 do
    if cjson.decode(all[i+1]).lease_until <= tonumber(ARGV[1]) then
        redis.call('HDEL', KEYS[1], all[i])
    else
        table.insert(result, all[i+1])
    end
end
return result
`

func (r *Registry) Resolve(ctx context.Context, tenant, user, session string) ([]routing.Location, error) {
	if tenant == "" || user == "" {
		return nil, routing.ErrInvalid
	}
	values, err := r.client.Eval(ctx, resolveScript, []string{r.key(tenant, "user", user)}, time.Now().UnixMilli(), session).StringSlice()
	if err != nil {
		return nil, err
	}
	result := make([]routing.Location, 0, len(values))
	for _, v := range values {
		var l routing.Location
		if err := json.Unmarshal([]byte(v), &l); err != nil {
			return nil, fmt.Errorf("registry decode: %w", err)
		}
		if time.Now().Before(l.ExpiresAt) {
			result = append(result, l)
		}
	}
	return result, nil
}
func member(l routing.Location) string {
	b, _ := json.Marshal(struct{ Node, User, Session, Generation string }{l.NodeID, l.UserID, l.SessionID, l.Generation})
	return string(b)
}

const joinScript = `
local old = redis.call('HGET', KEYS[1], ARGV[1])
if not old then return 0 end
local session = cjson.decode(old)
if session.generation ~= ARGV[2] or session.lease_until <= tonumber(ARGV[3]) then return 0 end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5])
redis.call('ZREMRANGEBYSCORE', KEYS[4], '-inf', ARGV[3])
redis.call('ZADD', KEYS[4], ARGV[4], ARGV[5])
local last = redis.call('ZREVRANGE', KEYS[4], 0, 0, 'WITHSCORES')
redis.call('ZADD', KEYS[3], last[2], ARGV[7])
for i=2,4 do
 if redis.call('PTTL', KEYS[i]) < tonumber(ARGV[6]) then
  redis.call('PEXPIRE', KEYS[i], ARGV[6])
 end
end
return 1
`

func (r *Registry) joinKeys(l routing.Location, room string) []string {
	return append([]string{r.key(l.TenantID, "user", l.UserID)}, r.roomKeys(l, room)...)
}
func (r *Registry) JoinRoom(ctx context.Context, l routing.Location, room string, ttl time.Duration) error {
	if err := valid(l, ttl); err != nil {
		return err
	}
	if room == "" {
		return routing.ErrInvalid
	}
	n, err := r.client.Eval(ctx, joinScript, r.joinKeys(l, room), l.SessionID, l.Generation, time.Now().UnixMilli(), time.Now().Add(ttl).UnixMilli(), member(l), ttl.Milliseconds(), l.NodeID).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		return routing.ErrStale
	}
	return nil
}

const leaveRoomScript = `
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', ARGV[3])
local last = redis.call('ZREVRANGE', KEYS[3], 0, 0, 'WITHSCORES')
if #last == 0 then
 redis.call('ZREM', KEYS[2], ARGV[2])
else
 redis.call('ZADD', KEYS[2], last[2], ARGV[2])
end
return 1
`

func (r *Registry) LeaveRoom(ctx context.Context, l routing.Location, room string) error {
	if !l.Valid() || room == "" {
		return routing.ErrInvalid
	}
	return r.client.Eval(ctx, leaveRoomScript, r.roomKeys(l, room), member(l), l.NodeID, time.Now().UnixMilli()).Err()
}

// Legacy writers do not maintain the node index. Aggregate their members in
// Redis so compatibility reads transfer/decode only the distinct node IDs.
const resolveLegacyRoomScript = `
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local members = redis.call('ZRANGE', KEYS[1], 0, -1)
local seen = {}
local nodes = {}
for _, value in ipairs(members) do
 local node = cjson.decode(value).Node
 if type(node) ~= 'string' or node == '' then return redis.error_reply('invalid room member') end
 if not seen[node] then seen[node] = true; table.insert(nodes,node) end
end
return nodes
`
const resolveRoomNodesScript = `
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
return redis.call('ZRANGE', KEYS[1], 0, -1)
`

func (r *Registry) ResolveRoom(ctx context.Context, tenant, room string) ([]string, error) {
	if tenant == "" || room == "" {
		return nil, routing.ErrInvalid
	}
	kind, script := "room", resolveLegacyRoomScript
	if r.roomNodeIndex {
		kind, script = "room_nodes", resolveRoomNodesScript
	}
	return r.client.Eval(ctx, script, []string{r.key(tenant, kind, room)}, time.Now().UnixMilli()).StringSlice()
}

func (r *Registry) RenewRooms(ctx context.Context, rooms []routing.RoomMembership, ttl time.Duration) error {
	for start := 0; start < len(rooms); start += 256 {
		end := start + 256
		if end > len(rooms) {
			end = len(rooms)
		}
		pipe := r.client.Pipeline()
		var cmds []*goredis.Cmd
		for _, m := range rooms[start:end] {
			l := m.Location
			if err := valid(l, ttl); err != nil {
				return err
			}
			if m.RoomID == "" {
				return routing.ErrInvalid
			}
			cmds = append(cmds, pipe.Eval(ctx, joinScript, r.joinKeys(l, m.RoomID), l.SessionID, l.Generation, time.Now().UnixMilli(), time.Now().Add(ttl).UnixMilli(), member(l), ttl.Milliseconds(), l.NodeID))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		for _, cmd := range cmds {
			n, err := cmd.Int()
			if err != nil {
				return err
			}
			if n != 1 {
				return routing.ErrStale
			}
		}
	}
	return nil
}
