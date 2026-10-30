// Package redis provides Redis session leases and targeted Pub/Sub delivery.
package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/websocket/routing"
)

type Registry struct {
	client      goredis.UniversalClient
	prefix      string
	maxSessions int
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

type storedLocation struct {
	routing.Location
	LeaseUntil int64 `json:"lease_until"`
}

func encode(l routing.Location) []byte {
	b, _ := json.Marshal(storedLocation{Location: l, LeaseUntil: l.ExpiresAt.UnixMilli()})
	return b
}

const registerScript = `
local all = redis.call('HGETALL', KEYS[1])
for i = 1, #all, 2 do
    if cjson.decode(all[i+1]).lease_until <= tonumber(ARGV[4]) then
        redis.call('HDEL', KEYS[1], all[i])
    end
end
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 and redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[5]) then
    return 0
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
	// Pipeline bounded chunks instead of a network round trip per session.
	for start := 0; start < len(ls); start += 256 {
		end := start + 256
		if end > len(ls) {
			end = len(ls)
		}
		pipe := r.client.Pipeline()
		cmds := make([]*goredis.Cmd, 0, end-start)
		for _, l := range ls[start:end] {
			if err := valid(l, ttl); err != nil {
				return err
			}
			l.ExpiresAt = time.Now().Add(ttl)
			b := encode(l)
			cmds = append(cmds, pipe.Eval(ctx, renewScript, []string{r.key(l.TenantID, "user", l.UserID)}, l.SessionID, l.Generation, b, ttl.Milliseconds(), r.maxSessions))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		for _, cmd := range cmds {
			n, err := cmd.Int()
			if err != nil {
				return err
			}
			if n == -1 {
				return routing.ErrCapacity
			}
			if n != 1 {
				return routing.ErrStale
			}
		}
	}
	return nil
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

const resolveScript = `
local all = redis.call('HGETALL', KEYS[1])
local result = {}
for i = 1, #all, 2 do
    if cjson.decode(all[i+1]).lease_until <= tonumber(ARGV[1]) then
        redis.call('HDEL', KEYS[1], all[i])
    elseif ARGV[2] == '' or all[i] == ARGV[2] then
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
if redis.call('PTTL', KEYS[2]) < tonumber(ARGV[6]) then
    redis.call('PEXPIRE', KEYS[2], ARGV[6])
end
return 1
`

func (r *Registry) JoinRoom(ctx context.Context, l routing.Location, room string, ttl time.Duration) error {
	if err := valid(l, ttl); err != nil {
		return err
	}
	if room == "" {
		return routing.ErrInvalid
	}
	n, err := r.client.Eval(ctx, joinScript, []string{r.key(l.TenantID, "user", l.UserID), r.key(l.TenantID, "room", room)}, l.SessionID, l.Generation, time.Now().UnixMilli(), time.Now().Add(ttl).UnixMilli(), member(l), ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if n != 1 {
		return routing.ErrStale
	}
	return nil
}
func (r *Registry) LeaveRoom(ctx context.Context, l routing.Location, room string) error {
	if !l.Valid() || room == "" {
		return routing.ErrInvalid
	}
	return r.client.ZRem(ctx, r.key(l.TenantID, "room", room), member(l)).Err()
}
func (r *Registry) ResolveRoom(ctx context.Context, tenant, room string) ([]string, error) {
	if tenant == "" || room == "" {
		return nil, routing.ErrInvalid
	}
	key := r.key(tenant, "room", room)
	// Removal bounds stale metadata while score filtering enforces individual leases.
	pipe := r.client.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprint(time.Now().UnixMilli()))
	cmd := pipe.ZRange(ctx, key, 0, -1)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	nodes := map[string]bool{}
	for _, v := range cmd.Val() {
		var m struct{ Node string }
		if err := json.Unmarshal([]byte(v), &m); err != nil {
			return nil, err
		}
		nodes[m.Node] = true
	}
	result := make([]string, 0, len(nodes))
	for n := range nodes {
		result = append(result, n)
	}
	return result, nil
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
			cmds = append(cmds, pipe.Eval(ctx, joinScript, []string{r.key(l.TenantID, "user", l.UserID), r.key(l.TenantID, "room", m.RoomID)}, l.SessionID, l.Generation, time.Now().UnixMilli(), time.Now().Add(ttl).UnixMilli(), member(l), ttl.Milliseconds()))
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
