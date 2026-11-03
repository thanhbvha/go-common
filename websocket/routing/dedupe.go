package routing

import (
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

// A gateway-local incarnation number avoids retaining a closed session/connection
// in the cache, and avoids repeatedly encoding its tenant/user/session tuple.
type dedupeKey struct {
	message string
	session uint64
}
type dedupeEntry struct {
	key     dedupeKey
	expires time.Time
}
type dedupeShard struct {
	mu             sync.Mutex
	entries        map[dedupeKey]time.Time
	order          []dedupeEntry
	next, capacity int
}
type deliveryCache struct {
	shards []dedupeShard
	ttl    time.Duration
}

func newDeliveryCache(capacity int, ttl time.Duration) *deliveryCache {
	c := &deliveryCache{shards: make([]dedupeShard, min(64, capacity)), ttl: ttl}
	for i := range c.shards {
		s := &c.shards[i]
		s.capacity = capacity / len(c.shards)
		if i < capacity%len(c.shards) {
			s.capacity++
		}
		s.entries = make(map[dedupeKey]time.Time)
	}
	return c
}
func (c *deliveryCache) shard(key dedupeKey) *dedupeShard {
	return &c.shards[(xxhash.Sum64String(key.message)^key.session)%uint64(len(c.shards))]
}
func (c *deliveryCache) contains(key dedupeKey) bool {
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	expires, ok := s.entries[key]
	return ok && time.Now().Before(expires)
}
func (c *deliveryCache) remember(key dedupeKey) {
	s := c.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	entry := dedupeEntry{key: key, expires: time.Now().Add(c.ttl)}
	if len(s.order) < s.capacity {
		s.order = append(s.order, entry)
	} else {
		old := s.order[s.next]
		if expires, ok := s.entries[old.key]; ok && expires.Equal(old.expires) {
			delete(s.entries, old.key)
		}
		s.order[s.next] = entry
		s.next = (s.next + 1) % s.capacity
	}
	s.entries[key] = entry.expires
}
func (c *deliveryCache) size() int {
	n := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		n += len(s.entries)
		s.mu.Unlock()
	}
	return n
}
