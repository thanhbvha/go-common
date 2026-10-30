package routing

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type GatewayConfig struct {
	NodeID          string
	Registry        SessionRegistry
	Transport       DeliveryTransport
	LeaseTTL        time.Duration
	RenewInterval   time.Duration
	DedupeTTL       time.Duration
	DedupeCapacity  int
	MaxPayloadBytes int
}
type localSession struct {
	location Location
	enqueue  func([]byte) error
	close    func()
	rooms    map[string]struct{}
}
type Gateway struct {
	lifecycle     sync.Mutex
	started       atomic.Bool
	cfg           GatewayConfig
	mu            sync.Mutex
	sessions      map[string]*localSession
	users         map[string]map[string]*localSession
	rooms         map[string]map[string]*localSession
	dedupe        map[string]time.Time
	dedupeOrder   []dedupeEntry
	dedupeNext    int
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	leaseUntil    atomic.Int64
	leaseFailures atomic.Uint64
	delivered     atomic.Uint64
	rejected      atomic.Uint64
	closed        atomic.Bool
}

func NewGateway(c GatewayConfig) (*Gateway, error) {
	if c.Registry == nil || c.Transport == nil {
		return nil, ErrInvalid
	}
	if c.NodeID == "" {
		c.NodeID = NewID()
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.RenewInterval <= 0 {
		c.RenewInterval = c.LeaseTTL / 3
	}
	if c.LeaseTTL < time.Millisecond || c.RenewInterval <= 0 || c.RenewInterval >= c.LeaseTTL {
		return nil, ErrInvalid
	}
	if c.DedupeTTL <= 0 {
		c.DedupeTTL = time.Minute
	}
	if c.DedupeCapacity <= 0 {
		c.DedupeCapacity = 100000
	}
	if c.MaxPayloadBytes <= 0 {
		c.MaxPayloadBytes = 1 << 20
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Gateway{cfg: c, sessions: make(map[string]*localSession), users: make(map[string]map[string]*localSession), rooms: make(map[string]map[string]*localSession), dedupe: make(map[string]time.Time), ctx: ctx, cancel: cancel, done: make(chan struct{})}, nil
}
func (g *Gateway) NodeID() string { return g.cfg.NodeID }
func (g *Gateway) Start(ctx context.Context) error {
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	if g.started.Load() || g.closed.Load() {
		return ErrInvalid
	}
	if err := g.cfg.Registry.Renew(ctx, nil, g.cfg.LeaseTTL); err != nil {
		return err
	}
	if err := g.cfg.Transport.Start(ctx, g.cfg.NodeID, g.Handle); err != nil {
		return err
	}
	g.started.Store(true)
	g.leaseUntil.Store(time.Now().Add(g.cfg.LeaseTTL).UnixNano())
	go g.renewLoop()
	return nil
}
func (g *Gateway) Ready() bool {
	return !g.closed.Load() && time.Now().UnixNano() < g.leaseUntil.Load() && g.cfg.Transport.Ready()
}
func userKey(t, u string) string { return tuple(t, u) }
func roomKey(t, r string) string { return tuple(t, r) }

// Register publishes a session. The enqueue callback must be nonblocking and must
// not mutate the immutable gateway-owned payload; retaining it for a write is safe.
func (g *Gateway) Register(ctx context.Context, l Location, enqueue func([]byte) error, closeSession func()) (Location, error) {
	if !g.Ready() {
		return Location{}, ErrUnavailable
	}
	if l.SessionID == "" {
		l.SessionID = NewID()
	}
	l.NodeID = g.cfg.NodeID
	l.Generation = NewID()
	if !l.Valid() || enqueue == nil {
		return Location{}, ErrInvalid
	}
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	if !g.Ready() {
		return Location{}, ErrUnavailable
	}
	// Install before publishing the location so a newly resolved command can enqueue immediately.
	s := &localSession{location: l, enqueue: enqueue, close: closeSession, rooms: make(map[string]struct{})}
	g.mu.Lock()
	old := g.sessions[l.Key()]
	if old != nil {
		g.removeLocal(old)
	}
	g.sessions[l.Key()] = s
	uk := userKey(l.TenantID, l.UserID)
	if g.users[uk] == nil {
		g.users[uk] = make(map[string]*localSession)
	}
	g.users[uk][l.Key()] = s
	g.mu.Unlock()
	if old != nil && old.close != nil {
		old.close()
	}
	registered, err := g.cfg.Registry.Register(ctx, l, g.cfg.LeaseTTL)
	if err != nil {
		g.mu.Lock()
		g.removeLocal(s)
		g.mu.Unlock()
		return Location{}, err
	}
	g.mu.Lock()
	s.location = registered
	g.mu.Unlock()
	return registered, nil
}
func (g *Gateway) removeLocal(s *localSession) {
	l := s.location
	delete(g.sessions, l.Key())
	uk := userKey(l.TenantID, l.UserID)
	delete(g.users[uk], l.Key())
	if len(g.users[uk]) == 0 {
		delete(g.users, uk)
	}
	for r := range s.rooms {
		rk := roomKey(l.TenantID, r)
		delete(g.rooms[rk], l.Key())
		if len(g.rooms[rk]) == 0 {
			delete(g.rooms, rk)
		}
	}
}
func (g *Gateway) Remove(ctx context.Context, l Location) error {
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	g.mu.Lock()
	s := g.sessions[l.Key()]
	if s != nil && s.location.Generation == l.Generation {
		g.removeLocal(s)
	} else {
		s = nil
	}
	g.mu.Unlock()
	var first error
	if s != nil {
		for room := range s.rooms {
			if err := g.cfg.Registry.LeaveRoom(ctx, l, room); err != nil && first == nil {
				first = err
			}
		}
	}
	if err := g.cfg.Registry.Remove(ctx, l); err != nil {
		return err
	}
	return first
}
func (g *Gateway) JoinRoom(ctx context.Context, l Location, room string) error {
	if room == "" {
		return ErrInvalid
	}
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	g.mu.Lock()
	s := g.sessions[l.Key()]
	if s == nil || s.location.Generation != l.Generation {
		g.mu.Unlock()
		return ErrStale
	}
	if _, joined := s.rooms[room]; joined {
		g.mu.Unlock()
		return nil
	}
	// Publish local membership first; delivery never waits on registry I/O.
	s.rooms[room] = struct{}{}
	rk := roomKey(l.TenantID, room)
	if g.rooms[rk] == nil {
		g.rooms[rk] = make(map[string]*localSession)
	}
	g.rooms[rk][l.Key()] = s
	g.mu.Unlock()
	if err := g.cfg.Registry.JoinRoom(ctx, l, room, g.cfg.LeaseTTL); err != nil {
		g.mu.Lock()
		delete(s.rooms, room)
		delete(g.rooms[rk], l.Key())
		if len(g.rooms[rk]) == 0 {
			delete(g.rooms, rk)
		}
		g.mu.Unlock()
		return err
	}
	return nil
}
func (g *Gateway) LeaveRoom(ctx context.Context, l Location, room string) error {
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	g.mu.Lock()
	s := g.sessions[l.Key()]
	if s == nil || s.location.Generation != l.Generation {
		g.mu.Unlock()
		return nil
	}
	delete(s.rooms, room)
	rk := roomKey(l.TenantID, room)
	delete(g.rooms[rk], l.Key())
	if len(g.rooms[rk]) == 0 {
		delete(g.rooms, rk)
	}
	g.mu.Unlock()
	return g.cfg.Registry.LeaveRoom(ctx, l, room)
}
func (g *Gateway) Handle(ctx context.Context, c DeliveryCommand) DeliveryResult {
	g.mu.Lock()
	defer g.mu.Unlock()
	result := DeliveryResult{}
	if c.RoomID != "" {
		result.Counts = make(map[Status]int)
	}
	if !g.Ready() || c.Version != 1 || c.TargetNode != g.cfg.NodeID || c.MessageID == "" || c.TenantID == "" || len(c.Payload) > g.cfg.MaxPayloadBytes || !time.Now().Before(c.ExpiresAt) || ctx.Err() != nil {
		return DeliveryResult{Outcomes: []Outcome{{Location: Location{NodeID: g.cfg.NodeID}, Status: Unavailable}}}
	}
	sessions := c.Sessions
	if c.RoomID != "" {
		sessions = nil
		for _, s := range g.rooms[roomKey(c.TenantID, c.RoomID)] {
			sessions = append(sessions, s.location)
		}
	}
	var payload []byte
	prepared := false
	for _, l := range sessions {
		status := NotFound
		s := g.sessions[l.Key()]
		if l.TenantID != c.TenantID || l.NodeID != g.cfg.NodeID {
			status = Stale
		} else if s != nil {
			if s.location.Generation != l.Generation {
				status = Stale
			} else {
				dk := tuple(c.MessageID, l.DeliveryKey())
				if expires, ok := g.dedupe[dk]; ok && time.Now().Before(expires) {
					status = Enqueued
				} else {
					if !prepared {
						payload = bytes.Clone(c.Payload)
						prepared = true
					}
					err := s.enqueue(payload)
					switch {
					case err == nil:
						status = Enqueued
						g.remember(dk)
						g.delivered.Add(1)
					case errors.Is(err, ErrQueueFull):
						status = QueueFull
					case errors.Is(err, ErrClosed):
						status = NotFound
					default:
						status = Unavailable
					}
				}
			}
		}
		if status != Enqueued {
			g.rejected.Add(1)
		}
		if c.RoomID != "" {
			result.Counts[status]++
		} else {
			result.Outcomes = append(result.Outcomes, Outcome{Location: l, Status: status})
		}
	}
	if c.RoomID != "" {
		for status := range result.Counts {
			result.Outcomes = append(result.Outcomes, Outcome{Location: Location{TenantID: c.TenantID, NodeID: g.cfg.NodeID}, Status: status})
		}
	}
	if len(result.Outcomes) == 0 {
		result.Offline = true
	}
	return result
}

type dedupeEntry struct {
	key     string
	expires time.Time
}

func (g *Gateway) remember(key string) {
	entry := dedupeEntry{key: key, expires: time.Now().Add(g.cfg.DedupeTTL)}
	if len(g.dedupeOrder) < g.cfg.DedupeCapacity {
		g.dedupeOrder = append(g.dedupeOrder, entry)
	} else {
		old := g.dedupeOrder[g.dedupeNext]
		if expires, ok := g.dedupe[old.key]; ok && expires.Equal(old.expires) {
			delete(g.dedupe, old.key)
		}
		g.dedupeOrder[g.dedupeNext] = entry
		g.dedupeNext = (g.dedupeNext + 1) % g.cfg.DedupeCapacity
	}
	g.dedupe[key] = entry.expires
}
func (g *Gateway) renewLoop() {
	defer close(g.done)
	tick := time.NewTicker(g.cfg.RenewInterval)
	defer tick.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-tick.C:
			g.renew()
		}
	}
}
func (g *Gateway) renew() {
	g.lifecycle.Lock()
	defer g.lifecycle.Unlock()
	renewedAt := time.Now()
	ctx, cancel := context.WithTimeout(g.ctx, g.cfg.RenewInterval)
	defer cancel()
	g.mu.Lock()
	ls := make([]Location, 0, len(g.sessions))
	for _, s := range g.sessions {
		ls = append(ls, s.location)
	}
	g.mu.Unlock()
	err := g.cfg.Registry.Renew(ctx, ls, g.cfg.LeaseTTL)
	if errors.Is(err, ErrStale) {
		for _, s := range g.sessions {
			locations, e := g.cfg.Registry.Resolve(ctx, s.location.TenantID, s.location.UserID, s.location.SessionID)
			if e != nil {
				continue
			}
			valid := false
			for _, l := range locations {
				if l.Generation == s.location.Generation {
					valid = true
				}
			}
			if !valid {
				g.mu.Lock()
				g.removeLocal(s)
				g.mu.Unlock()
				if s.close != nil {
					s.close()
				}
			}
		}
	}
	if err == nil {
		var rooms []RoomMembership
		for _, s := range g.sessions {
			for room := range s.rooms {
				rooms = append(rooms, RoomMembership{Location: s.location, RoomID: room})
			}
		}
		if batch, ok := g.cfg.Registry.(RoomLeaseRenewer); ok {
			err = batch.RenewRooms(ctx, rooms, g.cfg.LeaseTTL)
		} else {
			for _, m := range rooms {
				if e := g.cfg.Registry.JoinRoom(ctx, m.Location, m.RoomID, g.cfg.LeaseTTL); e != nil {
					err = e
				}
			}
		}
	}
	if err != nil {
		g.leaseFailures.Add(1)
		return
	}
	g.leaseUntil.Store(renewedAt.Add(g.cfg.LeaseTTL).UnixNano())
}
func (g *Gateway) Close(ctx context.Context) error {
	g.closed.Store(true)
	g.cancel()
	if g.started.Load() {
		select {
		case <-g.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return g.cfg.Transport.Close(ctx)
}

type GatewayStats struct {
	Ready                             bool
	Sessions, Rooms, DedupeEntries    int
	LeaseFailures, Enqueued, Rejected uint64
}

func (g *Gateway) Stats() GatewayStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return GatewayStats{g.Ready(), len(g.sessions), len(g.rooms), len(g.dedupe), g.leaseFailures.Load(), g.delivered.Load(), g.rejected.Load()}
}
