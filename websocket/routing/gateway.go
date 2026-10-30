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
	lifecycle          sync.Mutex
	started            atomic.Bool
	cfg                GatewayConfig
	mu                 sync.Mutex
	sessions           map[string]*localSession
	users              map[string]map[string]*localSession
	rooms              map[string]map[string]*localSession
	dedupe             map[string]time.Time
	dedupeOrder        []dedupeEntry
	dedupeNext         int
	ctx                context.Context
	cancel             context.CancelFunc
	done               chan struct{}
	leaseUntil         atomic.Int64
	leaseFailures      atomic.Uint64
	unregisterFailures atomic.Uint64
	unregisterRetries  atomic.Uint64
	delivered          atomic.Uint64
	rejected           atomic.Uint64
	closed             atomic.Bool
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
	var closeOld func()
	defer func() {
		g.lifecycle.Unlock()
		if closeOld != nil {
			closeOld()
		}
	}()
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
	registered, err := g.cfg.Registry.Register(ctx, l, g.cfg.LeaseTTL)
	if err != nil {
		g.mu.Lock()
		if g.removeLocal(s) && old != nil {
			g.installLocal(old)
		}
		g.mu.Unlock()
		return Location{}, err
	}
	g.mu.Lock()
	s.location = registered
	g.mu.Unlock()
	if old != nil {
		closeOld = old.close
		rooms := make([]string, 0, len(old.rooms))
		for room := range old.rooms {
			rooms = append(rooms, room)
		}
		// Publication is committed. Cleanup is generation-fenced and failures
		// are counted, with lease expiry as fallback; do not roll it back.
		_ = g.unregister(ctx, old.location, rooms)
	}
	return registered, nil
}

// installLocal restores all indexes; caller holds mu and lifecycle.
func (g *Gateway) installLocal(s *localSession) {
	l := s.location
	g.sessions[l.Key()] = s
	uk := userKey(l.TenantID, l.UserID)
	if g.users[uk] == nil {
		g.users[uk] = make(map[string]*localSession)
	}
	g.users[uk][l.Key()] = s
	for room := range s.rooms {
		rk := roomKey(l.TenantID, room)
		if g.rooms[rk] == nil {
			g.rooms[rk] = make(map[string]*localSession)
		}
		g.rooms[rk][l.Key()] = s
	}
}

// removeLocal only detaches the current owner; caller holds mu.
func (g *Gateway) removeLocal(s *localSession) bool {
	l := s.location
	if g.sessions[l.Key()] != s {
		return false
	}
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
	return true
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
	var rooms []string
	if s != nil {
		for room := range s.rooms {
			rooms = append(rooms, room)
		}
	}
	return g.unregister(ctx, l, rooms)
}

// unregister retries idempotent, generation-fenced cleanup within the caller's
// deadline. Local indexes are already detached and mu is never held during I/O.
func (g *Gateway) unregister(ctx context.Context, l Location, rooms []string) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			g.unregisterFailures.Add(1)
			return ctx.Err()
		}
		if atomic, ok := g.cfg.Registry.(SessionRemover); ok {
			err = atomic.RemoveSession(ctx, l, rooms)
		} else {
			err = nil
			for _, room := range rooms {
				err = errors.Join(err, g.cfg.Registry.LeaveRoom(ctx, l, room))
			}
			err = errors.Join(err, g.cfg.Registry.Remove(ctx, l))
		}
		if err == nil {
			return nil
		}
		g.unregisterFailures.Add(1)
		if errors.Is(err, ErrInvalid) || errors.Is(err, ErrStale) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || attempt == 2 {
			return err
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		g.unregisterRetries.Add(1)
	}
	return err
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

// sessionSnapshot copies mutable room maps under mu. lifecycle keeps registry
// operations ordered, including leave/remove versus in-flight lease renewal.
type sessionSnapshot struct {
	session  *localSession
	location Location
	rooms    []RoomMembership
}

func (g *Gateway) snapshotSessions() []sessionSnapshot {
	g.mu.Lock()
	defer g.mu.Unlock()
	snapshots := make([]sessionSnapshot, 0, len(g.sessions))
	for _, s := range g.sessions {
		item := sessionSnapshot{session: s, location: s.location}
		for room := range s.rooms {
			item.rooms = append(item.rooms, RoomMembership{Location: s.location, RoomID: room})
		}
		snapshots = append(snapshots, item)
	}
	return snapshots
}
func (g *Gateway) renew() {
	g.lifecycle.Lock()
	var closed []func()
	defer func() {
		g.lifecycle.Unlock()
		for _, closeSession := range closed {
			closeSession()
		}
	}()
	renewedAt := time.Now()
	ctx, cancel := context.WithTimeout(g.ctx, g.cfg.RenewInterval)
	defer cancel()
	snapshots := g.snapshotSessions()
	ls := make([]Location, 0, len(snapshots))
	for _, s := range snapshots {
		ls = append(ls, s.location)
	}
	results := make([]RenewResult, len(ls))
	var failure error
	if batch, ok := g.cfg.Registry.(SessionLeaseRenewer); ok && len(ls) > 0 {
		results = batch.RenewBatch(ctx, ls, g.cfg.LeaseTTL)
		if len(results) != len(ls) {
			g.leaseFailures.Add(1)
			return
		}
	} else {
		err := g.cfg.Registry.Renew(ctx, ls, g.cfg.LeaseTTL)
		// A legacy implementation may stop at the first stale session. Resolve
		// fences owners but cannot prove the remaining leases were extended.
		failure = err
		if len(ls) == 0 && err != nil {
			g.leaseFailures.Add(1)
			return
		}
		for i, l := range ls {
			results[i] = RenewResult{Location: l, Err: err}
			if errors.Is(err, ErrStale) {
				locations, e := g.cfg.Registry.Resolve(ctx, l.TenantID, l.UserID, l.SessionID)
				if e != nil {
					results[i].Err = e
					continue
				}
				for _, current := range locations {
					if current.Generation == l.Generation && current.NodeID == l.NodeID {
						results[i].Err = nil
						break
					}
				}
			}
		}
	}
	var rooms []RoomMembership
	for i, result := range results {
		snapshot := snapshots[i]
		if result.Location.DeliveryKey() != snapshot.location.DeliveryKey() || result.Location.NodeID != snapshot.location.NodeID {
			failure = errors.Join(failure, ErrInvalid)
			continue
		}
		if errors.Is(result.Err, ErrStale) {
			g.mu.Lock()
			removed := g.removeLocal(snapshot.session)
			g.mu.Unlock()
			if removed && snapshot.session.close != nil {
				closed = append(closed, snapshot.session.close)
			}
			if removed && len(snapshot.rooms) > 0 {
				roomIDs := make([]string, 0, len(snapshot.rooms))
				for _, room := range snapshot.rooms {
					roomIDs = append(roomIDs, room.RoomID)
				}
				_ = g.unregister(ctx, snapshot.location, roomIDs)
			}
			continue
		}
		if result.Err != nil {
			failure = errors.Join(failure, result.Err)
			continue
		}
		rooms = append(rooms, snapshot.rooms...)
	}
	if batch, ok := g.cfg.Registry.(RoomLeaseRenewer); ok {
		failure = errors.Join(failure, batch.RenewRooms(ctx, rooms, g.cfg.LeaseTTL))
	} else {
		for _, m := range rooms {
			failure = errors.Join(failure, g.cfg.Registry.JoinRoom(ctx, m.Location, m.RoomID, g.cfg.LeaseTTL))
		}
	}
	if failure != nil {
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
	Ready                                 bool
	Sessions, Rooms, DedupeEntries        int
	LeaseFailures, Enqueued, Rejected     uint64
	UnregisterFailures, UnregisterRetries uint64
}

func (g *Gateway) Stats() GatewayStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	return GatewayStats{Ready: g.Ready(), Sessions: len(g.sessions), Rooms: len(g.rooms), DedupeEntries: len(g.dedupe), LeaseFailures: g.leaseFailures.Load(), Enqueued: g.delivered.Load(), Rejected: g.rejected.Load(), UnregisterFailures: g.unregisterFailures.Load(), UnregisterRetries: g.unregisterRetries.Load()}
}
