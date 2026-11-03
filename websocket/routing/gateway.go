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
	// RegistryConcurrency bounds concurrent session/room registry operations.
	RegistryConcurrency int
}
type localSession struct {
	number     uint64
	location   Location
	enqueue    func([]byte) error
	close      func()
	rooms      map[string]struct{}
	leaseUntil atomic.Int64
	delivery   contextMutex
	active     atomic.Bool
}
type Gateway struct {
	lifecycle          contextMutex
	operations         operationGroup
	sessionLocks       sessionGates
	registrySlots      chan struct{}
	started            atomic.Bool
	cfg                GatewayConfig
	mu                 sync.Mutex
	sessions           map[string]*localSession
	users              map[string]map[string]*localSession
	rooms              map[string]map[string]*localSession
	dedupe             *deliveryCache
	nextSession        atomic.Uint64
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
	if c.RegistryConcurrency <= 0 {
		c.RegistryConcurrency = 32
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Gateway{cfg: c, registrySlots: make(chan struct{}, c.RegistryConcurrency), sessions: make(map[string]*localSession), users: make(map[string]map[string]*localSession), rooms: make(map[string]map[string]*localSession), dedupe: newDeliveryCache(c.DedupeCapacity, c.DedupeTTL), ctx: ctx, cancel: cancel, done: make(chan struct{})}, nil
}
func (g *Gateway) NodeID() string { return g.cfg.NodeID }
func (g *Gateway) Start(ctx context.Context) error {
	ctx, finish, err := g.operationContext(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if err := g.lifecycle.lock(ctx); err != nil {
		return err
	}
	defer g.lifecycle.unlock()
	if g.started.Load() || g.closed.Load() {
		return ErrInvalid
	}
	if err := g.cfg.Registry.Renew(ctx, nil, g.cfg.LeaseTTL); err != nil {
		return err
	}
	if err := g.cfg.Transport.Start(ctx, g.cfg.NodeID, g.Handle); err != nil {
		return err
	}
	if ctx.Err() != nil || g.closed.Load() {
		return ErrUnavailable
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
	ctx, finish, err := g.operationContext(ctx)
	if err != nil {
		return Location{}, err
	}
	defer finish()
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
	unlock, err := g.sessionLocks.acquire(ctx, l.Key())
	if err != nil {
		return Location{}, err
	}
	var closeOld func()
	defer func() {
		unlock()
		if closeOld != nil {
			closeOld()
		}
	}()
	if !g.Ready() {
		return Location{}, ErrUnavailable
	}
	release, err := g.registryPermit(ctx)
	if err != nil {
		return Location{}, err
	}
	defer release()
	// Install before publishing the location so a newly resolved command can enqueue immediately.
	s := &localSession{number: g.nextSession.Add(1), location: l, enqueue: enqueue, close: closeSession, rooms: make(map[string]struct{})}
	s.leaseUntil.Store(time.Now().Add(g.cfg.LeaseTTL).UnixNano())
	g.mu.Lock()
	old := g.sessions[l.Key()]
	if old != nil {
		g.removeLocal(old)
	}
	s.active.Store(true)
	g.sessions[l.Key()] = s
	uk := userKey(l.TenantID, l.UserID)
	if g.users[uk] == nil {
		g.users[uk] = make(map[string]*localSession)
	}
	g.users[uk][l.Key()] = s
	g.mu.Unlock()
	registered, err := g.cfg.Registry.Register(ctx, l, g.cfg.LeaseTTL)
	if err != nil || ctx.Err() != nil || g.closed.Load() {
		g.mu.Lock()
		if g.removeLocal(s) && old != nil {
			g.installLocal(old)
		}
		g.mu.Unlock()
		if ctx.Err() != nil || g.closed.Load() {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = g.unregister(cleanup, l, nil)
			cancel()
			if err == nil {
				err = ErrUnavailable
				if ctx.Err() != nil {
					err = ctx.Err()
				}
			}
		}
		return Location{}, err
	}
	g.mu.Lock()
	if g.sessions[l.Key()] != s {
		g.mu.Unlock()
		_ = g.unregister(ctx, registered, nil)
		return Location{}, ErrClosed
	}
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

// installLocal restores all indexes; caller holds mu.
func (g *Gateway) installLocal(s *localSession) {
	if s.number == 0 {
		s.number = g.nextSession.Add(1)
	}
	l := s.location
	s.active.Store(true)
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
	s.active.Store(false)
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
	g.mu.Lock()
	s := g.sessions[l.Key()]
	if s != nil && s.location.Generation == l.Generation {
		g.removeLocal(s)
	} else {
		s = nil
	}
	var rooms []string
	if s != nil {
		for room := range s.rooms {
			rooms = append(rooms, room)
		}
	}
	g.mu.Unlock()
	ctx, finish, err := g.operationContext(ctx)
	if err != nil {
		g.unregisterFailures.Add(1)
		return err
	}
	defer finish()
	unlock, err := g.sessionLocks.acquire(ctx, l.Key())
	if err != nil {
		g.unregisterFailures.Add(1)
		return err
	}
	defer unlock()
	release, err := g.registryPermit(ctx)
	if err != nil {
		g.unregisterFailures.Add(1)
		return err
	}
	defer release()
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
	ctx, finish, err := g.operationContext(ctx)
	if err != nil {
		return err
	}
	defer finish()
	if room == "" {
		return ErrInvalid
	}
	unlock, err := g.sessionLocks.acquire(ctx, l.Key())
	if err != nil {
		return err
	}
	defer unlock()
	release, err := g.registryPermit(ctx)
	if err != nil {
		return err
	}
	defer release()
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
	err = g.cfg.Registry.JoinRoom(ctx, l, room, g.cfg.LeaseTTL)
	g.mu.Lock()
	current := g.sessions[l.Key()] == s
	if err != nil || !current {
		delete(s.rooms, room)
		delete(g.rooms[rk], l.Key())
		if len(g.rooms[rk]) == 0 {
			delete(g.rooms, rk)
		}
	}
	g.mu.Unlock()
	if err != nil {
		return err
	}
	if !current {
		return ErrStale
	}
	return nil
}
func (g *Gateway) LeaveRoom(ctx context.Context, l Location, room string) error {
	ctx, finish, err := g.operationContext(ctx)
	if err != nil {
		return err
	}
	defer finish()
	unlock, err := g.sessionLocks.acquire(ctx, l.Key())
	if err != nil {
		return err
	}
	defer unlock()
	release, err := g.registryPermit(ctx)
	if err != nil {
		return err
	}
	defer release()
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

type deliveryTarget struct {
	location   Location
	generation string
	session    *localSession
}

func (g *Gateway) Handle(parent context.Context, c DeliveryCommand) DeliveryResult {
	if !g.operations.begin() {
		return DeliveryResult{Outcomes: []Outcome{{Location: Location{NodeID: g.NodeID()}, Status: Unavailable}}}
	}
	defer g.operations.end()
	if !g.Ready() || c.Version != 1 || c.TargetNode != g.cfg.NodeID || c.MessageID == "" || c.TenantID == "" || len(c.Payload) > g.cfg.MaxPayloadBytes || !time.Now().Before(c.ExpiresAt) || parent.Err() != nil {
		return DeliveryResult{Outcomes: []Outcome{{Location: Location{NodeID: g.cfg.NodeID}, Status: Unavailable}}}
	}
	ctx, cancel := context.WithDeadline(parent, c.ExpiresAt)
	defer cancel()
	g.mu.Lock()
	var targets []deliveryTarget
	if c.RoomID != "" {
		members := g.rooms[roomKey(c.TenantID, c.RoomID)]
		targets = make([]deliveryTarget, 0, len(members))
		for _, s := range members {
			targets = append(targets, deliveryTarget{s.location, s.location.Generation, s})
		}
	} else {
		targets = make([]deliveryTarget, 0, len(c.Sessions))
		for _, l := range c.Sessions {
			target := deliveryTarget{location: l, session: g.sessions[l.Key()]}
			if target.session != nil {
				target.generation = target.session.location.Generation
			}
			targets = append(targets, target)
		}
	}
	g.mu.Unlock()
	result := DeliveryResult{}
	if c.RoomID != "" {
		result.Counts = make(map[Status]int)
	} else {
		result.Outcomes = make([]Outcome, 0, len(targets))
	}
	var payload []byte
	prepared := false
	for _, target := range targets {
		status := g.deliver(ctx, c, target, &payload, &prepared)
		if status != Enqueued {
			g.rejected.Add(1)
		}
		if c.RoomID != "" {
			result.Counts[status]++
		} else {
			result.Outcomes = append(result.Outcomes, Outcome{Location: target.location, Status: status})
		}
	}
	if c.RoomID != "" {
		for status := range result.Counts {
			result.Outcomes = append(result.Outcomes, Outcome{Location: Location{TenantID: c.TenantID, NodeID: g.cfg.NodeID}, Status: status})
		}
	}
	result.Offline = len(result.Outcomes) == 0
	return result
}

func (g *Gateway) deliver(ctx context.Context, c DeliveryCommand, target deliveryTarget, payload *[]byte, prepared *bool) Status {
	l, s := target.location, target.session
	if l.TenantID != c.TenantID || l.NodeID != g.cfg.NodeID {
		return Stale
	}
	if s == nil {
		return NotFound
	}
	if target.generation != l.Generation {
		return Stale
	}
	if err := s.delivery.lockUntil(ctx, g.ctx.Done()); err != nil {
		return Unavailable
	}
	defer s.delivery.unlock()
	if !s.active.Load() {
		return NotFound
	}
	now := time.Now().UnixNano()
	if until := s.leaseUntil.Load(); until != 0 && now >= until {
		return Unavailable
	}
	if ctx.Err() != nil || g.closed.Load() || now >= g.leaseUntil.Load() {
		return Unavailable
	}
	key := dedupeKey{message: c.MessageID, session: s.number}
	if g.dedupe.contains(key) {
		return Enqueued
	}
	if !*prepared {
		*payload = bytes.Clone(c.Payload)
		*prepared = true
	}
	switch err := s.enqueue(*payload); {
	case err == nil:
		g.dedupe.remember(key)
		g.delivered.Add(1)
		return Enqueued
	case errors.Is(err, ErrQueueFull):
		return QueueFull
	case errors.Is(err, ErrClosed):
		return NotFound
	default:
		return Unavailable
	}
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

// Renewal takes only idle session gates, in bounded batches. A slow operation on
// one session must not stall renewal for unrelated sessions.
type sessionSnapshot struct {
	session  *localSession
	location Location
	rooms    []RoomMembership
	unlock   func()
}

func (g *Gateway) snapshotSessions(keys []string) []sessionSnapshot {
	var snapshots []sessionSnapshot
	for _, key := range keys {
		unlock, ok := g.sessionLocks.tryAcquire(key)
		if !ok {
			continue
		}
		g.mu.Lock()
		s := g.sessions[key]
		if s == nil {
			g.mu.Unlock()
			unlock()
			continue
		}
		item := sessionSnapshot{session: s, location: s.location, unlock: unlock}
		for room := range s.rooms {
			item.rooms = append(item.rooms, RoomMembership{Location: s.location, RoomID: room})
		}
		snapshots = append(snapshots, item)
		g.mu.Unlock()
	}
	return snapshots
}
func (g *Gateway) renew() {
	renewedAt := time.Now()
	ctx, cancel := context.WithTimeout(g.ctx, g.cfg.RenewInterval)
	defer cancel()
	release, err := g.registryPermit(ctx)
	if err != nil {
		g.leaseFailures.Add(1)
		return
	}
	defer release()
	g.mu.Lock()
	keys := make([]string, 0, len(g.sessions))
	for key := range g.sessions {
		keys = append(keys, key)
	}
	g.mu.Unlock()
	var failure error
	renewed := false
	for start := 0; start < len(keys); start += 256 {
		snapshots := g.snapshotSessions(keys[start:min(start+256, len(keys))])
		if len(snapshots) > 0 {
			renewed = true
			failure = errors.Join(failure, g.renewSessions(ctx, snapshots, renewedAt))
		}
	}
	if !renewed {
		failure = g.cfg.Registry.Renew(ctx, nil, g.cfg.LeaseTTL)
	}
	if failure != nil {
		g.leaseFailures.Add(1)
		return
	}
	g.leaseUntil.Store(renewedAt.Add(g.cfg.LeaseTTL).UnixNano())
}
func (g *Gateway) renewSessions(ctx context.Context, snapshots []sessionSnapshot, renewedAt time.Time) error {
	var closed []func()
	defer func() {
		for _, s := range snapshots {
			s.unlock()
		}
		for _, closeSession := range closed {
			closeSession()
		}
	}()
	ls := make([]Location, 0, len(snapshots))
	for _, s := range snapshots {
		ls = append(ls, s.location)
	}
	results := make([]RenewResult, len(ls))
	var failure error
	renewalConfirmed := true
	if batch, ok := g.cfg.Registry.(SessionLeaseRenewer); ok && len(ls) > 0 {
		results = batch.RenewBatch(ctx, ls, g.cfg.LeaseTTL)
		if len(results) != len(ls) {
			return ErrInvalid
		}
	} else {
		err := g.cfg.Registry.Renew(ctx, ls, g.cfg.LeaseTTL)
		renewalConfirmed = err == nil
		// A legacy implementation may stop at the first stale session. Resolve
		// fences owners but cannot prove the remaining leases were extended.
		failure = err
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
		// Resolve proves ownership, not lease extension after a partial legacy
		// renewal failure. Do not invent a new local expiry or refresh rooms.
		if !renewalConfirmed {
			continue
		}
		g.mu.Lock()
		if g.sessions[snapshot.location.Key()] == snapshot.session {
			snapshot.session.leaseUntil.Store(renewedAt.Add(g.cfg.LeaseTTL).UnixNano())
		}
		g.mu.Unlock()
		rooms = append(rooms, snapshot.rooms...)
	}
	if batch, ok := g.cfg.Registry.(RoomLeaseRenewer); ok {
		failure = errors.Join(failure, batch.RenewRooms(ctx, rooms, g.cfg.LeaseTTL))
	} else {
		for _, m := range rooms {
			failure = errors.Join(failure, g.cfg.Registry.JoinRoom(ctx, m.Location, m.RoomID, g.cfg.LeaseTTL))
		}
	}
	return failure
}
func (g *Gateway) Close(ctx context.Context) error {
	g.closed.Store(true)
	operationsDone := g.operations.stop()
	g.cancel()
	select {
	case <-operationsDone:
	case <-ctx.Done():
		return ctx.Err()
	}
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
	sessions, rooms := len(g.sessions), len(g.rooms)
	g.mu.Unlock()
	entries := g.dedupe.size()
	return GatewayStats{Ready: g.Ready(), Sessions: sessions, Rooms: rooms, DedupeEntries: entries, LeaseFailures: g.leaseFailures.Load(), Enqueued: g.delivered.Load(), Rejected: g.rejected.Load(), UnregisterFailures: g.unregisterFailures.Load(), UnregisterRetries: g.unregisterRetries.Load()}
}
