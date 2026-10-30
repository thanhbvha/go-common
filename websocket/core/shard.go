// package core contains the core WebSocket sharding, connection lifecycle, and manager logic.
package core

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/logger"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/websocket/internal/wsrecovery"
	"github.com/thanhbvha/go-common/websocket/pool"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

const (
	// Technical shard constraints
	broadcastChannelSize = 4096
	incomingMessageSize  = 4096
	maxConnections       = 100000
	cleanupInterval      = 30 * time.Second
)

// Shard maintains the set of active connections, manages room subscriptions, and broadcasts messages.
//
// By sharding connections across multiple Shards, we eliminate CPU lock contention
// and leverage multi-core processors. Note that a Shard is NOT a business chat room;
// a single Shard can host thousands of logical chat rooms (chatRooms).
// ShardCoordinator defines the interface required by a Shard to coordinate with the cluster.
type ShardCoordinator interface {
	GetShardID(userID string) string
	GetPubSubManager() pubsub.Manager
	GetEventExecutor() *pool.PartitionedExecutor
	// DecrTotalConnections decrements the manager-level connection counter.
	// Exposed via interface so Shard doesn't need a concrete *Manager reference.
	DecrTotalConnections()
}

type Shard struct {
	// activeConns tracks all physical WebSocket connections currently connected directly to this shard.
	activeConns map[*Connection]struct{}

	// userSessions maps an authenticated User ID to their active physical connections.
	// This structure naturally supports multi-device/multi-tab logins (1-to-many relationship).
	//   key:   userID (e.g. "user_123")
	//   value: map of active Connection pointers belonging to this user
	userSessions map[string]map[*Connection]struct{}

	// chatRooms tracks logical pub/sub message rooms hosted within this shard.
	//   key 1: roomID (e.g. "room_game_99")
	//   key 2: userID (to fast-lookup users in this room)
	//   value: connection pointer mapping
	chatRooms map[string]map[string]map[*Connection]struct{}

	incomingMessage   chan *EventMessage
	mu                sync.RWMutex
	connectionCount   int64
	ctx               context.Context
	cancel            context.CancelFunc
	name              string // Shard unique identifier (e.g. "shard_0", "shard_1")
	coordinator       ShardCoordinator
	config            ManagerConfig
	handlerSupervisor *HandlerSupervisor
	stopOnce          sync.Once
	shutdownOnce      sync.Once
	done              chan struct{}
}

var (
	// ErrShardStopped indicates that an operation targeted a stopped shard.
	ErrShardStopped = errors.New("websocket shard stopped")
	// ErrConnectionInactive indicates that a room operation targeted a
	// connection which is not active on that shard.
	ErrConnectionInactive = errors.New("websocket connection is not active on shard")
)

// NewShard creates a new Shard instance with the specified name and registers cross-node PubSub handlers.
func NewShard(name string, coordinator ShardCoordinator) *Shard {
	ctx, cancel := context.WithCancel(context.Background())
	config := coordinatorConfig(coordinator)
	supervisor := NewHandlerSupervisor(config.MaxHandlerGoroutines)
	if provider, ok := coordinator.(interface{ GetHandlerSupervisor() *HandlerSupervisor }); ok {
		supervisor = provider.GetHandlerSupervisor()
	}

	shard := &Shard{
		activeConns:       make(map[*Connection]struct{}),
		userSessions:      make(map[string]map[*Connection]struct{}),
		chatRooms:         make(map[string]map[string]map[*Connection]struct{}),
		incomingMessage:   make(chan *EventMessage, config.InboundQueueSize),
		ctx:               ctx,
		cancel:            cancel,
		name:              name,
		coordinator:       coordinator,
		config:            config,
		handlerSupervisor: supervisor,
		done:              make(chan struct{}),
	}

	return shard
}

func coordinatorConfig(coordinator ShardCoordinator) ManagerConfig {
	if configured, ok := coordinator.(interface{ GetConfig() ManagerConfig }); ok {
		config := configured.GetConfig()
		if config.EventExecutor == nil {
			config.EventExecutor = coordinator.GetEventExecutor()
		}
		config, _ = normalizeManagerConfig(config)
		return config
	}
	config, _ := normalizeManagerConfig(ManagerConfig{EventExecutor: coordinator.GetEventExecutor()})
	return config
}

func (s *Shard) eventRegistry() *EventRegistry {
	if configured, ok := s.coordinator.(interface{ GetEventRegistry() *EventRegistry }); ok {
		return configured.GetEventRegistry()
	}
	return defaultEventRegistry
}

func (s *Shard) authorizer() Authorizer {
	if configured, ok := s.coordinator.(interface{ GetAuthorizer() Authorizer }); ok {
		return configured.GetAuthorizer()
	}
	return currentAuthorizer()
}

func (s *Shard) observer() Observer { return s.config.Observer }

// GetConnectionCount returns the current number of active connections in the shard.
func (s *Shard) GetConnectionCount() int64 {
	return atomic.LoadInt64(&s.connectionCount)
}

// CanAcceptConnection checks if the shard has headroom to accept another connection.
func (s *Shard) CanAcceptConnection() bool {
	return atomic.LoadInt64(&s.connectionCount) < s.config.MaxShardConnections
}

// SendToUser transmits a message directly to a user's active connection(s) on this shard.
func (s *Shard) SendToUser(userID string, message []byte) {
	message = bytes.Clone(message)
	s.mu.RLock()
	userConns, exists := s.userSessions[userID]
	if !exists {
		s.mu.RUnlock()
		return
	}
	connections := make([]*Connection, 0, len(userConns))
	for conn := range userConns {
		connections = append(connections, conn)
	}
	s.mu.RUnlock()

	var deadConnections []*Connection
	for _, conn := range connections {
		if conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
			continue
		}

		if !conn.sendShared(message) && conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
		}
	}

	for _, conn := range deadConnections {
		s.Unregister(conn)
	}
}

// SendToRoom transmits a message to all users subscribed to a specific room.
func (s *Shard) SendToRoom(roomID string, message []byte) {
	message = bytes.Clone(message)
	s.mu.RLock()
	roomUsers, exists := s.chatRooms[roomID]
	if !exists {
		s.mu.RUnlock()
		return
	}
	connections := make([]*Connection, 0)
	for _, userConns := range roomUsers {
		for conn := range userConns {
			connections = append(connections, conn)
		}
	}
	s.mu.RUnlock()

	var deadConnections []*Connection
	for _, conn := range connections {
		if conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
			continue
		}
		if !conn.sendShared(message) && conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
		}
	}

	for _, conn := range deadConnections {
		s.Unregister(conn)
	}
}

// JoinRoom idempotently adds an active connection to a logical room.
func (s *Shard) JoinRoom(ctx context.Context, roomID string, conn *Connection) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if roomID == "" || conn == nil {
		return ErrConnectionInactive
	}
	select {
	case <-s.ctx.Done():
		return ErrShardStopped
	default:
	}
	if err := s.authorizer().CanSubscribe(ctx, conn.identity, roomID); err != nil {
		return ErrUnauthorized
	}
	conn.roomsMu.Lock()
	defer conn.roomsMu.Unlock()
	if _, exists := conn.rooms[roomID]; !exists && len(conn.rooms) >= conn.config.MaxRoomsPerConnection {
		return ErrRoomLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, active := s.activeConns[conn]; !active || conn.IsClosed() {
		delete(conn.rooms, roomID)
		return ErrConnectionInactive
	}
	if _, joined := conn.rooms[roomID]; joined {
		return nil
	}
	roomUsers, exists := s.chatRooms[roomID]
	if !exists {
		roomUsers = make(map[string]map[*Connection]struct{})
		s.chatRooms[roomID] = roomUsers
	}
	userConns, exists := roomUsers[conn.userID]
	if !exists {
		userConns = make(map[*Connection]struct{})
		roomUsers[conn.userID] = userConns
	}
	userConns[conn] = struct{}{}
	conn.rooms[roomID] = struct{}{}
	if gateway := s.config.Distributed; gateway != nil {
		if err := gateway.JoinRoom(ctx, conn.routingLocation(), roomID); err != nil {
			delete(userConns, conn)
			if len(userConns) == 0 {
				delete(roomUsers, conn.userID)
			}
			if len(roomUsers) == 0 {
				delete(s.chatRooms, roomID)
			}
			delete(conn.rooms, roomID)
			return err
		}
	}
	return nil
}

// LeaveRoom idempotently removes a connection from a logical room.
func (s *Shard) LeaveRoom(ctx context.Context, roomID string, conn *Connection) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if roomID == "" || conn == nil {
		return nil
	}
	conn.roomsMu.Lock()
	defer conn.roomsMu.Unlock()
	delete(conn.rooms, roomID)
	s.mu.Lock()
	defer s.mu.Unlock()
	roomUsers, exists := s.chatRooms[roomID]
	if !exists {
		return nil
	}
	if userConns, exists := roomUsers[conn.userID]; exists {
		delete(userConns, conn)
		if len(userConns) == 0 {
			delete(roomUsers, conn.userID)
		}
	}
	if len(roomUsers) == 0 {
		delete(s.chatRooms, roomID)
	}
	if gateway := s.config.Distributed; gateway != nil {
		return gateway.LeaveRoom(ctx, conn.routingLocation(), roomID)
	}
	return nil
}

// GetRoomSize returns the number of active physical connections in a room.
func (s *Shard) GetRoomSize(roomID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	size := 0
	for _, userConns := range s.chatRooms[roomID] {
		size += len(userConns)
	}
	return size
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Broadcast sends a raw byte slice to all connections managed by this shard.
func (s *Shard) Broadcast(message []byte) {
	s.handleBroadcast(bytes.Clone(message))
}

// BroadcastGlobal broadcasts a message to all local shard clients and routes it to peer cluster instances.
func (s *Shard) BroadcastGlobal(message []byte) {
	s.Broadcast(message)
	s.broadcastToOtherNodes(message)
}

// Register adds a connection synchronously. Pumps must not start until this
// returns, which prevents register/unregister reordering.
func (s *Shard) Register(conn *Connection) bool {
	if conn == nil {
		return false
	}
	select {
	case <-s.ctx.Done():
		conn.Close("Shard stopped")
		return false
	default:
	}
	return s.handleRegister(conn)
}

// Unregister synchronously disconnects and removes a connection.
func (s *Shard) Unregister(conn *Connection) {
	s.UnregisterContext(context.Background(), conn)
}

// UnregisterContext synchronously removes a connection unless the caller has
// already canceled the operation.
func (s *Shard) UnregisterContext(ctx context.Context, conn *Connection) bool {
	if conn == nil {
		return false
	}
	if err := contextError(ctx); err != nil {
		return false
	}
	reason := "Graceful unregistration"
	select {
	case <-s.ctx.Done():
		reason = "Shard stopped"
	default:
	}
	return s.removeConnection(conn, reason)
}

// forceUnregister synchronously pulls a connection out of the shard maps under heavy load.
func (s *Shard) forceUnregister(conn *Connection) {
	s.removeConnection(conn, "Force unregister")
}

func (s *Shard) removeConnection(conn *Connection, reason string) bool {
	conn.roomsMu.Lock()
	s.mu.Lock()
	if _, ok := s.activeConns[conn]; !ok {
		s.mu.Unlock()
		conn.roomsMu.Unlock()
		return false
	}
	delete(s.activeConns, conn)
	if userConns, exists := s.userSessions[conn.userID]; exists {
		delete(userConns, conn)
		if len(userConns) == 0 {
			delete(s.userSessions, conn.userID)
		}
	}
	for roomID := range conn.rooms {
		roomUsers, exists := s.chatRooms[roomID]
		if !exists {
			continue
		}
		if userConns, ok := roomUsers[conn.userID]; ok {
			delete(userConns, conn)
			if len(userConns) == 0 {
				delete(roomUsers, conn.userID)
			}
		}
		if len(roomUsers) == 0 {
			delete(s.chatRooms, roomID)
		}
	}
	conn.rooms = make(map[string]struct{})
	atomic.AddInt64(&s.connectionCount, -1)
	s.mu.Unlock()
	conn.roomsMu.Unlock()

	conn.Close(reason)
	if gateway := s.config.Distributed; gateway != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = gateway.Remove(ctx, conn.routingLocation())
		cancel()
	}
	s.releaseManagerConnection(conn)
	s.observer().ConnectionClosed()
	logger.DebugAsync("Connection unregistered", "reason", reason, "shard", s.name, "userID", conn.userID, "count", s.GetConnectionCount())
	return true
}

func (s *Shard) releaseManagerConnection(conn *Connection) {
	if releaser, ok := s.coordinator.(interface{ ReleaseConnectionQuota([]quotaKey) }); ok {
		releaser.ReleaseConnectionQuota(conn.quotaKeys)
		return
	}
	s.coordinator.DecrTotalConnections()
}

// Run processes the bounded inbound event queue until shutdown.
func (s *Shard) Run() {
	go s.cleanupRoutine()
	for {
		select {
		case <-s.ctx.Done():
			s.shutdown(context.Background())
			return
		case ev := <-s.incomingMessage:
			s.handleIncomingMessageEvent(ev)
		}
	}
}

// handleRegister registers a validated connection in the shard structure maps.
func (s *Shard) handleRegister(conn *Connection) bool {
	if conn == nil {
		logger.ErrorAsync("Cannot register nil connection", "shard", s.name)
		return false
	}

	s.mu.Lock()
	select {
	case <-s.ctx.Done():
		s.mu.Unlock()
		conn.Close("Shard stopped")
		return false
	default:
	}
	if atomic.LoadInt64(&s.connectionCount) >= s.config.MaxShardConnections {
		count := s.GetConnectionCount()
		s.mu.Unlock()
		logger.ErrorAsync("Shard at max capacity, rejecting connection", "shard", s.name, "count", count)
		conn.Close("Shard at max capacity")
		return false
	}
	s.activeConns[conn] = struct{}{}

	if _, exists := s.userSessions[conn.userID]; !exists {
		s.userSessions[conn.userID] = make(map[*Connection]struct{})
	}
	s.userSessions[conn.userID][conn] = struct{}{}

	atomic.AddInt64(&s.connectionCount, 1)
	s.mu.Unlock()

	return true
}

// handleBroadcast writes a broadcast message to all active connection send channels.
func (s *Shard) handleBroadcast(message []byte) {
	s.mu.RLock()
	connections := make([]*Connection, 0, len(s.activeConns))
	for conn := range s.activeConns {
		connections = append(connections, conn)
	}
	s.mu.RUnlock()

	var deadConnections []*Connection
	for _, conn := range connections {
		if conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
			continue
		}

		if !conn.sendShared(message) && conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
		}
	}

	for _, conn := range deadConnections {
		s.Unregister(conn)
	}
}

// handleIncomingMessageEvent dispatches raw client payloads to handlers.
func (s *Shard) handleIncomingMessageEvent(ev *EventMessage) {
	if ev == nil || ev.Conn == nil {
		return
	}
	HandleEvent(ev)
}

// cleanupRoutine periodically triggers cleanup of stale or dead connections.
func (s *Shard) cleanupRoutine() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer panicrecovery.Catch(wsrecovery.Handler(wsrecovery.Metadata{
					Component: "core.shard_cleanup",
					Operation: "cleanup",
					ShardID:   s.name,
				}))
				s.cleanup()
			}()
		}
	}
}

// cleanup sweeps and terminates connections which have missed heartbeats.
func (s *Shard) cleanup() {
	s.mu.RLock()
	connections := make([]*Connection, 0, len(s.activeConns))
	for conn := range s.activeConns {
		connections = append(connections, conn)
	}
	s.mu.RUnlock()

	var deadConnections []*Connection
	for _, conn := range connections {
		if conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
		} else {
			conn.mu.RLock()
			stale := time.Since(conn.lastPing) > 2*s.config.PongWait
			conn.mu.RUnlock()

			if stale {
				deadConnections = append(deadConnections, conn)
			}
		}
	}
	for _, conn := range deadConnections {
		s.Unregister(conn)
	}

	if len(deadConnections) > 0 {
		logger.InfoAsync("Cleaned up stale connections", "shard", s.name, "count", len(deadConnections), "remaining", s.GetConnectionCount())
	}
}

// shutdown disconnects all active sessions when the shard terminates.
func (s *Shard) shutdown(ctx context.Context) {
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		connections := make([]*Connection, 0, len(s.activeConns))
		for conn := range s.activeConns {
			connections = append(connections, conn)
		}
		s.activeConns = make(map[*Connection]struct{})
		s.userSessions = make(map[string]map[*Connection]struct{})
		s.chatRooms = make(map[string]map[string]map[*Connection]struct{})
		atomic.StoreInt64(&s.connectionCount, 0)
		s.mu.Unlock()

		logger.InfoAsync("Shutting down Shard", "shard", s.name, "connections", len(connections))
		for _, conn := range connections {
			conn.Close("Shard shutdown")
			s.releaseManagerConnection(conn)
			s.observer().ConnectionClosed()
		}
		for _, conn := range connections {
			if conn.hasStarted() {
				_ = conn.Wait(ctx)
			}
		}
		close(s.done)
	})
}

// broadcastToOtherNodes routes a broadcast payload to cluster nodes using Redis pubsub.
// It matches signature parameters from the pubsub coordinator.
func (s *Shard) broadcastToOtherNodes(message []byte) {
	pubsubManager := s.coordinator.GetPubSubManager()
	data := map[string]interface{}{
		"message": string(message),
	}
	if err := pubsubManager.BroadcastMessage(s.name, data); err != nil {
		logger.ErrorAsync("Failed to route message to peer cluster nodes", "error", err, "shard", s.name)
	}
}

// BroadcastUserNotification sends a cross-node message to a specific user.
func (s *Shard) BroadcastUserNotification(userID string, message []byte) {
	pubsubManager := s.coordinator.GetPubSubManager()
	data := map[string]interface{}{
		"message": string(message),
	}

	shardID := s.name
	userShardID := s.coordinator.GetShardID(userID)
	if shardID != userShardID {
		shardID = userShardID
	}

	if err := pubsubManager.BroadcastUserNotification(shardID, userID, data); err != nil {
		logger.ErrorAsync("Failed to broadcast user notification", "error", err, "shard", shardID, "userID", userID)
	}
}

// BroadcastRoomMessage distributes a message to a specific room across different cluster nodes.
func (s *Shard) BroadcastRoomMessage(roomID string, message []byte) {
	pubsubManager := s.coordinator.GetPubSubManager()
	data := map[string]interface{}{
		"message": string(message),
	}
	if err := pubsubManager.BroadcastRoomMessage(s.name, roomID, data); err != nil {
		logger.ErrorAsync("Failed to broadcast room message", "error", err, "shard", s.name, "roomID", roomID)
	}
}

// Shutdown gracefully stops the shard event loop and notifies peer nodes of shard termination.
func (s *Shard) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() {
		s.cancel()
		go s.shutdown(context.Background())
		// Coordination is best effort and must not delay socket cleanup or the caller's deadline.
		go func() { _ = s.coordinator.GetPubSubManager().NotifyShardDestroy(s.name) }()
	})
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
