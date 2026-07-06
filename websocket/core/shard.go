// package core contains the core WebSocket sharding, connection lifecycle, and manager logic.
package core

import (
	"context"
	"github.com/goccy/go-json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

const (
	// Technical shard constraints
	broadcastChannelSize = 4096
	incomingMessageSize  = 4096
	maxConnections       = 100000
	registerChannelSize  = 1000
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
}

type Shard struct {
	// activeConns tracks all physical WebSocket connections currently connected directly to this shard.
	activeConns     map[*Connection]bool

	// userSessions maps an authenticated User ID to their active physical connections.
	// This structure naturally supports multi-device/multi-tab logins (1-to-many relationship).
	//   key:   userID (e.g. "user_123")
	//   value: map of active Connection pointers belonging to this user
	userSessions    map[string]map[*Connection]bool

	// chatRooms tracks logical pub/sub message rooms hosted within this shard.
	//   key 1: roomID (e.g. "room_game_99")
	//   key 2: userID (to fast-lookup users in this room)
	//   value: connection pointer mapping
	chatRooms       map[string]map[string]map[*Connection]bool

	broadcast       chan []byte
	incomingMessage chan *EventMessage
	register        chan *Connection
	unregister      chan *Connection
	mu              sync.RWMutex
	connectionCount int64
	ctx             context.Context
	cancel          context.CancelFunc
	name            string // Shard unique identifier (e.g. "shard_0", "shard_1")
	coordinator     ShardCoordinator
}

// NewShard creates a new Shard instance with the specified name and registers cross-node PubSub handlers.
func NewShard(name string, coordinator ShardCoordinator) *Shard {
	ctx, cancel := context.WithCancel(context.Background())

	shard := &Shard{
		activeConns:     make(map[*Connection]bool),
		userSessions:    make(map[string]map[*Connection]bool),
		chatRooms:       make(map[string]map[string]map[*Connection]bool),
		broadcast:       make(chan []byte, broadcastChannelSize),
		incomingMessage: make(chan *EventMessage, incomingMessageSize),
		register:        make(chan *Connection, registerChannelSize),
		unregister:      make(chan *Connection, registerChannelSize),
		ctx:             ctx,
		cancel:          cancel,
		name:            name,
		coordinator:     coordinator,
	}

	shard.setupPubSubHandlers()

	return shard
}

// GetConnectionCount returns the current number of active connections in the shard.
func (s *Shard) GetConnectionCount() int64 {
	return atomic.LoadInt64(&s.connectionCount)
}

// CanAcceptConnection checks if the shard has headroom to accept another connection.
func (s *Shard) CanAcceptConnection() bool {
	return atomic.LoadInt64(&s.connectionCount) < maxConnections
}

// SendToUser transmits a message directly to a user's active connection(s) on this shard.
func (s *Shard) SendToUser(userID string, message []byte) {
	s.mu.RLock()
	userConns, exists := s.userSessions[userID]
	s.mu.RUnlock()

	if !exists {
		return
	}

	var deadConnections []*Connection

	for conn := range userConns {
		if conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
			continue
		}

		if !conn.Send(message) {
			deadConnections = append(deadConnections, conn)
		}
	}

	if len(deadConnections) > 0 {
		go func() {
			for _, conn := range deadConnections {
				s.Unregister(conn)
			}
		}()
	}
}

// SendToRoom transmits a message to all users subscribed to a specific room.
func (s *Shard) SendToRoom(roomID string, message []byte) {
	s.mu.RLock()
	roomUsers, exists := s.chatRooms[roomID]
	s.mu.RUnlock()

	if !exists {
		return
	}

	var deadConnections []*Connection

	for _, userConns := range roomUsers {
		for conn := range userConns {
			if conn.IsClosed() {
				deadConnections = append(deadConnections, conn)
				continue
			}

			if !conn.Send(message) {
				deadConnections = append(deadConnections, conn)
			}
		}
	}

	if len(deadConnections) > 0 {
		go func() {
			for _, conn := range deadConnections {
				s.Unregister(conn)
			}
		}()
	}
}

// Broadcast schedules a raw byte slice to be broadcast to all connections managed by this shard.
func (s *Shard) Broadcast(message []byte) {
	select {
	case s.broadcast <- message:
	default:
		logger.WarnAsync("Broadcast channel full, dropping message", "shard", s.name)
	}
}

// BroadcastGlobal broadcasts a message to all local shard clients and routes it to peer cluster instances.
func (s *Shard) BroadcastGlobal(message []byte) {
	s.Broadcast(message)
	s.broadcastToOtherNodes(message)
}

// Register enqueues a new Connection to be added to this shard.
func (s *Shard) Register(conn *Connection) {
	select {
	case s.register <- conn:
	default:
		logger.ErrorAsync("Register channel full, rejecting connection", "shard", s.name)
		conn.Close("Register channel full")
	}
}

// Unregister enqueues a Connection to be gracefully disconnected and removed.
func (s *Shard) Unregister(conn *Connection) {
	select {
	case s.unregister <- conn:
	default:
		s.forceUnregister(conn)
	}
}

// forceUnregister synchronously pulls a connection out of the shard maps under heavy load.
func (s *Shard) forceUnregister(conn *Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.activeConns[conn]; ok {
		delete(s.activeConns, conn)
		atomic.AddInt64(&s.connectionCount, -1)
		atomic.AddInt64(&GetGlobalManager().totalConnections, -1)
		conn.Close("Force unregister")
		logger.DebugAsync("Connection forcefully unregistered", "shard", s.name, "userID", conn.userID, "count", s.GetConnectionCount())
	}
}

// Run boots the main message select loop for the Shard, processing connections and broadcasts.
func (s *Shard) Run() {
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorAsync("Panic in Shard.Run select loop", "error", r, "shard", s.name)
		}
	}()

	go s.cleanupRoutine()

	for {
		select {
		case <-s.ctx.Done():
			s.shutdown()
			return
		case conn := <-s.register:
			s.handleRegister(conn)
		case conn := <-s.unregister:
			s.handleUnregister(conn)
		case message := <-s.broadcast:
			s.handleBroadcast(message)
		case ev := <-s.incomingMessage:
			s.handleIncomingMessageEvent(ev)
		}
	}
}

// handleRegister registers a validated connection in the shard structure maps.
func (s *Shard) handleRegister(conn *Connection) {
	if !s.CanAcceptConnection() {
		logger.ErrorAsync("Shard at max capacity, rejecting connection", "shard", s.name, "count", s.GetConnectionCount())
		conn.Close("Shard at max capacity")
		return
	}

	s.mu.Lock()
	s.activeConns[conn] = true

	if _, exists := s.userSessions[conn.userID]; !exists {
		s.userSessions[conn.userID] = make(map[*Connection]bool)
	}
	s.userSessions[conn.userID][conn] = true

	atomic.AddInt64(&s.connectionCount, 1)
	s.mu.Unlock()

	logger.InfoAsync("Connection registered to Shard", "shard", s.name, "userID", conn.userID, "count", s.GetConnectionCount())
}

// handleUnregister gracefully removes a connection from registration maps.
func (s *Shard) handleUnregister(conn *Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.activeConns[conn]; ok {
		delete(s.activeConns, conn)

		if userConns, exists := s.userSessions[conn.userID]; exists {
			delete(userConns, conn)
			if len(userConns) == 0 {
				delete(s.userSessions, conn.userID)
			}
		}

		atomic.AddInt64(&s.connectionCount, -1)
		atomic.AddInt64(&GetGlobalManager().totalConnections, -1)

		conn.Close("Graceful unregistration")
		logger.InfoAsync("Connection unregistered from Shard", "shard", s.name, "userID", conn.userID, "count", s.GetConnectionCount())
	}
}

// handleBroadcast writes a broadcast message to all active connection send channels.
func (s *Shard) handleBroadcast(message []byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var deadConnections []*Connection

	for conn := range s.activeConns {
		if conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
			continue
		}

		if !conn.Send(message) {
			deadConnections = append(deadConnections, conn)
		}
	}

	if len(deadConnections) > 0 {
		go func() {
			for _, conn := range deadConnections {
				s.Unregister(conn)
			}
		}()
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
			s.cleanup()
		}
	}
}

// cleanup sweeps and terminates connections which have missed heartbeats.
func (s *Shard) cleanup() {
	s.mu.RLock()
	var deadConnections []*Connection

	for conn := range s.activeConns {
		if conn.IsClosed() {
			deadConnections = append(deadConnections, conn)
		} else {
			conn.mu.RLock()
			stale := time.Since(conn.lastPing) > 2*pongWait
			conn.mu.RUnlock()

			if stale {
				deadConnections = append(deadConnections, conn)
			}
		}
	}
	s.mu.RUnlock()

	for _, conn := range deadConnections {
		s.Unregister(conn)
	}

	if len(deadConnections) > 0 {
		logger.InfoAsync("Cleaned up stale connections", "shard", s.name, "count", len(deadConnections), "remaining", s.GetConnectionCount())
	}
}

// shutdown disconnects all active sessions when the shard terminates.
func (s *Shard) shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger.InfoAsync("Shutting down Shard", "shard", s.name, "connections", len(s.activeConns))

	for conn := range s.activeConns {
		conn.Close("Shard shutdown")
	}

	s.activeConns = make(map[*Connection]bool)
	s.userSessions = make(map[string]map[*Connection]bool)
	atomic.StoreInt64(&s.connectionCount, 0)
}

// setupPubSubHandlers configures event callbacks for clustered Redis synchronization.
func (s *Shard) setupPubSubHandlers() {
	pubsubManager := s.coordinator.GetPubSubManager()

	// Direct user notification routing
	pubsubManager.RegisterHandler(pubsub.MessageTypeNotification, func(msg *pubsub.CrossNodeMessage) {
		if msg.ShardID != s.name || msg.UserID == "" {
			return
		}

		if data, ok := msg.Data["message"]; ok {
			if messageBytes, ok := data.(string); ok {
				s.SendToUser(msg.UserID, []byte(messageBytes))
			} else if messageData, err := json.Marshal(data); err == nil {
				s.SendToUser(msg.UserID, messageData)
			}
		}
	})

	// Chat room routing
	pubsubManager.RegisterHandler(pubsub.MessageTypeChatRoom, func(msg *pubsub.CrossNodeMessage) {
		if msg.ShardID != s.name || msg.RoomID == "" {
			return
		}

		if data, ok := msg.Data["message"]; ok {
			if messageBytes, ok := data.(string); ok {
				s.SendToRoom(msg.RoomID, []byte(messageBytes))
			} else if messageData, err := json.Marshal(data); err == nil {
				s.SendToRoom(msg.RoomID, messageData)
			}
		}
	})

	// Shard broadcast routing
	pubsubManager.RegisterHandler(pubsub.MessageTypeBroadcast, func(msg *pubsub.CrossNodeMessage) {
		if msg.ShardID != s.name {
			return
		}

		if data, ok := msg.Data["message"]; ok {
			if messageBytes, ok := data.(string); ok {
				s.Broadcast([]byte(messageBytes))
			} else if messageData, err := json.Marshal(data); err == nil {
				s.Broadcast(messageData)
			}
		}
	})

	// User connection details (for other nodes)
	pubsubManager.RegisterHandler(pubsub.MessageTypeUserJoin, func(msg *pubsub.CrossNodeMessage) {
		if msg.ShardID != s.name {
			return
		}
		logger.InfoAsync("User joined on peer cluster node", "shard", s.name, "userID", msg.UserID, "nodeID", msg.NodeID)
	})

	pubsubManager.RegisterHandler(pubsub.MessageTypeUserLeave, func(msg *pubsub.CrossNodeMessage) {
		if msg.ShardID != s.name {
			return
		}
		logger.InfoAsync("User left peer cluster node", "shard", s.name, "userID", msg.UserID, "nodeID", msg.NodeID)
	})

	// Subscribe to this shard's clustered route
	shardChannel := "shard:" + s.name
	if err := pubsubManager.Subscribe(shardChannel, "system"); err != nil {
		logger.ErrorAsync("Failed to subscribe shard to pub/sub cluster route", "error", err, "shard", s.name)
	}
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

// Shutdown gracefully shuts down the shard select loop and notifies peer nodes of shard termination.
func (s *Shard) Shutdown() {
	pubsubManager := s.coordinator.GetPubSubManager()
	_ = pubsubManager.NotifyShardDestroy(s.name)
	s.cancel()
}
