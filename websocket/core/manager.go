// Package core provides the central state management and orchestration for the WebSocket server.
//
// It handles connection lifecycle (upgrade, register, unregister), message broadcasting,
// sharded connection storage (to prevent lock contention), and integrates with pub/sub
// backends for multi-node clustering.
package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

const (
	maxShards           = 1000
	maxTotalConnections = 200000 // For high-scale connection support
	defaultShardName    = "default"
	shardCleanupInterval = 5 * time.Minute
)

// Manager handles the lifecycle of multiple Shard instances, balancing connections and routing messages across shards.
type Manager struct {
	shards           map[string]*Shard
	shardMutex       sync.RWMutex
	totalConnections int64
	ctx              context.Context
	cancel           context.CancelFunc
	connectionPool   sync.Pool
	nodeID           string
	pubsubManager    pubsub.Manager
}

var (
	globalManager *Manager
	managerOnce   sync.Once
)

// GetGlobalManager returns the singleton Manager instance, bootstrapping it if necessary.
// It accepts an optional adapterType (e.g., pubsub.AdapterNATS) which defaults to redis.
func GetGlobalManager(adapterType ...string) *Manager {
	managerOnce.Do(func() {
		globalManager = NewManager(adapterType...)
		go globalManager.Run()
	})
	return globalManager
}

// NewManager instantiates a new connection Manager and spins up standard pubsub hooks and default shard.
func NewManager(adapterType ...string) *Manager {
	at := pubsub.AdapterRedis
	if len(adapterType) > 0 && adapterType[0] != "" {
		at = adapterType[0]
	}

	ctx, cancel := context.WithCancel(context.Background())
	pubsubManager := pubsub.GetGlobalManager(at)

	m := &Manager{
		shards: make(map[string]*Shard),
		ctx:    ctx,
		cancel: cancel,
		connectionPool: sync.Pool{
			New: func() interface{} {
				return make(chan []byte, 256)
			},
		},
		nodeID:        pubsubManager.GetNodeID(),
		pubsubManager: pubsubManager,
	}

	m.setupPubSubHandlers()

	// Create and start the default shard
	defaultShard := NewShard(defaultShardName, m)
	m.shards[defaultShardName] = defaultShard
	go defaultShard.Run()

	m.sendNodeStatus()

	return m
}

// setupPubSubHandlers configures event callbacks for node configuration changes.
func (m *Manager) setupPubSubHandlers() {
	m.pubsubManager.RegisterHandler(pubsub.MessageTypeShardCreate, func(msg *pubsub.CrossNodeMessage) {
		logger.InfoAsync("Shard created on peer cluster node", "shardID", msg.ShardID, "nodeID", msg.NodeID)
	})

	m.pubsubManager.RegisterHandler(pubsub.MessageTypeShardDestroy, func(msg *pubsub.CrossNodeMessage) {
		logger.InfoAsync("Shard destroyed on peer cluster node", "shardID", msg.ShardID, "nodeID", msg.NodeID)
	})

	m.pubsubManager.RegisterHandler(pubsub.MessageTypeNodeStatus, func(msg *pubsub.CrossNodeMessage) {
		logger.InfoAsync("Cluster node status update", "nodeID", msg.NodeID, "data", msg.Data)
	})
}

// sendNodeStatus publishes current node metrics to the clustered coordination system.
func (m *Manager) sendNodeStatus() {
	stats := m.GetStats()
	data := map[string]interface{}{
		"total_connections": stats["totalConnections"],
		"total_shards":      stats["totalShards"],
		"timestamp":         time.Now(),
	}

	if err := m.pubsubManager.SendNodeStatus(data); err != nil {
		logger.ErrorAsync("Failed to publish node metrics", "error", err, "nodeID", m.nodeID)
	}
}

// Run executes the manager loop, managing cleanup of empty shards periodically.
func (m *Manager) Run() {
	ticker := time.NewTicker(shardCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			m.shutdown()
			return
		case <-ticker.C:
			m.cleanupEmptyShards()
		}
	}
}

// GetTotalConnections returns the total count of active connections handled by this manager.
func (m *Manager) GetTotalConnections() int64 {
	return atomic.LoadInt64(&m.totalConnections)
}

// CanAcceptConnection checks if there is headroom to accept another connection.
func (m *Manager) CanAcceptConnection() bool {
	return atomic.LoadInt64(&m.totalConnections) < maxTotalConnections
}

// AddShard dynamically instantiates a new Shard under the given name.
func (m *Manager) AddShard(shardID string) *Shard {
	m.shardMutex.Lock()
	defer m.shardMutex.Unlock()

	if len(m.shards) >= maxShards {
		logger.WarnAsync("Max Shard capacity reached, falling back to default Shard", "maxShards", maxShards)
		return m.shards[defaultShardName]
	}

	if shard, exists := m.shards[shardID]; exists {
		return shard
	}

	shard := NewShard(shardID, m)
	m.shards[shardID] = shard
	go shard.Run()

	_ = m.pubsubManager.NotifyShardCreate(shardID)

	logger.InfoAsync("Shard created", "shardID", shardID, "nodeID", m.nodeID)
	return shard
}

// RemoveShard gracefully terminates and removes a Shard instance.
func (m *Manager) RemoveShard(shardID string) {
	if shardID == defaultShardName {
		return
	}

	m.shardMutex.Lock()
	defer m.shardMutex.Unlock()

	if shard, exists := m.shards[shardID]; exists {
		shard.Shutdown()
		delete(m.shards, shardID)
		logger.InfoAsync("Shard removed", "shardID", shardID)
	}
}

// GetShard returns an active Shard by its ID.
func (m *Manager) GetShard(shardID string) (*Shard, bool) {
	m.shardMutex.RLock()
	defer m.shardMutex.RUnlock()
	shard, exists := m.shards[shardID]
	return shard, exists
}

// GenerateShardID hashes a user ID and maps it to a consistent shard index.
func (m *Manager) GenerateShardID(userID string) int {
	return int(xxhash.Sum64String(userID) % uint64(maxShards))
}

// GetShardID resolves the shard string routing identifier for a specific user ID.
func (m *Manager) GetShardID(userID string) string {
	shardIndex := m.GenerateShardID(userID)
	return fmt.Sprintf("shard-%d", shardIndex)
}

// GetPubSubManager returns the pubsub manager used by this connection manager.
func (m *Manager) GetPubSubManager() pubsub.Manager {
	return m.pubsubManager
}

// GetOrCreateShard resolves a Shard or creates it if it doesn't already exist.
func (m *Manager) GetOrCreateShard(shardID string) *Shard {
	if shard, exists := m.GetShard(shardID); exists {
		return shard
	}
	return m.AddShard(shardID)
}

// BroadcastMessage transmits a message to a specific Shard locally and routing globally.
func (m *Manager) BroadcastMessage(shardID string, message []byte) {
	if shard, exists := m.GetShard(shardID); exists {
		shard.BroadcastGlobal(message)
	}
}

// BroadcastToAll transmits a message to all shards globally.
func (m *Manager) BroadcastToAll(message []byte) {
	m.shardMutex.RLock()
	defer m.shardMutex.RUnlock()

	for _, shard := range m.shards {
		shard.BroadcastGlobal(message)
	}
}

// HandleConnection binds a generic Connection session wrapper and registers it inside the designated Shard.
// This function blocks, starting read message loop processes.
func (m *Manager) HandleConnection(conn Conn, shardID, userID, clientIP, requestID string) error {
	if !m.CanAcceptConnection() {
		logger.WarnAsync("Manager connection ceiling reached, rejecting connection", "total", m.GetTotalConnections())
		_ = conn.Close()
		return fmt.Errorf("server capacity reached")
	}

	if shardID == "" {
		shardID = defaultShardName
	}

	shard := m.GetOrCreateShard(shardID)
	if !shard.CanAcceptConnection() {
		logger.WarnAsync("Shard connection ceiling reached, rejecting connection", "shardID", shardID)
		_ = conn.Close()
		return fmt.Errorf("shard capacity reached")
	}

	connection := NewConnection(conn, shard, userID, shardID, clientIP, m.nodeID, requestID)
	if connection == nil {
		_ = conn.Close()
		return fmt.Errorf("failed to allocate connection wrapper")
	}

	shard.Register(connection)

	atomic.AddInt64(&m.totalConnections, 1)

	// Start write queue pump
	go connection.writePump()

	logger.InfoAsync("WebSocket connection established", "shardID", shardID, "userID", userID, "clientIP", clientIP)

	// Start read queue pump (blocking)
	connection.readPump()

	return nil
}

// cleanupEmptyShards sweeps and shuts down dynamic Shards containing zero active connections.
func (m *Manager) cleanupEmptyShards() {
	m.shardMutex.Lock()
	defer m.shardMutex.Unlock()

	var emptyShards []string

	for shardID, shard := range m.shards {
		if shardID != defaultShardName && shard.GetConnectionCount() == 0 {
			emptyShards = append(emptyShards, shardID)
		}
	}

	for _, shardID := range emptyShards {
		if shard, exists := m.shards[shardID]; exists {
			shard.Shutdown()
			delete(m.shards, shardID)
			logger.InfoAsync("Cleaned up idle Shard", "shardID", shardID)
		}
	}
}

// GetStats compiles manager health metrics and connections status summary.
func (m *Manager) GetStats() map[string]interface{} {
	m.shardMutex.RLock()
	defer m.shardMutex.RUnlock()

	shardStats := make(map[string]int64)
	totalShardConnections := int64(0)

	for shardID, shard := range m.shards {
		connections := shard.GetConnectionCount()
		shardStats[shardID] = connections
		totalShardConnections += connections
	}

	return map[string]interface{}{
		"totalShards":         len(m.shards),
		"totalConnections":    atomic.LoadInt64(&m.totalConnections),
		"shardConnections":    totalShardConnections,
		"shardStats":          shardStats,
		"maxTotalConnections": maxTotalConnections,
		"maxShards":           maxShards,
	}
}

// shutdown gracefully terminates active shards when manager is stopped.
func (m *Manager) shutdown() {
	m.shardMutex.Lock()
	defer m.shardMutex.Unlock()

	for _, shard := range m.shards {
		shard.Shutdown()
	}

	m.shards = make(map[string]*Shard)
	atomic.StoreInt64(&m.totalConnections, 0)
}

// Shutdown initiates graceful termination of the connection Manager.
func (m *Manager) Shutdown() {
	m.cancel()
}
