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
	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/websocket/pool"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

const (
	maxShards            = 1000
	maxTotalConnections  = 200000 // For high-scale connection support
	defaultShardName     = "default"
	shardCleanupInterval = 5 * time.Minute
)

// Manager handles the lifecycle of multiple Shard instances, balancing connections and routing messages across shards.
type Manager struct {
	shards              map[string]*Shard
	shardMutex          sync.RWMutex
	accountingMutex     sync.Mutex
	totalConnections    int64
	reservedConnections int64
	identityUsage       map[quotaKey]int64
	ctx                 context.Context
	cancel              context.CancelFunc
	nodeID              string
	pubsubManager       pubsub.Manager
	eventExecutor       *pool.PartitionedExecutor
	handlerSupervisor   *HandlerSupervisor
	config              ManagerConfig
	executorOwned       bool
	pubsubRegistrations []pubsub.HandlerRegistration
	state               atomic.Int32
	shutdownOnce        sync.Once
	done                chan struct{}
}

// ManagerState represents the manager lifecycle.
type ManagerState int32

const (
	StateStarting ManagerState = iota
	StateRunning
	StateDraining
	StateStopped
)

var (
	globalManager *Manager
	managerOnce   sync.Once
)

// GetGlobalManager returns the singleton Manager instance, bootstrapping it if necessary.
func GetGlobalManager() *Manager {
	managerOnce.Do(func() {
		globalManager = NewManager()
	})
	return globalManager
}

// NewManager instantiates a new connection Manager and spins up standard pubsub hooks and default shard.
func NewManager() *Manager {
	config := DefaultManagerConfig()
	config.PubSub = pubsub.GetGlobalManager()
	config.EventRegistry = defaultEventRegistry
	config.Authorizer = currentAuthorizer()
	config.Observer = defaultObserver
	return NewManagerWithConfig(config)
}

// NewManagerWithConfig creates an independently configured connection manager.
func NewManagerWithConfig(config ManagerConfig) *Manager {
	config, executorOwned := normalizeManagerConfig(config)
	ctx, cancel := context.WithCancel(context.Background())

	m := &Manager{
		shards:            make(map[string]*Shard),
		identityUsage:     make(map[quotaKey]int64),
		ctx:               ctx,
		cancel:            cancel,
		nodeID:            config.PubSub.GetNodeID(),
		pubsubManager:     config.PubSub,
		eventExecutor:     config.EventExecutor,
		handlerSupervisor: NewHandlerSupervisor(config.MaxHandlerGoroutines),
		config:            config,
		executorOwned:     executorOwned,
		done:              make(chan struct{}),
	}
	m.state.Store(int32(StateStarting))

	m.setupPubSubHandlers()

	// Create and start the default shard
	defaultShard := NewShard(defaultShardName, m)
	m.shards[defaultShardName] = defaultShard
	go defaultShard.Run()
	_ = m.pubsubManager.Subscribe("system", "shard:"+defaultShardName)

	m.sendNodeStatus()
	m.state.Store(int32(StateRunning))
	go m.Run()

	return m
}

// State returns the current manager lifecycle state.
func (m *Manager) State() ManagerState { return ManagerState(m.state.Load()) }

// IsAcceptingConnections reports whether upgrades may proceed.
func (m *Manager) IsAcceptingConnections() bool { return m.State() == StateRunning }

// GetEventExecutor returns the bounded executor used for business handlers.
func (m *Manager) GetEventExecutor() *pool.PartitionedExecutor { return m.eventExecutor }
func (m *Manager) GetHandlerSupervisor() *HandlerSupervisor    { return m.handlerSupervisor }

func (m *Manager) GetEventRegistry() *EventRegistry { return m.config.EventRegistry }
func (m *Manager) GetAuthorizer() Authorizer        { return m.config.Authorizer }
func (m *Manager) GetConfig() ManagerConfig {
	config := m.config
	if config.PubSub == nil {
		config.PubSub = m.pubsubManager
	}
	if config.EventExecutor == nil {
		config.EventExecutor = m.eventExecutor
	}
	config, _ = normalizeManagerConfig(config)
	return config
}

// setupPubSubHandlers configures event callbacks for node configuration changes.
func (m *Manager) setupPubSubHandlers() {
	m.pubsubRegistrations = append(m.pubsubRegistrations, m.pubsubManager.RegisterHandler(pubsub.MessageTypeNotification, func(msg *pubsub.CrossNodeMessage) {
		if shard, ok := m.GetShard(msg.ShardID); ok && msg.UserID != "" {
			shard.SendToUser(msg.UserID, crossNodePayload(msg))
		}
	}))
	m.pubsubRegistrations = append(m.pubsubRegistrations, m.pubsubManager.RegisterHandler(pubsub.MessageTypeChatRoom, func(msg *pubsub.CrossNodeMessage) {
		if shard, ok := m.GetShard(msg.ShardID); ok && msg.RoomID != "" {
			shard.SendToRoom(msg.RoomID, crossNodePayload(msg))
		}
	}))
	m.pubsubRegistrations = append(m.pubsubRegistrations, m.pubsubManager.RegisterHandler(pubsub.MessageTypeBroadcast, func(msg *pubsub.CrossNodeMessage) {
		if shard, ok := m.GetShard(msg.ShardID); ok {
			shard.Broadcast(crossNodePayload(msg))
		}
	}))
	m.pubsubRegistrations = append(m.pubsubRegistrations, m.pubsubManager.RegisterHandler(pubsub.MessageTypeUserJoin, func(msg *pubsub.CrossNodeMessage) {
		logger.InfoAsync("User joined on peer cluster node", "shardID", msg.ShardID, "nodeID", msg.NodeID)
	}))
	m.pubsubRegistrations = append(m.pubsubRegistrations, m.pubsubManager.RegisterHandler(pubsub.MessageTypeUserLeave, func(msg *pubsub.CrossNodeMessage) {
		logger.InfoAsync("User left peer cluster node", "shardID", msg.ShardID, "nodeID", msg.NodeID)
	}))
	m.pubsubRegistrations = append(m.pubsubRegistrations, m.pubsubManager.RegisterHandler(pubsub.MessageTypeShardCreate, func(msg *pubsub.CrossNodeMessage) {
		logger.InfoAsync("Shard created on peer cluster node", "shardID", msg.ShardID, "nodeID", msg.NodeID)
	}))

	m.pubsubRegistrations = append(m.pubsubRegistrations, m.pubsubManager.RegisterHandler(pubsub.MessageTypeShardDestroy, func(msg *pubsub.CrossNodeMessage) {
		logger.InfoAsync("Shard destroyed on peer cluster node", "shardID", msg.ShardID, "nodeID", msg.NodeID)
	}))

	m.pubsubRegistrations = append(m.pubsubRegistrations, m.pubsubManager.RegisterHandler(pubsub.MessageTypeNodeStatus, func(msg *pubsub.CrossNodeMessage) {
		logger.InfoAsync("Cluster node status update", "nodeID", msg.NodeID, "data", msg.Data)
	}))
}

func crossNodePayload(msg *pubsub.CrossNodeMessage) []byte {
	if msg == nil {
		return nil
	}
	data, ok := msg.Data["message"]
	if !ok {
		return nil
	}
	if text, ok := data.(string); ok {
		return []byte(text)
	}
	encoded, _ := json.Marshal(data)
	return encoded
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
			m.finishShutdown()
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
	m.accountingMutex.Lock()
	defer m.accountingMutex.Unlock()
	return atomic.LoadInt64(&m.totalConnections)+m.reservedConnections < m.GetConfig().MaxConnections
}

func (m *Manager) reserveConnection() bool {
	return m.reserveIdentityConnection(Identity{}) != nil
}

type quotaKey struct {
	dimension string
	value     string
}

type connectionReservation struct {
	keys []quotaKey
}

func (m *Manager) reserveIdentityConnection(identity Identity) *connectionReservation {
	if !m.IsAcceptingConnections() {
		return nil
	}
	select {
	case <-m.ctx.Done():
		return nil
	default:
	}
	m.accountingMutex.Lock()
	defer m.accountingMutex.Unlock()
	select {
	case <-m.ctx.Done():
		return nil
	default:
	}
	if atomic.LoadInt64(&m.totalConnections)+m.reservedConnections >= m.GetConfig().MaxConnections {
		return nil
	}
	config := m.GetConfig()
	checks := []struct {
		key   quotaKey
		limit int64
	}{
		{quotaKey{dimension: "user", value: identity.UserID}, config.MaxConnectionsPerUser},
		{quotaKey{dimension: "account", value: identity.AccountID}, config.MaxConnectionsPerAccount},
		{quotaKey{dimension: "tenant", value: identity.TenantID}, config.MaxConnectionsPerTenant},
	}
	keys := make([]quotaKey, 0, len(checks))
	for _, check := range checks {
		if check.key.value == "" || check.limit <= 0 {
			continue
		}
		if m.identityUsage[check.key] >= check.limit {
			return nil
		}
		keys = append(keys, check.key)
	}
	m.reservedConnections++
	for _, key := range keys {
		m.identityUsage[key]++
	}
	return &connectionReservation{keys: keys}
}

func (m *Manager) registerReservedConnection(shard *Shard, connection *Connection) bool {
	m.accountingMutex.Lock()
	defer m.accountingMutex.Unlock()
	if m.reservedConnections == 0 {
		return false
	}
	if !shard.Register(connection) {
		m.reservedConnections--
		m.releaseQuotaKeysLocked(connection.quotaKeys)
		return false
	}
	m.reservedConnections--
	atomic.AddInt64(&m.totalConnections, 1)
	return true
}

func (m *Manager) rollbackConnection() {
	m.rollbackReservation(nil)
}

func (m *Manager) rollbackReservation(reservation *connectionReservation) {
	m.accountingMutex.Lock()
	defer m.accountingMutex.Unlock()
	if m.reservedConnections > 0 {
		m.reservedConnections--
	}
	if reservation != nil {
		m.releaseQuotaKeysLocked(reservation.keys)
	}
}

func (m *Manager) releaseConnection() bool {
	return m.releaseConnectionQuota(nil)
}

func (m *Manager) releaseConnectionQuota(keys []quotaKey) bool {
	m.accountingMutex.Lock()
	defer m.accountingMutex.Unlock()
	current := atomic.LoadInt64(&m.totalConnections)
	if current == 0 {
		return false
	}
	atomic.StoreInt64(&m.totalConnections, current-1)
	m.releaseQuotaKeysLocked(keys)
	return true
}

func (m *Manager) releaseQuotaKeysLocked(keys []quotaKey) {
	for _, key := range keys {
		if m.identityUsage[key] <= 1 {
			delete(m.identityUsage, key)
		} else {
			m.identityUsage[key]--
		}
	}
}

// AddShard dynamically instantiates a new Shard under the given name.
func (m *Manager) AddShard(shardID string) *Shard {
	if !m.IsAcceptingConnections() {
		return nil
	}
	m.shardMutex.Lock()
	defer m.shardMutex.Unlock()
	select {
	case <-m.ctx.Done():
		return nil
	default:
	}

	maxConfiguredShards := m.GetConfig().MaxShards
	if len(m.shards) >= maxConfiguredShards {
		logger.WarnAsync("Max Shard capacity reached, falling back to default Shard", "maxShards", maxConfiguredShards)
		return m.shards[defaultShardName]
	}

	if shard, exists := m.shards[shardID]; exists {
		return shard
	}

	shard := NewShard(shardID, m)
	m.shards[shardID] = shard
	go shard.Run()
	_ = m.pubsubManager.Subscribe("shard:" + shardID)

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
	shard, exists := m.shards[shardID]
	if exists {
		delete(m.shards, shardID)
	}
	m.shardMutex.Unlock()
	if exists {
		_ = m.pubsubManager.Unsubscribe("shard:" + shardID)
		_ = shard.Shutdown(context.Background())
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
	return int(xxhash.Sum64String(userID) % uint64(m.GetConfig().MaxShards))
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

// DecrTotalConnections decrements the total connection counter atomically.
// Implements ShardCoordinator so Shard can update this without a concrete *Manager reference.
func (m *Manager) DecrTotalConnections() {
	m.releaseConnection()
}

func (m *Manager) ReleaseConnectionQuota(keys []quotaKey) {
	m.releaseConnectionQuota(keys)
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
	shards := make([]*Shard, 0, len(m.shards))
	for _, shard := range m.shards {
		shards = append(shards, shard)
	}
	m.shardMutex.RUnlock()

	for _, shard := range shards {
		shard.BroadcastGlobal(message)
	}
}

// HandleConnection binds a generic Connection session wrapper and registers it inside the designated Shard.
// This function blocks, starting read message loop processes.
func (m *Manager) HandleConnection(conn Conn, shardID, userID, clientIP, requestID string) error {
	return m.HandleIdentityConnection(conn, shardID, Identity{UserID: userID}, clientIP, requestID)
}

// HandleIdentityConnection registers a connection with a structured authenticated identity.
func (m *Manager) HandleIdentityConnection(conn Conn, shardID string, identity Identity, clientIP, requestID string) error {
	userID := identity.UserID
	if conn == nil {
		return fmt.Errorf("connection is nil")
	}
	reservation := m.reserveIdentityConnection(identity)
	if reservation == nil {
		logger.WarnAsync("Manager connection ceiling reached, rejecting connection", "total", m.GetTotalConnections())
		_ = conn.Close()
		return fmt.Errorf("server capacity reached")
	}
	reserved := true
	defer func() {
		if reserved {
			m.rollbackReservation(reservation)
		}
	}()

	if shardID == "" {
		shardID = defaultShardName
	}

	shard := m.GetOrCreateShard(shardID)
	if shard == nil {
		_ = conn.Close()
		return fmt.Errorf("manager is shutting down")
	}

	connection := NewConnection(conn, shard, userID, shardID, clientIP, m.nodeID, requestID)
	if connection == nil {
		_ = conn.Close()
		return fmt.Errorf("failed to allocate connection wrapper")
	}
	connection.setIdentity(identity)
	connection.quotaKeys = reservation.keys
	if !identity.ExpiresAt.IsZero() {
		timer := time.AfterFunc(time.Until(identity.ExpiresAt), func() { connection.Close("Authentication expired") })
		defer timer.Stop()
	}

	if !m.registerReservedConnection(shard, connection) {
		reserved = false
		connection.Close("registration rejected")
		return fmt.Errorf("failed to register connection")
	}
	m.config.Observer.ConnectionOpened()
	reserved = false

	if !m.config.DisableLifecycleLogging {
		logLifecycle("WebSocket connection established", "shardID", shardID, "userID", userID, "clientIP", clientIP)
	}

	connection.Run()

	return nil
}

// DisconnectIdentity closes active sessions matching the supplied predicate.
func (m *Manager) DisconnectIdentity(match func(Identity) bool, reason string) int {
	if match == nil {
		return 0
	}
	m.shardMutex.RLock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, shard := range m.shards {
		shards = append(shards, shard)
	}
	m.shardMutex.RUnlock()
	disconnected := 0
	for _, shard := range shards {
		shard.mu.RLock()
		connections := make([]*Connection, 0, len(shard.activeConns))
		for conn := range shard.activeConns {
			connections = append(connections, conn)
		}
		shard.mu.RUnlock()
		for _, conn := range connections {
			if match(conn.identity) {
				conn.Close(reason)
				disconnected++
			}
		}
	}
	return disconnected
}

// cleanupEmptyShards sweeps and shuts down dynamic Shards containing zero active connections.
func (m *Manager) cleanupEmptyShards() {
	m.shardMutex.Lock()
	var emptyShards []*Shard

	for shardID, shard := range m.shards {
		if shardID != defaultShardName && shard.GetConnectionCount() == 0 {
			emptyShards = append(emptyShards, shard)
			delete(m.shards, shardID)
		}
	}
	m.shardMutex.Unlock()

	for _, shard := range emptyShards {
		_ = shard.Shutdown(context.Background())
		logger.InfoAsync("Cleaned up idle Shard", "shardID", shard.name)
	}
}

// GetStats compiles manager health metrics and connections status summary.
func (m *Manager) GetStats() map[string]interface{} {
	m.shardMutex.RLock()
	defer m.shardMutex.RUnlock()
	config := m.GetConfig()

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
		"maxTotalConnections": config.MaxConnections,
		"maxShards":           config.MaxShards,
		"state":               m.State(),
		"eventExecutor":       m.eventExecutor.Stats(),
		"handlerSupervisor":   m.handlerSupervisor.Stats(),
		"events":              m.eventStats(),
		"runtime":             m.runtimeMetrics(),
		"eventLatency":        m.eventLatency(),
	}
}

func (m *Manager) runtimeMetrics() RuntimeMetrics {
	if observer, ok := m.config.Observer.(SnapshotObserver); ok {
		return observer.RuntimeMetrics()
	}
	return RuntimeMetrics{}
}

func (m *Manager) eventStats() EventStats {
	if observer, ok := m.config.Observer.(SnapshotObserver); ok {
		return observer.EventStats()
	}
	return EventStats{}
}

func (m *Manager) eventLatency() LatencyHistogram {
	if observer, ok := m.config.Observer.(*AtomicObserver); ok {
		return observer.LatencyHistogram()
	}
	return LatencyHistogram{}
}

func (m *Manager) finishShutdown() {
	m.shardMutex.Lock()
	shards := make([]*Shard, 0, len(m.shards))
	for _, shard := range m.shards {
		shards = append(shards, shard)
	}
	m.shards = make(map[string]*Shard)
	m.shardMutex.Unlock()

	for _, shard := range shards {
		_ = shard.Shutdown(context.Background())
	}
	for _, registration := range m.pubsubRegistrations {
		m.pubsubManager.UnregisterHandler(registration)
	}
	m.pubsubRegistrations = nil
	m.handlerSupervisor.Stop()
	if m.executorOwned {
		_ = m.eventExecutor.Shutdown(context.Background())
	}
	if m.config.PubSubOwnership == PubSubOwned {
		_ = m.pubsubManager.Shutdown()
	}
	m.state.Store(int32(StateStopped))
	close(m.done)
}

// Shutdown drains shards, connections, event handlers, and pub/sub consumers.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.shutdownOnce.Do(func() {
		m.state.Store(int32(StateDraining))
		m.cancel()
	})
	select {
	case <-m.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
