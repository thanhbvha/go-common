// package redispubsub handles multi-node coordination using Redis pub/sub to route messages in a clustered environment.
package redispubsub

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/v2/logger"
	panicrecovery "github.com/thanhbvha/go-common/v2/recovery"
	"github.com/thanhbvha/go-common/v2/redis"
	"github.com/thanhbvha/go-common/v2/websocket/internal/wsrecovery"
	"github.com/thanhbvha/go-common/v2/websocket/pool"
	"github.com/thanhbvha/go-common/v2/websocket/pubsub"
)

// Manager manages subscriptions and publication of messages across multiple instances in a cluster.
type Manager struct {
	redisClient        *redis.Client
	nodeID             string
	subscribers        map[string]*goredis.PubSub
	desired            map[string]struct{}
	handlers           map[string]map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage)
	registrations      map[pubsub.HandlerRegistration]string
	mu                 sync.RWMutex
	ctx                context.Context
	cancel             context.CancelFunc
	executor           *pool.PartitionedExecutor
	failurePolicy      pubsub.ClusterFailurePolicy
	supervisorWG       sync.WaitGroup
	reconnectAttempts  atomic.Uint64
	reconnectSuccesses atomic.Uint64
	reconnectFailures  atomic.Uint64
	lastReconnectError string
	auth               *pubsub.Authenticator
	maxMessageBytes    int
	rejected           atomic.Uint64
	dropLog            pubsub.DropLog
	execPartitions     int
	execQueueSize      int
}

// Option customizes a Manager at construction.
type Option func(*Manager)

// WithAuthenticator signs outbound and verifies inbound broker messages.
func WithAuthenticator(auth *pubsub.Authenticator) Option {
	return func(m *Manager) { m.auth = auth }
}

// WithExecutor sizes the dispatch executor (default 4 partitions x 256 slots).
// Handlers for the same user, room or shard stay ordered within a partition.
func WithExecutor(partitions, queueSize int) Option {
	return func(m *Manager) {
		if partitions > 0 && queueSize > 0 {
			m.execPartitions, m.execQueueSize = partitions, queueSize
		}
	}
}

// WithMaxMessageBytes bounds an inbound payload before it is decoded.
func WithMaxMessageBytes(n int) Option {
	return func(m *Manager) {
		if n > 0 {
			m.maxMessageBytes = n
		}
	}
}

var (
	globalManager *Manager
	managerOnce   sync.Once
)

// GetGlobalManager returns the default singleton Manager initialized with the default Redis client.
//
// Deprecated: process-global state couples unrelated services and tests. Build
// a Manager with NewManagerWithClient and pass it via core.ManagerConfig.PubSub.
func GetGlobalManager() *Manager {
	managerOnce.Do(func() {
		globalManager = NewManager()
	})
	return globalManager
}

// NewManager instantiates a Manager on the default Redis client. When no
// default client is registered the manager stays unready and rejects publishes
// rather than silently running as a single node; use
// NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone) to opt in to
// in-process loopback for development.
//
// Deprecated: depends on a process-global client. Prefer NewManagerWithClient.
func NewManager() *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	client, _ := redis.LookupDefault()

	if client == nil {
		logger.WarnAsync("Redis default client not set. WebSocket PubSub will stay unready until one is configured.")
	}

	manager := &Manager{
		maxMessageBytes: pubsub.DefaultMaxMessageBytes,
		redisClient:     client,
		nodeID:          pubsub.GenerateNodeID(),
		subscribers:     make(map[string]*goredis.PubSub),
		desired:         make(map[string]struct{}),
		handlers:        make(map[string]map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage)),
		registrations:   make(map[pubsub.HandlerRegistration]string),
		ctx:             ctx,
		cancel:          cancel,
		failurePolicy:   pubsub.StayUnready,
	}
	manager.startExecutor()
	manager.supervisorWG.Add(1)
	go manager.superviseSubscriptions()
	return manager
}

// NewManagerWithClient instantiates a Manager on a user-provided Redis client.
// It stays unready while the client is nil or disconnected. A nil client is a
// misconfiguration in a cluster, so it is not silently treated as standalone;
// opt in explicitly with NewManagerWithClientAndPolicy and pubsub.AllowStandalone.
func NewManagerWithClient(client *redis.Client, options ...Option) *Manager {
	return NewManagerWithClientAndPolicy(client, pubsub.StayUnready, options...)
}

// NewManagerWithClientAndPolicy is NewManagerWithClient with an explicit
// behaviour for an unavailable broker.
func NewManagerWithClientAndPolicy(client *redis.Client, policy pubsub.ClusterFailurePolicy, options ...Option) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	manager := &Manager{
		maxMessageBytes: pubsub.DefaultMaxMessageBytes,
		redisClient:     client,
		nodeID:          pubsub.GenerateNodeID(),
		subscribers:     make(map[string]*goredis.PubSub),
		desired:         make(map[string]struct{}),
		handlers:        make(map[string]map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage)),
		registrations:   make(map[pubsub.HandlerRegistration]string),
		ctx:             ctx,
		cancel:          cancel,
		failurePolicy:   policy,
	}
	for _, option := range options {
		option(manager)
	}
	manager.startExecutor()
	manager.supervisorWG.Add(1)
	go manager.superviseSubscriptions()
	return manager
}

// RegisterHandler binds a callback handler to a specific cross-node message type.
func (psm *Manager) RegisterHandler(messageType string, handler func(*pubsub.CrossNodeMessage)) pubsub.HandlerRegistration {
	registration := pubsub.NewHandlerRegistration()
	psm.mu.Lock()
	defer psm.mu.Unlock()
	if psm.handlers[messageType] == nil {
		psm.handlers[messageType] = make(map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage))
	}
	psm.handlers[messageType][registration] = handler
	psm.registrations[registration] = messageType
	return registration
}

func (psm *Manager) UnregisterHandler(registration pubsub.HandlerRegistration) {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	messageType, ok := psm.registrations[registration]
	if !ok {
		return
	}
	delete(psm.registrations, registration)
	delete(psm.handlers[messageType], registration)
	if len(psm.handlers[messageType]) == 0 {
		delete(psm.handlers, messageType)
	}
}

// Subscribe listens to the specified Redis channels and processes incoming messages.
func (psm *Manager) Subscribe(channels ...string) error {
	if psm.redisClient == nil {
		if psm.failurePolicy == pubsub.AllowStandalone {
			return nil
		}
		return fmt.Errorf("redis client is not configured")
	}

	// Broker round trips run outside psm.mu so a slow Redis cannot block
	// RegisterHandler, GetStats or message dispatch.
	psm.mu.Lock()
	missing := make([]string, 0, len(channels))
	for _, channel := range channels {
		psm.desired[channel] = struct{}{}
		if _, exists := psm.subscribers[channel]; !exists {
			missing = append(missing, channel)
		}
	}
	psm.mu.Unlock()

	for _, channel := range missing {
		sub, err := psm.redisClient.Subscribe(psm.ctx, channel)
		if err != nil || sub == nil {
			return fmt.Errorf("failed to subscribe to channel: %s, err: %v", channel, err)
		}
		psm.mu.Lock()
		_, desired := psm.desired[channel]
		_, exists := psm.subscribers[channel]
		if !desired || exists { // unsubscribed or raced by another Subscribe meanwhile
			psm.mu.Unlock()
			_ = sub.Close()
			continue
		}
		psm.subscribers[channel] = sub
		psm.mu.Unlock()
		go psm.handleChannelMessages(channel, sub)
	}

	logger.InfoAsync("Subscribed to channels", "node_id", psm.nodeID, "channels", channels)
	return nil
}

// Unsubscribe stops listening to and closes the specified channels.
func (psm *Manager) Unsubscribe(channels ...string) error {
	if psm.redisClient == nil {
		return nil // No-op in standalone/test mode
	}

	// Detach under the lock, talk to the broker outside it.
	type detached struct {
		channel string
		sub     *goredis.PubSub
	}
	psm.mu.Lock()
	var subs []detached
	for _, channel := range channels {
		delete(psm.desired, channel)
		if sub, exists := psm.subscribers[channel]; exists {
			delete(psm.subscribers, channel)
			subs = append(subs, detached{channel, sub})
		}
	}
	psm.mu.Unlock()

	var firstErr error
	for _, d := range subs {
		if err := d.sub.Unsubscribe(psm.ctx, d.channel); err != nil {
			logger.ErrorAsync("Failed to unsubscribe from channel", "channel", d.channel, "error", err)
			firstErr = errors.Join(firstErr, err)
		}
		if err := d.sub.Close(); err != nil {
			logger.ErrorAsync("Failed to close pubsub", "channel", d.channel, "error", err)
			firstErr = errors.Join(firstErr, err)
		}
	}
	if firstErr != nil {
		return firstErr
	}

	logger.InfoAsync("Unsubscribed from channels", "node_id", psm.nodeID, "channels", channels)
	return nil
}

// handleChannelMessages listens to the channel payload and dispatches events.
func (psm *Manager) handleChannelMessages(channel string, pubsub *goredis.PubSub) {
	defer psm.cleanupSubscription(channel, pubsub)
	defer panicrecovery.Catch(wsrecovery.Handler(wsrecovery.Metadata{
		Component: "pubsub.redis_listener",
		Operation: "listen",
		Channel:   channel,
		NodeID:    psm.nodeID,
	}))

	ch := pubsub.Channel()

	for {
		select {
		case <-psm.ctx.Done():
			logger.InfoAsync("Stopping channel message handler", "channel", channel, "node_id", psm.nodeID)
			return
		case msg, ok := <-ch:
			if !ok || msg == nil {
				return
			}

			psm.processMessage(channel, msg.Payload)
		}
	}
}

func (psm *Manager) cleanupSubscription(channel string, subscription *goredis.PubSub) {
	psm.mu.Lock()
	if current, ok := psm.subscribers[channel]; ok && current == subscription {
		delete(psm.subscribers, channel)
	}
	psm.mu.Unlock()
	_ = subscription.Close()
}

// processMessage decodes and directs a single payload to its corresponding handler.
func (psm *Manager) processMessage(channel, payload string) {
	raw, err := psm.auth.Open([]byte(payload), psm.maxMessageBytes)
	if err != nil {
		psm.rejected.Add(1)
		logger.WarnAsync("Rejected cross-node message", "reason", err.Error(), "channel", channel, "payloadBytes", len(payload))
		return
	}
	decoded, err := pubsub.DecodeCrossNodeMessage(raw, psm.maxMessageBytes)
	if err != nil {
		psm.rejected.Add(1)
		logger.WarnAsync("Rejected cross-node message", "reason", err.Error(), "channel", channel, "payloadBytes", len(payload))
		return
	}
	message := *decoded

	// Skip messages generated by this node
	if message.NodeID == psm.nodeID {
		return
	}

	psm.mu.RLock()
	handlers := copyHandlers(psm.handlers[message.Type])
	psm.mu.RUnlock()

	if len(handlers) == 0 {
		logger.WarnAsync("No handler for message type", "type", message.Type, "channel", channel)
		return
	}

	for _, handler := range handlers {
		if err := psm.executor.Submit(psm.ctx, messagePartitionKey(&message), func(context.Context) error {
			defer panicrecovery.Catch(wsrecovery.Handler(psm.recoveryMetadata("pubsub.redis_message_handler", channel, &message)))
			handler(&message)
			return nil
		}); err != nil {
			psm.dropLog.Report(err)
		}
	}
}

// Publish transmits a pubsub.CrossNodeMessage to the specified Redis channel.
func (psm *Manager) Publish(channel string, message *pubsub.CrossNodeMessage) error {
	if message.ID == "" {
		message.ID = pubsub.GenerateNodeID()
	}
	if message.Version == 0 {
		message.Version = 1
	}
	message.NodeID = psm.nodeID
	message.Timestamp = time.Now()

	if psm.redisClient == nil {
		if psm.failurePolicy != pubsub.AllowStandalone {
			return fmt.Errorf("redis cluster backend unavailable")
		}
		// Standalone/in-memory loopback mode
		psm.mu.RLock()
		handlers := copyHandlers(psm.handlers[message.Type])
		psm.mu.RUnlock()
		for _, handler := range handlers {
			if err := psm.executor.Submit(psm.ctx, messagePartitionKey(message), func(context.Context) error {
				defer panicrecovery.Catch(wsrecovery.Handler(psm.recoveryMetadata("pubsub.redis_loopback_handler", channel, message)))
				handler(message)
				return nil
			}); err != nil {
				psm.dropLog.Report(err)
			}
		}
		return nil
	}

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	if data, err = psm.auth.Seal(data); err != nil {
		return fmt.Errorf("failed to seal message: %w", err)
	}

	return psm.redisClient.Publish(psm.ctx, channel, data)
}

func (psm *Manager) recoveryMetadata(component, channel string, message *pubsub.CrossNodeMessage) wsrecovery.Metadata {
	metadata := wsrecovery.Metadata{
		Component: component,
		Operation: "dispatch",
		Channel:   channel,
		NodeID:    psm.nodeID,
	}
	if message != nil {
		metadata.MessageType = message.Type
		metadata.UserID = message.UserID
		metadata.ShardID = message.ShardID
		metadata.RoomID = message.RoomID
	}
	return metadata
}

// BroadcastMessage sends a broadcast event to all subscribers of a specific shard ID.
func (psm *Manager) BroadcastMessage(shardID string, data map[string]interface{}) error {
	message := &pubsub.CrossNodeMessage{
		Type:    pubsub.MessageTypeBroadcast,
		ShardID: shardID,
		Data:    data,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// BroadcastUserNotification sends a notification event to a specific user across different nodes.
func (psm *Manager) BroadcastUserNotification(shardID, userID string, data map[string]interface{}) error {
	message := &pubsub.CrossNodeMessage{
		Type:    pubsub.MessageTypeNotification,
		ShardID: shardID,
		UserID:  userID,
		Data:    data,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// BroadcastRoomMessage distributes a chat room message to all room members across nodes.
func (psm *Manager) BroadcastRoomMessage(shardID, roomID string, data map[string]interface{}) error {
	message := &pubsub.CrossNodeMessage{
		Type:    pubsub.MessageTypeChatRoom,
		ShardID: shardID,
		RoomID:  roomID,
		Data:    data,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// BroadcastChatMessage sends a direct chat message to a user on any cluster node.
func (psm *Manager) BroadcastChatMessage(shardID string, userID string, data map[string]interface{}) error {
	message := &pubsub.CrossNodeMessage{
		Type:    pubsub.MessageTypeChat,
		ShardID: shardID,
		UserID:  userID,
		Data:    data,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// NotifyUserJoin sends a join event to other nodes when a user connects.
func (psm *Manager) NotifyUserJoin(shardID, userID, clientIP string) error {
	message := &pubsub.CrossNodeMessage{
		Type:    pubsub.MessageTypeUserJoin,
		ShardID: shardID,
		UserID:  userID,
		Data: map[string]interface{}{
			"client_ip": clientIP,
		},
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// NotifyUserLeave sends a leave event to other nodes when a user disconnects.
func (psm *Manager) NotifyUserLeave(shardID, userID string) error {
	message := &pubsub.CrossNodeMessage{
		Type:    pubsub.MessageTypeUserLeave,
		ShardID: shardID,
		UserID:  userID,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// NotifyShardCreate broadcasts a shard creation notice globally to system subscribers.
func (psm *Manager) NotifyShardCreate(shardID string) error {
	message := &pubsub.CrossNodeMessage{
		Type:    pubsub.MessageTypeShardCreate,
		ShardID: shardID,
	}

	return psm.Publish("system", message)
}

// NotifyShardDestroy broadcasts a shard destruction notice globally.
func (psm *Manager) NotifyShardDestroy(shardID string) error {
	message := &pubsub.CrossNodeMessage{
		Type:    pubsub.MessageTypeShardDestroy,
		ShardID: shardID,
	}

	return psm.Publish("system", message)
}

// SendNodeStatus publishes node status metrics to the system control topic.
func (psm *Manager) SendNodeStatus(data map[string]interface{}) error {
	message := &pubsub.CrossNodeMessage{
		Type: pubsub.MessageTypeNodeStatus,
		Data: data,
	}

	return psm.Publish("system", message)
}

// GetNodeID returns the unique identifier of the current cluster node.
func (psm *Manager) GetNodeID() string {
	return psm.nodeID
}

// GetStats returns metrics and current status of the pub/sub connection.
func (psm *Manager) GetStats() map[string]interface{} {
	psm.mu.RLock()
	defer psm.mu.RUnlock()

	channels := make([]string, 0, len(psm.subscribers))
	for channel := range psm.subscribers {
		channels = append(channels, channel)
	}

	handlers := make([]string, 0, len(psm.handlers))
	for msgType := range psm.handlers {
		handlers = append(handlers, msgType)
	}

	return map[string]interface{}{
		"node_id":              psm.nodeID,
		"subscribed_channels":  len(psm.subscribers),
		"desired_channels":     len(psm.desired),
		"channels":             channels,
		"registered_handlers":  len(psm.registrations),
		"handlers":             handlers,
		"mode":                 map[bool]string{true: "redis", false: "standalone"}[psm.redisClient != nil],
		"dispatch":             psm.executor.Stats(),
		"rejected_messages":    psm.rejected.Load(),
		"ready":                psm.readyLocked(),
		"reconnect_state":      psm.reconnectStateLocked(),
		"reconnect_attempts":   psm.reconnectAttempts.Load(),
		"reconnect_successes":  psm.reconnectSuccesses.Load(),
		"reconnect_failures":   psm.reconnectFailures.Load(),
		"last_reconnect_error": psm.lastReconnectError,
	}
}

// Shutdown gracefully unsubscribes from all channels and releases the connections.
func (psm *Manager) Shutdown() error {
	logger.InfoAsync("Shutting down pub/sub manager", "node_id", psm.nodeID)

	psm.cancel()

	psm.mu.Lock()

	for channel, pubsub := range psm.subscribers {
		if err := pubsub.Close(); err != nil {
			logger.ErrorAsync("Error closing pubsub", "channel", channel, "error", err)
		}
	}

	psm.subscribers = make(map[string]*goredis.PubSub)
	psm.desired = make(map[string]struct{})
	psm.handlers = make(map[string]map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage))
	psm.registrations = make(map[pubsub.HandlerRegistration]string)
	psm.mu.Unlock()
	psm.supervisorWG.Wait()
	_ = psm.executor.Shutdown(context.Background())

	logger.InfoAsync("Pub/sub manager shutdown complete", "node_id", psm.nodeID)
	return nil
}

func copyHandlers(source map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage)) []func(*pubsub.CrossNodeMessage) {
	handlers := make([]func(*pubsub.CrossNodeMessage), 0, len(source))
	for _, handler := range source {
		handlers = append(handlers, handler)
	}
	return handlers
}

func (psm *Manager) Ready() bool {
	psm.mu.RLock()
	defer psm.mu.RUnlock()
	return psm.readyLocked()
}

func (psm *Manager) WaitReady(ctx context.Context) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if psm.Ready() {
			return nil
		}
		select {
		case <-psm.ctx.Done():
			return pubsub.ErrManagerClosed
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (psm *Manager) reconnectStateLocked() string {
	select {
	case <-psm.ctx.Done():
		return "closed"
	default:
	}
	if psm.readyLocked() {
		return "ready"
	}
	if psm.redisClient == nil {
		return "standalone"
	}
	if !psm.redisClient.IsConnected() {
		return "disconnected"
	}
	return "recovering_subscriptions"
}

func (psm *Manager) readyLocked() bool {
	if psm.redisClient == nil {
		return psm.failurePolicy == pubsub.AllowStandalone
	}
	return psm.redisClient.IsConnected() && len(psm.desired) == len(psm.subscribers)
}

func (psm *Manager) superviseSubscriptions() {
	defer psm.supervisorWG.Done()
	backoff := 100 * time.Millisecond
	for {
		timer := time.NewTimer(backoff + time.Duration(rand.Int63n(int64(backoff)+1)))
		select {
		case <-psm.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			psm.reconnectAttempts.Add(1)
			if err := psm.reconcileSubscriptions(); err != nil {
				psm.reconnectFailures.Add(1)
				psm.mu.Lock()
				psm.lastReconnectError = err.Error()
				psm.mu.Unlock()
				backoff *= 2
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
			} else {
				psm.reconnectSuccesses.Add(1)
				psm.mu.Lock()
				psm.lastReconnectError = ""
				psm.mu.Unlock()
				backoff = time.Second
			}
		}
	}
}

func (psm *Manager) reconcileSubscriptions() error {
	if psm.redisClient == nil || !psm.redisClient.IsConnected() {
		if psm.failurePolicy == pubsub.AllowStandalone {
			return nil
		}
		return fmt.Errorf("redis backend unavailable")
	}
	psm.mu.RLock()
	missing := make([]string, 0)
	for channel := range psm.desired {
		if _, active := psm.subscribers[channel]; !active {
			missing = append(missing, channel)
		}
	}
	psm.mu.RUnlock()
	if len(missing) > 0 {
		return psm.Subscribe(missing...)
	}
	return nil
}

func messagePartitionKey(message *pubsub.CrossNodeMessage) string {
	if message == nil {
		return ""
	}
	if message.UserID != "" {
		return "user:" + message.UserID
	}
	if message.RoomID != "" {
		return "room:" + message.RoomID
	}
	return "shard:" + message.ShardID
}

func (psm *Manager) startExecutor() {
	if psm.execPartitions <= 0 || psm.execQueueSize <= 0 {
		psm.execPartitions, psm.execQueueSize = 4, 256
	}
	psm.executor = pool.NewPartitionedExecutor(psm.execPartitions, psm.execQueueSize)
}
