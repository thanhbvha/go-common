package natspubsub

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/nats"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/websocket/internal/wsrecovery"
	"github.com/thanhbvha/go-common/websocket/pool"
	"github.com/thanhbvha/go-common/websocket/pubsub"
)

// Manager manages subscriptions and publication of messages across multiple instances in a cluster using NATS.
type Manager struct {
	natsClient         *nats.Client
	nodeID             string
	subscribers        map[string]*gonats.Subscription
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
}

var (
	globalManager *Manager
	managerOnce   sync.Once
)

// GetGlobalManager returns the default singleton Manager initialized with the default NATS client.
func GetGlobalManager() *Manager {
	managerOnce.Do(func() {
		globalManager = NewManager()
	})
	return globalManager
}

// NewManager instantiates a new Manager. If the default NATS client is not set,
// it gracefully falls back to standalone loopback mode to support zero-config standalone runs.
func NewManager() *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	client, _ := nats.LookupDefault()

	if client == nil {
		logger.WarnAsync("NATS default client not set. WebSocket PubSub will run in standalone loopback mode.")
	}

	manager := &Manager{
		natsClient:    client,
		nodeID:        pubsub.GenerateNodeID(),
		subscribers:   make(map[string]*gonats.Subscription),
		desired:       make(map[string]struct{}),
		handlers:      make(map[string]map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage)),
		registrations: make(map[pubsub.HandlerRegistration]string),
		ctx:           ctx,
		cancel:        cancel,
		executor:      pool.NewPartitionedExecutor(4, 256),
		failurePolicy: pubsub.AllowStandalone,
	}
	manager.supervisorWG.Add(1)
	go manager.superviseSubscriptions()
	return manager
}

// NewManagerWithClient instantiates a new Manager with a custom, user-provided NATS client.
func NewManagerWithClient(client *nats.Client) *Manager {
	if client == nil {
		return NewManagerWithClientAndPolicy(nil, pubsub.AllowStandalone)
	}
	return NewManagerWithClientAndPolicy(client, pubsub.StayUnready)
}

func NewManagerWithClientAndPolicy(client *nats.Client, policy pubsub.ClusterFailurePolicy) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	manager := &Manager{
		natsClient:    client,
		nodeID:        pubsub.GenerateNodeID(),
		subscribers:   make(map[string]*gonats.Subscription),
		desired:       make(map[string]struct{}),
		handlers:      make(map[string]map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage)),
		registrations: make(map[pubsub.HandlerRegistration]string),
		ctx:           ctx,
		cancel:        cancel,
		executor:      pool.NewPartitionedExecutor(4, 256),
		failurePolicy: policy,
	}
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

// Subscribe listens to the specified NATS channels (subjects) and processes incoming messages.
func (psm *Manager) Subscribe(channels ...string) error {
	if psm.natsClient == nil {
		if psm.failurePolicy == pubsub.AllowStandalone {
			return nil
		}
		return fmt.Errorf("nats client is not configured")
	}
	if !psm.natsClient.IsConnected() {
		psm.mu.Lock()
		for _, channel := range channels {
			psm.desired[channel] = struct{}{}
		}
		psm.mu.Unlock()
		return fmt.Errorf("nats client is not connected")
	}

	psm.mu.Lock()
	defer psm.mu.Unlock()

	nc := psm.natsClient.Native()
	if nc == nil {
		return fmt.Errorf("nats client is not connected")
	}

	for _, channel := range channels {
		psm.desired[channel] = struct{}{}
		if _, exists := psm.subscribers[channel]; exists {
			continue // Already subscribed
		}

		// In NATS, we supply a callback for the subscription.
		// We capture the channel variable to ensure correct closures.
		subChan := channel
		sub, err := nc.Subscribe(subChan, func(msg *gonats.Msg) {
			defer panicrecovery.Catch(wsrecovery.Handler(wsrecovery.Metadata{
				Component: "pubsub.nats_subscription_callback",
				Operation: "receive",
				Channel:   subChan,
				NodeID:    psm.nodeID,
			}))
			psm.processMessage(subChan, msg.Data)
		})
		if err != nil {
			return fmt.Errorf("failed to subscribe to channel: %s, err: %v", channel, err)
		}

		psm.subscribers[channel] = sub
	}

	logger.InfoAsync("Subscribed to NATS channels", "node_id", psm.nodeID, "channels", channels)
	return nil
}

// Unsubscribe stops listening to and closes the specified channels.
func (psm *Manager) Unsubscribe(channels ...string) error {
	if psm.natsClient == nil {
		return nil // No-op in standalone/test mode
	}

	psm.mu.Lock()
	defer psm.mu.Unlock()

	for _, channel := range channels {
		delete(psm.desired, channel)
		sub, exists := psm.subscribers[channel]
		if !exists {
			continue
		}

		if err := sub.Unsubscribe(); err != nil {
			logger.ErrorAsync("Failed to unsubscribe from NATS channel", "channel", channel, "error", err)
			return err
		}

		delete(psm.subscribers, channel)
	}

	logger.InfoAsync("Unsubscribed from NATS channels", "node_id", psm.nodeID, "channels", channels)
	return nil
}

// processMessage decodes and directs a single payload to its corresponding handler.
func (psm *Manager) processMessage(channel string, payload []byte) {
	if !stdjson.Valid(payload) {
		return
	}
	var message pubsub.CrossNodeMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		logger.ErrorAsync("Failed to unmarshal cross-node message", "error", err, "channel", channel, "payloadBytes", len(payload))
		return
	}

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
		handler := handler
		_ = psm.executor.Submit(psm.ctx, messagePartitionKey(&message), func(context.Context) error {
			defer panicrecovery.Catch(wsrecovery.Handler(psm.recoveryMetadata("pubsub.nats_message_handler", channel, &message)))
			handler(&message)
			return nil
		})
	}
}

// Publish transmits a pubsub.CrossNodeMessage to the specified NATS channel (subject).
func (psm *Manager) Publish(channel string, message *pubsub.CrossNodeMessage) error {
	if message.ID == "" {
		message.ID = pubsub.GenerateNodeID()
	}
	if message.Version == 0 {
		message.Version = 1
	}
	message.NodeID = psm.nodeID
	message.Timestamp = time.Now()

	if psm.natsClient == nil {
		if psm.failurePolicy != pubsub.AllowStandalone {
			return fmt.Errorf("nats cluster backend unavailable")
		}
		// Standalone/in-memory loopback mode
		psm.mu.RLock()
		handlers := copyHandlers(psm.handlers[message.Type])
		psm.mu.RUnlock()
		for _, handler := range handlers {
			handler := handler
			_ = psm.executor.Submit(psm.ctx, messagePartitionKey(message), func(context.Context) error {
				defer panicrecovery.Catch(wsrecovery.Handler(psm.recoveryMetadata("pubsub.nats_loopback_handler", channel, message)))
				handler(message)
				return nil
			})
		}
		return nil
	}
	if !psm.natsClient.IsConnected() {
		return fmt.Errorf("nats client is not connected")
	}

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	nc := psm.natsClient.Native()
	if nc == nil {
		return fmt.Errorf("nats client is not connected")
	}

	return nc.Publish(channel, data)
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
		"mode":                 "nats",
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
	logger.InfoAsync("Shutting down NATS pub/sub manager", "node_id", psm.nodeID)

	psm.cancel()

	psm.mu.Lock()

	for channel, sub := range psm.subscribers {
		if err := sub.Unsubscribe(); err != nil {
			logger.ErrorAsync("Error closing NATS subscription", "channel", channel, "error", err)
		}
	}

	psm.subscribers = make(map[string]*gonats.Subscription)
	psm.desired = make(map[string]struct{})
	psm.handlers = make(map[string]map[pubsub.HandlerRegistration]func(*pubsub.CrossNodeMessage))
	psm.registrations = make(map[pubsub.HandlerRegistration]string)
	psm.mu.Unlock()
	psm.supervisorWG.Wait()
	_ = psm.executor.Shutdown(context.Background())

	logger.InfoAsync("NATS pub/sub manager shutdown complete", "node_id", psm.nodeID)
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
	if psm.natsClient == nil {
		return "standalone"
	}
	if !psm.natsClient.IsConnected() {
		return "disconnected"
	}
	return "recovering_subscriptions"
}

func (psm *Manager) readyLocked() bool {
	if psm.natsClient == nil {
		return psm.failurePolicy == pubsub.AllowStandalone
	}
	return psm.natsClient.IsConnected() && len(psm.desired) == len(psm.subscribers)
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
	if psm.natsClient == nil || !psm.natsClient.IsConnected() {
		if psm.failurePolicy == pubsub.AllowStandalone {
			return nil
		}
		return fmt.Errorf("nats backend unavailable")
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
