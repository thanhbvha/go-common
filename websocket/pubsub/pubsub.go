// Package pubsub handles multi-node coordination using Redis pub/sub to route messages in a clustered environment.
package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/redis"
)

// Message types for cross-node communication.
const (
	MessageTypeChat          = "chat"
	MessageTypeChatRoom      = "chat_room"
	MessageTypeNotification  = "notification"
	MessageTypeBroadcast     = "broadcast"
	MessageTypeUserJoin      = "user_join"
	MessageTypeUserLeave     = "user_leave"
	MessageTypeShardCreate   = "shard_create"
	MessageTypeShardDestroy  = "shard_destroy"
	MessageTypeNodeStatus    = "node_status"
)

// CrossNodeMessage represents the payload wrapper sent between different cluster nodes.
type CrossNodeMessage struct {
	Type      string                 `json:"type"`
	NodeID    string                 `json:"node_id"`
	ShardID   string                 `json:"shard_id,omitempty"`
	UserID    string                 `json:"user_id,omitempty"`
	RoomID    string                 `json:"room_id,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
}

// PubSubManager manages subscriptions and publication of messages across multiple instances in a cluster.
type PubSubManager struct {
	redisClient *redis.Client
	nodeID      string
	subscribers map[string]*goredis.PubSub
	handlers    map[string]func(*CrossNodeMessage)
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
}

var (
	globalPubSub *PubSubManager
	pubsubOnce   sync.Once
)

// GetGlobalPubSub returns the default singleton PubSubManager initialized with the default Redis client.
func GetGlobalPubSub() *PubSubManager {
	pubsubOnce.Do(func() {
		globalPubSub = NewPubSubManager()
	})
	return globalPubSub
}

// NewPubSubManager instantiates a new PubSubManager. If the default Redis client is not set,
// it gracefully falls back to standalone loopback mode to support zero-config standalone runs.
func NewPubSubManager() *PubSubManager {
	ctx, cancel := context.WithCancel(context.Background())

	var client *redis.Client
	func() {
		defer func() {
			_ = recover() // Catch panic if redis.Default is not set
		}()
		client = redis.Default()
	}()

	if client == nil {
		logger.WarnAsync("Redis default client not set. WebSocket PubSub will run in standalone loopback mode.")
	}

	return &PubSubManager{
		redisClient: client,
		nodeID:      generateNodeID(),
		subscribers: make(map[string]*goredis.PubSub),
		handlers:    make(map[string]func(*CrossNodeMessage)),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// NewPubSubManagerWithClient instantiates a new PubSubManager with a custom, user-provided Redis client.
func NewPubSubManagerWithClient(client *redis.Client) *PubSubManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &PubSubManager{
		redisClient: client,
		nodeID:      generateNodeID(),
		subscribers: make(map[string]*goredis.PubSub),
		handlers:    make(map[string]func(*CrossNodeMessage)),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// generateNodeID generates a unique identifier for this server instance in the cluster.
func generateNodeID() string {
	return fmt.Sprintf("node_%d_%d", time.Now().Unix(), time.Now().UnixNano()%1000000)
}

// RegisterHandler binds a callback handler to a specific cross-node message type.
func (psm *PubSubManager) RegisterHandler(messageType string, handler func(*CrossNodeMessage)) {
	psm.mu.Lock()
	defer psm.mu.Unlock()
	psm.handlers[messageType] = handler
}

// Subscribe listens to the specified Redis channels and processes incoming messages.
func (psm *PubSubManager) Subscribe(channels ...string) error {
	if psm.redisClient == nil {
		return nil // No-op in standalone/test mode
	}

	psm.mu.Lock()
	defer psm.mu.Unlock()

	for _, channel := range channels {
		if _, exists := psm.subscribers[channel]; exists {
			continue // Already subscribed
		}

		pubsub, err := psm.redisClient.Subscribe(psm.ctx, channel)
		if err != nil || pubsub == nil {
			return fmt.Errorf("failed to subscribe to channel: %s, err: %v", channel, err)
		}

		psm.subscribers[channel] = pubsub

		go psm.handleChannelMessages(channel, pubsub)
	}

	logger.InfoAsync("Subscribed to channels", "node_id", psm.nodeID, "channels", channels)
	return nil
}

// Unsubscribe stops listening to and closes the specified channels.
func (psm *PubSubManager) Unsubscribe(channels ...string) error {
	if psm.redisClient == nil {
		return nil // No-op in standalone/test mode
	}

	psm.mu.Lock()
	defer psm.mu.Unlock()

	for _, channel := range channels {
		pubsub, exists := psm.subscribers[channel]
		if !exists {
			continue
		}

		if err := pubsub.Unsubscribe(psm.ctx, channel); err != nil {
			logger.ErrorAsync("Failed to unsubscribe from channel", "channel", channel, "error", err)
			return err
		}

		if err := pubsub.Close(); err != nil {
			logger.ErrorAsync("Failed to close pubsub", "channel", channel, "error", err)
			return err
		}

		delete(psm.subscribers, channel)
	}

	logger.InfoAsync("Unsubscribed from channels", "node_id", psm.nodeID, "channels", channels)
	return nil
}

// handleChannelMessages listens to the channel payload and dispatches events.
func (psm *PubSubManager) handleChannelMessages(channel string, pubsub *goredis.PubSub) {
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorAsync("Panic in channel message handler", "error", r, "channel", channel, "node_id", psm.nodeID)
		}
	}()

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

// processMessage decodes and directs a single payload to its corresponding handler.
func (psm *PubSubManager) processMessage(channel, payload string) {
	var message CrossNodeMessage
	if err := json.Unmarshal([]byte(payload), &message); err != nil {
		logger.ErrorAsync("Failed to unmarshal cross-node message", "error", err, "payload", payload)
		return
	}

	// Skip messages generated by this node
	if message.NodeID == psm.nodeID {
		return
	}

	psm.mu.RLock()
	handler, exists := psm.handlers[message.Type]
	psm.mu.RUnlock()

	if !exists {
		logger.WarnAsync("No handler for message type", "type", message.Type, "channel", channel)
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.ErrorAsync("Panic in message handler", "error", r, "type", message.Type, "node_id", psm.nodeID)
			}
		}()

		handler(&message)
	}()
}

// Publish transmits a CrossNodeMessage to the specified Redis channel.
func (psm *PubSubManager) Publish(channel string, message *CrossNodeMessage) error {
	message.NodeID = psm.nodeID
	message.Timestamp = time.Now()

	if psm.redisClient == nil {
		// Standalone/in-memory loopback mode
		psm.mu.RLock()
		handler, exists := psm.handlers[message.Type]
		psm.mu.RUnlock()
		if exists {
			go func() {
				defer func() {
					if r := recover(); r != nil {
						logger.ErrorAsync("Panic in loopback handler", "error", r)
					}
				}()
				handler(message)
			}()
		}
		return nil
	}

	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	return psm.redisClient.Publish(psm.ctx, channel, data)
}

// BroadcastMessage sends a broadcast event to all subscribers of a specific shard ID.
func (psm *PubSubManager) BroadcastMessage(shardID string, data map[string]interface{}) error {
	message := &CrossNodeMessage{
		Type:    MessageTypeBroadcast,
		ShardID: shardID,
		Data:    data,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// BroadcastUserNotification sends a notification event to a specific user across different nodes.
func (psm *PubSubManager) BroadcastUserNotification(shardID, userID string, data map[string]interface{}) error {
	message := &CrossNodeMessage{
		Type:    MessageTypeNotification,
		ShardID: shardID,
		UserID:  userID,
		Data:    data,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// BroadcastRoomMessage distributes a chat room message to all room members across nodes.
func (psm *PubSubManager) BroadcastRoomMessage(shardID, roomID string, data map[string]interface{}) error {
	message := &CrossNodeMessage{
		Type:    MessageTypeChatRoom,
		ShardID: shardID,
		RoomID:  roomID,
		Data:    data,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// BroadcastChatMessage sends a direct chat message to a user on any cluster node.
func (psm *PubSubManager) BroadcastChatMessage(shardID string, userID string, data map[string]interface{}) error {
	message := &CrossNodeMessage{
		Type:    MessageTypeChat,
		ShardID: shardID,
		UserID:  userID,
		Data:    data,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// NotifyUserJoin sends a join event to other nodes when a user connects.
func (psm *PubSubManager) NotifyUserJoin(shardID, userID, clientIP string) error {
	message := &CrossNodeMessage{
		Type:    MessageTypeUserJoin,
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
func (psm *PubSubManager) NotifyUserLeave(shardID, userID string) error {
	message := &CrossNodeMessage{
		Type:    MessageTypeUserLeave,
		ShardID: shardID,
		UserID:  userID,
	}

	channel := fmt.Sprintf("shard:%s", shardID)
	return psm.Publish(channel, message)
}

// NotifyShardCreate broadcasts a shard creation notice globally to system subscribers.
func (psm *PubSubManager) NotifyShardCreate(shardID string) error {
	message := &CrossNodeMessage{
		Type:    MessageTypeShardCreate,
		ShardID: shardID,
	}

	return psm.Publish("system", message)
}

// NotifyShardDestroy broadcasts a shard destruction notice globally.
func (psm *PubSubManager) NotifyShardDestroy(shardID string) error {
	message := &CrossNodeMessage{
		Type:    MessageTypeShardDestroy,
		ShardID: shardID,
	}

	return psm.Publish("system", message)
}

// SendNodeStatus publishes node status metrics to the system control topic.
func (psm *PubSubManager) SendNodeStatus(data map[string]interface{}) error {
	message := &CrossNodeMessage{
		Type: MessageTypeNodeStatus,
		Data: data,
	}

	return psm.Publish("system", message)
}

// GetNodeID returns the unique identifier of the current cluster node.
func (psm *PubSubManager) GetNodeID() string {
	return psm.nodeID
}

// GetStats returns metrics and current status of the pub/sub connection.
func (psm *PubSubManager) GetStats() map[string]interface{} {
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
		"node_id":             psm.nodeID,
		"subscribed_channels": len(psm.subscribers),
		"channels":            channels,
		"registered_handlers": len(psm.handlers),
		"handlers":            handlers,
		"mode":                "standalone/loopback",
	}
}

// Shutdown gracefully unsubscribes from all channels and releases the connections.
func (psm *PubSubManager) Shutdown() error {
	logger.InfoAsync("Shutting down pub/sub manager", "node_id", psm.nodeID)

	psm.cancel()

	psm.mu.Lock()
	defer psm.mu.Unlock()

	for channel, pubsub := range psm.subscribers {
		if err := pubsub.Close(); err != nil {
			logger.ErrorAsync("Error closing pubsub", "channel", channel, "error", err)
		}
	}

	psm.subscribers = make(map[string]*goredis.PubSub)
	psm.handlers = make(map[string]func(*CrossNodeMessage))

	logger.InfoAsync("Pub/sub manager shutdown complete", "node_id", psm.nodeID)
	return nil
}
