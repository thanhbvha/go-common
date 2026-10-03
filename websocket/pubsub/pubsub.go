// Package pubsub provides a distributed publish-subscribe abstraction for the WebSocket server.
//
// It allows multiple WebSocket nodes to form a cluster and broadcast messages across
// the entire network. Built-in implementations include Redis (via pub/sub) and NATS (JetStream).
package pubsub

import (
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// Message types for cross-node communication.
const (
	MessageTypeChat         = "chat"
	MessageTypeChatRoom     = "chat_room"
	MessageTypeNotification = "notification"
	MessageTypeBroadcast    = "broadcast"
	MessageTypeUserJoin     = "user_join"
	MessageTypeUserLeave    = "user_leave"
	MessageTypeShardCreate  = "shard_create"
	MessageTypeShardDestroy = "shard_destroy"
	MessageTypeNodeStatus   = "node_status"
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

// GenerateNodeID generates a unique identifier for this server instance in the cluster.
func GenerateNodeID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp if crypto/rand is unavailable (extremely rare)
		return fmt.Sprintf("node_%d_%d", time.Now().Unix(), time.Now().UnixNano())
	}
	return fmt.Sprintf("node_%x", b)
}

// Manager defines the interface for cross-node coordination.
type Manager interface {
	RegisterHandler(messageType string, handler func(*CrossNodeMessage))
	Subscribe(channels ...string) error
	Unsubscribe(channels ...string) error
	Publish(channel string, message *CrossNodeMessage) error

	BroadcastMessage(shardID string, data map[string]interface{}) error
	BroadcastUserNotification(shardID, userID string, data map[string]interface{}) error
	BroadcastRoomMessage(shardID, roomID string, data map[string]interface{}) error
	BroadcastChatMessage(shardID string, userID string, data map[string]interface{}) error

	NotifyUserJoin(shardID, userID, clientIP string) error
	NotifyUserLeave(shardID, userID string) error
	NotifyShardCreate(shardID string) error
	NotifyShardDestroy(shardID string) error

	SendNodeStatus(data map[string]interface{}) error
	GetNodeID() string
	GetStats() map[string]interface{}
	Shutdown() error
}

var (
	globalManager Manager
	managerOnce   sync.Once
)

// SetGlobalManager allows setting a custom PubSub manager (e.g., Redis or NATS) before starting the server.
func SetGlobalManager(m Manager) {
	globalManager = m
}

// GetGlobalManager returns the singleton instance of the pubsub manager.
// If it hasn't been initialized via SetGlobalManager, it returns a NoopManager to allow single-node operation without external dependencies.
func GetGlobalManager() Manager {
	managerOnce.Do(func() {
		if globalManager == nil {
			globalManager = NewNoopManager()
		}
	})
	return globalManager
}

// NoopManager is a default implementation of Manager that does nothing.
// It is used when the WebSocket server is running in standalone (single-node) mode.
type NoopManager struct {
	nodeID string
}

// NewNoopManager creates a new NoopManager.
func NewNoopManager() *NoopManager {
	return &NoopManager{nodeID: GenerateNodeID()}
}

func (m *NoopManager) RegisterHandler(messageType string, handler func(*CrossNodeMessage)) {}
func (m *NoopManager) Subscribe(channels ...string) error                                   { return nil }
func (m *NoopManager) Unsubscribe(channels ...string) error                                 { return nil }
func (m *NoopManager) Publish(channel string, message *CrossNodeMessage) error              { return nil }
func (m *NoopManager) BroadcastMessage(shardID string, data map[string]interface{}) error   { return nil }
func (m *NoopManager) BroadcastUserNotification(shardID, userID string, data map[string]interface{}) error {
	return nil
}
func (m *NoopManager) BroadcastRoomMessage(shardID, roomID string, data map[string]interface{}) error {
	return nil
}
func (m *NoopManager) BroadcastChatMessage(shardID string, userID string, data map[string]interface{}) error {
	return nil
}
func (m *NoopManager) NotifyUserJoin(shardID, userID, clientIP string) error { return nil }
func (m *NoopManager) NotifyUserLeave(shardID, userID string) error          { return nil }
func (m *NoopManager) NotifyShardCreate(shardID string) error                { return nil }
func (m *NoopManager) NotifyShardDestroy(shardID string) error               { return nil }
func (m *NoopManager) SendNodeStatus(data map[string]interface{}) error      { return nil }
func (m *NoopManager) GetNodeID() string                                     { return m.nodeID }
func (m *NoopManager) GetStats() map[string]interface{}                      { return nil }
func (m *NoopManager) Shutdown() error                                       { return nil }
