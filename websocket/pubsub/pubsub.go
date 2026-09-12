// Package pubsub provides a distributed publish-subscribe abstraction for the WebSocket server.
//
// It allows multiple WebSocket nodes to form a cluster and broadcast messages across
// the entire network. Built-in implementations include Redis (via pub/sub) and NATS (JetStream).
package pubsub

import (
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"time"
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

// generateNodeID generates a unique identifier for this server instance in the cluster.
func generateNodeID() string {
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

const (
	AdapterRedis = "redis"
	AdapterNATS  = "nats"
)

var (
	globalManager   Manager
	managerOnce     sync.Once
	initAdapterType string
)

// NewManager creates a new pubsub manager instance based on the provided adapter type.
func NewManager(adapterType string) Manager {
	switch strings.ToLower(adapterType) {
	case AdapterNATS:
		return NewNATSPubSubManager()
	case AdapterRedis:
		fallthrough
	default:
		return NewRedisPubSubManager()
	}
}

// GetGlobalManager returns the singleton instance of the specified pubsub manager.
// If it hasn't been initialized, it initializes it with the given adapterType.
func GetGlobalManager(adapterType string) Manager {
	managerOnce.Do(func() {
		initAdapterType = adapterType
		globalManager = NewManager(adapterType)
	})
	if adapterType != initAdapterType {
		// Cannot log with the logger package here (import cycle risk), use fmt.
		fmt.Printf("[pubsub] WARN: GetGlobalManager called with adapter %q but singleton was already initialized with %q — ignoring new adapter\n", adapterType, initAdapterType)
	}
	return globalManager
}

// GetDefaultManager returns the already-initialized global manager.
// Useful for calls where the adapter type is already determined at startup.
// It returns nil if GetGlobalManager hasn't been called yet.
func GetDefaultManager() Manager {
	return globalManager
}
