// Package ws contains the core WebSocket sharding, connection lifecycle, and manager logic.
package ws

import (
	"encoding/json"
	"sync"

	"github.com/thanhbvha/go-common/logger"
)

// UserInfo represents the minimal user details stored in a shard.
type UserInfo struct {
	ShardID  string `json:"shard_id"`
	UserID   string `json:"user_id"`
	ClientIP string `json:"client_ip"`
}

// IncomingMessage represents the envelope for incoming payloads from a client.
type IncomingMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// OutgoingMessage represents the envelope for outgoing payloads to a client.
type OutgoingMessage struct {
	Type      string      `json:"type"`
	Data      interface{} `json:"data"`
	RequestID string      `json:"request_id,omitempty"`
}

// JobHandler defines a callback function that handles a specific incoming message type.
type JobHandler func(conn *Connection, msg IncomingMessage) error

var (
	handlersMu sync.RWMutex
	handlers   = map[string]JobHandler{}
)

// RegisterHandler binds an event type to a specific JobHandler callback in a thread-safe manner.
func RegisterHandler(eventType string, handler JobHandler) {
	handlersMu.Lock()
	defer handlersMu.Unlock()
	handlers[eventType] = handler
}

// EventMessage represents a raw incoming payload coupled with its source Connection.
type EventMessage struct {
	Conn *Connection
	Raw  []byte
}

// HandleEvent unmarshals the raw message envelope and dispatches the task to the registered handler asynchronously.
func HandleEvent(ev *EventMessage) {
	if ev == nil || ev.Conn == nil {
		return
	}

	var inc IncomingMessage
	if err := json.Unmarshal(ev.Raw, &inc); err != nil {
		logger.ErrorAsync("Failed to unmarshal incoming message", "error", err, "shard", ev.Conn.shard.name, "userID", ev.Conn.userID, "raw", string(ev.Raw))
		return
	}

	if inc.Type == "" {
		logger.ErrorAsync("No event type specified in incoming message", "shard", ev.Conn.shard.name, "userID", ev.Conn.userID)
		return
	}

	handlersMu.RLock()
	handler, ok := handlers[inc.Type]
	handlersMu.RUnlock()

	if ok {
		// Execute handler in a separate goroutine to avoid blocking the shard processor loop
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.ErrorAsync("Panic in event handler execution", "error", r, "eventType", inc.Type, "userID", ev.Conn.userID)
				}
			}()

			if err := handler(ev.Conn, inc); err != nil {
				logger.ErrorAsync("Error executing event handler", "error", err, "eventType", inc.Type, "userID", ev.Conn.userID)
			}
		}()
	} else {
		logger.ErrorAsync("No handler registered for event type", "eventType", inc.Type, "shard", ev.Conn.shard.name, "userID", ev.Conn.userID)
	}
}
