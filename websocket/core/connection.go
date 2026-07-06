// package core contains the core WebSocket hub, connection lifecycle, and manager logic.
package core

import (
	"context"
	"github.com/goccy/go-json"
	"io"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/websocket/pool"
)

const (
	// Connection constants for high-performance tuning
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 1024 // 1024 bytes
)

// Conn defines a generic WebSocket connection interface to support multiple HTTP frameworks.
type Conn interface {
	// ReadMessage reads a single message from the connection.
	ReadMessage() (messageType int, p []byte, err error)
	// WriteMessage writes a single message to the connection.
	WriteMessage(messageType int, data []byte) error
	// NextWriter returns a writer for the next message to send.
	NextWriter(messageType int) (io.WriteCloser, error)
	// SetReadLimit sets the maximum size in bytes for a message read from the peer.
	SetReadLimit(limit int64)
	// SetReadDeadline sets the read deadline on the underlying network connection.
	SetReadDeadline(t time.Time) error
	// SetWriteDeadline sets the write deadline on the underlying network connection.
	SetWriteDeadline(t time.Time) error
	// SetPongHandler sets the handler for pong messages received from the peer.
	SetPongHandler(h func(string) error)
	// Close closes the underlying network connection.
	Close() error
}

// Connection represents an active WebSocket client session with thread-safe send channels and metadata.
type Connection struct {
	conn      Conn
	send      chan []byte
	shard     *Shard
	userID    string
	shardID   string
	clientIP  string
	lastPing  time.Time
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	closed    bool
	nodeID    string
	requestID string
}

// NewConnection instantiates a Connection session wrapper.
func NewConnection(conn Conn, shard *Shard, userID, shardID, clientIP, nodeID, requestID string) *Connection {
	if conn == nil {
		logger.ErrorAsync("CRITICAL: WebSocket connection is nil in NewConnection!")
		return nil
	}

	if shard == nil {
		logger.ErrorAsync("CRITICAL: Shard is nil in NewConnection!")
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	sendChan := pool.GetGlobalConnectionPool().GetChannel()

	return &Connection{
		conn:      conn,
		send:      sendChan,
		shard:     shard,
		userID:    userID,
		shardID:   shardID,
		clientIP:  clientIP,
		nodeID:    nodeID,
		requestID: requestID,
		lastPing:  time.Now(),
		ctx:       ctx,
		cancel:    cancel,
		closed:    false,
	}
}

// Close terminates the connection session and cleans up allocated memory pool channels.
func (c *Connection) Close(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	logger.InfoAsync("Closing connection", "reason", reason, "userID", c.userID, "shardID", c.shardID, "clientIP", c.clientIP, "requestID", c.requestID)

	c.closed = true
	c.cancel()

	// Return channel to pool before closing
	pool.GetGlobalConnectionPool().PutChannel(c.send)

	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// IsClosed reports whether this Connection session has been marked as closed.
func (c *Connection) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// readPump pumps messages from the websocket connection to the shard.
// The application runs readPump in a per-connection goroutine.
func (c *Connection) readPump() {
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorAsync("Panic in readPump", "error", r, "userID", c.userID, "clientIP", c.clientIP, "shardID", c.shardID)
		}
		c.shard.unregister <- c
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		c.mu.Lock()
		c.lastPing = time.Now()
		c.mu.Unlock()
		return nil
	})

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			buffer := pool.GetGlobalBufferPool().Get()

			if c.conn == nil {
				logger.ErrorAsync("readPump CRITICAL: c.conn is nil before read", "userID", c.userID)
				return
			}

			mt, message, err := c.conn.ReadMessage()
			if err != nil {
				pool.GetGlobalBufferPool().Put(buffer)
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					logger.ErrorAsync("WebSocket read error (unexpected close)", "error", err, "userID", c.userID, "clientIP", c.clientIP)
				}
				return
			}

			if mt != websocket.TextMessage && mt != websocket.BinaryMessage {
				pool.GetGlobalBufferPool().Put(buffer)
				continue
			}

			buffer = append(buffer, message...)
			messageCopy := make([]byte, len(buffer))
			copy(messageCopy, buffer)

			pool.GetGlobalBufferPool().Put(buffer)

			workerPool := pool.GetGlobalPool()
			taskSubmitted := workerPool.Submit(func() {
				ev := &EventMessage{
					Conn: c,
					Raw:  messageCopy,
				}
				select {
				case c.shard.incomingMessage <- ev:
				default:
					logger.WarnAsync("Incoming message channel full, dropping message", "shardID", c.shardID, "userID", c.userID)
				}
			})

			if !taskSubmitted {
				logger.WarnAsync("Worker pool full, dropping message", "shardID", c.shardID, "userID", c.userID)
			}
		}
	}
}

// writePump pumps messages from the send channel to the websocket connection.
// A goroutine running writePump is started for each connection.
func (c *Connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorAsync("Panic in writePump", "error", r, "userID", c.userID, "clientIP", c.clientIP)
		}
		ticker.Stop()
		c.Close("writePump finished")
	}()

	for {
		select {
		case <-c.ctx.Done():
			return
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				logger.ErrorAsync("Error getting next writer", "error", err, "userID", c.userID)
				return
			}
			_, _ = w.Write(message)

			// Batch additional queued messages together for performance optimization
			n := len(c.send)
			for i := 0; i < n; i++ {
				_, _ = w.Write([]byte{'\n'})
				_, _ = w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				logger.ErrorAsync("Error closing writer", "error", err, "userID", c.userID)
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				logger.ErrorAsync("Error sending ping message", "error", err, "userID", c.userID)
				return
			}
		}
	}
}

// SendJSON marshals interface data to JSON and transmits it to the client.
// It returns true if scheduling was successful.
func (c *Connection) SendJSON(data interface{}) bool {
	message, err := json.Marshal(data)
	if err != nil {
		logger.ErrorAsync("Error marshalling JSON", "error", err, "userID", c.userID)
		return false
	}
	return c.Send(message)
}

// Send schedules a raw byte slice message for delivery. Returns false if closed or send buffer is full.
func (c *Connection) Send(message []byte) bool {
	if c.IsClosed() {
		return false
	}

	select {
	case c.send <- message:
		return true
	default:
		logger.WarnAsync("Send channel full, dropping message", "userID", c.userID)
		return false
	}
}

// GetUserID returns the User ID associated with this Connection session.
func (c *Connection) GetUserID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.userID
}

// GetShardID returns the Shard ID associated with this Connection session.
func (c *Connection) GetShardID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.shardID
}

// GetClientIP returns the remote client IP address.
func (c *Connection) GetClientIP() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clientIP
}

// GetShard returns the Shard manager this Connection is registered to.
func (c *Connection) GetShard() *Shard {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.shard
}

// GetRequestID returns the trace request ID associated with the upgrading request.
func (c *Connection) GetRequestID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.requestID
}
