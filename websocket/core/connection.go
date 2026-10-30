// package core contains the core WebSocket hub, connection lifecycle, and manager logic.
package core

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"
	"github.com/thanhbvha/go-common/logger"
	panicrecovery "github.com/thanhbvha/go-common/recovery"
	"github.com/thanhbvha/go-common/websocket/internal/wsrecovery"
	"github.com/thanhbvha/go-common/websocket/limiter"
	"github.com/thanhbvha/go-common/websocket/routing"
)

const (
	// Connection constants for high-performance tuning
	writeWait            = 10 * time.Second
	pongWait             = 60 * time.Second
	pingPeriod           = (pongWait * 9) / 10
	maxMessageSize       = 1024 // 1024 bytes
	defaultSendQueueSize = 256
	maxPendingHandlers   = 64
)

var (
	nextConnectionID    atomic.Uint64
	ErrConnectionClosed = errors.New("websocket connection closed")
	ErrSendQueueFull    = errors.New("websocket send queue full")
)

// Conn defines a generic WebSocket connection interface to support multiple HTTP frameworks.
type Conn interface {
	// ReadMessage reads a single message from the connection.
	ReadMessage() (messageType int, p []byte, err error)
	// WriteMessage writes a single message to the connection.
	WriteMessage(messageType int, data []byte) error
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
	conn              Conn
	send              chan []byte
	shard             *Shard
	userID            string
	shardID           string
	clientIP          string
	lastPing          time.Time
	mu                sync.RWMutex
	lifecycleMu       sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
	closeOnce         sync.Once
	runOnce           sync.Once
	unregisterOnce    sync.Once
	pumps             sync.WaitGroup
	writerDone        chan struct{}
	done              chan struct{}
	nodeID            string
	requestID         string
	connectionID      string
	config            *ManagerConfig
	messageLimiter    *limiter.RateLimiter
	byteLimiter       *limiter.RateLimiter
	pendingHandlers   atomic.Int32
	malformedMessages atomic.Uint64
	inboundDrops      atomic.Uint64
	consecutiveDrops  atomic.Int32
	started           atomic.Bool
	identity          Identity
	roomsMu           sync.Mutex
	rooms             map[string]struct{}
	quotaKeys         []quotaKey
	location          *routing.Location
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
	config := &shard.config

	return &Connection{
		conn:           conn,
		send:           make(chan []byte, config.SendQueueSize),
		shard:          shard,
		userID:         userID,
		shardID:        shardID,
		clientIP:       clientIP,
		nodeID:         nodeID,
		requestID:      requestID,
		connectionID:   strconv.FormatUint(nextConnectionID.Add(1), 10),
		config:         config,
		identity:       Identity{UserID: userID},
		rooms:          make(map[string]struct{}),
		messageLimiter: limiter.NewRateLimiter(config.MessagesPerSecond, config.MessageBurst),
		byteLimiter:    limiter.NewRateLimiter(config.BytesPerSecond, config.ByteBurst),
		lastPing:       time.Now(),
		ctx:            ctx,
		cancel:         cancel,
		writerDone:     make(chan struct{}),
		done:           make(chan struct{}),
	}
}

func (c *Connection) setIdentity(identity Identity) { c.identity = identity }
func (c *Connection) Identity() Identity            { return c.identity }

func (c *Connection) addRoom(roomID string) bool {
	c.roomsMu.Lock()
	defer c.roomsMu.Unlock()
	if _, ok := c.rooms[roomID]; ok {
		return true
	}
	if len(c.rooms) >= c.config.MaxRoomsPerConnection {
		return false
	}
	c.rooms[roomID] = struct{}{}
	return true
}
func (c *Connection) removeRoom(roomID string) {
	c.roomsMu.Lock()
	delete(c.rooms, roomID)
	c.roomsMu.Unlock()
}

// Close signals connection shutdown. It is idempotent and deliberately does
// not wait for the read/write pumps, so it is safe to call from either pump.
func (c *Connection) Close(reason string) {
	c.closeOnce.Do(func() {
		if !c.config.DisableLifecycleLogging {
			logLifecycle("Closing connection", "reason", reason, "userID", c.userID, "shardID", c.shardID, "clientIP", c.clientIP, "requestID", c.requestID)
		}
		c.lifecycleMu.Lock()
		c.cancel()
		c.lifecycleMu.Unlock()
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

// IsClosed reports whether this Connection session has been marked as closed.
func (c *Connection) IsClosed() bool {
	select {
	case <-c.ctx.Done():
		return true
	default:
		return false
	}
}

// Run starts exactly one read pump and one write pump, then blocks until both
// have exited and the connection has been submitted for unregistration.
func (c *Connection) Run() {
	c.runOnce.Do(c.start)
	<-c.done
}

func (c *Connection) start() {
	c.started.Store(true)
	c.pumps.Add(2)
	go func() {
		defer c.pumps.Done()
		defer c.Close("readPump finished")
		c.readPump()
	}()
	go func() {
		defer c.pumps.Done()
		defer close(c.writerDone)
		defer c.Close("writePump finished")
		c.writePump()
	}()
	go func() {
		c.pumps.Wait()
		c.unregisterOnce.Do(func() {
			c.shard.removeConnection(c, "Connection lifecycle finished")
		})
		close(c.done)
	}()
}

func (c *Connection) hasStarted() bool { return c.started.Load() }

// Wait blocks until the complete connection lifecycle has finished or ctx is
// canceled. Run must have been called for lifecycle completion to occur.
func (c *Connection) Wait(ctx context.Context) error {
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// CloseAndWait signals shutdown and waits for lifecycle completion.
func (c *Connection) CloseAndWait(ctx context.Context, reason string) error {
	c.Close(reason)
	return c.Wait(ctx)
}

// readPump pumps messages from the websocket connection to the shard.
// The application runs readPump in a per-connection goroutine.
func (c *Connection) readPump() {
	defer panicrecovery.Catch(wsrecovery.Handler(c.recoveryMetadata("core.read_pump")))

	c.conn.SetReadLimit(c.config.MaxMessageBytes)
	if err := c.conn.SetReadDeadline(time.Now().Add(c.config.PongWait)); err != nil {
		logger.ErrorAsync("Failed to set WebSocket read deadline", "error", err, "userID", c.userID)
		return
	}
	c.conn.SetPongHandler(func(string) error {
		if err := c.conn.SetReadDeadline(time.Now().Add(c.config.PongWait)); err != nil {
			return err
		}
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
			if c.conn == nil {
				logger.ErrorAsync("readPump CRITICAL: c.conn is nil before read", "userID", c.userID)
				return
			}

			mt, message, err := c.conn.ReadMessage()
			if err != nil {
				if isUnexpectedReadClose(err) {
					logger.ErrorAsync("WebSocket read error (unexpected close)", "error", err, "userID", c.userID, "clientIP", c.clientIP)
				}
				return
			}

			if mt != websocket.TextMessage && mt != websocket.BinaryMessage {
				continue
			}

			c.shard.observer().InboundMessage(len(message))
			if !c.messageLimiter.Allow() || !c.byteLimiter.AllowN(int64(len(message))) {
				c.inboundDrops.Add(1)
				continue
			}
			ev := &EventMessage{Conn: c, Raw: bytes.Clone(message), queuedAt: time.Now()}
			select {
			case c.shard.incomingMessage <- ev:
			default:
				c.inboundDrops.Add(1)
			}
		}
	}
}

func isUnexpectedReadClose(err error) bool {
	var closeError *websocket.CloseError
	if !errors.As(err, &closeError) {
		return true
	}
	return closeError.Code != websocket.CloseNormalClosure && closeError.Code != websocket.CloseGoingAway
}

func (c *Connection) tryAcquireHandler() bool {
	for {
		current := c.pendingHandlers.Load()
		if current >= c.config.MaxPendingHandlers {
			c.inboundDrops.Add(1)
			return false
		}
		if c.pendingHandlers.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (c *Connection) releaseHandler() { c.pendingHandlers.Add(-1) }

// writePump pumps messages from the send channel to the websocket connection.
// A goroutine running writePump is started for each connection.
func (c *Connection) writePump() {
	ticker := time.NewTicker(c.config.PingPeriod)
	defer ticker.Stop()
	defer panicrecovery.Catch(wsrecovery.Handler(c.recoveryMetadata("core.write_pump")))

	for {
		select {
		case <-c.ctx.Done():
			return
		case message := <-c.send:
			if err := c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteWait)); err != nil {
				logger.ErrorAsync("Failed to set WebSocket write deadline", "error", err, "userID", c.userID)
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				logger.ErrorAsync("Error writing WebSocket message", "error", err, "userID", c.userID)
				return
			}
			c.shard.observer().OutboundMessage(len(message))

		case <-ticker.C:
			if err := c.conn.SetWriteDeadline(time.Now().Add(c.config.WriteWait)); err != nil {
				logger.ErrorAsync("Failed to set WebSocket ping deadline", "error", err, "userID", c.userID)
				return
			}
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				logger.ErrorAsync("Error sending ping message", "error", err, "userID", c.userID)
				return
			}
		}
	}
}

func (c *Connection) recoveryMetadata(component string) wsrecovery.Metadata {
	return wsrecovery.Metadata{
		Component: component,
		UserID:    c.userID,
		ClientIP:  c.clientIP,
		RequestID: c.requestID,
		ShardID:   c.shardID,
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
	return c.Enqueue(message) == nil
}

// Enqueue copies message and reports whether the outbound queue accepted it.
func (c *Connection) Enqueue(message []byte) error {
	return c.enqueueShared(bytes.Clone(message))
}

func (c *Connection) sendShared(message []byte) bool { return c.enqueueShared(message) == nil }

func (c *Connection) enqueueShared(message []byte) error {
	c.lifecycleMu.RLock()
	select {
	case <-c.ctx.Done():
		c.lifecycleMu.RUnlock()
		return ErrConnectionClosed
	default:
	}
	select {
	case c.send <- message:
		c.consecutiveDrops.Store(0)
		c.lifecycleMu.RUnlock()
		return nil
	default:
		c.lifecycleMu.RUnlock()
		closeSlow := c.consecutiveDrops.Add(1) >= c.config.SlowConsumerDropThreshold
		c.shard.observer().OutboundDropped(closeSlow)
		if closeSlow {
			c.Close("Outbound queue full")
		}
		return ErrSendQueueFull
	}
}

// GetUserID returns the User ID associated with this Connection session.
func (c *Connection) GetUserID() string {
	return c.userID
}

// GetShardID returns the Shard ID associated with this Connection session.
func (c *Connection) GetShardID() string {
	return c.shardID
}

// GetClientIP returns the remote client IP address.
func (c *Connection) GetClientIP() string {
	return c.clientIP
}

// GetShard returns the Shard manager this Connection is registered to.
func (c *Connection) GetShard() *Shard {
	return c.shard
}

// GetRequestID returns the trace request ID associated with the upgrading request.
func (c *Connection) GetRequestID() string {
	return c.requestID
}

func (c *Connection) routingLocation() routing.Location {
	if c.location != nil {
		return *c.location
	}
	return routing.Location{}
}
