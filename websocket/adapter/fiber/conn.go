// Package fiber provides an adapter to bridge the Fiber web framework and our generic WebSocket core.
package fiber

import (
	"time"

	"github.com/gofiber/websocket/v2"
	"github.com/thanhbvha/go-common/websocket/core"
)

// ConnAdapter wraps a fiber websocket.Conn to satisfy the generic core.Conn interface.
type ConnAdapter struct {
	conn *websocket.Conn
}

// NewConnAdapter instantiates a ConnAdapter wrapping the concrete Fiber websocket connection.
func NewConnAdapter(conn *websocket.Conn) core.Conn {
	return &ConnAdapter{conn: conn}
}

// ReadMessage delegates to the wrapped Fiber connection.
func (a *ConnAdapter) ReadMessage() (messageType int, p []byte, err error) {
	return a.conn.ReadMessage()
}

// WriteMessage delegates to the wrapped Fiber connection.
func (a *ConnAdapter) WriteMessage(messageType int, data []byte) error {
	return a.conn.WriteMessage(messageType, data)
}

// SetReadLimit delegates to the wrapped Fiber connection.
func (a *ConnAdapter) SetReadLimit(limit int64) {
	a.conn.SetReadLimit(limit)
}

// SetReadDeadline delegates to the wrapped Fiber connection.
func (a *ConnAdapter) SetReadDeadline(t time.Time) error {
	return a.conn.SetReadDeadline(t)
}

// SetWriteDeadline delegates to the wrapped Fiber connection.
func (a *ConnAdapter) SetWriteDeadline(t time.Time) error {
	return a.conn.SetWriteDeadline(t)
}

// SetPongHandler delegates to the wrapped Fiber connection.
func (a *ConnAdapter) SetPongHandler(h func(string) error) {
	a.conn.SetPongHandler(h)
}

// Close delegates to the wrapped Fiber connection.
func (a *ConnAdapter) Close() error {
	return a.conn.Close()
}
