package core

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/thanhbvha/go-common/websocket/pool"
)

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

// JobHandler is the compatibility handler signature.
type JobHandler func(conn *Connection, msg IncomingMessage) error

// ContextJobHandler is a cancellation-aware business event callback.
type ContextJobHandler func(ctx context.Context, conn *Connection, msg IncomingMessage) error

type handlerRegistration struct {
	run             ContextJobHandler
	timeout         time.Duration
	maxPayloadBytes int
	validate        func([]byte) error
}

// EventStats reports cumulative bounded dispatch outcomes.
type EventStats struct {
	Malformed uint64
	Unknown   uint64
	Submitted uint64
	Dropped   uint64
	Errors    uint64
	Timeouts  uint64
	Panics    uint64
}

type EventRegistry struct {
	mu       sync.RWMutex
	handlers map[string]handlerRegistration
}

func NewEventRegistry() *EventRegistry {
	return &EventRegistry{handlers: make(map[string]handlerRegistration)}
}

var (
	defaultEventRegistry = NewEventRegistry()
)

// RegisterHandler binds a compatibility handler with the default timeout.
func RegisterHandler(eventType string, handler JobHandler) {
	RegisterContextHandler(eventType, 30*time.Second, func(_ context.Context, conn *Connection, msg IncomingMessage) error {
		return handler(conn, msg)
	})
}

// RegisterContextHandler binds a context-aware handler and invocation timeout.
// Handlers must observe ctx for timeout cancellation; the executor never creates
// an unbounded goroutine to forcibly abandon a non-cooperative handler.
func RegisterContextHandler(eventType string, timeout time.Duration, handler ContextJobHandler) {
	RegisterHandlerConfig(eventType, HandlerConfig{Timeout: timeout, Handler: handler})
}

// RegisterHandlerConfig registers validation, timeout, authorization and execution policy.
func RegisterHandlerConfig(eventType string, config HandlerConfig) {
	handler := config.Handler
	if eventType == "" || handler == nil {
		return
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	defaultEventRegistry.Register(eventType, config)
}

func (r *EventRegistry) Register(eventType string, config HandlerConfig) {
	if r == nil || eventType == "" || config.Handler == nil {
		return
	}
	if config.Timeout <= 0 {
		config.Timeout = 30 * time.Second
	}
	r.mu.Lock()
	r.handlers[eventType] = handlerRegistration{run: config.Handler, timeout: config.Timeout, maxPayloadBytes: config.MaxPayloadBytes, validate: config.Validate}
	r.mu.Unlock()
}

func (r *EventRegistry) lookup(eventType string) (handlerRegistration, bool) {
	r.mu.RLock()
	registration, ok := r.handlers[eventType]
	r.mu.RUnlock()
	return registration, ok
}

// EventMessage represents a raw incoming payload coupled with its source Connection.
type EventMessage struct {
	Conn *Connection
	Raw  []byte
}

// HandleEvent parses and submits an event to the bounded per-manager executor.
func HandleEvent(ev *EventMessage) {
	if ev == nil || ev.Conn == nil {
		return
	}

	var inc IncomingMessage
	observer := ev.Conn.shard.observer()
	if !stdjson.Valid(ev.Raw) {
		observer.EventMalformed()
		ev.Conn.malformedMessages.Add(1)
		return
	}
	if err := json.Unmarshal(ev.Raw, &inc); err != nil || inc.Type == "" {
		observer.EventMalformed()
		ev.Conn.malformedMessages.Add(1)
		return
	}

	registry := ev.Conn.shard.eventRegistry()
	registration, ok := registry.lookup(inc.Type)
	if !ok {
		observer.EventUnknown()
		return
	}
	if registration.maxPayloadBytes > 0 && len(inc.Data) > registration.maxPayloadBytes {
		observer.EventMalformed()
		ev.Conn.malformedMessages.Add(1)
		return
	}
	if registration.validate != nil && registration.validate(inc.Data) != nil {
		observer.EventMalformed()
		ev.Conn.malformedMessages.Add(1)
		return
	}
	if err := ev.Conn.shard.authorizer().CanPublish(ev.Conn.ctx, ev.Conn.identity, inc.Type); err != nil {
		observer.EventDropped()
		return
	}
	if !ev.Conn.tryAcquireHandler() {
		observer.EventDropped()
		return
	}
	executor := ev.Conn.shard.coordinator.GetEventExecutor()
	if executor == nil {
		ev.Conn.releaseHandler()
		observer.EventDropped()
		return
	}

	err := executor.Submit(ev.Conn.ctx, ev.Conn.connectionID, func(parent context.Context) (handlerErr error) {
		defer ev.Conn.releaseHandler()
		ctx, cancel := context.WithTimeout(parent, registration.timeout)
		defer cancel()
		finishTrace := func(error) {}
		if tracer, ok := observer.(TraceObserver); ok {
			ctx, finishTrace = tracer.StartEvent(ctx, inc.Type)
		}
		started := time.Now()
		defer func() {
			if latency, ok := observer.(LatencyObserver); ok {
				latency.ObserveEventLatency(time.Since(started))
			}
			finishTrace(handlerErr)
		}()
		defer func() {
			if recovered := recover(); recovered != nil {
				observer.EventPanic()
				panic(recovered)
			}
		}()
		handlerErr = ev.Conn.shard.handlerSupervisor.Run(ctx, func(handlerCtx context.Context) error {
			return registration.run(handlerCtx, ev.Conn, inc)
		})
		if errors.Is(handlerErr, context.DeadlineExceeded) {
			observer.EventTimeout()
		}
		if handlerErr != nil {
			observer.EventError()
		}
		return handlerErr
	})
	if err != nil {
		ev.Conn.releaseHandler()
		observer.EventDropped()
		if !errors.Is(err, pool.ErrExecutorFull) && !errors.Is(err, pool.ErrExecutorStopped) {
			observer.EventError()
		}
		return
	}
	observer.EventSubmitted()
}

// GetEventStats returns cumulative event dispatch counters.
func GetEventStats() EventStats {
	return defaultObserver.EventStats()
}
