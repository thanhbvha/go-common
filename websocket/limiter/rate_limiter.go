// Package limiter provides token-bucket rate limiting tailored for WebSocket connections.
//
// It helps prevent abuse by limiting the number of messages a single connection
// can send over a specific time window, protecting the server from DoS attacks.
package limiter

import (
	"sync"
	"time"
)

// RateLimiter implements a high-performance token bucket rate limiting algorithm.
type RateLimiter struct {
	rate       float64
	capacity   int64
	tokens     float64
	lastUpdate time.Time
	mu         sync.Mutex
}

// NewRateLimiter creates a new RateLimiter with the specified token generation rate per second and maximum capacity.
func NewRateLimiter(rate float64, capacity int64) *RateLimiter {
	return &RateLimiter{
		rate:       rate,
		capacity:   capacity,
		tokens:     float64(capacity),
		lastUpdate: time.Now(),
	}
}

// Allow is a shorthand for AllowN(1) to check if a single operation is permitted.
func (rl *RateLimiter) Allow() bool {
	return rl.AllowN(1)
}

// AllowN checks if n operations are allowed. Returns true if permitted, false otherwise.
func (rl *RateLimiter) AllowN(n int64) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.lastUpdate).Seconds()

	// Fill bucket with tokens accrued since last update
	rl.tokens += elapsed * rl.rate
	if rl.tokens > float64(rl.capacity) {
		rl.tokens = float64(rl.capacity)
	}

	rl.lastUpdate = now

	// Consume tokens if enough are available
	if rl.tokens >= float64(n) {
		rl.tokens -= float64(n)
		return true
	}

	return false
}

// ConnectionLimiter tracks and restricts concurrent connection counts per unique identifier (e.g. IP or User ID).
type ConnectionLimiter struct {
	connections map[string]int
	maxPerKey   int
	mu          sync.RWMutex
	cleanupTick time.Duration
	lastCleanup time.Time
}

// NewConnectionLimiter creates a new ConnectionLimiter with a limit on concurrent connections per unique key.
func NewConnectionLimiter(maxPerKey int) *ConnectionLimiter {
	return &ConnectionLimiter{
		connections: make(map[string]int),
		maxPerKey:   maxPerKey,
		cleanupTick: 5 * time.Minute,
		lastCleanup: time.Now(),
	}
}

// CanConnect checks if a new connection is allowed for the given key without modifying the count.
func (cl *ConnectionLimiter) CanConnect(key string) bool {
	cl.mu.RLock()
	current := cl.connections[key]
	cl.mu.RUnlock()

	return current < cl.maxPerKey
}

// AddConnection registers a new connection for the given key if it falls below the limit.
// Returns true on success, or false if the key is already at capacity.
func (cl *ConnectionLimiter) AddConnection(key string) bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	current := cl.connections[key]
	if current >= cl.maxPerKey {
		return false
	}

	cl.connections[key] = current + 1
	return true
}

// RemoveConnection decrements the active connection count for the given key.
func (cl *ConnectionLimiter) RemoveConnection(key string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if current := cl.connections[key]; current > 0 {
		cl.connections[key] = current - 1
		if cl.connections[key] == 0 {
			delete(cl.connections, key)
		}
	}
}

// GetConnectionCount returns the active connection count for a specific key.
func (cl *ConnectionLimiter) GetConnectionCount(key string) int {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.connections[key]
}

// GetStats returns usage statistics about the ConnectionLimiter.
func (cl *ConnectionLimiter) GetStats() map[string]interface{} {
	cl.mu.RLock()
	defer cl.mu.RUnlock()

	totalConnections := 0
	for _, count := range cl.connections {
		totalConnections += count
	}

	return map[string]interface{}{
		"totalKeys":        len(cl.connections),
		"totalConnections": totalConnections,
		"maxPerKey":        cl.maxPerKey,
	}
}

// Cleanup removes stale/empty connection tracking records.
func (cl *ConnectionLimiter) Cleanup() {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	for key, count := range cl.connections {
		if count <= 0 {
			delete(cl.connections, key)
		}
	}

	cl.lastCleanup = time.Now()
}

var (
	globalRateLimiter       *RateLimiter
	globalConnectionLimiter *ConnectionLimiter
	rateLimiterOnce         sync.Once
	connectionLimiterOnce   sync.Once
)

// GetGlobalRateLimiter returns the default singleton RateLimiter (1000 requests/sec limit, 2000 burst capacity).
func GetGlobalRateLimiter() *RateLimiter {
	rateLimiterOnce.Do(func() {
		globalRateLimiter = NewRateLimiter(1000.0, 2000)
	})
	return globalRateLimiter
}

// GetGlobalConnectionLimiter returns the default singleton ConnectionLimiter (max 100 connections per key).
func GetGlobalConnectionLimiter() *ConnectionLimiter {
	connectionLimiterOnce.Do(func() {
		globalConnectionLimiter = NewConnectionLimiter(100)
	})
	return globalConnectionLimiter
}
