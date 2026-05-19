// Package pool provides memory and concurrency pools to optimize resource usage under high traffic.
package pool

import (
	"sync"
	"sync/atomic"
)

// BufferPool manages reusable byte buffers to prevent memory allocation overhead and GC pauses.
type BufferPool struct {
	pool       sync.Pool
	bufferSize int
	totalGets  int64
	totalPuts  int64
}

// NewBufferPool creates a new BufferPool with the specified buffer size in bytes.
// If bufferSize is less than or equal to 0, it defaults to 1024 bytes (1KB).
func NewBufferPool(bufferSize int) *BufferPool {
	if bufferSize <= 0 {
		bufferSize = 1024 // Default 1KB buffers
	}

	bp := &BufferPool{
		bufferSize: bufferSize,
	}

	bp.pool = sync.Pool{
		New: func() interface{} {
			b := make([]byte, bufferSize)
			return &b
		},
	}

	return bp
}

// Get retrieves a zero-length byte slice from the pool with capacity equal to the pool's buffer size.
func (bp *BufferPool) Get() []byte {
	atomic.AddInt64(&bp.totalGets, 1)
	p := bp.pool.Get().(*[]byte)
	buf := *p
	return buf[:0] // Reset length to 0 but keep capacity
}

// Put returns a byte slice to the pool. Slices with capacity not matching the pool's buffer size are discarded.
func (bp *BufferPool) Put(buf []byte) {
	if cap(buf) != bp.bufferSize {
		return // Don't put back buffers of wrong size
	}
	atomic.AddInt64(&bp.totalPuts, 1)
	bp.pool.Put(&buf)
}

// GetStats returns statistics about the BufferPool, including get/put counts and buffer size.
func (bp *BufferPool) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"gets":       atomic.LoadInt64(&bp.totalGets),
		"puts":       atomic.LoadInt64(&bp.totalPuts),
		"bufferSize": bp.bufferSize,
	}
}

// ConnectionPool manages pre-allocated message channels to improve memory efficiency per connection.
type ConnectionPool struct {
	pool      sync.Pool
	totalGets int64
	totalPuts int64
}

// NewConnectionPool creates a new ConnectionPool.
func NewConnectionPool() *ConnectionPool {
	cp := &ConnectionPool{}

	cp.pool = sync.Pool{
		New: func() interface{} {
			return make(chan []byte, 256) // Buffered channel for messages
		},
	}

	return cp
}

// GetChannel retrieves an empty buffered byte slice channel from the pool.
func (cp *ConnectionPool) GetChannel() chan []byte {
	atomic.AddInt64(&cp.totalGets, 1)
	ch := cp.pool.Get().(chan []byte)

	// Drain any remaining messages
	for len(ch) > 0 {
		<-ch
	}

	return ch
}

// PutChannel returns a channel to the pool. Closed or nil channels are ignored.
func (cp *ConnectionPool) PutChannel(ch chan []byte) {
	if ch == nil {
		return
	}

	// Don't put back closed channels
	select {
	case <-ch:
		return
	default:
	}

	atomic.AddInt64(&cp.totalPuts, 1)
	cp.pool.Put(ch)
}

// GetStats returns statistics about the ConnectionPool.
func (cp *ConnectionPool) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"gets": atomic.LoadInt64(&cp.totalGets),
		"puts": atomic.LoadInt64(&cp.totalPuts),
	}
}

// Global pool instances.
var (
	globalBufferPool     *BufferPool
	globalConnectionPool *ConnectionPool
	bufferPoolOnce       sync.Once
	connectionPoolOnce   sync.Once
)

// GetGlobalBufferPool returns the default singleton BufferPool with 4KB buffers.
func GetGlobalBufferPool() *BufferPool {
	bufferPoolOnce.Do(func() {
		globalBufferPool = NewBufferPool(4096) // 4KB buffers
	})
	return globalBufferPool
}

// GetGlobalConnectionPool returns the default singleton ConnectionPool.
func GetGlobalConnectionPool() *ConnectionPool {
	connectionPoolOnce.Do(func() {
		globalConnectionPool = NewConnectionPool()
	})
	return globalConnectionPool
}
