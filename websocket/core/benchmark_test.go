package core

import (
	"context"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type benchmarkConn struct{}

type benchmarkCoordinator struct{ lifecycleCoordinator }

func (c *benchmarkCoordinator) GetConfig() ManagerConfig {
	config := DefaultManagerConfig()
	config.DisableLifecycleLogging = true
	config.MaxShardConnections = math.MaxInt64
	config.EventExecutor = c.GetEventExecutor()
	return config
}

func (benchmarkConn) ReadMessage() (int, []byte, error) { return 0, nil, io.EOF }
func (benchmarkConn) WriteMessage(int, []byte) error    { return nil }
func (benchmarkConn) SetReadLimit(int64)                {}
func (benchmarkConn) SetReadDeadline(time.Time) error   { return nil }
func (benchmarkConn) SetWriteDeadline(time.Time) error  { return nil }
func (benchmarkConn) SetPongHandler(func(string) error) {}
func (benchmarkConn) Close() error                      { return nil }

func newBenchmarkConnection(shard *Shard, user string) *Connection {
	return NewConnection(benchmarkConn{}, shard, user, shard.name, "127.0.0.1", "benchmark", "benchmark")
}

func BenchmarkConnectionSend(b *testing.B) {
	coordinator := &benchmarkCoordinator{}
	b.Cleanup(func() { _ = coordinator.GetEventExecutor().Shutdown(context.Background()) })
	shard := NewShard("benchmark", coordinator)
	connection := newBenchmarkConnection(shard, "user")
	payload := []byte(`{"type":"benchmark","data":"value"}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !connection.Send(payload) {
			b.Fatal("send rejected")
		}
		<-connection.send
	}
}

func BenchmarkShardRegister(b *testing.B) {
	coordinator := &benchmarkCoordinator{}
	b.Cleanup(func() { _ = coordinator.GetEventExecutor().Shutdown(context.Background()) })
	shard := NewShard("benchmark", coordinator)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := newBenchmarkConnection(shard, fmt.Sprintf("user-%d", i))
		if !shard.Register(conn) {
			b.Fatal("register rejected")
		}
		shard.removeConnection(conn, "benchmark")
	}
}

func BenchmarkShardUnregister(b *testing.B) {
	coordinator := &benchmarkCoordinator{}
	b.Cleanup(func() { _ = coordinator.GetEventExecutor().Shutdown(context.Background()) })
	shard := NewShard("benchmark", coordinator)
	connections := make([]*Connection, b.N)
	for i := range connections {
		connections[i] = newBenchmarkConnection(shard, fmt.Sprintf("user-%d", i))
		shard.Register(connections[i])
	}
	b.ReportAllocs()
	b.ResetTimer()
	for _, conn := range connections {
		shard.removeConnection(conn, "benchmark")
	}
}

func BenchmarkSendToUser(b *testing.B)     { benchmarkFanOut(b, 10, false) }
func BenchmarkSendToRoom(b *testing.B)     { benchmarkFanOut(b, 100, true) }
func BenchmarkBroadcast100(b *testing.B)   { benchmarkBroadcast(b, 100) }
func BenchmarkBroadcast1000(b *testing.B)  { benchmarkBroadcast(b, 1000) }
func BenchmarkBroadcast10000(b *testing.B) { benchmarkBroadcast(b, 10000) }

// Each iteration drains every recipient and verifies the audience stays fixed.
// These benchmarks measure fan-out plus queue drain, not real socket writes.
func benchmarkFanOut(b *testing.B, count int, room bool) {
	coordinator := &benchmarkCoordinator{}
	b.Cleanup(func() { _ = coordinator.GetEventExecutor().Shutdown(context.Background()) })
	shard := NewShard("benchmark", coordinator)
	connections := make([]*Connection, 0, count)
	for i := 0; i < count; i++ {
		conn := newBenchmarkConnection(shard, "user")
		shard.Register(conn)
		connections = append(connections, conn)
		if room {
			if err := shard.JoinRoom(context.Background(), "room", conn); err != nil {
				b.Fatal(err)
			}
		}
	}
	benchmarkDeliveries(b, shard, connections, func(payload []byte) {
		if room {
			shard.SendToRoom("room", payload)
		} else {
			shard.SendToUser("user", payload)
		}
	})
}
func benchmarkBroadcast(b *testing.B, count int) {
	coordinator := &benchmarkCoordinator{}
	b.Cleanup(func() { _ = coordinator.GetEventExecutor().Shutdown(context.Background()) })
	shard := NewShard("benchmark", coordinator)
	connections := make([]*Connection, 0, count)
	for i := 0; i < count; i++ {
		conn := newBenchmarkConnection(shard, fmt.Sprintf("user-%d", i))
		shard.Register(conn)
		connections = append(connections, conn)
	}
	benchmarkDeliveries(b, shard, connections, shard.Broadcast)
}
func benchmarkDeliveries(b *testing.B, shard *Shard, connections []*Connection, send func([]byte)) {
	payload := []byte("benchmark")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		send(payload)
		for _, c := range connections {
			select {
			case <-c.send:
			default:
				b.Fatal("recipient did not receive payload")
			}
		}
	}
	b.StopTimer()
	if shard.GetConnectionCount() != int64(len(connections)) {
		b.Fatal("benchmark audience changed")
	}
	b.ReportMetric(float64(len(connections)), "recipients/op")
	b.Cleanup(func() {
		for _, c := range connections {
			shard.Unregister(c)
		}
	})
}

func BenchmarkEventDispatch(b *testing.B) {
	coordinator := &benchmarkCoordinator{}
	b.Cleanup(func() { _ = coordinator.GetEventExecutor().Shutdown(context.Background()) })
	shard := NewShard("benchmark", coordinator)
	conn := newBenchmarkConnection(shard, "user")
	eventType := "benchmark-event"
	var completed atomic.Uint64
	RegisterHandler(eventType, func(*Connection, IncomingMessage) error { completed.Add(1); return nil })
	raw := []byte(`{"type":"benchmark-event","data":null}`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HandleEvent(&EventMessage{Conn: conn, Raw: raw})
		for conn.pendingHandlers.Load() != 0 {
			runtime.Gosched()
		}
	}
	_ = coordinator.GetEventExecutor().Shutdown(context.Background())
}

func BenchmarkDataStructureMemory(b *testing.B) {
	coordinator := &benchmarkCoordinator{}
	b.Cleanup(func() { _ = coordinator.GetEventExecutor().Shutdown(context.Background()) })
	shard := NewShard("benchmark", coordinator)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = newBenchmarkConnection(shard, "user")
	}
}
