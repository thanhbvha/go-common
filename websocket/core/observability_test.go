package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestManagerReadinessLifecycleAndAccounting(t *testing.T) {
	manager := NewManager()
	if readiness := manager.Readiness(); !readiness.Ready {
		t.Fatalf("running manager not ready: %+v", readiness)
	}
	atomic.StoreInt64(&manager.totalConnections, 1)
	if readiness := manager.Readiness(); readiness.Ready || readiness.AccountingValid {
		t.Fatalf("accounting mismatch did not fail readiness: %+v", readiness)
	}
	atomic.StoreInt64(&manager.totalConnections, 0)
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if readiness := manager.Readiness(); readiness.Ready || readiness.State != StateStopped {
		t.Fatalf("stopped manager readiness: %+v", readiness)
	}
}

func TestRuntimeMetricsTrackConnectionLifecycle(t *testing.T) {
	transport := newMockConn()
	connection, shard, _ := newRunningTestConnection(t, transport)
	observer := shard.observer().(SnapshotObserver)
	before := observer.RuntimeMetrics()
	shard.observer().ConnectionOpened()
	connection.Close("metrics test")
	waitConnection(t, connection)
	after := observer.RuntimeMetrics()
	if after.ConnectionsOpened != before.ConnectionsOpened+1 || after.ConnectionsClosed != before.ConnectionsClosed+1 {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
}

func TestManagerObserversAreIsolated(t *testing.T) {
	first := NewManagerWithConfig(ManagerConfig{})
	second := NewManagerWithConfig(ManagerConfig{})
	t.Cleanup(func() { _ = first.Shutdown(context.Background()); _ = second.Shutdown(context.Background()) })
	first.config.Observer.ConnectionOpened()
	if first.runtimeMetrics().ConnectionsOpened != 1 || second.runtimeMetrics().ConnectionsOpened != 0 {
		t.Fatalf("first=%+v second=%+v", first.runtimeMetrics(), second.runtimeMetrics())
	}
}

func TestAtomicObserverRecordsEventLatency(t *testing.T) {
	observer := &AtomicObserver{}
	observer.ObserveEventLatency(500 * time.Microsecond)
	observer.ObserveEventLatency(75 * time.Millisecond)
	observer.ObserveEventLatency(150 * time.Millisecond)
	histogram := observer.LatencyHistogram()
	if histogram.Under1ms != 1 || histogram.Under100ms != 1 || histogram.Over100ms != 1 {
		t.Fatalf("histogram = %+v", histogram)
	}
}
