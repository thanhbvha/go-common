package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadinessToleratesTransientAccountingSkew(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	manager.accountingGrace = time.Hour
	atomic.StoreInt64(&manager.totalConnections, 1)
	defer atomic.StoreInt64(&manager.totalConnections, 0)

	if readiness := manager.Readiness(); !readiness.Ready || !readiness.AccountingValid {
		t.Fatalf("brief connect/disconnect skew failed readiness: %+v", readiness)
	}
}

func TestReadinessFailsWhenAccountingSkewPersists(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	manager.accountingGrace = 10 * time.Millisecond
	atomic.StoreInt64(&manager.totalConnections, 1)
	defer atomic.StoreInt64(&manager.totalConnections, 0)

	manager.Readiness()
	time.Sleep(30 * time.Millisecond)
	if readiness := manager.Readiness(); readiness.Ready || readiness.AccountingValid {
		t.Fatalf("persistent accounting skew did not fail readiness: %+v", readiness)
	}
}

func TestReadinessRecoversWhenSkewClears(t *testing.T) {
	manager := NewManager()
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	manager.accountingGrace = 10 * time.Millisecond
	atomic.StoreInt64(&manager.totalConnections, 1)
	manager.Readiness()
	time.Sleep(30 * time.Millisecond)
	atomic.StoreInt64(&manager.totalConnections, 0)
	if readiness := manager.Readiness(); !readiness.Ready {
		t.Fatalf("readiness did not recover: %+v", readiness)
	}
	atomic.StoreInt64(&manager.totalConnections, 1)
	defer atomic.StoreInt64(&manager.totalConnections, 0)
	if readiness := manager.Readiness(); !readiness.Ready {
		t.Fatalf("grace window did not restart after recovery: %+v", readiness)
	}
}

func BenchmarkReadiness(b *testing.B) {
	manager := NewManager()
	b.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
	b.ReportAllocs()
	for b.Loop() {
		if !manager.Readiness().Ready {
			b.Fatal("not ready")
		}
	}
}
