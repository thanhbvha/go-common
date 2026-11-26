package core

import (
	"context"
	"fmt"
	"sync"
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

// A connect/disconnect storm updates the manager and shard counters at
// different moments. Readiness must stay true throughout (the grace window
// absorbs the skew) and the counters must converge to zero afterwards.
func TestReadinessStaysStableDuringConnectStorm(t *testing.T) {
	manager := NewManagerWithConfig(ManagerConfig{DisableLifecycleLogging: true})
	t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })

	var notReady atomic.Int32
	var probes atomic.Int64
	var first atomic.Value
	stopProbe := make(chan struct{})
	probeDone := make(chan struct{})
	go func() {
		defer close(probeDone)
		for {
			select {
			case <-stopProbe:
				return
			default:
			}
			probes.Add(1)
			if r := manager.Readiness(); !r.Ready {
				notReady.Add(1)
				first.CompareAndSwap(nil, r)
			}
		}
	}()

	const workers, rounds = 32, 40
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				transport := newMockConn()
				done := make(chan struct{})
				go func() {
					defer close(done)
					_ = manager.HandleIdentityConnection(transport, "", Identity{UserID: fmt.Sprintf("u-%d-%d", w, i%4)}, "", "")
				}()
				if i%2 == 0 {
					_ = transport.Close() // peer drops immediately
				} else {
					time.Sleep(time.Millisecond)
					_ = transport.Close()
				}
				<-done
			}
		}(w)
	}
	wg.Wait()
	close(stopProbe)
	<-probeDone

	if n := notReady.Load(); n != 0 {
		t.Fatalf("readiness flapped %d times out of %d probes; first: %+v", n, probes.Load(), first.Load())
	}
	deadline := time.Now().Add(5 * time.Second)
	for manager.GetTotalConnections() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := manager.GetTotalConnections(); got != 0 || !manager.accountingConsistent() {
		t.Fatalf("counters did not converge: total=%d consistent=%v", got, manager.accountingConsistent())
	}
	if probes.Load() < 100 {
		t.Fatalf("only %d probes ran, the storm was not observed", probes.Load())
	}
}
