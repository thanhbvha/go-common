package eventbus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/driver/memory"
)

func TestEngine_PubSub(t *testing.T) {
	trans := memory.New()
	cfg := DefaultConfig()
	cfg.Namespace = "testpubsub"
	cfg.Concurrency = 10
	cfg.MaxDeliveries = 3
	cfg.ProcessTimeout = 2 * time.Second
	cfg.RetryPolicy = ConstantBackoff{Delay: 1 * time.Second}
	b, err := Open(cfg, WithTransport(trans))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	handler1Calls := 0
	handler2Calls := 0
	var mu sync.Mutex

	// Subscribe handler 1
	err = b.Subscribe("order_created", "send_email", func(ctx context.Context, job Job) error {
		mu.Lock()
		handler1Calls++
		mu.Unlock()
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Subscribe handler 2
	err = b.Subscribe("order_created", "update_stock", func(ctx context.Context, job Job) error {
		mu.Lock()
		handler2Calls++
		mu.Unlock()
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := b.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for memory transport consumers to spin up
	time.Sleep(100 * time.Millisecond)

	// Publish one event
	err = b.PublishEvent(ctx, "order_created", map[string]string{"order_id": "123"})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for both handlers to process the event")
	}

	mu.Lock()
	defer mu.Unlock()
	if handler1Calls != 1 || handler2Calls != 1 {
		t.Fatalf("Expected 1 call each, got h1=%d, h2=%d", handler1Calls, handler2Calls)
	}

	b.Stop(ctx)
}
