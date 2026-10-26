package eventbus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/registry"
)

func TestEngine_C8_ShutdownGracefully(t *testing.T) {
	mem, _ := registry.Get("memory")
	cfg := DefaultConfig()
	cfg.ProcessTimeout = 2 * time.Second

	bus, _ := Open(cfg, WithTransport(mem))

	started := make(chan struct{})
	var handlerCompleted int32

	if err := bus.DeclareTask("test-shutdown"); err != nil {
		t.Fatal(err)
	}
	if err := bus.HandleTask("test-shutdown", func(ctx context.Context, job Job) error {
		close(started) // signal that we have picked up the job

		// simulate long processing
		time.Sleep(500 * time.Millisecond)
		atomic.StoreInt32(&handlerCompleted, 1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	bus.Start(ctx)
	bus.Enqueue(ctx, "test-shutdown", "payload")

	// wait for handler to pick it up
	<-started

	// trigger stop while handler is running
	stopStart := time.Now()
	err := bus.Stop(ctx)
	if err != nil {
		t.Fatalf("Stop error: %v", err)
	}
	stopDuration := time.Since(stopStart)

	if atomic.LoadInt32(&handlerCompleted) != 1 {
		t.Fatalf("Bug C8 NOT fixed! Stop() returned before handler finished.")
	}

	if stopDuration < 500*time.Millisecond {
		t.Fatalf("Bug C8 NOT fixed! Stop() didn't wait for in-flight job, took %v", stopDuration)
	}

	// Verify the job was ACKed successfully
	stats, _ := bus.TopologyStats(ctx, TaskRef("test-shutdown"))
	if stats.Pending != 0 {
		t.Fatalf("Job was not acked! Pending = %d", stats.Pending)
	}

	t.Logf("Bug C8 fixed: Stop() waits for in-flight jobs and context is detached for ACK. Stop took %v", stopDuration)
}
