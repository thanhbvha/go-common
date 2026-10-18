package demo

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus"
)

func TestMemoryRuntimeEndToEnd(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Driver = "memory"
	cfg.Namespace = "example_test"
	logger := NewLogger("test")
	rt, err := OpenRuntime(context.Background(), cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	bus, err := NewBus(cfg, rt, logger, NewMetrics(), NewTracer(logger))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := CloseAll(bus, rt); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	if err := bus.DeclareTask(TaskName, TopologyOptions("memory")...); err != nil {
		t.Fatal(err)
	}
	received := make(chan UserMessage, 1)
	if err := bus.HandleTask(TaskName, eventbus.Handle(func(_ context.Context, _ eventbus.Job, payload UserMessage) error {
		received <- payload
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := Start(context.Background(), bus); err != nil {
		t.Fatal(err)
	}
	want := UserMessage{UserID: "test-user", Email: "test@example.invalid"}
	if err := bus.Enqueue(context.Background(), TaskName, want); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got != want {
			t.Fatalf("payload = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for memory delivery")
	}
}

func TestConfigValidationAndCapabilities(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Driver = "unknown"
	if err := cfg.Validate(); err == nil {
		t.Fatal("unsupported driver accepted")
	}
	cfg.Driver = "memory"
	rt, err := OpenRuntime(context.Background(), cfg, NewLogger("test"))
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	if !SupportsDedupe(rt.Transport) {
		t.Fatal("memory transport should advertise topology dedupe")
	}
}

func TestSplitCSV(t *testing.T) {
	got := SplitCSV(" one, ,two ")
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("SplitCSV = %#v", got)
	}
}

func TestSafeNATSURLsRejectsCredentials(t *testing.T) {
	if _, err := safeNATSURLs("nats://user:secret@localhost:4222"); err == nil {
		t.Fatal("credential-bearing NATS URL accepted")
	}
	got, err := safeNATSURLs("nats://localhost:4222,tls://nats.example:4223")
	if err != nil || len(got) != 2 {
		t.Fatalf("safeNATSURLs = %#v, %v", got, err)
	}
}
