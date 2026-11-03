package nats

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConnectCanceledBeforeDial(t *testing.T) {
	cfg := DefaultConfig()
	cfg.URLs = []string{"nats://127.0.0.1:1"}
	cfg.MaxConnRetries = 3
	c := New(cfg)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := c.Connect(ctx)
	if !errors.Is(err, context.Canceled) || time.Since(started) > 200*time.Millisecond {
		t.Fatalf("Connect=%v elapsed=%s", err, time.Since(started))
	}
}
func TestCloseIsTerminal(t *testing.T) {
	c := New(DefaultConfig())
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Connect(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Connect after close=%v", err)
	}
}

func TestNewSnapshotsURLs(t *testing.T) {
	cfg := DefaultConfig()
	c := New(cfg)
	cfg.URLs[0] = "nats://mutated.invalid:4222"
	if c.cfg.URLs[0] == cfg.URLs[0] {
		t.Fatal("caller mutated connection URLs")
	}
	_ = c.Close()
}
