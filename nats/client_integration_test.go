package nats

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	server "github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
)

func localClient(t *testing.T, jetstream bool) *Client {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: jetstream, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	t.Cleanup(s.Shutdown)
	cfg := DefaultConfig()
	cfg.URLs = []string{s.ClientURL()}
	c := New(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func TestDrainWaitsForCallbacksAndClosesNative(t *testing.T) {
	c := localClient(t, false)
	entered, release := make(chan struct{}), make(chan struct{})
	_, err := c.Subscribe("drain.test", func(*gonats.Msg) { close(entered); <-release })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.Publish("drain.test", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	native := c.Native()
	done := make(chan error, 1)
	go func() { done <- c.Drain(ctx) }()
	select {
	case err := <-done:
		close(release)
		t.Fatalf("Drain returned before callback: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if !native.IsClosed() {
		t.Fatal("native connection not closed")
	}
	if err = c.Connect(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("terminal Connect=%v", err)
	}
}
func TestDrainDeadlineForceCloses(t *testing.T) {
	c := localClient(t, false)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	_, err := c.Subscribe("drain.timeout", func(*gonats.Msg) { close(entered); <-release })
	if err != nil {
		t.Fatal(err)
	}
	flush, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = c.Ping(flush); err != nil {
		t.Fatal(err)
	}
	if err = c.Publish("drain.timeout", nil); err != nil {
		t.Fatal(err)
	}
	<-entered
	native := c.Native()
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	if err = c.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Drain=%v", err)
	}
	if !native.IsClosed() {
		t.Fatal("deadline did not close native")
	}
}
func TestAdminRequestsHonorDeadline(t *testing.T) {
	c := localClient(t, false)
	// A core server plus a silent API subscriber makes the request wait, rather
	// than fail immediately with no responders. This catches ignored deadlines.
	sub, err := c.Subscribe("$JS.API.>", func(*gonats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	flush, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err = c.Ping(flush); err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func(context.Context) error{
		"stream":   func(ctx context.Context) error { return c.AddStream(ctx, "DEADLINE", []string{"deadline.>"}) },
		"consumer": func(ctx context.Context) error { return c.AddConsumer(ctx, "DEADLINE", "worker") },
		"kv":       func(ctx context.Context) error { _, err := c.KVGet(ctx, "DEADLINE", "key"); return err },
	} {
		t.Run(name, func(t *testing.T) {
			ctx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer stop()
			started := time.Now()
			err := call(ctx)
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 300*time.Millisecond {
				t.Fatalf("deadline: err=%v elapsed=%s", err, time.Since(started))
			}
		})
	}
}
func TestKVContextOperationsAndConfigSnapshot(t *testing.T) {
	c := localClient(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.KVCreate(ctx, "SETTINGS", WithKVHistory(3)); err != nil {
		t.Fatal(err)
	}
	rev, err := c.KVPut(ctx, "SETTINGS", "key", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.KVUpdate(ctx, "SETTINGS", "key", []byte("two"), rev); err != nil {
		t.Fatal(err)
	}
	entry, err := c.KVGet(ctx, "SETTINGS", "key")
	if err != nil || string(entry.Value) != "two" {
		t.Fatalf("get=%+v %v", entry, err)
	}
	if _, err = c.KVGetRevision(ctx, "SETTINGS", "key", rev); err != nil {
		t.Fatal(err)
	}
	if entries, err := c.KVHistory(ctx, "SETTINGS", "key"); err != nil || len(entries) != 2 {
		t.Fatalf("history=%d %v", len(entries), err)
	}
	if keys, err := c.KVKeys(ctx, "SETTINGS"); err != nil || len(keys) != 1 {
		t.Fatalf("keys=%v %v", keys, err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err = c.KVPut(canceled, "SETTINGS", "key", []byte("three")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write=%v", err)
	}
}
func TestConnectCancellationInterruptsHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	cfg := DefaultConfig()
	cfg.URLs = []string{"nats://" + listener.Addr().String()}
	c := New(cfg)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Connect(ctx) }()
	conn := <-accepted
	defer conn.Close()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel handshake=%v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("startup handshake ignored cancellation")
	}
}

func TestRequestHonorsCancellationWithoutDeadline(t *testing.T) {
	c := localClient(t, false)
	received := make(chan struct{}, 1)
	sub, err := c.Subscribe("request.cancel", func(*gonats.Msg) { received <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()
	flush, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = c.Ping(flush); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.Request(ctx, "request.cancel", nil); done <- err }()
	select {
	case <-received:
	case <-flush.Done():
		t.Fatal(flush.Err())
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Request error=%v", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("Request ignored cancellation")
	}
}
