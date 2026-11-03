package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestConnectionDrainFlushesAcceptedMessagesAndRejectsNewOnes(t *testing.T) {
	tr := newMockConn()
	tr.writeGate = make(chan struct{})
	c, shard, _ := newRunningTestConnection(t, tr)
	for _, message := range []string{"one", "two", "three"} {
		if !c.Send([]byte(message)) {
			t.Fatal("enqueue")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- shard.Shutdown(ctx) }()
	waitForCondition(t, c.draining.Load)
	if c.Send([]byte("late")) {
		t.Fatal("drain accepted new outbound work")
	}
	close(tr.writeGate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"one", "two", "three"} {
		select {
		case frame := <-tr.writes:
			if frame.messageType != websocket.TextMessage || string(frame.data) != expected {
				t.Fatalf("frame=%+v expected=%s", frame, expected)
			}
		case <-ctx.Done():
			t.Fatal("accepted message lost")
		}
	}
	metrics := shard.config.Observer.(SnapshotObserver).RuntimeMetrics()
	if metrics.ConnectionsDrained != 1 || metrics.DrainForced != 0 || metrics.DrainDiscarded != 0 {
		t.Fatalf("drain metrics: %+v", metrics)
	}
}

func TestConnectionDrainDeadlineForcesCloseAndCountsUnwrittenMessages(t *testing.T) {
	tr := newMockConn()
	tr.writeGate = make(chan struct{})
	c, shard, _ := newRunningTestConnection(t, tr)
	if !c.Send([]byte("one")) || !c.Send([]byte("two")) {
		t.Fatal("enqueue")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := c.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain=%v", err)
	}
	waitConnection(t, c)
	metrics := shard.config.Observer.(SnapshotObserver).RuntimeMetrics()
	if metrics.DrainForced != 1 || metrics.DrainDiscarded != 2 {
		t.Fatalf("lost queue not counted: %+v", metrics)
	}
}

func TestManagerShutdownDrainsAcceptedOutbound(t *testing.T) {
	cfg := DefaultManagerConfig()
	cfg.DisableLifecycleLogging = true
	m := NewManagerWithConfig(cfg)
	tr := newMockConn()
	tr.writeGate = make(chan struct{})
	handler := make(chan error, 1)
	go func() { handler <- m.HandleConnection(tr, "", "u", "", "") }()
	waitForCondition(t, func() bool { return m.GetTotalConnections() == 1 })
	shard, _ := m.GetShard(defaultShardName)
	shard.SendToUser("u", []byte("accepted"))
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { done <- m.Shutdown(ctx) }()
	waitForCondition(t, func() bool { return m.State() == StateDraining })
	close(tr.writeGate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-handler; err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-tr.writes:
		if string(frame.data) != "accepted" {
			t.Fatalf("frame=%q", frame.data)
		}
	case <-ctx.Done():
		t.Fatal("manager dropped accepted message")
	}
}
