package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/websocket/routing"
)

type heldPublisher struct {
	entered, release chan struct{}
	calls            atomic.Int32
}

func (p *heldPublisher) PublishCommand(context.Context, routing.BusinessCommand) error {
	p.calls.Add(1)
	close(p.entered)
	<-p.release
	return nil
}
func TestCommandIntakeStopsAdmissionAndWaitsForAcceptedPublication(t *testing.T) {
	next := &heldPublisher{entered: make(chan struct{}), release: make(chan struct{})}
	p := &commandIntake{next: next, done: make(chan struct{})}
	completed := make(chan error, 1)
	go func() { completed <- p.PublishCommand(context.Background(), routing.BusinessCommand{}) }()
	<-next.entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		close(next.release)
		t.Fatal("stop did not wait for accepted publication")
	}
	if err := p.PublishCommand(context.Background(), routing.BusinessCommand{}); !errors.Is(err, routing.ErrUnavailable) {
		close(next.release)
		t.Fatal("stopped intake accepted publication")
	}
	close(next.release)
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	if err := p.stop(context.Background()); err != nil || next.calls.Load() != 1 {
		t.Fatal("accepted publication did not finish exactly once")
	}
}
