package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNoopManagerWaitReady(t *testing.T) {
	manager := NewNoopManager()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestManagerClosedErrorIsStable(t *testing.T) {
	if !errors.Is(ErrManagerClosed, ErrManagerClosed) {
		t.Fatal("closed error is not stable")
	}
}
