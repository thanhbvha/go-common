package graceful_test

import (
	"context"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/utils/graceful"
)

func TestShutdownLIFO(t *testing.T) {
	var order []int
	sd := graceful.NewShutdown(5 * time.Second)
	sd.Register(func(ctx context.Context) error { order = append(order, 1); return nil })
	sd.Register(func(ctx context.Context) error { order = append(order, 2); return nil })
	sd.Register(func(ctx context.Context) error { order = append(order, 3); return nil })
	sd.Execute()

	// LIFO order: 3 registered last -> runs first
	if len(order) != 3 || order[0] != 3 || order[1] != 2 || order[2] != 1 {
		t.Errorf("expected LIFO [3,2,1], got %v", order)
	}
}

func TestShutdownIsolation(t *testing.T) {
	sd1 := graceful.NewShutdown(5 * time.Second)
	sd2 := graceful.NewShutdown(5 * time.Second)

	ran1, ran2 := false, false
	sd1.Register(func(ctx context.Context) error { ran1 = true; return nil })
	sd2.Register(func(ctx context.Context) error { ran2 = true; return nil })

	sd1.Execute()
	if !ran1 { t.Error("sd1 cleanup should have run") }
	if ran2  { t.Error("sd2 cleanup should NOT have run") }
}
