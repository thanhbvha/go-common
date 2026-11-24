package pubsub

import (
	"errors"
	"testing"
)

func TestDropLogSuppressesBursts(t *testing.T) {
	var d DropLog
	for range 100 {
		d.Report(errors.New("full"))
	}
	if got := d.suppressed.Load(); got != 99 {
		t.Fatalf("suppressed = %d, want 99 (only the first report logs)", got)
	}
}
