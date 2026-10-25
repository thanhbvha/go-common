package transport

import (
	"errors"
	"sync/atomic"
	"testing"
)

func TestSettlementGuardIsSingleUse(t *testing.T) {
	var guard SettlementGuard
	var calls atomic.Int32
	decision := Decision{Kind: DecisionAck}

	if err := guard.Settle(decision, func() error {
		calls.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("first settlement: %v", err)
	}
	if err := guard.Settle(decision, func() error {
		calls.Add(1)
		return nil
	}); !errors.Is(err, ErrDeliverySettled) {
		t.Fatalf("second settlement error = %v, want ErrDeliverySettled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("operation calls = %d, want 1", got)
	}
}

func TestSettlementGuardFailureCanBeRetried(t *testing.T) {
	var guard SettlementGuard
	wantErr := errors.New("broker unavailable")
	decision := Decision{Kind: DecisionRetry}

	if err := guard.Settle(decision, func() error { return wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("failed settlement error = %v, want %v", err, wantErr)
	}
	if err := guard.Settle(decision, func() error { return nil }); err != nil {
		t.Fatalf("retry settlement: %v", err)
	}
	if err := guard.Settle(decision, func() error { return nil }); !errors.Is(err, ErrDeliverySettled) {
		t.Fatalf("post-success settlement error = %v, want ErrDeliverySettled", err)
	}
}

func TestSettlementGuardRejectsConcurrentSettlement(t *testing.T) {
	var guard SettlementGuard
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	decision := Decision{Kind: DecisionDeadLetter}

	go func() {
		done <- guard.Settle(decision, func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	if err := guard.Settle(decision, func() error { return nil }); !errors.Is(err, ErrSettlementInProgress) {
		t.Fatalf("concurrent settlement error = %v, want ErrSettlementInProgress", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("original settlement: %v", err)
	}
}

func TestSettlementGuardRejectsInvalidDecisionWithoutClosing(t *testing.T) {
	var guard SettlementGuard
	if err := guard.Settle(Decision{}, func() error { return nil }); !errors.Is(err, ErrInvalidDecision) {
		t.Fatalf("invalid decision error = %v, want ErrInvalidDecision", err)
	}
	if err := guard.Settle(Decision{Kind: DecisionAck}, func() error { return nil }); err != nil {
		t.Fatalf("valid settlement after invalid decision: %v", err)
	}
}
