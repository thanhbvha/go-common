package eventbus

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type publicationDedupeTransport struct {
	benchmarkTransport
	mu               sync.Mutex
	state            transport.DedupeReservationState
	owner            string
	publishErr       error
	abortErr         error
	confirmErr       error
	abortHasDeadline bool
	publishes        int
}

func (t *publicationDedupeTransport) Publish(context.Context, transport.Topology, transport.Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.publishes++
	return t.publishErr
}

func (t *publicationDedupeTransport) BeginPublication(_ context.Context, _ transport.Topology, _, reservationID string, _ time.Duration) (transport.DedupeReservationState, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == 0 {
		t.state = transport.DedupeReservationPending
		t.owner = reservationID
		return transport.DedupeReservationAcquired, nil
	}
	return t.state, nil
}

func (t *publicationDedupeTransport) ConfirmPublication(ctx context.Context, _ transport.Topology, _, reservationID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, t.abortHasDeadline = ctx.Deadline()
	if t.confirmErr != nil {
		return t.confirmErr
	}
	if t.owner != reservationID || t.state != transport.DedupeReservationPending {
		return transport.ErrDedupeAmbiguous
	}
	t.state = transport.DedupeReservationPublished
	return nil
}

func (t *publicationDedupeTransport) AbortPublication(ctx context.Context, _ transport.Topology, _, reservationID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, t.abortHasDeadline = ctx.Deadline()
	if t.abortErr != nil {
		return t.abortErr
	}
	if t.owner != reservationID || t.state != transport.DedupeReservationPending {
		return transport.ErrDedupeAmbiguous
	}
	t.state = 0
	t.owner = ""
	return nil
}

func openPublicationDedupeBus(t *testing.T, backend *publicationDedupeTransport) Bus {
	t.Helper()
	b, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close(context.Background()) })
	if err := b.DeclareTask("task"); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDedupePublishAndCleanupFailureNeverBecomesFalseSuccess(t *testing.T) {
	publishErr := errors.New("publish rejected before acceptance")
	backend := &publicationDedupeTransport{publishErr: publishErr, abortErr: errors.New("cleanup unavailable")}
	b := openPublicationDedupeBus(t, backend)

	err := b.Enqueue(context.Background(), "task", 1, WithDedupeKey("same"))
	if !errors.Is(err, publishErr) || !errors.Is(err, ErrDedupeAmbiguous) {
		t.Fatalf("first publish error = %v", err)
	}
	if !backend.abortHasDeadline {
		t.Fatal("dedupe cleanup did not receive a bounded context")
	}

	backend.publishErr = nil
	err = b.Enqueue(context.Background(), "task", 2, WithDedupeKey("same"))
	if !errors.Is(err, ErrDedupeAmbiguous) {
		t.Fatalf("retry error = %v, want ErrDedupeAmbiguous", err)
	}
	if backend.publishes != 1 {
		t.Fatalf("retry published through an unconfirmed reservation: publishes=%d", backend.publishes)
	}
}

func TestDedupeConfirmedPublicationIsTheOnlySuccessfulDuplicate(t *testing.T) {
	backend := &publicationDedupeTransport{}
	b := openPublicationDedupeBus(t, backend)
	if err := b.Enqueue(context.Background(), "task", 1, WithDedupeKey("same")); err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(context.Background(), "task", 2, WithDedupeKey("same")); err != nil {
		t.Fatalf("confirmed duplicate: %v", err)
	}
	if backend.state != transport.DedupeReservationPublished || backend.publishes != 1 {
		t.Fatalf("state=%d publishes=%d", backend.state, backend.publishes)
	}
}

func TestDedupeConfirmationFailureRemainsObservable(t *testing.T) {
	backend := &publicationDedupeTransport{confirmErr: errors.New("confirmation response lost")}
	b := openPublicationDedupeBus(t, backend)
	err := b.Enqueue(context.Background(), "task", 1, WithDedupeKey("same"))
	if !errors.Is(err, ErrDedupeAmbiguous) {
		t.Fatalf("confirmation error = %v, want ErrDedupeAmbiguous", err)
	}
	if !backend.abortHasDeadline || backend.publishes != 1 {
		t.Fatalf("bounded=%v publishes=%d", backend.abortHasDeadline, backend.publishes)
	}
}
