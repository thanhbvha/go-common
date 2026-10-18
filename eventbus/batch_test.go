package eventbus

import (
	"context"
	"errors"
	"testing"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

type batchRecordingTransport struct {
	benchmarkTransport
	payloads [][]byte
}

func (t *batchRecordingTransport) Publish(_ context.Context, _ transport.Topology, msg transport.Message) error {
	t.payloads = append(t.payloads, append([]byte(nil), msg.Payload...))
	return nil
}

func TestEnqueueBatchIsBoundedOrderedAndReportsPartialFailure(t *testing.T) {
	backend := &batchRecordingTransport{}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("batch"); err != nil {
		t.Fatal(err)
	}
	jobs := []BatchJob{
		{Payload: 1},
		{Payload: 2, Options: []JobOption{nil}},
		{Payload: 3},
	}
	err = bus.EnqueueBatch(context.Background(), "batch", jobs)
	var batchErr *BatchError
	if !errors.As(err, &batchErr) || batchErr.Index != 1 || !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("EnqueueBatch error = %v", err)
	}
	if len(backend.payloads) != 1 {
		t.Fatalf("published = %d, want only the prefix before failure", len(backend.payloads))
	}

	tooLarge := make([]BatchJob, MaxBatchJobs+1)
	if err := bus.EnqueueBatch(context.Background(), "batch", tooLarge); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("oversized batch = %v", err)
	}
	if err := bus.EnqueueBatch(context.Background(), "batch", nil); !errors.Is(err, ErrInvalidOption) {
		t.Fatalf("empty batch = %v", err)
	}
}

func TestEnqueueBatchPublishesEveryItem(t *testing.T) {
	backend := &batchRecordingTransport{}
	bus, err := Open(DefaultConfig(), WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.DeclareTask("batch"); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnqueueBatch(context.Background(), "batch", []BatchJob{{Payload: 1}, {Payload: 2}, {Payload: 3}}); err != nil {
		t.Fatal(err)
	}
	if len(backend.payloads) != 3 {
		t.Fatalf("published = %d", len(backend.payloads))
	}
}
