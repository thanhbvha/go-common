package kafka

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
	"time"
)

type heldProducer struct{ callbacks chan func(*kgo.Record, error) }

func (p heldProducer) Produce(_ context.Context, _ *kgo.Record, cb func(*kgo.Record, error)) {
	p.callbacks <- cb
}
func TestProduceBatchCancellationReturnsStableSnapshot(t *testing.T) {
	p := heldProducer{callbacks: make(chan func(*kgo.Record, error), 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan []ProduceResult, 1)
	go func() {
		results, err := produceBatch(ctx, p, []Message{{Topic: "jobs"}, {Topic: "jobs"}})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error=%v", err)
		}
		done <- results
	}()
	first, second := <-p.callbacks, <-p.callbacks
	first(&kgo.Record{Topic: "jobs", Offset: 11}, nil)
	cancel()
	var results []ProduceResult
	select {
	case results = <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation blocked")
	}
	before := append([]ProduceResult(nil), results...)
	second(&kgo.Record{Topic: "jobs", Offset: 12}, nil)
	for i := range results {
		if results[i] != before[i] {
			t.Fatalf("result mutated after return: %+v -> %+v", before, results)
		}
	}
}

func TestMessageToRecordOwnsCallerBytes(t *testing.T) {
	message := Message{
		Topic:   "jobs",
		Key:     []byte("key"),
		Value:   []byte("payload"),
		Headers: []Header{{Key: "trace", Value: []byte("header")}, {Key: "empty", Value: nil}},
	}
	record := messageToRecord(message)
	message.Key[0] = 'K'
	message.Value[0] = 'P'
	message.Headers[0].Value[0] = 'H'
	message.Headers[0].Key = "changed"
	if string(record.Key) != "key" || string(record.Value) != "payload" || record.Headers[0].Key != "trace" || string(record.Headers[0].Value) != "header" {
		t.Fatalf("native record aliases caller memory: key=%q value=%q headers=%v", record.Key, record.Value, record.Headers)
	}
	if record.Headers[1].Value != nil {
		t.Fatal("nil header value was not preserved")
	}
	empty := messageToRecord(Message{})
	if empty.Key != nil || empty.Value != nil {
		t.Fatal("nil payload slices were not preserved")
	}
}
