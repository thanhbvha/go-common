package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kgo"
)

type recordProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

// The current settlement mode is synchronous publish-before-commit. A shared
// producer transaction cannot safely include offsets from independent consumer
// sessions. Do not silently treat transactional clients as this fallback.
func validateSettlementProducer(client *kgo.Client) error {
	if client == nil {
		return fmt.Errorf("kafka: producer is nil")
	}
	if client.OptValue(kgo.RequiredAcks) == kgo.NoAck() {
		return fmt.Errorf("%w: kafka EventBus requires broker-acknowledged production", transport.ErrNotSupported)
	}
	if id, _ := client.OptValue(kgo.TransactionalID).(string); id != "" {
		return fmt.Errorf("%w: kafka EventBus transactional settlement is unavailable; use a non-transactional producer", transport.ErrNotSupported)
	}
	return nil
}

func messageRecord(topic string, msg transport.Message) *kgo.Record {
	record := &kgo.Record{Topic: topic, Value: msg.Payload}
	if msg.OrderingKey != "" {
		record.Key = []byte(msg.OrderingKey)
	}
	keys := make([]string, 0, len(msg.Headers))
	for key := range msg.Headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: key, Value: []byte(msg.Headers[key])})
	}
	return record
}

func recordMessage(record *kgo.Record) transport.Message {
	msg := transport.Message{Payload: record.Value, OrderingKey: string(record.Key)}
	if len(record.Headers) > 0 {
		msg.Headers = make(map[string]string, len(record.Headers))
		for _, header := range record.Headers {
			msg.Headers[header.Key] = string(header.Value)
		}
	}
	return msg
}

func (t *kafkaTransport) produceConfirmed(ctx context.Context, record *kgo.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.producerErr != nil {
		return t.producerErr
	}
	if t.producer == nil {
		return fmt.Errorf("kafka: producer is unavailable")
	}
	// Only a successful broker response allows the caller to request a source
	// commit. An ambiguous response leaves the source available for redelivery.
	return t.producer.ProduceSync(ctx, record).FirstErr()
}
