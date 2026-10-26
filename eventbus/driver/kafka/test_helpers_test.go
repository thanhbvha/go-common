package kafka

import (
	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newTestTransport(base *kgo.Client, brokers []string) transport.Transport {
	overrides := make([]kgo.Opt, 0, 1)
	if len(brokers) > 0 {
		overrides = append(overrides, kgo.SeedBrokers(brokers...))
	}
	factory, _ := commonkafka.NewNativeClientFactoryFromClient(base, overrides...)
	return newKafkaTransport(base, factory, false)
}
