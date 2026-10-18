package kafka

import (
	"crypto/tls"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNativeClientFactoryPropagatesSecureConsumerConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Brokers = []string{"broker-a:9093", "broker-b:9093"}
	cfg.ClientID = "eventbus-secure"
	cfg.SecurityProtocol = SASLSSL
	cfg.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS13, ServerName: "kafka.internal"}
	cfg.SASLMechanism = SASLSCRAMSHA512
	cfg.SASLUsername = "synthetic-user"
	cfg.SASLPassword = "synthetic-password"
	cfg.DialTimeout = 7 * time.Second
	cfg.ReadTimeout = 11 * time.Second
	cfg.WriteTimeout = 9 * time.Second
	cfg.ConsumerSessionTimeout = 27 * time.Second
	cfg.ConsumerHeartbeat = 4 * time.Second
	cfg.ConsumerFetchMaxWait = 650 * time.Millisecond

	factory, err := NewNativeClientFactory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Mutating the caller's config after construction must not alter clients.
	cfg.Brokers[0] = "mutated:9092"
	cfg.TLSConfig.ServerName = "mutated.invalid"

	client, err := factory.NewConsumer(
		kgo.ConsumerGroup("topology-group"),
		kgo.ConsumeTopics("topic-a", "topic-b"),
		kgo.FetchMaxWait(125*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	assertKafkaOpt(t, client, kgo.SeedBrokers, []string{"broker-a:9093", "broker-b:9093"})
	assertKafkaOpt(t, client, kgo.ClientID, "eventbus-secure")
	assertKafkaOpt(t, client, kgo.DialTimeout, 7*time.Second)
	assertKafkaOpt(t, client, kgo.RequestTimeoutOverhead, 11*time.Second)
	assertKafkaOpt(t, client, kgo.ConsumerGroup, "topology-group")
	gotTopics := client.GetConsumeTopics()
	slices.Sort(gotTopics)
	if !slices.Equal(gotTopics, []string{"topic-a", "topic-b"}) {
		t.Fatalf("consume topics = %#v", gotTopics)
	}
	assertKafkaOpt(t, client, kgo.SessionTimeout, 27*time.Second)
	assertKafkaOpt(t, client, kgo.HeartbeatInterval, 4*time.Second)
	assertKafkaOpt(t, client, kgo.FetchMaxWait, 125*time.Millisecond)
	if got := client.OptValue(kgo.DialTLSConfig); got == nil {
		t.Fatal("TLS config was not applied")
	}
	if got := client.OptValue(kgo.SASL); got == nil || reflect.ValueOf(got).Len() != 1 {
		t.Fatalf("SASL mechanisms = %#v, want one", got)
	}
}

func TestNativeClientFactoryValidatesSASL(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "missing mechanism", mutate: func(cfg *Config) { cfg.SecurityProtocol = SASLSSL }},
		{name: "missing plain password", mutate: func(cfg *Config) {
			cfg.SecurityProtocol = SASLPlaintext
			cfg.SASLMechanism = SASLPlain
			cfg.SASLUsername = "user"
		}},
		{name: "oauth URL without implemented exchange", mutate: func(cfg *Config) {
			cfg.SecurityProtocol = SASLSSL
			cfg.SASLMechanism = SASLOAuthBearer
			cfg.SASLTokenURL = "https://issuer.invalid/token"
		}},
		{name: "invalid protocol", mutate: func(cfg *Config) { cfg.SecurityProtocol = SecurityProtocol(99) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			if _, err := NewNativeClientFactory(cfg); err == nil {
				t.Fatal("factory unexpectedly accepted invalid security config")
			}
		})
	}
}

func TestClientConfigReturnsIndependentCopy(t *testing.T) {
	original := DefaultConfig()
	original.TLSConfig = &tls.Config{ServerName: "kafka.internal", MinVersion: tls.VersionTLS12}
	client := New(original)
	copyConfig := client.Config()
	copyConfig.Brokers[0] = "mutated:9092"
	copyConfig.TLSConfig.ServerName = "mutated.invalid"
	again := client.Config()
	if again.Brokers[0] != original.Brokers[0] || again.TLSConfig.ServerName != "kafka.internal" {
		t.Fatalf("client config was mutated through returned copy: %#v", again)
	}
}

func assertKafkaOpt(t *testing.T, client *kgo.Client, option any, want any) {
	t.Helper()
	if got := client.OptValue(option); !reflect.DeepEqual(got, want) {
		t.Fatalf("option %v = %#v (%T), want %#v (%T)", option, got, got, want, want)
	}
}

func TestProducerIdempotencyAndAcks(t *testing.T) {
	for _, idempotent := range []bool{false, true} {
		for _, acks := range []int{-2, -1, 0, 1, 2} {
			t.Run(fmt.Sprintf("idempotent=%t/acks=%d", idempotent, acks), func(t *testing.T) {
				cfg := DefaultConfig()
				cfg.ProducerIdempotent, cfg.ProducerAcks = idempotent, acks
				factory, err := NewNativeClientFactory(cfg)
				invalid := acks < -1 || acks > 1 || (idempotent && acks != -1)
				if invalid {
					if err == nil {
						t.Fatal("invalid ACK/idempotency combination accepted")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				client, err := factory.NewProducer()
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				assertKafkaOpt(t, client, kgo.DisableIdempotentWrite, !idempotent)
			})
		}
	}
}

func TestClientSnapshotsInputBeforeBuildingOptions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SecurityProtocol = SSL
	cfg.TLSConfig = &tls.Config{ServerName: "original.invalid", MinVersion: tls.VersionTLS13}
	wrapper := New(cfg)
	defer wrapper.Close()
	cfg.Brokers[0] = "mutated.invalid:9092"
	cfg.TLSConfig.ServerName = "mutated.invalid"
	if got := wrapper.Config(); got.Brokers[0] != "localhost:9092" || got.TLSConfig.ServerName != "original.invalid" {
		t.Fatal("input mutation changed wrapper snapshot")
	}
	opts, err := wrapper.buildOptions()
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	factory, err := NewNativeClientFactory(wrapper.Config())
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := factory.NewConsumer(kgo.ConsumerGroup("snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	for _, client := range []*kgo.Client{producer, consumer} {
		assertKafkaOpt(t, client, kgo.SeedBrokers, []string{"localhost:9092"})
		tlsConfig := client.OptValue(kgo.DialTLSConfig).(*tls.Config)
		if tlsConfig.ServerName != "original.invalid" {
			t.Fatal("producer/consumer TLS snapshot diverged")
		}
	}
}

func TestFactoryManualCommitConsumerStillConstructs(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ConsumerCommitInterval = 0
	factory, err := NewNativeClientFactory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := factory.NewConsumer(kgo.ConsumerGroup("manual"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	assertKafkaOpt(t, client, kgo.DisableAutoCommit, true)
	assertKafkaOpt(t, client, kgo.AutoCommitMarks, false)
}

func TestFactoryExplicitManualConsumerOverridesAutoCommitConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ConsumerCommitInterval <= 0 {
		t.Fatal("test requires automatic commits in the input config")
	}
	factory, err := NewNativeClientFactory(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := factory.NewManualConsumer(kgo.ConsumerGroup("coordinated"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	assertKafkaOpt(t, client, kgo.DisableAutoCommit, true)
	assertKafkaOpt(t, client, kgo.AutoCommitMarks, false)
	assertKafkaOpt(t, client, kgo.SessionTimeout, cfg.ConsumerSessionTimeout)
}
