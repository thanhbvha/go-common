//go:build integration

package kafka

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	bus "github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProvisioningBypassesCachedMissingLayoutTopic(t *testing.T) {
	if os.Getenv("WS_KAFKA_INTEGRATION") != "1" {
		t.Skip("set WS_KAFKA_INTEGRATION=1 and WS_KAFKA_BROKERS")
	}
	brokers := strings.Split(os.Getenv("WS_KAFKA_BROKERS"), ",")
	if brokers[0] == "" {
		t.Fatal("WS_KAFKA_BROKERS is required")
	}
	// Keep a negative lookup fresh for the entire test unless provisioning bypasses it.
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.MetadataMinAge(30*time.Minute), kgo.MetadataMaxAge(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	backend := newTestTransport(client, brokers).(*kafkaTransport)
	namespace := "ws_metadata_" + uuid.NewString()
	topology := transport.Topology{Namespace: namespace, Kind: transport.EventTopology, JobType: "fresh"}
	layout := backend.eventLayoutTopic(topology)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := backend.adm.DeleteTopics(cleanup, backend.mainTopic(topology), backend.dlqTopic(topology), layout, backend.dlqAdminLockTopic(topology))
		if err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	metadata, err := backend.adm.Metadata(ctx, layout)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(metadata.Topics[layout].Err, kerr.UnknownTopicOrPartition) {
		t.Fatalf("test must seed a missing topic lookup: %+v", metadata.Topics[layout])
	}
	cfg := bus.DefaultConfig()
	cfg.Namespace = namespace
	b, err := bus.Open(cfg, bus.WithTransport(backend))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close(context.Background())
	if err = b.DeclareEvent("fresh", WithEventLayoutTargeted(), bus.WithDelayDisabled()); err != nil {
		t.Fatal(err)
	}
	startup, stop := context.WithTimeout(ctx, 3*time.Second)
	defer stop()
	if err = b.Start(startup); err != nil {
		t.Fatalf("cached negative metadata blocked startup: %v", err)
	}
}
