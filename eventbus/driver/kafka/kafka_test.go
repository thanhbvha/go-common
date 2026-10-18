//go:build integration

package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
	commonkafka "github.com/thanhbvha/go-common/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewWithClientSnapshotAndManualCommitPolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	probe, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if err := probe.Ping(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	for _, interval := range []time.Duration{0, time.Second} {
		cfg := commonkafka.DefaultConfig()
		cfg.ConsumerCommitInterval = interval
		wrapper := commonkafka.New(cfg)
		if err := wrapper.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		defer wrapper.Close()
		cfg.Brokers[0] = "mutated.invalid:9092"
		backend, err := NewWithClient(wrapper)
		if err != nil {
			t.Fatal(err)
		}
		consumer, err := backend.(*kafkaTransport).newManualConsumer(kgo.ConsumerGroup("snapshot-review"))
		if err != nil {
			t.Fatal(err)
		}
		defer consumer.Close()
		for _, client := range []*kgo.Client{wrapper.Native(), consumer} {
			brokers := client.OptValue(kgo.SeedBrokers).([]string)
			if len(brokers) != 1 || brokers[0] != "localhost:9092" {
				t.Fatal("client snapshots diverged")
			}
		}
	}
}

func TestKafkaTransport_PublishAndConsume(t *testing.T) {
	// Create base kgo.Client
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	// Need a quick ping to see if kafka is available
	if err := cl.Ping(context.Background()); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	trans := New(cl, []string{"localhost:9092"})
	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: fmt.Sprintf("testjob_kafka_%d", time.Now().UnixNano())}

	err = trans.EnsureTopology(ctx, topo)
	if err != nil {
		t.Fatalf("EnsureTopology: %v", err)
	}

	// Wait a moment for topic creation to propagate
	time.Sleep(1 * time.Second)

	// Publish
	err = trans.Publish(ctx, topo, transport.Message{
		Payload: []byte("hello kafka"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Consume
	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	received := make(chan transport.Delivery)

	go func() {
		err := trans.Consume(consumeCtx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			received <- d
		})
		if err != nil && err != context.Canceled {
			t.Errorf("Consume error: %v", err)
		}
	}()

	select {
	case d := <-received:
		msg := d.Message()
		if string(msg.Payload) != "hello kafka" {
			t.Fatalf("Expected payload, got %s", msg.Payload)
		}
		// Ack
		err = d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck})
		if err != nil {
			t.Fatalf("Ack: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("Timeout waiting for message")
	}

	trans.Close(ctx)
}

func TestKafkaTransport_Delay(t *testing.T) {
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	defer cl.Close()

	if err := cl.Ping(context.Background()); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	trans := New(cl, []string{"localhost:9092"})
	ctx := context.Background()
	topo := transport.Topology{Namespace: "testns", JobType: fmt.Sprintf("testjob_delay_%d", time.Now().UnixNano())}

	trans.EnsureTopology(ctx, topo)
	time.Sleep(1 * time.Second)

	// The durable tier scheduler reads RunAt from the envelope, so publish a
	// complete envelope rather than an arbitrary payload.

	runAt := time.Now().Add(3 * time.Second)
	payload := `{"id":"123","type":"testjob_kafka_delay","payload":"\"hello\"","run_at":` + fmt.Sprintf("%d", runAt.UnixMilli()) + `}`

	err = trans.PublishAt(ctx, topo, transport.Message{
		Payload: []byte(payload),
	}, runAt)

	if err != nil {
		t.Fatalf("PublishAt: %v", err)
	}

	consumeCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	received := make(chan transport.Delivery)

	go func() {
		_ = trans.Consume(consumeCtx, transport.Subscription{Topology: topo}, func(d transport.Delivery) {
			received <- d
		})
	}()

	select {
	case d := <-received:
		if earlyBy := time.Until(runAt); earlyBy > 100*time.Millisecond {
			t.Fatalf("Message received %v before its absolute deadline", earlyBy)
		}
		// Ack
		d.Settle(ctx, transport.Decision{Kind: transport.DecisionAck})
	case <-time.After(8 * time.Second):
		t.Fatalf("Timeout waiting for delayed message")
	}

	trans.Close(ctx)
}

func TestKafkaTransport_PartitionProcessing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := cl.Ping(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}
	backend := New(cl, nil).(*kafkaTransport)
	topo := transport.Topology{Namespace: "testns", JobType: fmt.Sprintf("partition_%d", time.Now().UnixNano())}
	responses, err := backend.adm.CreateTopics(ctx, 2, 1, nil, topo.Name())
	if err != nil {
		t.Fatal(err)
	}
	for _, response := range responses {
		if response.Err != nil {
			t.Fatal(response.Err)
		}
	}
	defer backend.adm.DeleteTopics(context.Background(), topo.Name())
	for partition := int32(0); partition < 2; partition++ {
		for i := 0; i < 2; i++ {
			if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topo.Name(), Partition: partition, Value: []byte(fmt.Sprintf("p%d-%d", partition, i))}).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	consumeCtx, stop := context.WithCancel(ctx)
	received := make(chan *kafkaDelivery, 4)
	done := make(chan error, 1)
	go func() {
		done <- backend.Consume(consumeCtx, transport.Subscription{Topology: topo, Concurrency: 2, Prefetch: 2}, func(d transport.Delivery) { received <- d.(*kafkaDelivery) })
	}()
	defer func() { stop(); <-done }()
	receive := func() *kafkaDelivery {
		t.Helper()
		select {
		case delivery := <-received:
			return delivery
		case <-ctx.Done():
			t.Fatal("timed out waiting for partition delivery")
			return nil
		}
	}
	counts := make(map[int32]int)
	for range 4 {
		delivery := receive()
		partition := delivery.record.Partition
		want := fmt.Sprintf("p%d-%d", partition, counts[partition])
		if string(delivery.Message().Payload) != want {
			t.Fatal("partition processing order violated")
		}
		counts[partition]++
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
	}
	if counts[0] != 2 || counts[1] != 2 {
		t.Fatal("not all partitions processed")
	}
}

func TestKafkaOffsetCoordinator_ContiguousBrokerCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.Ping(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	backend := New(producer, nil).(*kafkaTransport)
	topic := fmt.Sprintf("eventbus-p53-%d", time.Now().UnixNano())
	group := topic + "-group"
	created, err := backend.adm.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, response := range created {
		if response.Err != nil {
			t.Fatal(response.Err)
		}
	}
	defer backend.adm.DeleteTopics(context.Background(), topic)

	for i := range 2 {
		if err := producer.ProduceSync(ctx, &kgo.Record{
			Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("record-%d", i)),
		}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	opts := mainConsumerOptions(topic, group, transport.Topology{})
	opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	consumer, err := backend.newManualConsumer(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.CloseAllowingRebalance()

	var records []*kgo.Record
	for len(records) < 2 {
		fetches := consumer.PollRecords(ctx, 2-len(records))
		if err := fetches.Err(); err != nil {
			t.Fatal(err)
		}
		records = append(records, fetches.Records()...)
	}
	low, high := records[0], records[1]
	if low.Partition != high.Partition || low.Offset >= high.Offset {
		t.Fatalf("unexpected fetched offsets: %d/%d partitions %d/%d", low.Offset, high.Offset, low.Partition, high.Partition)
	}

	coordinator := newOffsetCoordinator(consumer, 2)
	defer coordinator.close()
	if err := coordinator.register(ctx, records); err != nil {
		t.Fatal(err)
	}
	highRequest := &offsetCommitRequest{ctx: ctx, record: high, result: make(chan error, 1)}
	select {
	case coordinator.commands <- highRequest:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A subsequent registration is a barrier proving the coordinator has
	// observed the higher completion before the broker offset is inspected.
	if err := coordinator.register(ctx, nil); err != nil {
		t.Fatal(err)
	}

	committedOffset := func() int64 {
		t.Helper()
		offsets, err := backend.adm.FetchOffsetsForTopics(ctx, group, topic)
		if err != nil {
			t.Fatal(err)
		}
		offset, ok := offsets.Lookup(topic, 0)
		if !ok || offset.Err != nil {
			t.Fatalf("committed offset lookup: found=%t error=%v", ok, offset.Err)
		}
		return offset.At
	}
	if got := committedOffset(); got != -1 {
		t.Fatalf("higher settled offset advanced broker commit to %d before lower offset settled", got)
	}

	if err := coordinator.settle(ctx, low); err != nil {
		t.Fatal(err)
	}
	if err := <-highRequest.result; err != nil {
		t.Fatal(err)
	}
	if got, want := committedOffset(), high.Offset+1; got != want {
		t.Fatalf("broker committed offset = %d, want highest contiguous offset %d", got, want)
	}
}

func TestKafkaRebalanceDrainsAndTransfersPartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	producer, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.Ping(ctx); err != nil {
		t.Skipf("Kafka not available: %v", err)
	}

	backend := New(producer, nil).(*kafkaTransport)
	topic := fmt.Sprintf("eventbus-p54-%d", time.Now().UnixNano())
	group := topic + "-group"
	created, err := backend.adm.CreateTopics(ctx, 2, 1, nil, topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, response := range created {
		if response.Err != nil {
			t.Fatal(response.Err)
		}
	}
	defer backend.adm.DeleteTopics(context.Background(), topic)
	produce := func(record *kgo.Record) error {
		for {
			err := producer.ProduceSync(ctx, record).FirstErr()
			if err == nil {
				return nil
			}
			if !errors.Is(err, kerr.UnknownTopicOrPartition) && !errors.Is(err, kerr.LeaderNotAvailable) {
				return err
			}
			producer.ForceMetadataRefresh()
			timer := time.NewTimer(25 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	for partition := int32(0); partition < 2; partition++ {
		for offset := range 2 {
			if err := produce(&kgo.Record{
				Topic: topic, Partition: partition, Value: []byte(fmt.Sprintf("p%d-%d", partition, offset)),
			}); err != nil {
				t.Fatal(err)
			}
		}
	}

	type session struct {
		client      *kgo.Client
		coordinator *offsetCoordinator
		cancel      context.CancelFunc
		done        chan error
		deliveries  chan *kafkaDelivery
	}
	startSession := func(onBlocked func()) *session {
		t.Helper()
		rebalances := newRebalanceController()
		rebalances.onBlocked = onBlocked
		opts := mainConsumerOptions(topic, group, transport.Topology{})
		opts = append(opts,
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.Balancers(kgo.RangeBalancer()),
		)
		opts = append(opts, rebalances.options()...)
		client, err := backend.newManualConsumer(opts...)
		if err != nil {
			t.Fatal(err)
		}
		coordinator := newOffsetCoordinator(client, 4)
		rebalances.bind(coordinator)
		sessionCtx, stop := context.WithCancel(ctx)
		s := &session{
			client: client, coordinator: coordinator, cancel: stop,
			done: make(chan error, 1), deliveries: make(chan *kafkaDelivery, 4),
		}
		go func() {
			s.done <- backend.consumeRecords(sessionCtx, transport.Topology{}, client, coordinator, rebalances, 4,
				func(delivery transport.Delivery) { s.deliveries <- delivery.(*kafkaDelivery) })
		}()
		return s
	}
	stopSession := func(s *session) {
		s.cancel()
		if err := <-s.done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("consume session: %v", err)
		}
		s.client.CloseAllowingRebalance()
		s.coordinator.close()
	}

	blocked := make(chan struct{}, 1)
	first := startSession(func() {
		select {
		case blocked <- struct{}{}:
		default:
		}
	})
	defer stopSession(first)
	initial := make([]*kafkaDelivery, 0, 2)
	for len(initial) < 2 {
		select {
		case delivery := <-first.deliveries:
			initial = append(initial, delivery)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}

	second := startSession(nil)
	defer stopSession(second)
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("rebalance did not block on the first member's in-flight work")
	}
	select {
	case delivery := <-first.deliveries:
		t.Fatalf("first member admitted %q after rebalance was requested", delivery.Message().Payload)
	case delivery := <-second.deliveries:
		t.Fatalf("second member received %q before the first member drained", delivery.Message().Payload)
	default:
	}
	for _, delivery := range initial {
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
	}

	seen := make(map[string]bool)
	secondReceived := false
	for len(seen) < 2 {
		var delivery *kafkaDelivery
		select {
		case delivery = <-first.deliveries:
		case delivery = <-second.deliveries:
			secondReceived = true
		case <-ctx.Done():
			t.Fatal("timed out waiting for post-rebalance records")
		}
		payload := string(delivery.Message().Payload)
		if seen[payload] {
			t.Fatalf("duplicate post-rebalance delivery %q", payload)
		}
		seen[payload] = true
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
	}
	if !seen["p0-1"] || !seen["p1-1"] {
		t.Fatalf("post-rebalance records = %#v", seen)
	}
	if !secondReceived {
		t.Fatal("new group member did not receive an assigned partition")
	}
}
