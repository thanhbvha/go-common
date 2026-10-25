//go:build integration

package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/goccy/go-json"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestNATSDLQAdminPreservesMetadataAndRequeues(t *testing.T) {
	_, backend, topo, _, delivery, stop := settlementFixture(t, "dlq_admin")
	failedAt := time.UnixMilli(1700000000001)
	source := transport.Message{
		ID: "logical-id", Type: topo.JobType, Payload: []byte(`{"id":"logical-id","type":"job"}`),
		OrderingKey: "account-1", DedupeKey: "dedupe-1", Headers: map[string]string{"correlation-id": "corr-1"},
	}
	decision := transport.Decision{
		Kind: transport.DecisionDeadLetter, Source: &source, Reason: "permanent_error",
		Cause: errors.New("synthetic failure"), ErrorCode: "synthetic_failure", Attempts: 3, FailedAt: failedAt,
	}
	if err := delivery.Settle(context.Background(), decision); err != nil {
		stop()
		t.Fatal(err)
	}
	stop()

	admin := transport.DLQAdmin(backend)
	if count, err := admin.Count(context.Background(), topo); err != nil || count != 1 {
		t.Fatalf("DLQ count = %d, %v", count, err)
	}
	records, err := admin.List(context.Background(), topo, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(records))
	}
	record := records[0]
	if record.ID == "" || record.Message.ID != source.ID || record.Message.Type != source.Type ||
		record.Message.OrderingKey != source.OrderingKey || record.Message.DedupeKey != source.DedupeKey ||
		record.Message.Headers["correlation-id"] != "corr-1" || string(record.Message.Payload) != string(source.Payload) {
		t.Fatalf("DLQ message = %#v", record.Message)
	}
	if record.Reason != decision.Reason || record.LastError != decision.ErrorCode ||
		record.Attempts != decision.Attempts || !record.FailedAt.Equal(failedAt) {
		t.Fatalf("DLQ failure metadata = %#v", record)
	}
	if record.Source != backend.streamName(topo) || record.SourceGroup == "" || record.SourcePosition == "" {
		t.Fatalf("DLQ source metadata = %#v", record)
	}

	page, err := backend.ListPage(context.Background(), topo, "", 1)
	if err != nil || len(page.Records) != 1 || page.NextCursor != "" {
		t.Fatalf("DLQ page = %#v, %v", page, err)
	}
	if err := admin.Requeue(context.Background(), topo, []string{record.ID, record.ID}); err != nil {
		t.Fatal(err)
	}
	if err := admin.Requeue(context.Background(), topo, []string{record.ID}); err != nil {
		t.Fatal(err)
	}
	if count, err := admin.Count(context.Background(), topo); err != nil || count != 0 {
		t.Fatalf("DLQ count after requeue = %d, %v", count, err)
	}

	requeued, stopRequeued := receiveNATSDelivery(t, backend, topo)
	if requeued.Message().ID != source.ID || string(requeued.Message().Payload) != string(source.Payload) {
		stopRequeued()
		t.Fatalf("requeued message = %#v", requeued.Message())
	}
	if err := requeued.Settle(context.Background(), transport.Decision{
		Kind: transport.DecisionDeadLetter, Reason: "purge-test", Attempts: 1,
	}); err != nil {
		stopRequeued()
		t.Fatal(err)
	}
	stopRequeued()

	if count, err := admin.Count(context.Background(), topo); err != nil || count != 1 {
		t.Fatalf("DLQ count before purge = %d, %v", count, err)
	}
	if err := admin.Purge(context.Background(), topo); err != nil {
		t.Fatal(err)
	}
	if count, err := admin.Count(context.Background(), topo); err != nil || count != 0 {
		t.Fatalf("DLQ count after purge = %d, %v", count, err)
	}
}

func TestNATSDLQRecordSanitizesMetadataAndBoundsSize(t *testing.T) {
	_, _, _, _, delivery, stop := settlementFixture(t, "dlq_safety")
	defer stop()
	message := transport.Message{Payload: []byte(`{"v":3}`)}
	payload, err := delivery.deadLetterRecord(message, transport.Decision{Reason: "contains secret", ErrorCode: "also secret", Cause: hostileNATSError{}})
	if err != nil {
		t.Fatal(err)
	}
	var record dlqRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		t.Fatal(err)
	}
	if record.Reason != "delivery_failure" || record.LastError != "delivery_error" {
		t.Fatalf("metadata = %q/%q", record.Reason, record.LastError)
	}
	message.Payload = make([]byte, transport.MaxEncodedDLQRecordBytes+1)
	if _, err := delivery.deadLetterRecord(message, transport.Decision{Reason: "failure"}); err == nil {
		t.Fatal("oversized DLQ record unexpectedly encoded")
	}
}

func TestEventDLQRequeueTargetsSourceSubscriberAndFailsClosed(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := targetedEventTopology(fmt.Sprintf("event_dlq_%d", time.Now().UnixNano()), "event")
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	alpha := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	beta := transport.Subscription{Topology: topo, Group: "beta", Concurrency: 1, Prefetch: 1}
	for _, sub := range []transport.Subscription{alpha, beta} {
		if err := backend.EnsureSubscription(ctx, sub); err != nil {
			t.Fatal(err)
		}
	}
	if err := backend.Publish(ctx, topo, transport.Message{ID: "source", Payload: []byte("source")}); err != nil {
		t.Fatal(err)
	}
	alphaDelivery, stopAlpha := receiveNATSSubscriptionDelivery(t, backend, alpha)
	if err := alphaDelivery.Settle(ctx, transport.Decision{Kind: transport.DecisionDeadLetter, Attempts: 1}); err != nil {
		stopAlpha()
		t.Fatal(err)
	}
	stopAlpha()
	betaDelivery, stopBeta := receiveNATSSubscriptionDelivery(t, backend, beta)
	if err := betaDelivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
		stopBeta()
		t.Fatal(err)
	}
	stopBeta()
	records, err := backend.List(ctx, topo, 0, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("event DLQ records = %#v, %v", records, err)
	}
	if records[0].SourceGroup != backend.consumerName(alpha) {
		t.Fatalf("source group = %q", records[0].SourceGroup)
	}
	if err := backend.Requeue(ctx, topo, []string{records[0].ID}); err != nil {
		t.Fatal(err)
	}
	requeued, stopRequeued := receiveNATSSubscriptionDelivery(t, backend, alpha)
	if err := requeued.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
		stopRequeued()
		t.Fatal(err)
	}
	stopRequeued()
	stats, err := backend.SubscriptionStats(ctx, beta)
	if err != nil || stats.Lag != 0 {
		t.Fatalf("beta stats after requeue = %#v, %v", stats, err)
	}

	legacy := dlqRecord{
		Version: natsDLQRecordVersion, Message: transport.Message{ID: "legacy", Payload: []byte("legacy")},
		FailedAt: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.publish(ctx, backend.dlqStreamName(topo), legacy.Message.ID, "legacy-dlq", data); err != nil {
		t.Fatal(err)
	}
	if err := backend.Requeue(ctx, topo, []string{"2"}); err == nil || !strings.Contains(err.Error(), "no source subscriber") {
		t.Fatalf("missing-source requeue = %v", err)
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 1 {
		t.Fatalf("retained DLQ count = %d, %v", count, err)
	}

	incompatible := legacy
	incompatible.Message.ID = "incompatible"
	incompatible.SourceGroup = backend.consumerName(alpha)
	data, err = json.Marshal(incompatible)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.publish(ctx, backend.dlqStreamName(topo), incompatible.Message.ID, "incompatible-dlq", data); err != nil {
		t.Fatal(err)
	}
	retryStream, err := js.StreamInfo(backend.retryStreamName(topo, incompatible.SourceGroup))
	if err != nil {
		t.Fatal(err)
	}
	retryStream.Config.Retention = gonats.InterestPolicy
	if _, err := js.UpdateStream(&retryStream.Config); err != nil {
		t.Fatal(err)
	}
	if err := backend.Requeue(ctx, topo, []string{"3"}); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("incompatible-source requeue = %v", err)
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 2 {
		t.Fatalf("retained incompatible DLQ count = %d, %v", count, err)
	}
}

type hostileNATSError struct{}

func (hostileNATSError) Error() string { panic("must not render") }

func TestNATSStatsUseConsumerObligations(t *testing.T) {
	s, js := provisioningJetStream(t)
	defer s.Shutdown()
	backend := New(js).(*natsTransport)
	topo := transport.Topology{Namespace: fmt.Sprintf("stats_%d", time.Now().UnixNano()), JobType: "job"}
	ctx := context.Background()
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	if err := backend.Close(ctx); err != nil {
		t.Fatal(err)
	}
	assertNATSStats(t, backend, topo, 0, 0, 0, 0)
	if err := backend.Publish(ctx, topo, transport.Message{ID: "ready", Payload: []byte("ready")}); err != nil {
		t.Fatal(err)
	}
	assertNATSStats(t, backend, topo, 1, 0, 0, 0)

	delivery, stop := receiveNATSDelivery(t, backend, topo)
	assertNATSStats(t, backend, topo, 0, 0, 1, 0)
	if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
		stop()
		t.Fatal(err)
	}
	stop()
	assertNATSStats(t, backend, topo, 0, 0, 0, 0)

	if err := backend.PublishAt(ctx, topo, transport.Message{ID: "delayed", Payload: []byte("delayed")}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	assertNATSStats(t, backend, topo, 0, 1, 0, 0)

	if err := backend.Publish(ctx, topo, transport.Message{ID: "dead", Payload: []byte("dead")}); err != nil {
		t.Fatal(err)
	}
	dead, stopDead := receiveNATSDelivery(t, backend, topo)
	if err := dead.Settle(ctx, transport.Decision{Kind: transport.DecisionDeadLetter, Reason: "test", Attempts: 1}); err != nil {
		stopDead()
		t.Fatal(err)
	}
	stopDead()
	assertNATSStats(t, backend, topo, 0, 1, 0, 1)
}

func receiveNATSDelivery(t *testing.T, backend *natsTransport, topo transport.Topology) (transport.Delivery, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	deliveries := make(chan transport.Delivery, 1)
	done := make(chan error, 1)
	go func() {
		done <- backend.Consume(ctx, transport.Subscription{Topology: topo, Concurrency: 1, Prefetch: 1}, func(delivery transport.Delivery) {
			deliveries <- delivery
		})
	}()
	var delivery transport.Delivery
	select {
	case delivery = <-deliveries:
	case err := <-done:
		cancel()
		t.Fatalf("consume ended before delivery: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for NATS delivery")
	}
	stop := func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("consume shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop")
		}
	}
	return delivery, stop
}

func receiveNATSSubscriptionDelivery(t *testing.T, backend *natsTransport, sub transport.Subscription) (transport.Delivery, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	deliveries := make(chan transport.Delivery, 1)
	done := make(chan error, 1)
	go func() {
		done <- backend.Consume(ctx, sub, func(delivery transport.Delivery) { deliveries <- delivery })
	}()
	var delivery transport.Delivery
	select {
	case delivery = <-deliveries:
	case err := <-done:
		cancel()
		t.Fatalf("consume ended before delivery: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("timed out waiting for NATS subscription delivery")
	}
	return delivery, func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("consume shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop")
		}
	}
}

func assertNATSStats(t *testing.T, backend *natsTransport, topo transport.Topology, pending, delayed, inFlight, dlq int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		stats, err := backend.Stats(context.Background(), topo)
		if err == nil && stats.Pending == pending && stats.Delayed == delayed && stats.InFlight == inFlight &&
			stats.DLQ == dlq && stats.Lag == pending+inFlight && stats.LagKnown {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats = %#v, %v; want pending=%d delayed=%d in-flight=%d DLQ=%d", stats, err, pending, delayed, inFlight, dlq)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
