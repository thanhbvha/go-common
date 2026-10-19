//go:build integration

package nats

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	gonats "github.com/nats-io/nats.go"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func TestEnsureTopologyCreatesAndUpdatesManagedStreamConfig(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := transport.Topology{
		Namespace: "provision", JobType: "updates",
		Options: map[string]any{"nats.maxage": time.Hour, "nats.retention": gonats.LimitsPolicy},
	}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	mainName, dlqName := backend.streamName(topo), backend.dlqStreamName(topo)
	main, err := js.StreamInfo(mainName)
	if err != nil {
		t.Fatal(err)
	}
	main.Config.MaxMsgs = 77
	if _, err := js.UpdateStream(&main.Config); err != nil {
		t.Fatal(err)
	}

	updated := topo
	updated.Options = map[string]any{"nats.maxage": 2 * time.Hour, "nats.retention": gonats.InterestPolicy}
	if err := backend.EnsureTopology(ctx, updated); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{mainName, dlqName} {
		info, err := js.StreamInfo(name)
		if err != nil {
			t.Fatal(err)
		}
		if info.Config.MaxAge != 2*time.Hour || info.Config.Retention != gonats.InterestPolicy ||
			info.Config.Storage != gonats.FileStorage || info.Config.Replicas != 1 ||
			len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != name {
			t.Fatalf("stream %s config = %+v", name, info.Config)
		}
		if name == mainName && info.Config.MaxMsgs != 77 {
			t.Fatal("reconciliation overwrote an unmanaged stream limit")
		}
	}
}

func TestEventProvisioningUsesPrivateSubscriberRetryLane(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := targetedEventTopology("provision", "event_filters")
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	stream, err := js.StreamInfo(backend.streamName(topo))
	if err != nil {
		t.Fatal(err)
	}
	wantSubjects := []string{backend.broadcastSubject(topo)}
	if !slices.Equal(stream.Config.Subjects, wantSubjects) {
		t.Fatalf("event stream subjects = %q, want %q", stream.Config.Subjects, wantSubjects)
	}
	consumer, err := js.ConsumerInfo(stream.Config.Name, backend.consumerName(sub))
	if err != nil {
		t.Fatal(err)
	}
	if consumer.Config.FilterSubject != backend.broadcastSubject(topo) || len(consumer.Config.FilterSubjects) != 0 {
		t.Fatalf("event consumer filters = %q/%q", consumer.Config.FilterSubject, consumer.Config.FilterSubjects)
	}
	durable := backend.consumerName(sub)
	retry, err := js.StreamInfo(backend.retryStreamName(topo, durable))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(retry.Config.Subjects, []string{backend.retrySubject(topo, durable)}) || retry.Config.Retention != gonats.LimitsPolicy {
		t.Fatalf("retry stream config = %+v", retry.Config)
	}
	retryConsumer, err := js.ConsumerInfo(retry.Config.Name, backend.retryConsumerName(topo, durable))
	if err != nil {
		t.Fatal(err)
	}
	if retryConsumer.Config.FilterSubject != backend.retrySubject(topo, durable) {
		t.Fatalf("retry consumer filter = %q", retryConsumer.Config.FilterSubject)
	}
}

func TestEnsureTopologyReturnsIncompatibleStreamUpdate(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "provision", JobType: "incompatible"}
	name := backend.streamName(topo)
	if _, err := js.AddStream(&gonats.StreamConfig{Name: name, Subjects: []string{name}, Storage: gonats.MemoryStorage}); err != nil {
		t.Fatal(err)
	}
	if err := backend.EnsureTopology(ctx, topo); err == nil || !strings.Contains(err.Error(), "update stream") {
		t.Fatalf("incompatible stream update = %v", err)
	}
}

func TestConsumerProvisioningUpdatesCapacityAndPreservesConfig(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{
		Namespace: "provision", JobType: "consumer",
		Options: map[string]any{"nats.ack_wait": time.Second},
	}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	stream := backend.streamName(topo)
	sub := transport.Subscription{Topology: topo, Concurrency: 2, Prefetch: 3}
	if err := backend.reconcileConsumer(ctx, stream, sub); err != nil {
		t.Fatal(err)
	}
	name := backend.consumerName(sub)
	info, err := js.ConsumerInfo(stream, name)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAckPending != 5 || info.Config.AckWait != time.Second {
		t.Fatalf("initial consumer config = %+v", info.Config)
	}
	info.Config.MaxDeliver = 7
	if _, err := js.UpdateConsumer(stream, &info.Config); err != nil {
		t.Fatal(err)
	}

	updatedTopo := topo
	updatedTopo.Options = map[string]any{"nats.ack_wait": 2 * time.Second}
	sub.Topology = updatedTopo
	sub.Concurrency, sub.Prefetch = 4, 7
	if err := backend.reconcileConsumer(ctx, stream, sub); err != nil {
		t.Fatal(err)
	}
	info, err = js.ConsumerInfo(stream, name)
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAckPending != 11 || info.Config.AckWait != 2*time.Second || info.Config.MaxDeliver != 7 {
		t.Fatalf("updated consumer config = %+v", info.Config)
	}
}

func TestConsumerProvisioningRejectsIncompatibleMode(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "provision", JobType: "consumer_mode"}
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Concurrency: 1, Prefetch: 1}
	name := backend.consumerName(sub)
	if _, err := js.AddConsumer(backend.streamName(topo), &gonats.ConsumerConfig{Durable: name, AckPolicy: gonats.AckNonePolicy}); err != nil {
		t.Fatal(err)
	}
	if err := backend.reconcileConsumer(ctx, backend.streamName(topo), sub); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("incompatible consumer = %v", err)
	}
}

func TestTargetVerificationRejectsIncompatibleRetryLane(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := targetedEventTopology("provision", "target_verify")
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	durable := backend.consumerName(sub)
	info, err := js.StreamInfo(backend.retryStreamName(topo, durable))
	if err != nil {
		t.Fatal(err)
	}
	info.Config.Retention = gonats.InterestPolicy
	if _, err := js.UpdateStream(&info.Config); err != nil {
		t.Fatal(err)
	}
	if err := backend.verifyRetryLane(ctx, topo, durable); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("target verification = %v", err)
	}
}

func TestTargetVerificationRejectsMissingOrDriftedRetryConsumer(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	ctx := context.Background()
	topo := targetedEventTopology("provision", "target_consumer_verify")
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	durable := backend.consumerName(sub)
	consumer := backend.retryConsumerName(topo, durable)
	stream := backend.retryStreamName(topo, durable)
	if err := js.DeleteConsumer(stream, consumer); err != nil {
		t.Fatal(err)
	}
	if err := backend.withSubscriberLane(ctx, topo, durable, "test", func(*subscriberLaneLeaseHandle, subscriberLaneState) error { return nil }); err == nil || !strings.Contains(err.Error(), "consumer") {
		t.Fatalf("missing retry consumer verification = %v", err)
	}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	info, err := js.ConsumerInfo(stream, consumer)
	if err != nil {
		t.Fatal(err)
	}
	info.Config.MaxAckPending++
	if _, err := js.UpdateConsumer(stream, &info.Config); err != nil {
		t.Fatal(err)
	}
	if err := backend.withSubscriberLane(ctx, topo, durable, "test", func(*subscriberLaneLeaseHandle, subscriberLaneState) error { return nil }); err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("drifted retry consumer verification = %v", err)
	}
}

func TestCleanupSubscriptionQuarantinesPrivateLane(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	topo := targetedEventTopology("provision", "cleanup")
	ctx := context.Background()
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	durable := backend.consumerName(sub)
	if err := backend.CleanupSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	if _, err := js.ConsumerInfo(backend.streamName(topo), durable); !errors.Is(err, gonats.ErrConsumerNotFound) {
		t.Fatalf("broadcast consumer after cleanup = %v", err)
	}
	retry, err := js.StreamInfo(backend.retryStreamName(topo, durable))
	if err != nil || retry.State.Msgs != 0 || retry.State.Consumers != 0 {
		t.Fatalf("quarantined retry lane = %+v, %v", retry, err)
	}
	if err := backend.EnsureSubscription(ctx, sub); err == nil || !strings.Contains(err.Error(), "quarantined") {
		t.Fatalf("reprovision quarantined subscriber = %v", err)
	}
	if _, err := js.ConsumerInfo(backend.streamName(topo), durable); !errors.Is(err, gonats.ErrConsumerNotFound) {
		t.Fatalf("reprovision recreated broadcast consumer: %v", err)
	}
}

func TestCleanupWaitsForCrossProcessTargetedPublishAndFailsClosedOnAcceptedWork(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	producer := New(js).(*natsTransport)
	cleaner := New(js).(*natsTransport)
	t.Cleanup(func() {
		_ = producer.Close(context.Background())
		_ = cleaner.Close(context.Background())
	})
	ctx := context.Background()
	topo := targetedEventTopology("provision", "cleanup_publish_race")
	if err := producer.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := producer.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	durable := producer.consumerName(sub)
	entered, release := make(chan struct{}), make(chan struct{})
	producer.laneOperationHook = func(operation string) {
		if operation == "schedule" {
			close(entered)
			<-release
		}
	}
	published := make(chan error, 1)
	go func() {
		published <- producer.scheduleRetry(ctx, topo, transport.Message{ID: "accepted", Payload: []byte("accepted")}, time.Now().Add(time.Hour), "accepted-transfer", durable)
	}()
	<-entered
	cleanup := make(chan error, 1)
	go func() { cleanup <- cleaner.CleanupSubscription(ctx, sub) }()
	select {
	case err := <-cleanup:
		t.Fatalf("cleanup passed active publisher: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-published; err != nil {
		t.Fatal(err)
	}
	if err := <-cleanup; err == nil || !strings.Contains(err.Error(), "delayed retry work") {
		t.Fatalf("cleanup after accepted publish = %v", err)
	}
	state, err := cleaner.subscriberLaneState(topo, durable)
	if err != nil || state.State != "active" {
		t.Fatalf("lane after failed cleanup = %+v, %v", state, err)
	}
}

func TestCleanupSerializesCrossProcessProvisioning(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	provisioner := New(js).(*natsTransport)
	cleaner := New(js).(*natsTransport)
	t.Cleanup(func() {
		_ = provisioner.Close(context.Background())
		_ = cleaner.Close(context.Background())
	})
	ctx := context.Background()
	topo := targetedEventTopology("provision", "cleanup_provision_race")
	if err := provisioner.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	entered, release := make(chan struct{}), make(chan struct{})
	provisioner.laneOperationHook = func(operation string) {
		if operation == "provision" {
			close(entered)
			<-release
		}
	}
	provisioned := make(chan error, 1)
	go func() { provisioned <- provisioner.EnsureSubscription(ctx, sub) }()
	<-entered
	cleanup := make(chan error, 1)
	go func() { cleanup <- cleaner.CleanupSubscription(ctx, sub) }()
	select {
	case err := <-cleanup:
		t.Fatalf("cleanup passed active provisioner: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-provisioned; err != nil {
		t.Fatal(err)
	}
	if err := <-cleanup; err != nil {
		t.Fatal(err)
	}
	if err := provisioner.EnsureSubscription(ctx, sub); err == nil || !strings.Contains(err.Error(), "quarantined") {
		t.Fatalf("post-quarantine provisioning = %v", err)
	}
}

func TestSubscriberLaneRenewalFailureFencesBeforeTargetedMutation(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := targetedEventTopology("provision", "renewal_failure_fence")
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	durable := backend.consumerName(sub)
	entered, release, renewalFailed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var renewalOnce sync.Once
	backend.laneLeaseDuration = 300 * time.Millisecond
	backend.laneOperationHook = func(operation string) {
		if operation == "schedule" {
			close(entered)
			<-release
		}
	}
	backend.laneRenewalHook = func(context.Context) error {
		renewalOnce.Do(func() { close(renewalFailed) })
		return errors.New("injected renewal failure")
	}
	result := make(chan error, 1)
	go func() {
		result <- backend.scheduleRetry(ctx, topo, transport.Message{ID: "stale", Payload: []byte("stale")}, time.Now().Add(time.Hour), "renewal-failed", durable)
	}()
	<-entered
	select {
	case <-renewalFailed:
	case <-time.After(time.Second):
		t.Fatal("subscriber lane renewal did not run")
	}
	close(release)
	if err := <-result; !errors.Is(err, errSubscriberLaneLeaseLost) {
		t.Fatalf("renewal failure result = %v", err)
	}
	kv, err := backend.getKV(backend.kvBucketName(topo))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Get("transfer.renewal-failed"); !errors.Is(err, gonats.ErrKeyNotFound) && !errors.Is(err, gonats.ErrKeyDeleted) {
		t.Fatalf("fenced holder created transfer marker: %v", err)
	}
}

func TestExpiredSubscriberLaneHolderCannotMutateAfterTakeover(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	stale := New(js).(*natsTransport)
	winner := New(js).(*natsTransport)
	t.Cleanup(func() {
		_ = stale.Close(context.Background())
		_ = winner.Close(context.Background())
	})
	ctx := context.Background()
	topo := targetedEventTopology("provision", "expired_holder_fence")
	if err := stale.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := stale.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	durable := stale.consumerName(sub)
	kv, err := stale.getKV(stale.kvBucketName(topo))
	if err != nil {
		t.Fatal(err)
	}
	runAt := time.Now().Add(time.Hour)
	record := delayRecord{
		Version: delayRecordVersion, ScheduleID: "stale-schedule", MessageID: "stale", RunAt: runAt.UnixMilli(),
		Payload: []byte("stale"), TransferID: "stale-transfer", Target: durable,
	}
	key := putDelayRecord(t, kv, runAt, record)
	entry, err := kv.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	entered, release, renewalStarted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var renewalOnce sync.Once
	stale.laneLeaseDuration = 90 * time.Millisecond
	stale.laneOperationHook = func(operation string) {
		if operation == "dispatch" {
			close(entered)
			<-release
		}
	}
	stale.laneRenewalHook = func(ctx context.Context) error {
		renewalOnce.Do(func() { close(renewalStarted) })
		<-ctx.Done()
		return ctx.Err()
	}
	staleResult := make(chan error, 1)
	go func() {
		_, _, err := stale.dispatchDelayEntry(ctx, topo, kv, entry, runAt.Add(time.Second))
		staleResult <- err
	}()
	<-entered
	select {
	case <-renewalStarted:
	case <-time.After(time.Second):
		t.Fatal("subscriber lane renewal did not start")
	}
	if err := winner.scheduleRetry(ctx, topo, transport.Message{ID: "winner", Payload: []byte("winner")}, time.Now().Add(time.Hour), "winner-transfer", durable); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-staleResult; !errors.Is(err, errSubscriberLaneLeaseLost) {
		t.Fatalf("stale holder result = %v", err)
	}
	if _, err := kv.Get("transfer.winner-transfer"); err != nil {
		t.Fatalf("takeover holder did not mutate: %v", err)
	}
	if _, err := kv.Get(key); err != nil {
		t.Fatalf("stale holder deleted delayed source: %v", err)
	}
	retry, err := js.StreamInfo(stale.retryStreamName(topo, durable))
	if err != nil || retry.State.Msgs != 0 {
		t.Fatalf("stale holder published after takeover: %+v, %v", retry, err)
	}
}

func TestRetryLaneNamesCannotOverlapLegacyTargetSubjects(t *testing.T) {
	backend := &natsTransport{}
	topo := transport.Topology{Namespace: "legacy", JobType: "names", Kind: transport.EventTopology}
	durable := topo.ConsumerNameWithSeparator("alpha", "_")
	legacyPrefix := backend.streamName(topo) + ".target."
	if strings.HasPrefix(backend.retrySubject(topo, durable), legacyPrefix) || strings.Contains(backend.retryStreamName(topo, durable), durable) {
		t.Fatalf("retry lane overlaps legacy naming: %q/%q", backend.retrySubject(topo, durable), backend.retryStreamName(topo, durable))
	}
}

func TestDuplicateWindowIsExplicitOnAllStreams(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	window := 7 * time.Minute
	topo := transport.Topology{
		Namespace: "provision", JobType: "duplicates", Kind: transport.EventTopology,
		Options: map[string]any{"nats.duplicate_window": window, natsEventLayoutOption: natsTargetedLayoutV1},
	}
	ctx := context.Background()
	if err := backend.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
	sub := transport.Subscription{Topology: topo, Group: "alpha", Concurrency: 1, Prefetch: 1}
	if err := backend.EnsureSubscription(ctx, sub); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{backend.streamName(topo), backend.dlqStreamName(topo), backend.retryStreamName(topo, backend.consumerName(sub))} {
		info, err := js.StreamInfo(stream)
		if err != nil || info.Config.Duplicates != window {
			t.Fatalf("stream %q duplicate window = %s, %v", stream, info.Config.Duplicates, err)
		}
	}
}

func TestConcurrentTopologyAndConsumerProvisioningIsIdempotent(t *testing.T) {
	server, js := provisioningJetStream(t)
	defer server.Shutdown()
	backend := New(js).(*natsTransport)
	t.Cleanup(func() { _ = backend.Close(context.Background()) })
	ctx := context.Background()
	topo := transport.Topology{Namespace: "provision", JobType: "concurrent", Options: map[string]any{"nats.ack_wait": time.Second}}

	runConcurrent := func(operation func() error) {
		t.Helper()
		const instances = 8
		var wg sync.WaitGroup
		errs := make(chan error, instances)
		for range instances {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs <- operation()
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	runConcurrent(func() error { return backend.EnsureTopology(ctx, topo) })
	sub := transport.Subscription{Topology: topo, Concurrency: 3, Prefetch: 4}
	runConcurrent(func() error { return backend.reconcileConsumer(ctx, backend.streamName(topo), sub) })
	info, err := js.ConsumerInfo(backend.streamName(topo), backend.consumerName(sub))
	if err != nil {
		t.Fatal(err)
	}
	if info.Config.MaxAckPending != 7 || info.Config.AckWait != time.Second {
		t.Fatalf("concurrently provisioned consumer = %+v", info.Config)
	}
}

func provisioningJetStream(t *testing.T) (*server.Server, gonats.JetStreamContext) {
	t.Helper()
	s := RunServerOnPort(t, server.RANDOM_PORT)
	nc, err := gonats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		s.Shutdown()
		t.Fatal(err)
	}
	return s, js
}
