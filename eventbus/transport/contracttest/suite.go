// Package contracttest provides reusable behavioral checks for EventBus
// transports. Driver packages supply only lifecycle and deterministic clock
// controls; the assertions remain transport-independent.
package contracttest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/eventbus/transport"
)

// Harness adapts a transport to the shared contract suite.
type Harness struct {
	New                        func(*testing.T) transport.Transport
	Topology                   func(testName string, kind transport.TopologyKind) transport.Topology
	DispatchDue                func(context.Context, transport.Transport, transport.Topology, time.Time) (int64, error)
	ExpireDedupe               func(*testing.T, time.Duration)
	PrepareUnsettledRedelivery func(*testing.T, transport.Topology)
	SubscriberRetryIsolation   bool
	SubscriberDLQIsolation     bool
	PendingIncludesInFlight    bool
	ReceiveWithin              time.Duration
}

// Run executes the durable transport contract that is independent of core
// handler classification. Core tests separately cover panic, timeout, attempt,
// reconnect, and drain semantics once a transport honors this boundary.
func Run(t *testing.T, h Harness) {
	t.Helper()
	if h.New == nil || h.Topology == nil || h.DispatchDue == nil {
		t.Fatal("contracttest: incomplete harness")
	}
	if h.ReceiveWithin <= 0 {
		h.ReceiveWithin = 5 * time.Second
	}

	t.Run("publish_consume_ack", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		mustPublish(t, ctx, tr, topo, "ack")
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		delivery := active.Delivery
		if string(delivery.Message().Payload) != "ack" || delivery.DeliveryAttempt() < 1 {
			t.Fatalf("delivery = %#v, attempt %d", delivery.Message(), delivery.DeliveryAttempt())
		}
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		active.Close()
		assertStats(t, ctx, tr, topo, 0, 0, 0, 0)
	})

	t.Run("double_settlement_rejected", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		mustPublish(t, ctx, tr, topo, "single-use")
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		if err := active.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		if err := active.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); !errors.Is(err, transport.ErrDeliverySettled) {
			t.Fatalf("second settlement = %v, want ErrDeliverySettled", err)
		}
		active.Close()
		assertStats(t, ctx, tr, topo, 0, 0, 0, 0)
	})

	t.Run("unsettled_session_cancellation_redelivers", func(t *testing.T) {
		if h.PrepareUnsettledRedelivery == nil {
			t.Skip("harness has no deterministic unsettled-redelivery control")
		}
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		mustPublish(t, ctx, tr, topo, "unsettled")
		first := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		if first.Delivery.DeliveryAttempt() < 1 {
			t.Fatalf("first attempt = %d", first.Delivery.DeliveryAttempt())
		}
		first.Close()
		h.PrepareUnsettledRedelivery(t, topo)
		second := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		if string(second.Delivery.Message().Payload) != "unsettled" || second.Delivery.DeliveryAttempt() < 2 {
			t.Fatalf("redelivery = %#v, attempt %d", second.Delivery.Message(), second.Delivery.DeliveryAttempt())
		}
		if err := second.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		second.Close()
	})

	t.Run("stats_transitions", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		assertStats(t, ctx, tr, topo, 0, 0, 0, 0)
		mustPublish(t, ctx, tr, topo, "stats")
		assertStats(t, ctx, tr, topo, 1, 0, 0, 0)
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		if h.PendingIncludesInFlight {
			assertStats(t, ctx, tr, topo, 1, 0, 0, 0)
		} else {
			assertStats(t, ctx, tr, topo, 0, 0, 1, 0)
		}
		if err := active.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		active.Close()
		assertStats(t, ctx, tr, topo, 0, 0, 0, 0)
	})

	t.Run("retry_transfer", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		mustPublish(t, ctx, tr, topo, "source")
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		delivery := active.Delivery
		retryAt := time.Now().Add(time.Minute)
		if err := delivery.Settle(ctx, transport.Decision{
			Kind: transport.DecisionRetry, Next: transport.Message{Payload: []byte("retry")}, RetryAt: retryAt,
		}); err != nil {
			t.Fatal(err)
		}
		active.Close()
		assertStats(t, ctx, tr, topo, 0, 1, 0, 0)
		if moved, err := h.DispatchDue(ctx, tr, topo, retryAt.Add(delayPrecision(tr))); err != nil || moved != 1 {
			t.Fatalf("dispatch due = %d, %v", moved, err)
		}
		active = receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		retried := active.Delivery
		if string(retried.Message().Payload) != "retry" {
			t.Fatalf("retry payload = %q", retried.Message().Payload)
		}
		if err := retried.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		active.Close()
	})

	t.Run("dead_letter_admin", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		for _, payload := range []string{"requeue", "purge"} {
			mustPublish(t, ctx, tr, topo, payload)
			active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
			if err := active.Delivery.Settle(ctx, transport.Decision{
				Kind: transport.DecisionDeadLetter, Reason: "contract", Cause: errors.New("synthetic"), ErrorCode: "synthetic", Attempts: 1,
			}); err != nil {
				t.Fatal(err)
			}
			active.Close()
		}
		admin, ok := tr.(transport.DLQAdmin)
		if !ok {
			t.Fatal("DLQAdmin not advertised")
		}
		if count, err := admin.Count(ctx, topo); err != nil || count != 2 {
			t.Fatalf("DLQ count = %d, %v", count, err)
		}
		records, err := admin.List(ctx, topo, 0, 10)
		if err != nil || len(records) != 2 || records[0].ID == "" || records[0].Reason != "contract" || records[0].LastError != "synthetic" {
			t.Fatalf("DLQ records = %#v, %v", records, err)
		}
		if err := admin.Requeue(ctx, topo, []string{records[0].ID, records[0].ID}); err != nil {
			t.Fatal(err)
		}
		if err := admin.Requeue(ctx, topo, []string{records[0].ID}); err != nil {
			t.Fatalf("idempotent missing requeue: %v", err)
		}
		if count, err := admin.Count(ctx, topo); err != nil || count != 1 {
			t.Fatalf("DLQ after requeue = %d, %v", count, err)
		}
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		requeued := active.Delivery
		if string(requeued.Message().Payload) != "requeue" {
			t.Fatalf("requeued payload = %q", requeued.Message().Payload)
		}
		if err := requeued.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		active.Close()
		if err := admin.Purge(ctx, topo); err != nil {
			t.Fatal(err)
		}
		if count, err := admin.Count(ctx, topo); err != nil || count != 0 {
			t.Fatalf("DLQ after purge = %d, %v", count, err)
		}
	})

	t.Run("delayed_delivery", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		runAt := time.Now().Add(time.Minute)
		if err := tr.PublishAt(ctx, topo, transport.Message{Payload: []byte("later")}, runAt); err != nil {
			t.Fatal(err)
		}
		earlyBy := delayPrecision(tr)
		if moved, err := h.DispatchDue(ctx, tr, topo, runAt.Add(-earlyBy)); err != nil || moved != 0 {
			t.Fatalf("early dispatch = %d, %v", moved, err)
		}
		assertStats(t, ctx, tr, topo, 0, 1, 0, 0)
		if moved, err := h.DispatchDue(ctx, tr, topo, runAt.Add(earlyBy)); err != nil || moved != 1 {
			t.Fatalf("due dispatch = %d, %v", moved, err)
		}
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		delivery := active.Delivery
		if string(delivery.Message().Payload) != "later" {
			t.Fatalf("delayed payload = %q", delivery.Message().Payload)
		}
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		active.Close()
	})

	t.Run("event_fanout_groups", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.EventTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		if provisioner, ok := tr.(transport.SubscriptionProvisioner); ok {
			for _, group := range []string{"alpha", "beta"} {
				if err := provisioner.EnsureSubscription(ctx, transport.Subscription{Topology: topo, Group: group}); err != nil {
					t.Fatal(err)
				}
			}
		}
		mustPublish(t, ctx, tr, topo, "event")
		for _, group := range []string{"alpha", "beta"} {
			active := receive(t, tr, transport.Subscription{Topology: topo, Group: group}, h.ReceiveWithin)
			delivery := active.Delivery
			if string(delivery.Message().Payload) != "event" {
				t.Fatalf("subscriber %s payload = %q", group, delivery.Message().Payload)
			}
			if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
				t.Fatal(err)
			}
			active.Close()
		}
		assertStats(t, ctx, tr, topo, 0, 0, 0, 0)
	})

	t.Run("task_event_same_name_routing_isolation", func(t *testing.T) {
		tr := h.New(t)
		ctx := context.Background()
		task := h.Topology(t.Name(), transport.TaskTopology)
		event := h.Topology(t.Name(), transport.EventTopology)
		mustEnsure(t, ctx, tr, task)
		mustEnsure(t, ctx, tr, event)
		eventSub := transport.Subscription{Topology: event, Group: "isolated"}
		if provisioner, ok := tr.(transport.SubscriptionProvisioner); ok {
			if err := provisioner.EnsureSubscription(ctx, eventSub); err != nil {
				t.Fatal(err)
			}
		}
		mustPublish(t, ctx, tr, task, "task-only")
		mustPublish(t, ctx, tr, event, "event-only")
		taskDelivery := receive(t, tr, transport.Subscription{Topology: task}, h.ReceiveWithin)
		if string(taskDelivery.Delivery.Message().Payload) != "task-only" {
			t.Fatalf("task received %q", taskDelivery.Delivery.Message().Payload)
		}
		if err := taskDelivery.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		taskDelivery.Close()
		eventDelivery := receive(t, tr, eventSub, h.ReceiveWithin)
		if string(eventDelivery.Delivery.Message().Payload) != "event-only" {
			t.Fatalf("event received %q", eventDelivery.Delivery.Message().Payload)
		}
		if err := eventDelivery.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		eventDelivery.Close()
	})

	t.Run("event_subscriber_retry_isolation", func(t *testing.T) {
		if !h.SubscriberRetryIsolation {
			t.Skip("driver has not implemented source-subscriber retry routing")
		}
		tr := h.New(t)
		reader, ok := tr.(transport.SubscriptionStatsReader)
		if !ok {
			t.Skip("transport does not advertise subscriber stats")
		}
		ctx := context.Background()
		topo := h.Topology(t.Name(), transport.EventTopology)
		mustEnsure(t, ctx, tr, topo)
		provisioner, ok := tr.(transport.SubscriptionProvisioner)
		if !ok {
			t.Skip("transport cannot provision subscriber groups before publish")
		}
		alpha := transport.Subscription{Topology: topo, Group: "alpha"}
		beta := transport.Subscription{Topology: topo, Group: "beta"}
		for _, sub := range []transport.Subscription{alpha, beta} {
			if err := provisioner.EnsureSubscription(ctx, sub); err != nil {
				t.Fatal(err)
			}
		}
		mustPublish(t, ctx, tr, topo, "source")
		alphaDelivery := receive(t, tr, alpha, h.ReceiveWithin)
		retryAt := time.Now().Add(time.Minute)
		if err := alphaDelivery.Delivery.Settle(ctx, transport.Decision{
			Kind: transport.DecisionRetry, Next: transport.Message{ID: "retry", Payload: []byte("retry")}, RetryAt: retryAt,
		}); err != nil {
			t.Fatal(err)
		}
		alphaDelivery.Close()
		betaDelivery := receive(t, tr, beta, h.ReceiveWithin)
		if string(betaDelivery.Delivery.Message().Payload) != "source" {
			t.Fatalf("beta source payload = %q", betaDelivery.Delivery.Message().Payload)
		}
		if err := betaDelivery.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		betaDelivery.Close()
		if moved, err := h.DispatchDue(ctx, tr, topo, retryAt.Add(delayPrecision(tr))); err != nil || moved != 1 {
			t.Fatalf("dispatch subscriber retry = %d, %v", moved, err)
		}
		betaStats, err := reader.SubscriptionStats(ctx, beta)
		if err != nil {
			t.Fatal(err)
		}
		if betaStats.Pending != 0 || betaStats.InFlight != 0 || betaStats.Lag != 0 {
			t.Fatalf("retry leaked to beta subscriber: %+v", betaStats)
		}
		retry := receive(t, tr, alpha, h.ReceiveWithin)
		if string(retry.Delivery.Message().Payload) != "retry" {
			t.Fatalf("alpha retry payload = %q", retry.Delivery.Message().Payload)
		}
		if err := retry.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		retry.Close()
	})

	t.Run("event_subscriber_dlq_requeue_isolation", func(t *testing.T) {
		if !h.SubscriberDLQIsolation {
			t.Skip("driver has not implemented source-subscriber DLQ requeue routing")
		}
		tr := h.New(t)
		reader, ok := tr.(transport.SubscriptionStatsReader)
		if !ok {
			t.Skip("transport does not advertise subscriber stats")
		}
		admin, ok := tr.(transport.DLQAdmin)
		if !ok {
			t.Fatal("DLQAdmin not advertised")
		}
		ctx := context.Background()
		topo := h.Topology(t.Name(), transport.EventTopology)
		mustEnsure(t, ctx, tr, topo)
		provisioner, ok := tr.(transport.SubscriptionProvisioner)
		if !ok {
			t.Skip("transport cannot provision subscriber groups before publish")
		}
		alpha := transport.Subscription{Topology: topo, Group: "alpha"}
		beta := transport.Subscription{Topology: topo, Group: "beta"}
		for _, sub := range []transport.Subscription{alpha, beta} {
			if err := provisioner.EnsureSubscription(ctx, sub); err != nil {
				t.Fatal(err)
			}
		}
		mustPublish(t, ctx, tr, topo, "event-dlq")
		alphaDelivery := receive(t, tr, alpha, h.ReceiveWithin)
		if err := alphaDelivery.Delivery.Settle(ctx, transport.Decision{
			Kind: transport.DecisionDeadLetter, Reason: "contract", ErrorCode: "contract", Attempts: 1,
		}); err != nil {
			t.Fatal(err)
		}
		alphaDelivery.Close()
		betaDelivery := receive(t, tr, beta, h.ReceiveWithin)
		if err := betaDelivery.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		betaDelivery.Close()
		records, err := admin.List(ctx, topo, 0, 10)
		if err != nil || len(records) != 1 || records[0].ID == "" || records[0].SourceGroup == "" {
			t.Fatalf("event DLQ records = %#v, %v", records, err)
		}
		if err := admin.Requeue(ctx, topo, []string{records[0].ID}); err != nil {
			t.Fatal(err)
		}
		betaStats, err := reader.SubscriptionStats(ctx, beta)
		if err != nil {
			t.Fatal(err)
		}
		if betaStats.Pending != 0 || betaStats.InFlight != 0 || betaStats.Lag != 0 {
			t.Fatalf("DLQ requeue leaked to beta subscriber: %+v", betaStats)
		}
		requeued := receive(t, tr, alpha, h.ReceiveWithin)
		if string(requeued.Delivery.Message().Payload) != "event-dlq" {
			t.Fatalf("alpha requeue payload = %q", requeued.Delivery.Message().Payload)
		}
		if err := requeued.Delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionAck}); err != nil {
			t.Fatal(err)
		}
		requeued.Close()
	})

	t.Run("dedupe_scope_expiry_release", func(t *testing.T) {
		tr := h.New(t)
		deduper, ok := tr.(transport.TopologyDeduper)
		if !ok {
			t.Skip("transport does not advertise deduplication")
		}
		if h.ExpireDedupe == nil {
			t.Fatal("contracttest: dedupe transport requires ExpireDedupe")
		}
		ctx := context.Background()
		first := h.Topology(t.Name()+"-first", transport.TaskTopology)
		second := h.Topology(t.Name()+"-second", transport.TaskTopology)
		otherNamespace := first
		otherNamespace.Namespace += "-other"
		sameNameEvent := h.Topology(first.JobType, transport.EventTopology)
		topologies := []transport.Topology{first, second, otherNamespace, sameNameEvent}
		for _, topo := range topologies {
			mustEnsure(t, ctx, tr, topo)
		}
		const ttl = 50 * time.Millisecond
		for _, topo := range topologies {
			if acquired, err := deduper.ReserveTopology(ctx, topo, "key", ttl); err != nil || !acquired {
				t.Fatalf("initial reserve = %v, %v", acquired, err)
			}
			if acquired, err := deduper.ReserveTopology(ctx, topo, "key", ttl); err != nil || acquired {
				t.Fatalf("duplicate reserve = %v, %v", acquired, err)
			}
		}
		if err := deduper.ReleaseTopology(ctx, first, "key"); err != nil {
			t.Fatal(err)
		}
		if acquired, err := deduper.ReserveTopology(ctx, first, "key", ttl); err != nil || !acquired {
			t.Fatalf("reserve after release = %v, %v", acquired, err)
		}
		h.ExpireDedupe(t, ttl)
		for _, topo := range topologies {
			if acquired, err := deduper.ReserveTopology(ctx, topo, "key", ttl); err != nil || !acquired {
				t.Fatalf("reserve after expiry = %v, %v", acquired, err)
			}
		}
	})

	t.Run("publication_deduper_state", func(t *testing.T) {
		tr := h.New(t)
		deduper, ok := tr.(transport.PublicationDeduper)
		if !ok {
			t.Skip("transport does not advertise publication deduplication")
		}
		ctx := context.Background()
		topo := h.Topology(t.Name(), transport.TaskTopology)
		mustEnsure(t, ctx, tr, topo)
		const ttl = time.Minute
		state, err := deduper.BeginPublication(ctx, topo, "abort", "owner-1", ttl)
		if err != nil || state != transport.DedupeReservationAcquired {
			t.Fatalf("initial begin = %d, %v", state, err)
		}
		state, err = deduper.BeginPublication(ctx, topo, "abort", "owner-2", ttl)
		if err != nil || state != transport.DedupeReservationPending {
			t.Fatalf("pending begin = %d, %v", state, err)
		}
		if err := deduper.AbortPublication(ctx, topo, "abort", "owner-2"); !errors.Is(err, transport.ErrDedupeAmbiguous) {
			t.Fatalf("wrong-owner abort = %v", err)
		}
		if err := deduper.AbortPublication(ctx, topo, "abort", "owner-1"); err != nil {
			t.Fatal(err)
		}
		state, err = deduper.BeginPublication(ctx, topo, "abort", "owner-3", ttl)
		if err != nil || state != transport.DedupeReservationAcquired {
			t.Fatalf("begin after abort = %d, %v", state, err)
		}
		state, err = deduper.BeginPublication(ctx, topo, "confirm", "owner-4", ttl)
		if err != nil || state != transport.DedupeReservationAcquired {
			t.Fatalf("confirm begin = %d, %v", state, err)
		}
		if err := deduper.ConfirmPublication(ctx, topo, "confirm", "owner-5"); !errors.Is(err, transport.ErrDedupeAmbiguous) {
			t.Fatalf("wrong-owner confirm = %v", err)
		}
		if err := deduper.ConfirmPublication(ctx, topo, "confirm", "owner-4"); err != nil {
			t.Fatal(err)
		}
		state, err = deduper.BeginPublication(ctx, topo, "confirm", "owner-6", ttl)
		if err != nil || state != transport.DedupeReservationPublished {
			t.Fatalf("published begin = %d, %v", state, err)
		}
	})

	t.Run("scheduler_health_presence_and_sanitization", func(t *testing.T) {
		tr := h.New(t)
		reader, ok := tr.(transport.SchedulerHealthReader)
		if !ok {
			t.Skip("transport does not advertise scheduler health")
		}
		ctx := context.Background()
		topo := h.Topology(t.Name(), transport.TaskTopology)
		mustEnsure(t, ctx, tr, topo)
		runAt := time.Now().Add(time.Minute)
		if err := tr.PublishAt(ctx, topo, transport.Message{Payload: []byte("scheduler-health-secret")}, runAt); err != nil {
			t.Fatal(err)
		}
		if moved, err := h.DispatchDue(ctx, tr, topo, runAt.Add(delayPrecision(tr))); err != nil || moved != 1 {
			t.Fatalf("scheduler dispatch = %d, %v", moved, err)
		}
		statuses := reader.SchedulerHealth(topo)
		if len(statuses) == 0 {
			t.Fatal("scheduler health is empty after a poll")
		}
		observed := false
		for _, status := range statuses {
			if status.Transport == "" || status.Topology != topo.Name() || status.Worker == "" {
				t.Fatalf("incomplete scheduler status: %+v", status)
			}
			if (!status.LastPoll.IsZero() && status.Polls > 0) ||
				(!status.LastSuccess.IsZero() && (status.Dispatches > 0 || status.Transfers > 0 || status.Commits > 0)) {
				observed = true
			}
			if len(status.LastError) > transport.MaxSchedulerErrorBytes {
				t.Fatalf("scheduler error exceeds bound: %d", len(status.LastError))
			}
			if strings.Contains(status.LastError, "scheduler-health-secret") {
				t.Fatalf("scheduler status exposed raw error text: %q", status.LastError)
			}
		}
		if !observed {
			t.Fatalf("scheduler health has no observed activity: %+v", statuses)
		}
	})

	t.Run("malformed_payload_remains_settleable", func(t *testing.T) {
		tr := h.New(t)
		topo := h.Topology(t.Name(), transport.TaskTopology)
		ctx := context.Background()
		mustEnsure(t, ctx, tr, topo)
		mustPublish(t, ctx, tr, topo, "{not-json")
		active := receive(t, tr, transport.Subscription{Topology: topo}, h.ReceiveWithin)
		delivery := active.Delivery
		if string(delivery.Message().Payload) != "{not-json" {
			t.Fatal("transport altered malformed payload")
		}
		if err := delivery.Settle(ctx, transport.Decision{Kind: transport.DecisionDeadLetter, Reason: "decode"}); err != nil {
			t.Fatal(err)
		}
		active.Close()
		assertStats(t, ctx, tr, topo, 0, 0, 0, 1)
	})
}

func delayPrecision(tr transport.Transport) time.Duration {
	precision := tr.Capabilities().DelayPrecision
	if precision <= 0 {
		return time.Millisecond
	}
	return precision
}

func mustEnsure(t *testing.T, ctx context.Context, tr transport.Transport, topo transport.Topology) {
	t.Helper()
	if err := tr.EnsureTopology(ctx, topo); err != nil {
		t.Fatal(err)
	}
}

func mustPublish(t *testing.T, ctx context.Context, tr transport.Transport, topo transport.Topology, payload string) {
	t.Helper()
	if err := tr.Publish(ctx, topo, transport.Message{ID: payload, Type: topo.JobType, Payload: []byte(payload)}); err != nil {
		t.Fatal(err)
	}
}

type activeDelivery struct {
	transport.Delivery
	close func()
}

func (d *activeDelivery) Close() { d.close() }

func receive(t *testing.T, tr transport.Transport, sub transport.Subscription, timeout time.Duration) *activeDelivery {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	deliveries := make(chan transport.Delivery, 1)
	done := make(chan error, 1)
	go func() {
		done <- tr.Consume(ctx, sub, func(delivery transport.Delivery) {
			select {
			case deliveries <- delivery:
			default:
			}
		})
	}()
	var delivery transport.Delivery
	select {
	case delivery = <-deliveries:
	case err := <-done:
		cancel()
		t.Fatalf("consume ended before delivery: %v", err)
	case <-time.After(timeout):
		cancel()
		t.Fatal("timed out waiting for delivery")
	}
	var once sync.Once
	active := &activeDelivery{Delivery: delivery}
	active.close = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("consume shutdown: %v", err)
				}
			case <-time.After(timeout):
				t.Error("consume did not stop after cancellation")
			}
		})
	}
	t.Cleanup(active.Close)
	return active
}

func assertStats(t *testing.T, ctx context.Context, tr transport.Transport, topo transport.Topology, pending, delayed, inFlight, dlq int64) {
	t.Helper()
	reader, ok := tr.(transport.StatsReader)
	if !ok {
		t.Fatal("StatsReader not advertised")
	}
	stats, err := reader.Stats(ctx, topo)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Pending != pending || stats.Delayed != delayed || stats.InFlight != inFlight || stats.DLQ != dlq || stats.Lag != pending+inFlight || !stats.LagKnown {
		t.Fatalf("stats = %s", fmt.Sprintf("%+v", stats))
	}
}
