package redis

import (
	"context"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/thanhbvha/go-common/eventbus"
	"github.com/thanhbvha/go-common/eventbus/transport"
)

func addDLQRecord(t *testing.T, backend *redisTransport, rc *goredis.Client, topo transport.Topology, recordID string, index int) []byte {
	t.Helper()
	payload, _, err := eventbus.NewCodec(nil).EncodeTopology(eventbus.TopologyKind(topo.Kind), topo.JobType, index, 3,
		eventbus.WithOrderingKey("order"), eventbus.WithDedupeKey("dedupe"), eventbus.WithHeader("correlation-id", "synthetic"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.XAdd(context.Background(), &goredis.XAddArgs{
		Stream: backend.dlqKey(topo),
		ID:     recordID,
		Values: map[string]any{
			"payload":       payload,
			"reason":        "permanent_error",
			"error":         "synthetic failure",
			"attempts":      3,
			"failed_at":     int64(1700000000000 + index),
			"source_stream": backend.streamKey(topo),
			"source_group":  "workers",
			"source_id":     "source-" + recordID,
		},
	}).Err(); err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestRedisDLQCursorPaginationIsStableAcrossDeletion(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "cursor", Kind: transport.TaskTopology}
	for i := 1; i <= 5; i++ {
		addDLQRecord(t, backend, rc, topo, streamID(i), i)
	}

	page, err := backend.ListPage(ctx, topo, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 2 || page.Records[0].ID != "1-0" || page.Records[1].ID != "2-0" || page.NextCursor != "2-0" {
		t.Fatalf("first page = %#v", page)
	}
	record := page.Records[0]
	if record.Message.ID == "" || record.Message.Type != topo.JobType || record.Message.OrderingKey != "order" || record.Message.DedupeKey != "dedupe" || record.Message.Headers["correlation-id"] != "synthetic" {
		t.Fatalf("message metadata = %#v", record.Message)
	}
	if record.Reason != "permanent_error" || record.LastError != "synthetic failure" || record.Attempts != 3 || !record.FailedAt.Equal(time.UnixMilli(1700000000001)) {
		t.Fatalf("failure metadata = %#v", record)
	}
	if record.Source != backend.streamKey(topo) || record.SourceGroup != "workers" || record.SourcePosition != "source-1-0" {
		t.Fatalf("source metadata = %#v", record)
	}

	// Removing an entry before the cursor must not shift the following page.
	if err := backend.Requeue(ctx, topo, []string{"1-0"}); err != nil {
		t.Fatal(err)
	}
	page, err = backend.ListPage(ctx, topo, page.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 2 || page.Records[0].ID != "3-0" || page.Records[1].ID != "4-0" || page.NextCursor != "4-0" {
		t.Fatalf("second page after deletion = %#v", page)
	}
	page, err = backend.ListPage(ctx, topo, page.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 1 || page.Records[0].ID != "5-0" || page.NextCursor != "" {
		t.Fatalf("final page = %#v", page)
	}
}

func TestRedisDLQRequeueIsAtomicAndIdempotent(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "requeue", Kind: transport.EventTopology}
	wantPayload := addDLQRecord(t, backend, rc, topo, "1-0", 1)
	addDLQRecord(t, backend, rc, topo, "2-0", 2)

	if err := backend.Requeue(ctx, topo, []string{"1-0", "1-0", "9-0"}); err != nil {
		t.Fatal(err)
	}
	if err := backend.Requeue(ctx, topo, []string{"1-0"}); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	ready, err := rc.XRange(ctx, backend.streamKey(topo), "-", "+").Result()
	if err != nil || len(ready) != 1 || ready[0].Values["payload"] != string(wantPayload) {
		t.Fatalf("ready records = %#v, err = %v", ready, err)
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 1 {
		t.Fatalf("DLQ count = %d, err = %v", count, err)
	}
}

func TestRedisDLQRequeueFailureKeepsSource(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "failure"}
	addDLQRecord(t, backend, rc, topo, "1-0", 1)
	if err := rc.Set(ctx, backend.streamKey(topo), "wrong type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := backend.Requeue(ctx, topo, []string{"1-0"}); err == nil {
		t.Fatal("requeue accepted an invalid destination")
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 1 {
		t.Fatalf("failed requeue removed source: count = %d, err = %v", count, err)
	}
}

func TestRedisDLQRequeueValidatesBatchBeforeMutation(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "batch-validation"}
	addDLQRecord(t, backend, rc, topo, "1-0", 1)
	if err := rc.XAdd(ctx, &goredis.XAddArgs{
		Stream: backend.dlqKey(topo), ID: "2-0",
		Values: map[string]any{"reason": "missing payload"},
	}).Err(); err != nil {
		t.Fatal(err)
	}

	if err := backend.Requeue(ctx, topo, []string{"1-0", "2-0"}); err == nil {
		t.Fatal("malformed batch was accepted")
	}
	if count, err := backend.Count(ctx, topo); err != nil || count != 2 {
		t.Fatalf("partially removed DLQ batch: count = %d, err = %v", count, err)
	}
	if count, err := rc.XLen(ctx, backend.streamKey(topo)).Result(); err != nil || count != 0 {
		t.Fatalf("partially published requeue batch: count = %d, err = %v", count, err)
	}
}

func TestRedisDLQPurgeAndTopologyIsolation(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	task := transport.Topology{Namespace: "test", JobType: "same", Kind: transport.TaskTopology}
	event := transport.Topology{Namespace: "test", JobType: "same", Kind: transport.EventTopology}
	addDLQRecord(t, backend, rc, task, "1-0", 1)
	addDLQRecord(t, backend, rc, event, "1-0", 2)

	if err := backend.Purge(ctx, task); err != nil {
		t.Fatal(err)
	}
	if count, err := backend.Count(ctx, task); err != nil || count != 0 {
		t.Fatalf("task count = %d, err = %v", count, err)
	}
	if count, err := backend.Count(ctx, event); err != nil || count != 1 {
		t.Fatalf("event count = %d, err = %v", count, err)
	}
}

func TestRedisDLQAdminRejectsInvalidAndMalformedInput(t *testing.T) {
	backend, rc := delayedBackend(t)
	ctx := context.Background()
	topo := transport.Topology{Namespace: "test", JobType: "invalid"}
	if _, err := backend.ListPage(ctx, topo, "invalid", 1); err == nil {
		t.Fatal("invalid cursor accepted")
	}
	if _, err := backend.ListPage(ctx, topo, "", 0); err == nil {
		t.Fatal("zero limit accepted")
	}
	if err := backend.Requeue(ctx, topo, []string{"invalid"}); err == nil {
		t.Fatal("invalid record ID accepted")
	}
	if err := rc.XAdd(ctx, &goredis.XAddArgs{Stream: backend.dlqKey(topo), ID: "1-0", Values: map[string]any{"payload": "{}"}}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.ListPage(ctx, topo, "", 1); err == nil {
		t.Fatal("malformed DLQ metadata accepted")
	}
}

func streamID(sequence int) string {
	return strconv.Itoa(sequence) + "-0"
}
