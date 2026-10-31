//go:build integration

package mongodb_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoDBChangeStreamIntegration(t *testing.T) {
	uri := os.Getenv("MONGODB_INTEGRATION_URI")
	if uri == "" {
		t.Skip("set MONGODB_INTEGRATION_URI to an isolated replica set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var aggregates atomic.Int64
	// Native options provide command observation only for verifying driver resume.
	// No command payloads, connection details or documents are logged.
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetAppName("go-common-integration").SetTimeout(5 * time.Second).SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if e.CommandName == "aggregate" {
				aggregates.Add(1)
			}
		},
	}))
	if err != nil {
		t.Fatal("change stream client startup failed (details omitted)")
	}
	db := client.Database("go_common_it_" + bson.NewObjectID().Hex())
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := db.Drop(cleanup); err != nil {
			t.Error("change stream temporary database cleanup failed")
		}
		if err := client.Disconnect(cleanup); err != nil {
			t.Error("change stream client cleanup failed")
		}
	})
	pipeline := mongo.Pipeline{bson.D{{Key: "$match", Value: bson.M{"operationType": "insert"}}}}
	closeStream := func(stream *mongo.ChangeStream) {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := stream.Close(cleanup); err != nil {
			t.Error("change stream cleanup failed")
		}
	}
	newRepo := func(t *testing.T, name string) *mongodb.Repository[integrationDocument] {
		t.Helper()
		if err := db.CreateCollection(ctx, name); err != nil {
			t.Fatal("change stream fixture creation failed")
		}
		return mongodb.NewRepository[integrationDocument](db, name)
	}
	readID := func(t *testing.T, stream *mongo.ChangeStream, expected int) {
		t.Helper()
		if !stream.Next(ctx) {
			t.Fatal("expected change stream insert was not received")
		}
		var change mongodb.ChangeEvent[integrationDocument]
		if err := stream.Decode(&change); err != nil || change.OperationType != "insert" || change.FullDocument == nil || change.FullDocument.ID != expected || change.Namespace.Database != db.Name() || len(change.ResumeToken) == 0 {
			t.Fatal("change stream payload, order or metadata was incorrect")
		}
	}
	t.Run("insert_filter_order_and_raw_ownership", func(t *testing.T) {
		r := newRepo(t, "watch_filter")
		stream, err := r.Watch(ctx, pipeline, options.ChangeStream().SetBatchSize(1))
		if err != nil {
			t.Fatal("watch could not open")
		}
		defer closeStream(stream)
		if _, err := r.InsertOne(ctx, &integrationDocument{ID: 1}); err != nil {
			t.Fatal("insert fixture failed")
		}
		if _, err := r.UpdateOne(ctx, bson.M{"_id": 1}, bson.M{"$set": bson.M{"score": 9}}); err != nil {
			t.Fatal("update fixture failed")
		}
		if _, err := r.InsertOne(ctx, &integrationDocument{ID: 2}); err != nil {
			t.Fatal("second insert fixture failed")
		}
		readID(t, stream, 1)
		readID(t, stream, 2)
		// Raw ownership remains with the caller until Close.
		if stream.ID() == 0 {
			t.Fatal("raw stream closed prematurely")
		}
	})
	t.Run("typed_callback_failure_and_cancellation", func(t *testing.T) {
		r := newRepo(t, "watch_callback")
		stream, err := r.Watch(ctx, nil)
		if err != nil {
			t.Fatal("watch could not open")
		}
		token := append(bson.Raw(nil), stream.ResumeToken()...)
		closeStream(stream)
		if len(token) == 0 {
			t.Fatal("initial resume position was not supplied")
		}
		for i := 1; i <= 2; i++ {
			if _, err := r.InsertOne(ctx, &integrationDocument{ID: i}); err != nil {
				t.Fatal("callback fixture insert failed")
			}
		}
		stop := errors.New("expected notification failure")
		calls := 0
		err = r.WatchEach(ctx, pipeline, func(_ context.Context, change *mongodb.ChangeEvent[integrationDocument]) error {
			calls++
			if change.FullDocument == nil || change.FullDocument.ID != 1 {
				t.Error("typed callback received wrong insert")
			}
			return stop
		}, options.ChangeStream().SetResumeAfter(token).SetBatchSize(1))
		if !errors.Is(err, stop) || calls != 1 {
			t.Fatal("callback error did not stop consumption")
		}
		workerCtx, stopWorker := context.WithCancel(ctx)
		defer stopWorker()
		calls = 0
		err = r.WatchEach(workerCtx, pipeline, func(_ context.Context, change *mongodb.ChangeEvent[integrationDocument]) error {
			calls++
			stopWorker()
			return nil
		}, options.ChangeStream().SetResumeAfter(token).SetBatchSize(1))
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatal("cancellation did not stop consumption")
		}
	})
	t.Run("idle_cancellation", func(t *testing.T) {
		r := newRepo(t, "watch_idle")
		idle, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		defer stop()
		err := r.WatchEach(idle, nil, func(context.Context, *mongodb.ChangeEvent[integrationDocument]) error {
			t.Error("idle stream unexpectedly received a change")
			return nil
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("idle stream did not stop at its deadline")
		}
	})
	t.Run("driver_resumes_after_getmore_failure", func(t *testing.T) {
		if os.Getenv("MONGODB_TEST_FAILPOINTS") != "1" {
			t.Skip("set MONGODB_TEST_FAILPOINTS=1 on an isolated test server")
		}
		r := newRepo(t, "watch_resume")
		stream, err := r.Watch(ctx, pipeline, options.ChangeStream().SetBatchSize(1))
		if err != nil {
			t.Fatal("watch could not open")
		}
		defer closeStream(stream)
		if _, err := r.InsertOne(ctx, &integrationDocument{ID: 1}); err != nil {
			t.Fatal("first resume fixture insert failed")
		}
		readID(t, stream, 1)
		before := aggregates.Load()
		configureFailCommand(t, db, bson.M{"failCommands": bson.A{"getMore"}, "errorCode": 43, "errorLabels": bson.A{"ResumableChangeStreamError"}})
		if _, err := r.InsertOne(ctx, &integrationDocument{ID: 2}); err != nil {
			t.Fatal("second resume fixture insert failed")
		}
		readID(t, stream, 2)
		if aggregates.Load() <= before {
			t.Fatal("driver did not reopen the stream after resumable failure")
		}
	})
}
