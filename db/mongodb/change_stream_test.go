package mongodb

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func changeDocument(operation string, id int32, full any) bson.D {
	doc := bson.D{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "opaque-event-token"}}},
		{Key: "operationType", Value: operation},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: id}}},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "test"}, {Key: "coll", Value: "items"}}},
		{Key: "clusterTime", Value: bson.Timestamp{T: 10, I: 1}},
	}
	if full != nil {
		doc = append(doc, bson.E{Key: "fullDocument", Value: full})
	}
	return doc
}

func TestWatchEachTypedBatchesAndOptions(t *testing.T) {
	type document struct {
		ID int `bson:"_id"`
	}
	client, _, commands := mockClient(t,
		cursorResponse("firstBatch", 123, changeDocument("insert", 1, bson.M{"_id": 1})),
		cursorResponse("nextBatch", 0, changeDocument("delete", 2, nil)),
	)
	r := NewRepository[document](client.Database("test"), "items")
	var events []*ChangeEvent[document]
	err := r.WatchEach(context.Background(), nil, func(_ context.Context, change *ChangeEvent[document]) error {
		events = append(events, change)
		return nil
	}, options.ChangeStream().SetBatchSize(1).SetMaxAwaitTime(time.Second).SetFullDocument(options.UpdateLookup).SetResumeAfter(bson.M{"_data": "previous-token"}))
	if err != nil || len(events) != 2 {
		t.Fatalf("typed events count=%d err=%v", len(events), err)
	}
	if events[0].FullDocument == nil || events[0].FullDocument.ID != 1 || events[1].FullDocument != nil || events[1].OperationType != "delete" {
		t.Fatal("fullDocument presence or event order was lost")
	}
	if events[0].ResumeToken.Lookup("_data").StringValue() != "opaque-event-token" || events[0].DocumentKey.Lookup("_id").Int32() != 1 || events[0].Namespace.Database != "test" || events[0].Namespace.Collection != "items" || events[0].ClusterTime.T != 10 {
		t.Fatal("event metadata was lost")
	}
	stages, err := (*commands)[0].Lookup("pipeline").Array().Values()
	if err != nil || len(stages) != 1 {
		t.Fatal("nil pipeline was not normalized to an empty pipeline")
	}
	watch := stages[0].Document().Lookup("$changeStream").Document()
	if watch.Lookup("fullDocument").StringValue() != "updateLookup" || watch.Lookup("resumeAfter").Document().Lookup("_data").StringValue() != "previous-token" {
		t.Fatal("change stream options were not forwarded")
	}
	if (*commands)[1].Lookup("maxTimeMS").Int64() != 1000 {
		t.Fatal("max await time was not forwarded")
	}
}

func TestWatchEachStopsAndCloses(t *testing.T) {
	for _, mode := range []string{"callback", "cancel", "decode", "panic", "cleanup_error"} {
		t.Run(mode, func(t *testing.T) {
			full := any(bson.M{"_id": 1})
			if mode == "decode" {
				full = "invalid-document"
			}
			closeResponse := bson.D{{Key: "ok", Value: 1}, {Key: "cursorsKilled", Value: bson.A{int64(123)}}}
			if mode == "cleanup_error" {
				closeResponse = bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 2}, {Key: "errmsg", Value: "synthetic cleanup failure"}}
			}
			client, _, commands := mockClient(t, cursorResponse("firstBatch", 123, changeDocument("insert", 1, full)), closeResponse)
			r := NewRepository[bson.M](client.Database("test"), "items")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stop := errors.New("callback stopped")
			calls := 0
			var err error
			func() {
				defer func() {
					if value := recover(); value != nil {
						if mode != "panic" || value != stop {
							t.Fatalf("unexpected panic: %v", value)
						}
					} else if mode == "panic" {
						t.Error("callback panic was swallowed")
					}
				}()
				err = r.WatchEach(ctx, nil, func(context.Context, *ChangeEvent[bson.M]) error {
					calls++
					if mode == "cancel" {
						cancel()
						return nil
					}
					if mode == "panic" {
						panic(stop)
					}
					return stop
				})
			}()
			if mode == "callback" || mode == "cleanup_error" {
				if !errors.Is(err, stop) {
					t.Fatalf("callback error lost: %v", err)
				}
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			if mode == "decode" && (err == nil || calls != 0) {
				t.Fatal("decode failure invoked callback")
			}
			if mode == "cleanup_error" {
				var commandError mongo.CommandError
				if !errors.As(err, &commandError) || commandError.Code != 2 {
					t.Fatalf("cleanup error lost: %v", err)
				}
			}
			if len(*commands) != 2 || (*commands)[1].Lookup("killCursors").StringValue() != "items" {
				t.Fatal("stream was not closed with an independent cleanup context")
			}
		})
	}
}

func TestWatchValidationBeforeAccess(t *testing.T) {
	client, _, commands := mockClient(t)
	r := NewRepository[bson.M](client.Database("test"), "items")
	if err := r.WatchEach(context.Background(), nil, nil); err == nil {
		t.Fatal("nil handler was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.Watch(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled context was accepted")
	}
	r.transaction = &Transaction{}
	if _, err := r.Watch(context.Background(), nil); !errors.Is(err, ErrChangeStreamInTransaction) {
		t.Fatal("transaction repository was accepted")
	}
	r.transaction = nil
	session, err := client.StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.EndSession(context.Background())
	if err := session.StartTransaction(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Watch(mongo.NewSessionContext(context.Background(), session), nil); !errors.Is(err, ErrChangeStreamInTransaction) {
		t.Fatal("active transaction context was accepted")
	}
	if len(*commands) != 0 {
		t.Fatal("invalid watch accessed the server")
	}
}

func TestWatchEachDriverErrors(t *testing.T) {
	denied := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 13}, {Key: "errmsg", Value: "synthetic permission failure"}}
	for _, duringRead := range []bool{false, true} {
		name := "open"
		responses := []bson.D{denied}
		if duringRead {
			name = "getmore"
			responses = []bson.D{cursorResponse("firstBatch", 123), denied, {{Key: "ok", Value: 1}}}
		}
		t.Run(name, func(t *testing.T) {
			client, _, _ := mockClient(t, responses...)
			r := NewRepository[bson.M](client.Database("test"), "items")
			err := r.WatchEach(context.Background(), nil, func(context.Context, *ChangeEvent[bson.M]) error {
				t.Error("driver failure invoked handler")
				return nil
			})
			var commandError mongo.CommandError
			if !errors.As(err, &commandError) || commandError.Code != 13 {
				t.Fatalf("driver error lost: %v", err)
			}
		})
	}
}
