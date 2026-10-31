package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/db/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Notification struct {
	ID        bson.ObjectID `bson:"_id"`
	RoomID    string        `bson:"room_id"`
	Message   string        `bson:"message"`
	ContentID string        `bson:"content_id"`
	Type      string        `bson:"type"`
	IsRead    bool          `bson:"is_read"`
}

// NotificationPayload is the application message, independent of MongoDB and of
// any Socket.IO/WebSocket transport. Real senders must enforce room authorization.
type NotificationPayload struct {
	ID        string `json:"id"`
	RoomID    string `json:"room_id"`
	Message   string `json:"message"`
	ContentID string `json:"content_id"`
	Type      string `json:"type"`
	IsRead    bool   `json:"is_read"`
}

type notificationSender interface {
	Send(context.Context, NotificationPayload) error
}

// A bounded in-memory stand-in for the application's notification transport.
type notificationRecorder struct{ sent []NotificationPayload }

func (s *notificationRecorder) Send(ctx context.Context, payload NotificationPayload) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(s.sent) >= 3 {
		return errors.New("unexpected extra notification")
	}
	s.sent = append(s.sent, payload)
	return nil
}

func sendNotification(ctx context.Context, sender notificationSender, change *mongodb.ChangeEvent[Notification]) error {
	if change.OperationType != "insert" || change.FullDocument == nil {
		return errors.New("unexpected notification event")
	}
	doc := change.FullDocument
	return sender.Send(ctx, NotificationPayload{
		ID: doc.ID.Hex(), RoomID: doc.RoomID, Message: doc.Message,
		ContentID: doc.ContentID, Type: doc.Type, IsRead: doc.IsRead,
	})
}

func (s *exampleState) watch(ctx context.Context) error {
	db, err := s.manager.Lookup("orders")
	if err != nil {
		return err
	}
	if err := db.CreateCollection(ctx, "portal_users_notification"); err != nil {
		return err
	}
	r := mongodb.NewRepository[Notification](db, "portal_users_notification")
	pipeline := mongo.Pipeline{bson.D{{Key: "$match", Value: bson.M{"operationType": "insert"}}}}
	sender := &notificationRecorder{}
	watchCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var token bson.Raw
	docs := []Notification{
		{ID: bson.NewObjectID(), RoomID: "example-room", Message: "synthetic-one", ContentID: "one", Type: "example"},
		{ID: bson.NewObjectID(), RoomID: "example-room", Message: "synthetic-two", ContentID: "two", Type: "example"},
		{ID: bson.NewObjectID(), RoomID: "example-room", Message: "synthetic-three", ContentID: "three", Type: "example"},
	}
	// Open synchronously BEFORE writes. Watch returns the native stream so callers
	// can coordinate readiness, own iteration and choose when to close it.
	err = func() (result error) {
		stream, err := r.Watch(watchCtx, pipeline, options.ChangeStream().SetBatchSize(1).SetMaxAwaitTime(100*time.Millisecond))
		if err != nil {
			return err
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			result = errors.Join(result, stream.Close(cleanup))
		}()
		if _, err := r.InsertOne(ctx, &docs[0]); err != nil {
			return err
		}
		if _, err := r.UpdateOne(ctx, bson.M{"_id": docs[0].ID}, bson.M{"$set": bson.M{"is_read": true}}); err != nil {
			return err
		}
		if _, err := r.InsertOne(ctx, &docs[1]); err != nil {
			return err
		}
		for len(sender.sent) < 2 && stream.Next(watchCtx) {
			var change mongodb.ChangeEvent[Notification]
			if err := stream.Decode(&change); err != nil {
				return err
			}
			if err := sendNotification(watchCtx, sender, &change); err != nil {
				return err
			}
			token = append(bson.Raw(nil), change.ResumeToken...)
		}
		if len(sender.sent) != 2 || len(token) == 0 {
			return errors.Join(errors.New("notification stream did not receive both inserts"), stream.Err(), watchCtx.Err())
		}
		return nil
	}()
	if err != nil {
		return err
	}
	// An in-process token lets WatchEach catch an insert made after the first
	// stream closed. This is not a persisted checkpoint or delivery guarantee.
	if _, err := r.InsertOne(ctx, &docs[2]); err != nil {
		return err
	}
	workerCtx, stopWorker := context.WithCancel(watchCtx)
	defer stopWorker()
	err = r.WatchEach(workerCtx, pipeline, func(ctx context.Context, change *mongodb.ChangeEvent[Notification]) error {
		if err := sendNotification(ctx, sender, change); err != nil {
			return err
		}
		stopWorker()
		return nil
	}, options.ChangeStream().SetResumeAfter(token).SetBatchSize(1).SetMaxAwaitTime(100*time.Millisecond))
	if !errors.Is(err, context.Canceled) {
		return errors.Join(errors.New("notification worker did not stop on cancellation"), err)
	}
	if len(sender.sent) != len(docs) {
		return errors.New("notification count differs from inserted fixtures")
	}
	for i, doc := range docs {
		expected := NotificationPayload{ID: doc.ID.Hex(), RoomID: doc.RoomID, Message: doc.Message, ContentID: doc.ContentID, Type: doc.Type, IsRead: doc.IsRead}
		if sender.sent[i] != expected {
			return errors.New("notification order, insert filtering or payload changed")
		}
	}
	fmt.Fprintln(s.out, "  notification inserts received in order; intermediate update filtered; ObjectID payload verified")
	fmt.Fprintln(s.out, "  WatchEach resumed with an in-memory token; cancellation closed the stream before database cleanup")
	return nil
}
