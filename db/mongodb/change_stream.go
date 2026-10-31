package mongodb

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ChangeNamespace identifies the source collection of a change event.
type ChangeNamespace struct {
	Database   string `bson:"db"`
	Collection string `bson:"coll"`
}

// ChangeEvent is a change notification, not a stored document. ResumeToken is
// opaque BSON. FullDocument may be nil (for example for deletes or updates
// without lookup). DocumentKey preserves the original BSON identifier type.
type ChangeEvent[T any] struct {
	ResumeToken   bson.Raw        `bson:"_id"`
	OperationType string          `bson:"operationType"`
	FullDocument  *T              `bson:"fullDocument"`
	DocumentKey   bson.Raw        `bson:"documentKey"`
	Namespace     ChangeNamespace `bson:"ns"`
	ClusterTime   bson.Timestamp  `bson:"clusterTime"`
}

// Watch opens a collection change stream. The caller owns and must close it.
// A nil pipeline means all changes. Driver options and resumability are retained;
// no checkpoint persistence or application-level restart loop is added.
// Change streams require a replica set or supported sharded deployment.
func (r *Repository[T]) Watch(ctx context.Context, pipeline mongo.Pipeline, opts ...options.Lister[options.ChangeStreamOptions]) (*mongo.ChangeStream, error) {
	if r.transaction != nil {
		return nil, ErrChangeStreamInTransaction
	}
	if session := mongo.SessionFromContext(ctx); session != nil && session.TransactionRunning() {
		return nil, ErrChangeStreamInTransaction
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pipeline == nil {
		pipeline = mongo.Pipeline{}
	}
	return r.coll.Watch(ctx, pipeline, opts...)
}

// WatchEach delivers typed events sequentially until cancellation, a callback or
// driver error, or server-side closure. The callback must observe ctx and return;
// its errors stop consumption without replay. It must not operate on the same
// stream concurrently. Cancel and wait for watchers before DisconnectAll: manager
// transaction draining does not track change streams. Cleanup is bounded even
// when ctx has expired. Panics propagate after cleanup.
func (r *Repository[T]) WatchEach(ctx context.Context, pipeline mongo.Pipeline, handler func(context.Context, *ChangeEvent[T]) error, opts ...options.Lister[options.ChangeStreamOptions]) (result error) {
	if handler == nil {
		return errors.New("mongodb change stream callback is required")
	}
	stream, err := r.Watch(ctx, pipeline, opts...)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		result = errors.Join(result, stream.Close(cleanup))
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !stream.Next(ctx) {
			return errors.Join(ctx.Err(), stream.Err())
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var change ChangeEvent[T]
		if err := stream.Decode(&change); err != nil {
			return err
		}
		if err := handler(ctx, &change); err != nil {
			return err
		}
	}
}
