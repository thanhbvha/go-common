package mongodb

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func closeCursor(ctx context.Context, cursor *mongo.Cursor) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return cursor.Close(cleanup)
}

func iterateCursor[T any](ctx context.Context, cursor *mongo.Cursor, fn func(context.Context, *T) error) (result error) {
	defer func() { result = errors.Join(result, closeCursor(ctx, cursor)) }()
	for cursor.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return err
		}
		var value T
		if err := cursor.Decode(&value); err != nil {
			return err
		}
		if err := fn(ctx, &value); err != nil {
			return err
		}
	}
	return cursor.Err()
}

// ExecuteAggregateEach streams a pipeline into an independently typed output.
// The driver batch size limits buffering, not total work; supply a context budget.
func ExecuteAggregateEach[T any](ctx context.Context, coll *mongo.Collection, pipeline any, fn func(context.Context, *T) error, opts ...options.Lister[options.AggregateOptions]) error {
	if fn == nil {
		return errors.New("mongodb aggregate callback is required")
	}
	cursor, err := coll.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return err
	}
	return iterateCursor(ctx, cursor, fn)
}

func (r *Repository[T]) AggregateEach(ctx context.Context, pipeline any, fn func(context.Context, *T) error, opts ...options.Lister[options.AggregateOptions]) error {
	if err := r.checkContext(ctx); err != nil {
		return err
	}
	return ExecuteAggregateEach(ctx, r.coll, pipeline, fn, opts...)
}

// findEach shares decoding and cleanup semantics with streaming aggregation.
func findEach[T any](ctx context.Context, coll *mongo.Collection, filter any, fn func(context.Context, *T) error, opts ...options.Lister[options.FindOptions]) error {
	if fn == nil {
		return errors.New("mongodb document callback is required")
	}
	if filter == nil {
		filter = bson.M{}
	}
	cursor, err := coll.Find(ctx, filter, opts...)
	if err != nil {
		return err
	}
	return iterateCursor(ctx, cursor, fn)
}
