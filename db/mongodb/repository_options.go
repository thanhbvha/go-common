package mongodb

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// WithOptions variants preserve the original method signatures (including
// interface/function-value compatibility) while retaining transaction guards.
func (r *Repository[T]) FindByIDWithOptions(ctx context.Context, id any, opts ...options.Lister[options.FindOneOptions]) (*T, error) {
	return r.FindOneWithOptions(ctx, bson.M{"_id": id}, opts...)
}
func (r *Repository[T]) FindOneWithOptions(ctx context.Context, filter any, opts ...options.Lister[options.FindOneOptions]) (*T, error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	var value T
	err := r.coll.FindOne(ctx, filter, opts...).Decode(&value)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &value, nil
}
func (r *Repository[T]) InsertOneWithOptions(ctx context.Context, doc *T, opts ...options.Lister[options.InsertOneOptions]) (*mongo.InsertOneResult, error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	return r.coll.InsertOne(ctx, doc, opts...)
}
func (r *Repository[T]) InsertManyWithOptions(ctx context.Context, docs []any, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	return r.coll.InsertMany(ctx, docs, opts...)
}
func (r *Repository[T]) InsertManyTyped(ctx context.Context, docs []T, opts ...options.Lister[options.InsertManyOptions]) (*mongo.InsertManyResult, error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	values := make([]any, len(docs))
	for i := range docs {
		values[i] = docs[i]
	}
	return r.coll.InsertMany(ctx, values, opts...)
}
func (r *Repository[T]) UpdateOneWithOptions(ctx context.Context, filter, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	return r.coll.UpdateOne(ctx, filter, update, opts...)
}
func (r *Repository[T]) UpsertOneWithOptions(ctx context.Context, filter, update any, opts ...options.Lister[options.UpdateOneOptions]) (*mongo.UpdateResult, error) {
	all := append(append([]options.Lister[options.UpdateOneOptions]{}, opts...), options.UpdateOne().SetUpsert(true))
	return r.UpdateOneWithOptions(ctx, filter, update, all...)
}
func (r *Repository[T]) UpdateManyWithOptions(ctx context.Context, filter, update any, opts ...options.Lister[options.UpdateManyOptions]) (*mongo.UpdateResult, error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	return r.coll.UpdateMany(ctx, filter, update, opts...)
}
func (r *Repository[T]) DeleteOneWithOptions(ctx context.Context, filter any, opts ...options.Lister[options.DeleteOneOptions]) (*mongo.DeleteResult, error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	return r.coll.DeleteOne(ctx, filter, opts...)
}
func (r *Repository[T]) DeleteManyWithOptions(ctx context.Context, filter any, opts ...options.Lister[options.DeleteManyOptions]) (*mongo.DeleteResult, error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	return r.coll.DeleteMany(ctx, filter, opts...)
}
func (r *Repository[T]) CountWithOptions(ctx context.Context, filter any, opts ...options.Lister[options.CountOptions]) (int64, error) {
	if err := r.checkContext(ctx); err != nil {
		return 0, err
	}
	return r.coll.CountDocuments(ctx, filter, opts...)
}
func (r *Repository[T]) ExistsWithOptions(ctx context.Context, filter any, opts ...options.Lister[options.FindOneOptions]) (bool, error) {
	if err := r.checkContext(ctx); err != nil {
		return false, err
	}
	if filter == nil {
		filter = bson.M{}
	}
	all := append(append([]options.Lister[options.FindOneOptions]{}, opts...), options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	err := r.coll.FindOne(ctx, filter, all...).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	return err == nil, err
}
func (r *Repository[T]) BulkWriteWithOptions(ctx context.Context, models []mongo.WriteModel, opts ...options.Lister[options.BulkWriteOptions]) (*mongo.BulkWriteResult, error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	return r.coll.BulkWrite(ctx, models, opts...)
}
func (r *Repository[T]) CreateIndexWithOptions(ctx context.Context, model mongo.IndexModel, opts ...options.Lister[options.CreateIndexesOptions]) (string, error) {
	if err := r.checkContext(ctx); err != nil {
		return "", err
	}
	return r.coll.Indexes().CreateOne(ctx, model, opts...)
}
