package mongodb

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Repository is a generic data access object for MongoDB.
type Repository[T any] struct {
	coll *mongo.Collection
}

// NewRepository creates a new generic repository for the given collection.
func NewRepository[T any](db *mongo.Database, collectionName string) *Repository[T] {
	return &Repository[T]{
		coll: db.Collection(collectionName),
	}
}

// Collection returns the underlying mongo.Collection for advanced usages.
func (r *Repository[T]) Collection() *mongo.Collection {
	return r.coll
}

// FindByID retrieves a single document by its ID.
func (r *Repository[T]) FindByID(ctx context.Context, id interface{}) (*T, error) {
	var result T
	err := r.coll.FindOne(ctx, bson.M{"_id": id}).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil // Return nil instead of error when not found
		}
		return nil, err
	}
	return &result, nil
}

// FindOne retrieves a single document matching the filter.
func (r *Repository[T]) FindOne(ctx context.Context, filter interface{}) (*T, error) {
	var result T
	err := r.coll.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, err
	}
	return &result, nil
}

// FindAll retrieves all documents matching the filter.
func (r *Repository[T]) FindAll(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOptions]) ([]T, error) {
	cursor, err := r.coll.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []T
	if err := cursor.All(ctx, &results); err != nil {
		return nil, err
	}

	if results == nil {
		results = []T{}
	}
	return results, nil
}

// Distinct retrieves a list of distinct values for a single field across a single collection.
// results must be a pointer to a slice (e.g. &[]string{} or &[]int{}).
func (r *Repository[T]) Distinct(ctx context.Context, fieldName string, filter interface{}, results interface{}, opts ...options.Lister[options.DistinctOptions]) error {
	if filter == nil {
		filter = bson.M{}
	}
	res := r.coll.Distinct(ctx, fieldName, filter, opts...)
	if err := res.Err(); err != nil {
		return err
	}
	return res.Decode(results)
}

// InsertOne inserts a single document.
func (r *Repository[T]) InsertOne(ctx context.Context, doc *T) (*mongo.InsertOneResult, error) {
	return r.coll.InsertOne(ctx, doc)
}

// InsertMany inserts multiple documents.
func (r *Repository[T]) InsertMany(ctx context.Context, docs []interface{}) (*mongo.InsertManyResult, error) {
	return r.coll.InsertMany(ctx, docs)
}

// UpdateOne updates a single document matching the filter.
func (r *Repository[T]) UpdateOne(ctx context.Context, filter interface{}, update interface{}) (*mongo.UpdateResult, error) {
	return r.coll.UpdateOne(ctx, filter, update)
}

// UpsertOne creates or updates a single document matching the filter.
func (r *Repository[T]) UpsertOne(ctx context.Context, filter interface{}, update interface{}) (*mongo.UpdateResult, error) {
	opts := options.UpdateOne().SetUpsert(true)
	return r.coll.UpdateOne(ctx, filter, update, opts)
}

// FindOneAndUpdate updates a single document and retrieves either the original or updated document.
func (r *Repository[T]) FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opts ...options.Lister[options.FindOneAndUpdateOptions]) (*T, error) {
	var result T
	err := r.coll.FindOneAndUpdate(ctx, filter, update, opts...).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &result, nil
}

// UpdateMany updates multiple documents matching the filter.
func (r *Repository[T]) UpdateMany(ctx context.Context, filter interface{}, update interface{}) (*mongo.UpdateResult, error) {
	return r.coll.UpdateMany(ctx, filter, update)
}

// DeleteOne deletes a single document matching the filter.
func (r *Repository[T]) DeleteOne(ctx context.Context, filter interface{}) (*mongo.DeleteResult, error) {
	return r.coll.DeleteOne(ctx, filter)
}

// FindOneAndDelete deletes a single document and retrieves it.
func (r *Repository[T]) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...options.Lister[options.FindOneAndDeleteOptions]) (*T, error) {
	var result T
	err := r.coll.FindOneAndDelete(ctx, filter, opts...).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &result, nil
}

// DeleteMany deletes multiple documents matching the filter.
func (r *Repository[T]) DeleteMany(ctx context.Context, filter interface{}) (*mongo.DeleteResult, error) {
	return r.coll.DeleteMany(ctx, filter)
}

// Count returns the number of documents matching the filter.
func (r *Repository[T]) Count(ctx context.Context, filter interface{}) (int64, error) {
	return r.coll.CountDocuments(ctx, filter)
}

// Exists checks if at least one document matches the filter.
func (r *Repository[T]) Exists(ctx context.Context, filter interface{}) (bool, error) {
	opts := options.Find().SetLimit(1)
	cursor, err := r.coll.Find(ctx, filter, opts)
	if err != nil {
		return false, err
	}
	defer cursor.Close(ctx)

	// If cursor has at least one document
	return cursor.Next(ctx), nil
}

// Aggregate executes an aggregation pipeline.
// results must be a pointer to a slice, e.g. &[]CustomOutputStruct{}
func (r *Repository[T]) Aggregate(ctx context.Context, pipeline interface{}, results interface{}, opts ...options.Lister[options.AggregateOptions]) error {
	cursor, err := r.coll.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	return cursor.All(ctx, results)
}

// BulkWrite executes a bulk write operation.
func (r *Repository[T]) BulkWrite(ctx context.Context, models []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
	return r.coll.BulkWrite(ctx, models)
}

// CreateIndex creates a single index.
func (r *Repository[T]) CreateIndex(ctx context.Context, model mongo.IndexModel) (string, error) {
	return r.coll.Indexes().CreateOne(ctx, model)
}

// Paginate uses the built-in ExecutePagination function on this collection.
func (r *Repository[T]) Paginate(ctx context.Context, filter interface{}, req PageRequest, opts ...options.Lister[options.FindOptions]) (*PageResponse[T], error) {
	return ExecutePagination[T](ctx, r.coll, filter, req, opts...)
}
