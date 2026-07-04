package mongodb

import (
	"context"
	"math"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// PageRequest represents a standard pagination request from an API.
type PageRequest struct {
	Page  int      `json:"page" query:"page" form:"page"`
	Size  int      `json:"size" query:"size" form:"size"`
	Sorts []string `json:"sorts" query:"sorts" form:"sorts"` // Must be parsed into bson.D in business logic if needed
}

// PageResponse is a generic struct for returning paginated items.
type PageResponse[T any] struct {
	TotalRows  int64 `json:"total_rows"`
	TotalPages int   `json:"total_pages"`
	Page       int   `json:"page"`
	Size       int   `json:"size"`
	Items      []T   `json:"items"`
}

func (p *PageRequest) GetPage() int {
	if p.Page <= 0 {
		return 1
	}
	return p.Page
}

func (p *PageRequest) GetSize() int {
	if p.Size <= 0 {
		return 10
	}
	if p.Size > 1000 {
		return 1000 // Hard cap to prevent memory issues
	}
	return p.Size
}

func (p *PageRequest) GetSkip() int64 {
	return int64((p.GetPage() - 1) * p.GetSize())
}

func (p *PageRequest) GetLimit() int64 {
	return int64(p.GetSize())
}

// ExecutePagination helps run the CountDocuments and Find queries for MongoDB in one go.
// T is the slice type.
func ExecutePagination[T any](ctx context.Context, coll *mongo.Collection, filter interface{}, req PageRequest, findOpts ...options.Lister[options.FindOptions]) (*PageResponse[T], error) {
	if filter == nil {
		filter = bson.M{}
	}

	// 1. Count Total
	totalRows, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}

	// 2. Prepare FindOptions with Skip and Limit
	opts := options.Find().SetSkip(req.GetSkip()).SetLimit(req.GetLimit())
	
	// Apply user-provided options (like Sort, Projection) if any
	allOpts := append([]options.Lister[options.FindOptions]{opts}, findOpts...)

	// 3. Find
	cursor, err := coll.Find(ctx, filter, allOpts...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var items []T
	if err := cursor.All(ctx, &items); err != nil {
		return nil, err
	}

	// Ensure slice is not nil for JSON marshalling
	if items == nil {
		items = []T{}
	}

	totalPages := int(math.Ceil(float64(totalRows) / float64(req.GetSize())))

	return &PageResponse[T]{
		TotalRows:  totalRows,
		TotalPages: totalPages,
		Page:       req.GetPage(),
		Size:       req.GetSize(),
		Items:      items,
	}, nil
}
