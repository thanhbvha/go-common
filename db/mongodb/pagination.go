package mongodb

import (
	"context"
	"errors"
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
	skip, err := p.checkedSkip()
	if err != nil {
		return math.MaxInt64 // Saturate for legacy callers; pagination returns an error.
	}
	return skip
}

func (p *PageRequest) checkedSkip() (int64, error) {
	page, size := int64(p.GetPage()-1), int64(p.GetSize())
	if page > math.MaxInt64/size {
		return 0, errors.New("mongodb pagination offset overflows int64")
	}
	return page * size, nil
}

func (p *PageRequest) GetLimit() int64 {
	return int64(p.GetSize())
}

// ExecutePagination helps run the CountDocuments and Find queries for MongoDB in one go.
// T is the slice type.
func ExecutePagination[T any](ctx context.Context, coll *mongo.Collection, filter interface{}, req PageRequest, findOpts ...options.Lister[options.FindOptions]) (*PageResponse[T], error) {
	skip, err := req.checkedSkip()
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter = bson.M{}
	}

	// 1. Count Total
	totalRows, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, err
	}

	// 2. Prepare FindOptions with Skip and Limit
	allOpts := pageOptions(findOpts, skip, req.GetLimit())

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

	pages := totalRows / req.GetLimit()
	if totalRows%req.GetLimit() != 0 {
		pages++
	}
	if pages > int64(math.MaxInt) {
		return nil, errors.New("mongodb total pages overflows int")
	}
	totalPages := int(pages)

	return &PageResponse[T]{
		TotalRows:  totalRows,
		TotalPages: totalPages,
		Page:       req.GetPage(),
		Size:       req.GetSize(),
		Items:      items,
	}, nil
}

// pageOptions supplies a stable default order. Custom sorts should include a
// unique tie-breaker such as _id. Pagination owns skip/limit, overriding callers.
func pageOptions(opts []options.Lister[options.FindOptions], skip, limit int64) []options.Lister[options.FindOptions] {
	all := make([]options.Lister[options.FindOptions], 0, len(opts)+2)
	all = append(all, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
	all = append(all, opts...)
	return append(all, options.Find().SetSkip(skip).SetLimit(limit))
}

// SlicePageResponse omits exact totals to avoid a separate CountDocuments query.
type SlicePageResponse[T any] struct {
	Page    int  `json:"page"`
	Size    int  `json:"size"`
	HasMore bool `json:"has_more"`
	Items   []T  `json:"items"`
}

// ExecuteSlicePagination fetches at most size+1 documents. Like offset pagination,
// it is intended for shallow pages; use indexed range queries for deep navigation.
func ExecuteSlicePagination[T any](ctx context.Context, coll *mongo.Collection, filter interface{}, req PageRequest, findOpts ...options.Lister[options.FindOptions]) (*SlicePageResponse[T], error) {
	skip, err := req.checkedSkip()
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter = bson.M{}
	}
	cursor, err := coll.Find(ctx, filter, pageOptions(findOpts, skip, req.GetLimit()+1)...)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	items := make([]T, 0)
	if err := cursor.All(ctx, &items); err != nil {
		return nil, err
	}
	hasMore := len(items) > req.GetSize()
	if hasMore {
		items = items[:req.GetSize()]
	}
	return &SlicePageResponse[T]{Page: req.GetPage(), Size: req.GetSize(), HasMore: hasMore, Items: items}, nil
}
