package orm

import (
	"context"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Direction string

const (
	Ascending  Direction = "asc"
	Descending Direction = "desc"
)

type Sort struct {
	Field     string    `json:"field"`
	Direction Direction `json:"direction"`
}
type PageRequest struct {
	Page  int    `json:"page"`
	Size  int    `json:"size"`
	Sorts []Sort `json:"sorts"`
}
type PageResponse[T any] struct {
	TotalRows  int64 `json:"total_rows"`
	TotalPages int64 `json:"total_pages"`
	Page       int   `json:"page"`
	Size       int   `json:"size"`
	Items      []T   `json:"items"`
}

func (r *Repository[T]) page(req PageRequest) (PageRequest, int, error) {
	if req.Page == 0 {
		req.Page = 1
	}
	if req.Size == 0 {
		req.Size = 10
		if req.Size > r.opts.MaxPageSize {
			req.Size = r.opts.MaxPageSize
		}
	}
	if req.Page < 1 || req.Size < 1 || req.Size > r.opts.MaxPageSize {
		return req, 0, ErrInvalidArgument
	}
	// Division bounds multiplication before it happens, including MaxInt input.
	if req.Page-1 > r.opts.MaxOffset/req.Size {
		return req, 0, ErrInvalidArgument
	}
	if len(req.Sorts) > 8 {
		return req, 0, ErrInvalidArgument
	}
	seen := map[string]bool{}
	for _, s := range req.Sorts {
		if !r.sortable[s.Field] || seen[s.Field] || (s.Direction != Ascending && s.Direction != Descending) {
			return req, 0, ErrInvalidArgument
		}
		seen[s.Field] = true
	}
	return req, (req.Page - 1) * req.Size, nil
}
func (r *Repository[T]) ordered(db *gorm.DB, sorts []Sort) *gorm.DB {
	hasPK := false
	for _, s := range sorts {
		db = db.Order(clause.OrderByColumn{Column: clause.Column{Table: clause.CurrentTable, Name: s.Field}, Desc: s.Direction == Descending})
		hasPK = hasPK || s.Field == r.pk
	}
	if !hasPK {
		db = db.Order(clause.OrderByColumn{Column: clause.Column{Table: clause.CurrentTable, Name: r.pk}})
	}
	return db
}

// Paginate issues separate count/read statements. Totals can change under
// concurrent writes. Use a repeatable-read transaction for snapshot consistency.
// Filters must add predicates only (not limits, ordering, projections or groups).
func (r *Repository[T]) Paginate(ctx context.Context, req PageRequest, filters ...Scope) (*PageResponse[T], error) {
	req, offset, err := r.page(req)
	if err != nil {
		return nil, err
	}
	db, cancel, err := r.query(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	db, err = applyFilters(db, filters)
	if err != nil {
		return nil, err
	}
	var total int64
	if err := r.resultError("count", db.Session(&gorm.Session{}).Count(&total)); err != nil {
		return nil, err
	}
	items := make([]T, 0)
	if err := r.resultError("paginate", r.ordered(db.Session(&gorm.Session{}), req.Sorts).Offset(offset).Limit(req.Size).Find(&items)); err != nil {
		return nil, err
	}
	pages := total / int64(req.Size)
	if total%int64(req.Size) != 0 {
		pages++
	}
	return &PageResponse[T]{TotalRows: total, TotalPages: pages, Page: req.Page, Size: req.Size, Items: items}, nil
}
