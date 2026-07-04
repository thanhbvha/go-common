package orm

import (
	"math"

	"gorm.io/gorm"
)

// PageRequest represents a standard pagination request from an API.
type PageRequest struct {
	Page  int      `json:"page" query:"page" form:"page"`
	Size  int      `json:"size" query:"size" form:"size"`
	Sorts []string `json:"sorts" query:"sorts" form:"sorts"` // e.g. "created_at desc"
}

// PageResponse is a generic struct for returning paginated items.
type PageResponse[T any] struct {
	TotalRows  int64 `json:"total_rows"`
	TotalPages int   `json:"total_pages"`
	Page       int   `json:"page"`
	Size       int   `json:"size"`
	Items      []T   `json:"items"`
}

// GetPage safely gets the page number (1-indexed).
func (p *PageRequest) GetPage() int {
	if p.Page <= 0 {
		return 1
	}
	return p.Page
}

// GetSize safely gets the page size.
func (p *PageRequest) GetSize() int {
	if p.Size <= 0 {
		return 10
	}
	if p.Size > 1000 {
		return 1000 // Hard cap to prevent memory exhaustion
	}
	return p.Size
}

// GetOffset calculates the SQL offset.
func (p *PageRequest) GetOffset() int {
	return (p.GetPage() - 1) * p.GetSize()
}

// Paginate is a GORM Scope that automatically applies OFFSET, LIMIT, and ORDER BY.
func Paginate(req PageRequest) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if len(req.Sorts) > 0 {
			for _, sort := range req.Sorts {
				db = db.Order(sort)
			}
		}
		return db.Offset(req.GetOffset()).Limit(req.GetSize())
	}
}

// ExecutePagination runs the Count and Find queries, returning a populated PageResponse.
// db should be the query with all filters applied (e.g. Where).
func ExecutePagination[T any](db *gorm.DB, req PageRequest) (*PageResponse[T], error) {
	var totalRows int64
	var items []T

	// Run Count before applying pagination offsets
	if err := db.Model(new(T)).Count(&totalRows).Error; err != nil {
		return nil, err
	}

	// Apply pagination and fetch items
	if err := db.Scopes(Paginate(req)).Find(&items).Error; err != nil {
		return nil, err
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
