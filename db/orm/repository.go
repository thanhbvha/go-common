package orm

import (
	"context"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Repository is a generic data access object for GORM.
type Repository[T any] struct {
	db *gorm.DB
}

// NewRepository creates a new generic repository.
func NewRepository[T any](db *gorm.DB) *Repository[T] {
	return &Repository[T]{
		db: db,
	}
}

// DB returns the underlying gorm.DB for advanced usages.
func (r *Repository[T]) DB() *gorm.DB {
	return r.db
}

// WithTx returns a new repository instance bound to the given transaction.
func (r *Repository[T]) WithTx(tx *gorm.DB) *Repository[T] {
	return &Repository[T]{db: tx}
}

// WithUnscoped returns a new repository instance that bypasses soft-delete filters.
func (r *Repository[T]) WithUnscoped() *Repository[T] {
	return &Repository[T]{db: r.db.Unscoped()}
}

// FindByID retrieves a single record by its ID.
func (r *Repository[T]) FindByID(ctx context.Context, id interface{}) (*T, error) {
	var result T
	err := r.db.WithContext(ctx).First(&result, id).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil // Return nil instead of error when not found
		}
		return nil, err
	}
	return &result, nil
}

// FindOne retrieves a single record matching the conditions.
// conds is usually a query string and arguments, e.g. FindOne(ctx, "email = ?", "test@example.com")
func (r *Repository[T]) FindOne(ctx context.Context, conds ...interface{}) (*T, error) {
	var result T
	query := r.db.WithContext(ctx)
	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}
	err := query.First(&result).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &result, nil
}

// FindAll retrieves all records matching the conditions.
func (r *Repository[T]) FindAll(ctx context.Context, conds ...interface{}) ([]T, error) {
	var results []T
	query := r.db.WithContext(ctx)
	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}
	err := query.Find(&results).Error
	if err != nil {
		return nil, err
	}
	if results == nil {
		results = []T{}
	}
	return results, nil
}

// FindDistinct retrieves distinct records based on specific columns.
// e.g. FindDistinct(ctx, []string{"status"}, "age > ?", 18)
func (r *Repository[T]) FindDistinct(ctx context.Context, columns []string, conds ...interface{}) ([]T, error) {
	var results []T
	args := make([]interface{}, len(columns))
	for i, c := range columns {
		args[i] = c
	}
	
	query := r.db.WithContext(ctx).Distinct(args...)
	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}
	err := query.Find(&results).Error
	if err != nil {
		return nil, err
	}
	if results == nil {
		results = []T{}
	}
	return results, nil
}

// Pluck retrieves a single column from the database and scans it into the dest slice.
// If distinct is true, it applies the DISTINCT keyword to the column.
// e.g. Pluck(ctx, "status", &statuses, true, "age > ?", 18)
func (r *Repository[T]) Pluck(ctx context.Context, column string, dest interface{}, distinct bool, conds ...interface{}) error {
	query := r.db.WithContext(ctx).Model(new(T))
	if distinct {
		query = query.Distinct(column)
	}
	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}
	return query.Pluck(column, dest).Error
}

// RawQuery executes a raw SQL query and scans the result into the provided dest interface.
// dest must be a pointer to a slice or a struct, e.g. &[]CustomOutputStruct{}
func (r *Repository[T]) RawQuery(ctx context.Context, sql string, dest interface{}, values ...interface{}) error {
	return r.db.WithContext(ctx).Raw(sql, values...).Scan(dest).Error
}

// Aggregate allows executing complex queries (e.g. Group, Select, Joins) using GORM's query builder.
// dest must be a pointer to a slice or struct to hold the results.
// e.g. Aggregate(ctx, &results, func(db *gorm.DB) *gorm.DB { return db.Select("status, count(*) as total").Group("status") })
func (r *Repository[T]) Aggregate(ctx context.Context, dest interface{}, queryBuilder func(db *gorm.DB) *gorm.DB) error {
	query := r.db.WithContext(ctx).Model(new(T))
	if queryBuilder != nil {
		query = queryBuilder(query)
	}
	return query.Scan(dest).Error
}

// Insert creates a new record.
func (r *Repository[T]) Insert(ctx context.Context, model *T) error {
	return r.db.WithContext(ctx).Create(model).Error
}

// InsertMany creates multiple records in bulk.
func (r *Repository[T]) InsertMany(ctx context.Context, models []T) error {
	if len(models) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Create(&models).Error
}

// Upsert creates or updates a record. 
// - If conflict occurs on conflictColumns, it updates all fields by default.
// - If updateColumns are provided, it only updates those specific columns.
func (r *Repository[T]) Upsert(ctx context.Context, model *T, conflictColumns []string, updateColumns ...string) error {
	var cols []clause.Column
	for _, c := range conflictColumns {
		cols = append(cols, clause.Column{Name: c})
	}
	
	onConflict := clause.OnConflict{
		Columns: cols,
	}

	if len(updateColumns) > 0 {
		onConflict.DoUpdates = clause.AssignmentColumns(updateColumns)
	} else {
		onConflict.UpdateAll = true
	}

	return r.db.WithContext(ctx).Clauses(onConflict).Create(model).Error
}

// UpsertIgnore creates a record but silently does nothing if a conflict occurs on conflictColumns.
func (r *Repository[T]) UpsertIgnore(ctx context.Context, model *T, conflictColumns []string) error {
	var cols []clause.Column
	for _, c := range conflictColumns {
		cols = append(cols, clause.Column{Name: c})
	}
	
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   cols,
		DoNothing: true,
	}).Create(model).Error
}

// UpsertMany creates or updates multiple records in bulk. 
// - If conflict occurs on conflictColumns, it updates all fields by default.
// - If updateColumns are provided, it only updates those specific columns.
func (r *Repository[T]) UpsertMany(ctx context.Context, models []T, conflictColumns []string, updateColumns ...string) error {
	if len(models) == 0 {
		return nil
	}
	var cols []clause.Column
	for _, c := range conflictColumns {
		cols = append(cols, clause.Column{Name: c})
	}
	
	onConflict := clause.OnConflict{
		Columns: cols,
	}

	if len(updateColumns) > 0 {
		onConflict.DoUpdates = clause.AssignmentColumns(updateColumns)
	} else {
		onConflict.UpdateAll = true
	}

	return r.db.WithContext(ctx).Clauses(onConflict).Create(&models).Error
}

// UpsertIgnoreMany creates multiple records in bulk but silently does nothing for those that conflict on conflictColumns.
func (r *Repository[T]) UpsertIgnoreMany(ctx context.Context, models []T, conflictColumns []string) error {
	if len(models) == 0 {
		return nil
	}
	var cols []clause.Column
	for _, c := range conflictColumns {
		cols = append(cols, clause.Column{Name: c})
	}
	
	return r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   cols,
		DoNothing: true,
	}).Create(&models).Error
}

// Update saves all fields of the model.
func (r *Repository[T]) Update(ctx context.Context, model *T) error {
	return r.db.WithContext(ctx).Save(model).Error
}

// UpdateColumns updates specific columns of a record by ID.
func (r *Repository[T]) UpdateColumns(ctx context.Context, id interface{}, values interface{}) error {
	var model T
	return r.db.WithContext(ctx).Model(&model).Where("id = ?", id).Updates(values).Error
}

// Delete removes a record by ID (Soft delete if DeletedAt is present in model).
func (r *Repository[T]) Delete(ctx context.Context, id interface{}) error {
	var model T
	return r.db.WithContext(ctx).Delete(&model, id).Error
}

// Restore removes the soft-delete marker from a record, effectively restoring it.
func (r *Repository[T]) Restore(ctx context.Context, id interface{}) error {
	var model T
	return r.db.WithContext(ctx).Unscoped().Model(&model).Where("id = ?", id).Update("deleted_at", nil).Error
}

// Count returns the number of records matching the conditions.
func (r *Repository[T]) Count(ctx context.Context, conds ...interface{}) (int64, error) {
	var count int64
	var model T
	query := r.db.WithContext(ctx).Model(&model)
	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}
	err := query.Count(&count).Error
	return count, err
}

// Paginate executes a paginated query matching the conditions.
func (r *Repository[T]) Paginate(ctx context.Context, req PageRequest, conds ...interface{}) (*PageResponse[T], error) {
	query := r.db.WithContext(ctx).Model(new(T))
	if len(conds) > 0 {
		query = query.Where(conds[0], conds[1:]...)
	}
	return ExecutePagination[T](query, req)
}
