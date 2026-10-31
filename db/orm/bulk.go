package orm

import (
	"context"
	"database/sql/driver"
	"reflect"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// bulkChunks conservatively budgets bound values and SQL/protocol overhead.
// Custom Valuers/serializers must be pure; hooks remain trusted code and can
// change values later, so the database driver's packet limit remains authoritative.
func (r *Repository[T]) bulkChunks(ctx context.Context, models []T) ([][2]int, error) {
	limit := r.source.owner().capabilities.MaxBatchBytes
	base := 1024 + 2*len(r.schema.Table)
	for _, name := range r.schema.DBNames {
		base += 2*len(name) + 32
	}
	if base >= limit {
		return nil, ErrUnsupportedFeature
	}
	chunks := make([][2]int, 0)
	start, size := 0, base
	for i := range models {
		if err := ctx.Err(); err != nil {
			return nil, normalize("bulk_budget", err)
		}
		rowSize := 32
		for _, field := range r.schema.Fields {
			if field.DBName == "" || !field.Creatable {
				continue
			}
			value, _ := field.ValueOf(ctx, reflect.ValueOf(&models[i]))
			converted, err := driver.DefaultParameterConverter.ConvertValue(value)
			if err != nil {
				return nil, r.source.owner().normalize("bulk_value", err)
			}
			bytes := 64
			switch v := converted.(type) {
			case string:
				if len(v) > limit/2 {
					return nil, ErrInvalidArgument
				}
				bytes = 2*len(v) + 32
			case []byte:
				if len(v) > limit/2 {
					return nil, ErrInvalidArgument
				}
				bytes = 2*len(v) + 32
			case time.Time:
				bytes = 64
			}
			if bytes > limit-rowSize {
				return nil, ErrInvalidArgument
			}
			rowSize += bytes
		}
		if rowSize > limit-base {
			return nil, ErrInvalidArgument
		}
		if i > start && (i-start >= r.opts.BatchSize || rowSize > limit-size) {
			chunks = append(chunks, [2]int{start, i})
			start = i
			size = base
		}
		size += rowSize
	}
	if start < len(models) {
		chunks = append(chunks, [2]int{start, len(models)})
	}
	return chunks, nil
}
func (r *Repository[T]) createBatches(op string, db *gorm.DB, models []T, affected ...*int64) error {
	if _, inTransaction := r.source.(*Transaction); inTransaction && !r.source.owner().capabilities.Savepoints {
		return ErrUnsupportedFeature
	}
	chunks, err := r.bulkChunks(db.Statement.Context, models)
	if err != nil {
		return err
	}
	var rows int64
	err = db.Transaction(func(tx *gorm.DB) error {
		for _, chunk := range chunks {
			if err := db.Statement.Context.Err(); err != nil {
				return err
			}
			part := models[chunk[0]:chunk[1]]
			result := tx.Session(&gorm.Session{SkipDefaultTransaction: true}).Omit(clause.Associations).Create(&part)
			if err := r.resultError(op, result); err != nil {
				return err
			}
			rows += result.RowsAffected
		}
		return nil
	})
	if err != nil {
		return r.source.owner().normalize(op, err)
	}
	if len(affected) > 0 {
		*affected[0] = rows
	}
	return nil
}
