package orm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// CursorRequest traverses the unique primary key only. Immutable, indexed keys
// avoid deep OFFSET scans. Cursors are positions, never authorization tokens;
// keep the same application filters and tenant scope on every page.
type CursorRequest struct {
	Size      int       `json:"size"`
	After     string    `json:"after"`
	Direction Direction `json:"direction"`
}
type CursorResponse[T any] struct {
	Items []T    `json:"items"`
	Next  string `json:"next,omitempty"`
}
type cursorToken struct {
	Version   int       `json:"v"`
	Table     string    `json:"t"`
	Key       string    `json:"k"`
	Direction Direction `json:"d"`
	Value     string    `json:"p"`
}

func (r *Repository[T]) decodeCursor(raw string, d Direction) (any, error) {
	if len(raw) > 8192 {
		return nil, ErrInvalidArgument
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, ErrInvalidArgument
	}
	var token cursorToken
	if json.Unmarshal(data, &token) != nil || token.Version != 1 || token.Table != r.schema.Table || token.Key != r.pk || token.Direction != d || len(token.Value) > 4096 {
		return nil, ErrInvalidArgument
	}
	field := r.schema.PrimaryFields[0]
	switch field.DataType {
	case schema.Int:
		v, e := strconv.ParseInt(token.Value, 10, 64)
		if e != nil || v <= 0 {
			return nil, ErrInvalidArgument
		}
		return v, nil
	case schema.Uint:
		v, e := strconv.ParseUint(token.Value, 10, 64)
		if e != nil || v == 0 {
			return nil, ErrInvalidArgument
		}
		return v, nil
	case schema.String:
		if token.Value == "" {
			return nil, ErrInvalidArgument
		}
		return token.Value, nil
	}
	return nil, ErrInvalidArgument
}
func (r *Repository[T]) CursorPage(ctx context.Context, req CursorRequest, filters ...Scope) (*CursorResponse[T], error) {
	if req.Size == 0 {
		req.Size = 10
		if req.Size > r.opts.MaxPageSize {
			req.Size = r.opts.MaxPageSize
		}
	}
	if req.Direction == "" {
		req.Direction = Ascending
	}
	if req.Size < 1 || req.Size > r.opts.MaxPageSize || (req.Direction != Ascending && req.Direction != Descending) {
		return nil, ErrInvalidArgument
	}
	var after any
	if req.After != "" {
		var err error
		after, err = r.decodeCursor(req.After, req.Direction)
		if err != nil {
			return nil, err
		}
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
	column := clause.Column{Table: clause.CurrentTable, Name: r.pk}
	if after != nil {
		if req.Direction == Descending {
			db = db.Where(clause.Lt{Column: column, Value: after})
		} else {
			db = db.Where(clause.Gt{Column: column, Value: after})
		}
	}
	items := make([]T, 0)
	if err := r.resultError("cursor", db.Order(clause.OrderByColumn{Column: column, Desc: req.Direction == Descending}).Limit(req.Size+1).Find(&items)); err != nil {
		return nil, err
	}
	result := &CursorResponse[T]{Items: items}
	if len(items) > req.Size {
		result.Items = items[:req.Size]
		value, zero := r.schema.PrimaryFields[0].ValueOf(ctx, reflect.ValueOf(&result.Items[req.Size-1]))
		if zero {
			return nil, ErrInvalidArgument
		}
		scalar, err := scalarValue(value)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(cursorToken{Version: 1, Table: r.schema.Table, Key: r.pk, Direction: req.Direction, Value: fmt.Sprint(scalar)})
		if err != nil {
			return nil, normalize("cursor", err)
		}
		result.Next = base64.RawURLEncoding.EncodeToString(data)
		if len(result.Next) > 8192 {
			return nil, ErrInvalidArgument
		}
	}
	return result, nil
}
