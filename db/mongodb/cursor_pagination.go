package mongodb

import (
	"context"
	"encoding/base64"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CursorPageRequest uses indexed _id ranges; the collection must use a uniform
// scalar _id type. After is an opaque BSON/base64 token, not an authorization token.
// Keep filter/collation/direction consistent between pages. Size defaults to 10,
// capped at 1000. This API owns sort, skip, limit and cursor type.
type CursorPageRequest struct {
	Size       int    `json:"size"`
	After      string `json:"after"`
	Descending bool   `json:"descending"`
}
type CursorPageResponse[T any] struct {
	Items      []T    `json:"items"`
	HasMore    bool   `json:"has_more"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type cursorToken struct {
	ID         bson.RawValue `bson:"id"`
	Descending bool          `bson:"descending"`
}

const maxCursorLength = 4096

func validCursorID(id bson.RawValue) bool {
	switch id.Type {
	case bson.TypeString, bson.TypeObjectID, bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128, bson.TypeDateTime, bson.TypeBinary, bson.TypeBoolean:
		return true
	default:
		return false
	}
}
func decodeCursor(req CursorPageRequest) (bson.RawValue, error) {
	if len(req.After) > maxCursorLength {
		return bson.RawValue{}, ErrInvalidCursor
	}
	data, err := base64.RawURLEncoding.DecodeString(req.After)
	if err != nil {
		return bson.RawValue{}, ErrInvalidCursor
	}
	var token cursorToken
	if err := bson.Unmarshal(data, &token); err != nil || token.Descending != req.Descending || !validCursorID(token.ID) {
		return bson.RawValue{}, ErrInvalidCursor
	}
	return token.ID, nil
}

func ExecuteCursorPagination[T any](ctx context.Context, coll *mongo.Collection, filter any, req CursorPageRequest, findOpts ...options.Lister[options.FindOptions]) (result *CursorPageResponse[T], resultErr error) {
	if filter == nil {
		filter = bson.D{}
	}
	if req.After != "" {
		id, err := decodeCursor(req)
		if err != nil {
			return nil, err
		}
		operator := "$gt"
		if req.Descending {
			operator = "$lt"
		}
		filter = bson.D{{Key: "$and", Value: bson.A{filter, bson.D{{Key: "_id", Value: bson.D{{Key: operator, Value: id}}}}}}}
	}
	page := PageRequest{Size: req.Size}
	direction := 1
	if req.Descending {
		direction = -1
	}
	all := append([]options.Lister[options.FindOptions]{}, findOpts...)
	all = append(all, options.Find().SetSort(bson.D{{Key: "_id", Value: direction}}).SetSkip(0).SetLimit(page.GetLimit()+1).SetCursorType(options.NonTailable))
	cursor, err := coll.Find(ctx, filter, all...)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, closeCursor(ctx, cursor))
		if resultErr != nil {
			result = nil
		}
	}()
	items := make([]T, 0, page.GetSize())
	var lastID bson.RawValue
	hasMore := false
	for cursor.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(items) == page.GetSize() {
			hasMore = true
			break
		}
		id := cursor.Current.Lookup("_id")
		if !validCursorID(id) {
			return nil, ErrInvalidCursor
		} // Projection must retain _id.
		lastID = bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)}
		var item T
		if err := cursor.Decode(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}
	next := ""
	if hasMore {
		data, err := bson.Marshal(cursorToken{ID: lastID, Descending: req.Descending})
		if err != nil {
			return nil, ErrInvalidCursor
		}
		next = base64.RawURLEncoding.EncodeToString(data)
		if len(next) > maxCursorLength {
			return nil, ErrInvalidCursor
		}
	}
	return &CursorPageResponse[T]{Items: items, HasMore: hasMore, NextCursor: next}, nil
}

func (r *Repository[T]) PaginateCursor(ctx context.Context, filter any, req CursorPageRequest, opts ...options.Lister[options.FindOptions]) (*CursorPageResponse[T], error) {
	if err := r.checkContext(ctx); err != nil {
		return nil, err
	}
	return ExecuteCursorPagination[T](ctx, r.coll, filter, req, opts...)
}
