package graph

import (
	"context"
	"github.com/thanhbvha/go-common/examples/graphql/graph/model"
	"github.com/thanhbvha/go-common/examples/graphql/service"
	common "github.com/thanhbvha/go-common/graphql"
	"github.com/thanhbvha/go-common/xerrors"
)

type loadersKey struct{}
type Loaders struct {
	Users         *common.DataLoader[int, *model.User]
	PostsByAuthor *common.DataLoader[int, []*model.Post]
}

// WithLoaders is called by the core after establishing the request deadline.
// Authentication middleware, when present, must wrap the core handler.
func WithLoaders(ctx context.Context) (context.Context, error) {
	users, err := common.NewDataLoader(ctx, service.FetchUsersBatch, common.LoaderConfig{})
	if err != nil {
		return nil, err
	}
	posts, err := common.NewDataLoader(ctx, service.FetchPostsByAuthorBatch, common.LoaderConfig{})
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, loadersKey{}, &Loaders{Users: users, PostsByAuthor: posts}), nil
}
func requestLoaders(ctx context.Context) (*Loaders, error) {
	loaders, ok := ctx.Value(loadersKey{}).(*Loaders)
	if !ok || loaders == nil {
		return nil, xerrors.New("INTERNAL_ERROR", "Loaders not configured", 500)
	}
	return loaders, nil
}

func page[T any](items []T, first, offset int) ([]T, error) {
	if first < 1 || first > 100 || offset < 0 {
		return nil, xerrors.New("BAD_USER_INPUT", "first must be 1..100 and offset must be non-negative", 400)
	}
	if offset >= len(items) {
		return []T{}, nil
	}
	end := len(items)
	if first < end-offset {
		end = offset + first
	}
	return items[offset:end], nil
}

// ListCost caps invalid arguments before multiplication; the resolver rejects
// those arguments rather than using them to allocate/query an unbounded list.
func ListCost(childComplexity, first, offset int) int {
	if first < 1 || first > 100 {
		first = 101
	}
	maxInt := int(^uint(0) >> 1)
	if childComplexity > (maxInt-1)/first {
		return maxInt
	}
	return 1 + first*childComplexity
}
