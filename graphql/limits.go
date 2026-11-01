package graphql

import (
	"context"

	"github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type operationLimits struct{ depth, fields int }

func (operationLimits) ExtensionName() string                   { return "OperationLimits" }
func (operationLimits) Validate(graphql.ExecutableSchema) error { return nil }
func (l operationLimits) MutateOperationContext(ctx context.Context, op *graphql.OperationContext) *gqlerror.Error {
	if op.Operation.Operation == ast.Subscription {
		return limitError("UNSUPPORTED_OPERATION", "Subscriptions are not supported")
	}
	remaining, visits := l.fields, max(10000, l.fields)
	active := map[string]bool{}
	// A visit budget also bounds fragment chains containing no fields.
	var walk func(ast.SelectionSet, int) bool
	walk = func(set ast.SelectionSet, depth int) bool {
		for _, selection := range set {
			visits--
			if visits < 0 || ctx.Err() != nil {
				return false
			}
			switch v := selection.(type) {
			case *ast.Field:
				remaining--
				if remaining < 0 || depth > l.depth {
					return false
				}
				if !walk(v.SelectionSet, depth+1) {
					return false
				}
			case *ast.InlineFragment:
				if !walk(v.SelectionSet, depth) {
					return false
				}
			case *ast.FragmentSpread:
				f := op.Doc.Fragments.ForName(v.Name)
				if f == nil || active[v.Name] {
					return false
				}
				active[v.Name] = true
				ok := walk(f.SelectionSet, depth)
				delete(active, v.Name)
				if !ok {
					return false
				}
			}
		}
		return true
	}
	if !walk(op.Operation.SelectionSet, 1) {
		if ctx.Err() != nil {
			return gqlerror.Wrap(ctx.Err())
		}
		return limitError("QUERY_LIMIT_EXCEEDED", "Operation exceeds selection limits")
	}
	return nil
}
func limitError(code, message string) *gqlerror.Error {
	return &gqlerror.Error{Message: message, Extensions: map[string]any{"code": code}}
}
