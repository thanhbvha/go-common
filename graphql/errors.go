package graphql

import (
	"context"
	"errors"

	"github.com/99designs/gqlgen/graphql"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/xerrors"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// ErrorPresenter formats GraphQL errors if they belong to the xerrors system
func ErrorPresenter(ctx context.Context, e error) *gqlerror.Error {
	// Use the default formatter to get path and location
	err := graphql.DefaultErrorPresenter(ctx, e)

	var customErr *xerrors.CustomError
	if errors.As(e, &customErr) {
		err.Message = customErr.Message
		
		if err.Extensions == nil {
			err.Extensions = map[string]interface{}{}
		}
		
		// Push custom information out to the extension for the client
		err.Extensions["code"] = customErr.Code
		err.Extensions["http_status"] = customErr.HTTPStatus
	} else {
		// Hide the actual system error details from the outside if not using xerrors
		err.Message = "Internal Server Error"
		if err.Extensions == nil {
			err.Extensions = map[string]interface{}{}
		}
		err.Extensions["code"] = "INTERNAL_ERROR"
		err.Extensions["http_status"] = 500
	}

	return err
}

// RecoverFunc automatically catches panics in resolvers to prevent server crashes
func RecoverFunc(ctx context.Context, err interface{}) error {
	logger.Error("GraphQL Panic", "error", err)
	return xerrors.New("INTERNAL_ERROR", "The system encountered an unexpected error, please try again later.", 500)
}
