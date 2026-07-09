package graphql

import (
	"context"
	"errors"

	"github.com/99designs/gqlgen/graphql"
	"github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/xerrors"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// ErrorPresenter format lại lỗi của GraphQL nếu nó là lỗi thuộc hệ thống xerrors
func ErrorPresenter(ctx context.Context, e error) *gqlerror.Error {
	// Dùng bộ format mặc định để lấy path và location
	err := graphql.DefaultErrorPresenter(ctx, e)

	var customErr *xerrors.CustomError
	if errors.As(e, &customErr) {
		err.Message = customErr.Message
		
		if err.Extensions == nil {
			err.Extensions = map[string]interface{}{}
		}
		
		// Đẩy các thông tin custom ra extension cho client
		err.Extensions["code"] = customErr.Code
		err.Extensions["http_status"] = customErr.HTTPStatus
	} else {
		// Ẩn chi tiết lỗi thật của hệ thống ra bên ngoài nếu không dùng xerrors
		err.Message = "Internal Server Error"
		if err.Extensions == nil {
			err.Extensions = map[string]interface{}{}
		}
		err.Extensions["code"] = "INTERNAL_ERROR"
		err.Extensions["http_status"] = 500
	}

	return err
}

// RecoverFunc tự động bắt panic trong resolver để tránh sập server
func RecoverFunc(ctx context.Context, err interface{}) error {
	logger.Error("GraphQL Panic", "error", err)
	return xerrors.New("INTERNAL_ERROR", "Hệ thống đang gặp sự cố, vui lòng thử lại sau.", 500)
}
