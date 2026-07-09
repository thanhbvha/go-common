# GraphQL Module

A production-ready, framework-agnostic GraphQL wrapper for `go-common`, built on top of [gqlgen](https://gqlgen.com/).

## Tính năng nổi bật
- **Framework-Agnostic**: Hỗ trợ tích hợp mượt mà với **Fiber**, **Gin**, và **Echo** thông qua các Adapter riêng biệt.
- **Generic DataLoader (Go 1.22+)**: Giải quyết bài toán N+1 hiệu suất cao, không cần sinh code rườm rà.
- **Auto Telemetry**: Tự động đo lường hiệu năng và OpenTelemetry Tracing cho từng GraphQL Query.
- **Smart Error Handling**: Bắt panic tự động và mapping mã lỗi từ thư viện `xerrors` ra chuẩn GraphQL extensions.

## Hướng dẫn sử dụng

### 1. Chuẩn bị (Tạo Schema & Gen Code)
Thư viện này đóng vai trò là Runtime Engine. Ở dự án thật, bạn vẫn cần sử dụng `gqlgen` để sinh cấu trúc dữ liệu từ file `.graphqls`:
```bash
go run github.com/99designs/gqlgen generate
```

### 2. Tích hợp (Ví dụ với Fiber Framework)

```go
package main

import (
	"context"

	"github.com/gofiber/fiber/v2"
	
	// Package graph sinh bởi gqlgen ở bước 1
	// "my-app/graph" 
	
	common_graphql "github.com/thanhbvha/go-common/graphql"
	fiber_adapter "github.com/thanhbvha/go-common/graphql/adapter/fiber"
)

func main() {
	app := fiber.New()

	// 1. Khởi tạo Schema từ thư mục graph của bạn
	var es graphql.ExecutableSchema // = graph.NewExecutableSchema(...)

	// 2. Khởi tạo GraphQL Core Server (với Telemetry)
	coreSrv := common_graphql.NewServer(es, common_graphql.Config{
		EnableTelemetry: true,
	})

	// 3. Bọc Server qua Fiber Adapter
	gqlHandler := fiber_adapter.NewHandler(coreSrv, fiber_adapter.Config{
		ContextSetup: func(ctx context.Context, c *fiber.Ctx) context.Context {
			// (Tùy chọn) Bơm UserID hoặc DataLoaders vào Context tại đây
			return ctx
		},
	})

	// 4. Cấu hình GraphQL Playground UI
	corePlayground := common_graphql.PlaygroundHandler("GraphQL API", "/query")
	playgroundHandler := fiber_adapter.PlaygroundHandler(corePlayground)

	// 5. Đăng ký Routes
	app.All("/query", gqlHandler)
	app.Get("/", playgroundHandler)

	app.Listen(":3000")
}
```

> **Lưu ý:** Tương tự với Fiber, nếu dùng Gin hoặc Echo, bạn chỉ cần thay đổi đường dẫn import thành `github.com/thanhbvha/go-common/graphql/adapter/gin` (hoặc `echo`).

### 3. Tối ưu hiệu năng (Chống N+1) với DataLoader
DataLoader là công cụ tối quan trọng trong GraphQL. Module này cung cấp DataLoader sử dụng **Generics**, giúp code cực kỳ ngắn gọn mà không cần sinh file.

```go
// Khởi tạo hàm lấy dữ liệu hàng loạt (Batching Function)
fetchUsersBatch := func(ctx context.Context, ids []int) (map[int]*User, error) {
    // Truy vấn DB TẠI ĐÂY (Vd: db.Where("id IN ?", ids).Find(&users))
    res := make(map[int]*User)
    // ... loop query result into map ...
    return res, nil
}

// Khởi tạo DataLoader
userLoader := common_graphql.NewDataLoader(fetchUsersBatch, common_graphql.ConfigDL{
    MaxBatch: 100, // Gom tối đa 100 ID / 1 câu query
})

// Đẩy Loader vào Context ở bước (3) bên trên để sử dụng trong Resolvers:
// userLoader.Load(ctx, userID)
```
