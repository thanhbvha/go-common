//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/99designs/gqlgen/graphql"
	"github.com/gofiber/fiber/v2"

	// Giả lập package graph được gen bởi gqlgen
	// "my-app/graph"

	common_graphql "github.com/thanhbvha/go-common/graphql"
	fiber_adapter "github.com/thanhbvha/go-common/graphql/adapter/fiber"
)

// User mô phỏng Model User
type User struct {
	ID   int
	Name string
}

func main() {
	app := fiber.New()

	// 1. Giả lập một ExecutableSchema từ gqlgen
	// Thực tế bạn sẽ dùng: es := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})
	var es graphql.ExecutableSchema = nil

	// 2. Khởi tạo DataLoaders (Ví dụ UserLoader)
	fetchUsersBatch := func(ctx context.Context, ids []int) (map[int]*User, error) {
		// Code gọi DB thật ở đây, ví dụ: db.Where("id IN ?", ids).Find(&users)
		fmt.Printf("Batch fetching users: %v\n", ids)
		res := make(map[int]*User)
		for _, id := range ids {
			res[id] = &User{ID: id, Name: fmt.Sprintf("User %d", id)}
		}
		return res, nil
	}

	userLoader := common_graphql.NewDataLoader(fetchUsersBatch, common_graphql.ConfigDL{
		MaxBatch: 100,
	})

	// 3. Khởi tạo GraphQL Server core (chuẩn net/http)
	coreSrv := common_graphql.NewServer(es, common_graphql.Config{
		EnableTelemetry: true,
	})

	// 4. Bọc Server lại bằng Fiber Adapter
	gqlHandler := fiber_adapter.NewHandler(coreSrv, fiber_adapter.Config{
		ContextSetup: func(ctx context.Context, c *fiber.Ctx) context.Context {
			// Bơm userLoader vào context của mỗi Request
			return context.WithValue(ctx, "userLoader", userLoader)
		},
	})

	// 5. Khởi tạo Playground (cũng bọc qua Fiber Adapter)
	corePlayground := common_graphql.PlaygroundHandler("GraphQL API", "/query")
	playgroundHandler := fiber_adapter.PlaygroundHandler(corePlayground)

	// 6. Đăng ký Routes
	app.All("/query", gqlHandler)
	app.Get("/", playgroundHandler)

	// 5. Chạy server
	fmt.Println("Server is running at http://localhost:3000")
	if err := app.Listen(":3000"); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
