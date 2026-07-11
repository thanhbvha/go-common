package main

import (
	"context"

	"github.com/gofiber/fiber/v2"

	"github.com/thanhbvha/go-common/examples/graphql_web/graph"
	"github.com/thanhbvha/go-common/examples/graphql_web/service"
	common_graphql "github.com/thanhbvha/go-common/graphql"
	fiber_adapter "github.com/thanhbvha/go-common/graphql/adapter/fiber"
	common_logger "github.com/thanhbvha/go-common/logger"
)

func main() {
	// Initialize standard logger from go-common
	l := common_logger.New(common_logger.Options{
		StdOut: true,
	})
	common_logger.SetDefault(l)
	defer common_logger.Close()

	app := fiber.New()

	// Attach Fiber's Logger Middleware to track request flow and latency
	app.Use(common_logger.FiberRequestIDMiddleware())
	app.Use(common_logger.FiberMiddleware())

	// 1. Initialize ExecutableSchema from gqlgen
	es := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})

	// 2. Initialize DataLoaders (using batch fetcher function from service)
	userLoader := common_graphql.NewDataLoader(service.FetchUsersBatch, common_graphql.ConfigDL{
		MaxBatch: 100,
	})

	// 3. Initialize core GraphQL Server (standard net/http)
	coreSrv := common_graphql.NewServer(es, common_graphql.Config{
		EnableTelemetry: true,
	})

	// 4. Wrap the Server with Fiber Adapter
	gqlHandler := fiber_adapter.NewHandler(coreSrv, fiber_adapter.Config{
		ContextSetup: func(ctx context.Context, c *fiber.Ctx) context.Context {
			// Inject userLoader into the context of each Request
			return context.WithValue(ctx, "userLoader", userLoader)
		},
	})

	// 5. Initialize Playground (also wrapped via Fiber Adapter)
	corePlayground := common_graphql.PlaygroundHandler("GraphQL API", "/query")
	playgroundHandler := fiber_adapter.PlaygroundHandler(corePlayground)

	// 6. Register Routes
	app.All("/query", gqlHandler)
	app.Get("/", playgroundHandler)

	// 7. Start the server
	common_logger.Info("GraphQL Server is running at http://localhost:3000")
	if err := app.Listen(":3000"); err != nil {
		common_logger.Error("Server error", "err", err)
	}
}
