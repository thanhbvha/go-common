package main

import (
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/thanhbvha/go-common/examples/graphql_web/graph"
	"github.com/thanhbvha/go-common/examples/graphql_web/graph/model"
	"github.com/thanhbvha/go-common/examples/graphql_web/service"
	common_graphql "github.com/thanhbvha/go-common/graphql"
	echo_adapter "github.com/thanhbvha/go-common/graphql/adapter/echo"
	fiber_adapter "github.com/thanhbvha/go-common/graphql/adapter/fiber"
	gin_adapter "github.com/thanhbvha/go-common/graphql/adapter/gin"
	common_logger "github.com/thanhbvha/go-common/logger"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/utils/graceful"
	web_middleware "github.com/thanhbvha/go-common/web/middleware"
	"net/http"
)

func main() {
	// Initialize standard logger from go-common
	l := common_logger.New(common_logger.Options{
		StdOut: true,
	})
	common_logger.SetDefault(l)
	defer common_logger.Close()

	common_logger.Info("=== GraphQL Web Module Examples ===")
	common_logger.Info("Uncomment one of the functions below to run a specific example.")

	RunFiberExample()
	// RunGinExample()
	// RunEchoExample()
}

// ---------------------------------------------------------
// Helper: Setup core GraphQL Server and DataLoader
// ---------------------------------------------------------
func setupCoreGraphQL() (*handler.Server, *common_graphql.DataLoader[int, *model.User]) {
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

	return coreSrv, userLoader
}

// =====================================================================
// 1. FIBER FRAMEWORK
// =====================================================================
func RunFiberExample() {
	coreSrv, userLoader := setupCoreGraphQL()

	app := fiber.New(fiber.Config{
		ErrorHandler: web_middleware.ErrorHandler,
	})
	app.Use(common_logger.FiberRequestIDMiddleware())
	app.Use(common_logger.FiberMiddleware())

	// 4. Wrap the Server with Fiber Adapter
	gqlHandler := fiber_adapter.NewHandler(coreSrv, fiber_adapter.Config{
		ContextSetup: func(ctx context.Context, c *fiber.Ctx) context.Context {
			// Inject userLoader into the context of each Request using typed key
			return ctxkey.SetDataLoader(ctx, userLoader)
		},
	})

	// 5. Initialize Playground (also wrapped via Fiber Adapter)
	corePlayground := common_graphql.PlaygroundHandler("GraphQL API", "/query")
	playgroundHandler := fiber_adapter.PlaygroundHandler(corePlayground)

	// 6. Register Routes
	app.All("/query", gqlHandler)
	app.Get("/", playgroundHandler)

	// 7. Start the server
	go func() {
		common_logger.Info("[Fiber] GraphQL Server is running at http://localhost:3000")
		if err := app.Listen(":3000"); err != nil {
			common_logger.Error("Server error", "err", err)
		}
	}()

	// 8. Graceful shutdown
	graceful.Register(func(ctx context.Context) error {
		common_logger.Info("Shutting down Fiber server...")
		return app.ShutdownWithContext(ctx)
	})
	// CRITICAL: Always use graceful.Wait() in GraphQL servers. It blocks the main thread
	// until an interrupt signal is received, preventing abrupt termination of queries.
	graceful.Wait(10 * time.Second)
}

// =====================================================================
// 2. GIN FRAMEWORK
// =====================================================================
func RunGinExample() {
	coreSrv, userLoader := setupCoreGraphQL()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(common_logger.GinRequestIDMiddleware())
	r.Use(common_logger.GinMiddleware())

	// 4. Wrap the Server with Gin Adapter
	gqlHandler := gin_adapter.NewHandler(coreSrv, gin_adapter.Config{
		ContextSetup: func(ctx context.Context, c *gin.Context) context.Context {
			return ctxkey.SetDataLoader(ctx, userLoader)
		},
	})

	// 5. Initialize Playground
	corePlayground := common_graphql.PlaygroundHandler("GraphQL API", "/query")
	playgroundHandler := gin_adapter.PlaygroundHandler(corePlayground)

	// 6. Register Routes
	r.Any("/query", gqlHandler)
	r.GET("/", playgroundHandler)

	// 7. Start the server
	srv := &http.Server{
		Addr:    ":3000",
		Handler: r,
	}
	go func() {
		common_logger.Info("[Gin] GraphQL Server is running at http://localhost:3000")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			common_logger.Error("Server error", "err", err)
		}
	}()

	// 8. Graceful shutdown
	graceful.Register(func(ctx context.Context) error {
		common_logger.Info("Shutting down Gin server...")
		return srv.Shutdown(ctx)
	})
	// CRITICAL: Always use graceful.Wait() in GraphQL servers. It blocks the main thread
	// until an interrupt signal is received, preventing abrupt termination of queries.
	graceful.Wait(10 * time.Second)
}

// =====================================================================
// 3. ECHO FRAMEWORK
// =====================================================================
func RunEchoExample() {
	coreSrv, userLoader := setupCoreGraphQL()

	e := echo.New()
	e.HideBanner = true
	e.Use(common_logger.EchoRequestIDMiddleware())
	e.Use(common_logger.EchoMiddleware())

	// 4. Wrap the Server with Echo Adapter
	gqlHandler := echo_adapter.NewHandler(coreSrv, echo_adapter.Config{
		ContextSetup: func(ctx context.Context, c echo.Context) context.Context {
			return ctxkey.SetDataLoader(ctx, userLoader)
		},
	})

	// 5. Initialize Playground
	corePlayground := common_graphql.PlaygroundHandler("GraphQL API", "/query")
	playgroundHandler := echo_adapter.PlaygroundHandler(corePlayground)

	// 6. Register Routes
	e.Any("/query", gqlHandler)
	e.GET("/", playgroundHandler)

	// 7. Start the server
	go func() {
		common_logger.Info("[Echo] GraphQL Server is running at http://localhost:3000")
		if err := e.Start(":3000"); err != nil {
			common_logger.Error("Server error", "err", err)
		}
	}()

	// 8. Graceful shutdown
	graceful.Register(func(ctx context.Context) error {
		common_logger.Info("Shutting down Echo server...")
		return e.Shutdown(ctx)
	})
	// CRITICAL: Always use graceful.Wait() in GraphQL servers. It blocks the main thread
	// until an interrupt signal is received, preventing abrupt termination of queries.
	graceful.Wait(10 * time.Second)
}
