package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/examples/graphql_web/graph"
	"github.com/thanhbvha/go-common/examples/graphql_web/graph/model"
	"github.com/thanhbvha/go-common/examples/graphql_web/service"
	common_graphql "github.com/thanhbvha/go-common/graphql"
	echoadapter "github.com/thanhbvha/go-common/graphql/adapter/echo"
	fiberadapter "github.com/thanhbvha/go-common/graphql/adapter/fiber"
	ginadapter "github.com/thanhbvha/go-common/graphql/adapter/gin"
	common_logger "github.com/thanhbvha/go-common/logger"
	echolog "github.com/thanhbvha/go-common/logger/middleware/echo"
	fiberlog "github.com/thanhbvha/go-common/logger/middleware/fiber"
	ginlog "github.com/thanhbvha/go-common/logger/middleware/gin"
	nethttplog "github.com/thanhbvha/go-common/logger/middleware/nethttp"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/utils/graceful"
	echomw "github.com/thanhbvha/go-common/web/middleware/echo"
	fibermw "github.com/thanhbvha/go-common/web/middleware/fiber"
	ginmw "github.com/thanhbvha/go-common/web/middleware/gin"
	nethttpmw "github.com/thanhbvha/go-common/web/middleware/nethttp"
)

func main() {
	stack := flag.String("stack", "nethttp", "HTTP stack: nethttp, fiber, gin, or echo")
	flag.Parse()

	// Initialize standard logger from go-common
	l := common_logger.New(common_logger.Options{
		StdOut: true,
	})
	common_logger.SetDefault(l)
	defer common_logger.Close()

	common_logger.Info("=== GraphQL Web Module Examples ===")
	switch *stack {
	case "nethttp":
		RunNetHTTPExample()
	case "fiber":
		RunFiberExample()
	case "gin":
		RunGinExample()
	case "echo":
		RunEchoExample()
	default:
		fmt.Fprintln(os.Stderr, "invalid -stack; use nethttp, fiber, gin, or echo")
		os.Exit(2)
	}
}

// ---------------------------------------------------------
// Helper: Setup core GraphQL Server
// ---------------------------------------------------------
func setupCoreGraphQL() *handler.Server {
	// 1. Initialize ExecutableSchema from gqlgen
	es := graph.NewExecutableSchema(graph.Config{Resolvers: &graph.Resolver{}})

	// 2. Initialize core GraphQL Server (standard net/http)
	coreSrv := common_graphql.NewServer(es, common_graphql.Config{
		EnableTelemetry: true,
	})

	return coreSrv
}

func newUserLoader() *common_graphql.DataLoader[int, *model.User] {
	return common_graphql.NewDataLoader(service.FetchUsersBatch, common_graphql.ConfigDL{MaxBatch: 100})
}

// =====================================================================
// 1. STANDARD LIBRARY NET/HTTP
// =====================================================================
func RunNetHTTPExample() {
	srv := &http.Server{
		Addr:              ":3000",
		Handler:           newNetHTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		common_logger.Info("[net/http] GraphQL Server is running at http://localhost:3000")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			common_logger.Error("Server error", "err", err)
		}
	}()

	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error {
		common_logger.Info("Shutting down net/http server...")
		return srv.Shutdown(ctx)
	})
	shutdown.Wait()
}

func newNetHTTPHandler() http.Handler {
	coreSrv := setupCoreGraphQL()
	withDataLoader := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := ctxkey.SetDataLoader(r.Context(), newUserLoader())
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}

	mux := http.NewServeMux()
	mux.Handle("/query", withDataLoader(coreSrv))
	mux.Handle("/", common_graphql.PlaygroundHandler("GraphQL API", "/query"))
	return nethttplog.RequestIDMiddleware(nethttplog.Middleware(nethttpmw.Recover(nethttpmw.Telemetry("GraphQL HTTP")(mux))))
}

// =====================================================================
// 2. FIBER FRAMEWORK
// =====================================================================
func RunFiberExample() {
	coreSrv := setupCoreGraphQL()

	app := fiber.New(fiber.Config{
		ErrorHandler: fibermw.ErrorHandler,
	})
	app.Use(fiberlog.RequestIDMiddleware(), fiberlog.Middleware(), fibermw.Recover(), fibermw.Telemetry("GraphQL HTTP"))

	// 4. Wrap the Server with Fiber Adapter
	gqlHandler := fiberadapter.NewHandler(coreSrv, fiberadapter.Config{
		ContextSetup: func(ctx context.Context, c *fiber.Ctx) context.Context {
			// Inject userLoader into the context of each Request using typed key
			return ctxkey.SetDataLoader(ctx, newUserLoader())
		},
	})

	// 5. Initialize Playground (also wrapped via Fiber Adapter)
	corePlayground := common_graphql.PlaygroundHandler("GraphQL API", "/query")
	playgroundHandler := fiberadapter.PlaygroundHandler(corePlayground)

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
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error {
		common_logger.Info("Shutting down Fiber server...")
		return app.ShutdownWithContext(ctx)
	})
	// CRITICAL: Always use graceful.Wait() in GraphQL servers. It blocks the main thread
	// until an interrupt signal is received, preventing abrupt termination of queries.
	shutdown.Wait()
}

// =====================================================================
// 3. GIN FRAMEWORK
// =====================================================================
func RunGinExample() {
	coreSrv := setupCoreGraphQL()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(ginlog.RequestIDMiddleware(), ginlog.Middleware(), ginmw.Recover(), ginmw.Telemetry("GraphQL HTTP"))

	// 4. Wrap the Server with Gin Adapter
	gqlHandler := ginadapter.NewHandler(coreSrv, ginadapter.Config{
		ContextSetup: func(ctx context.Context, c *gin.Context) context.Context {
			return ctxkey.SetDataLoader(ctx, newUserLoader())
		},
	})

	// 5. Initialize Playground
	corePlayground := common_graphql.PlaygroundHandler("GraphQL API", "/query")
	playgroundHandler := ginadapter.PlaygroundHandler(corePlayground)

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
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error {
		common_logger.Info("Shutting down Gin server...")
		return srv.Shutdown(ctx)
	})
	// CRITICAL: Always use graceful.Wait() in GraphQL servers. It blocks the main thread
	// until an interrupt signal is received, preventing abrupt termination of queries.
	shutdown.Wait()
}

// =====================================================================
// 4. ECHO FRAMEWORK
// =====================================================================
func RunEchoExample() {
	coreSrv := setupCoreGraphQL()

	e := echo.New()
	e.HideBanner = true
	e.Use(echolog.RequestIDMiddleware(), echolog.Middleware(), echomw.Recover(), echomw.Telemetry("GraphQL HTTP"))

	// 4. Wrap the Server with Echo Adapter
	gqlHandler := echoadapter.NewHandler(coreSrv, echoadapter.Config{
		ContextSetup: func(ctx context.Context, c echo.Context) context.Context {
			return ctxkey.SetDataLoader(ctx, newUserLoader())
		},
	})

	// 5. Initialize Playground
	corePlayground := common_graphql.PlaygroundHandler("GraphQL API", "/query")
	playgroundHandler := echoadapter.PlaygroundHandler(corePlayground)

	// 6. Register Routes
	e.Any("/query", gqlHandler)
	e.GET("/", playgroundHandler)

	// 7. Start the server
	go func() {
		common_logger.Info("[Echo] GraphQL Server is running at http://localhost:3000")
		if err := e.Start(":3000"); err != nil && !errors.Is(err, http.ErrServerClosed) {
			common_logger.Error("Server error", "err", err)
		}
	}()

	// 8. Graceful shutdown
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error {
		common_logger.Info("Shutting down Echo server...")
		return e.Shutdown(ctx)
	})
	// CRITICAL: Always use graceful.Wait() in GraphQL servers. It blocks the main thread
	// until an interrupt signal is received, preventing abrupt termination of queries.
	shutdown.Wait()
}
