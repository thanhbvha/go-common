package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/v2/logger"
	echolog "github.com/thanhbvha/go-common/v2/logger/middleware/echo"
	fiberlog "github.com/thanhbvha/go-common/v2/logger/middleware/fiber"
	ginlog "github.com/thanhbvha/go-common/v2/logger/middleware/gin"
	nethttplog "github.com/thanhbvha/go-common/v2/logger/middleware/nethttp"
	"github.com/thanhbvha/go-common/v2/telemetry"
	"github.com/thanhbvha/go-common/v2/utils/graceful"
	"github.com/thanhbvha/go-common/v2/utils/str"
	echomw "github.com/thanhbvha/go-common/v2/web/middleware/echo"
	fibermw "github.com/thanhbvha/go-common/v2/web/middleware/fiber"
	ginmw "github.com/thanhbvha/go-common/v2/web/middleware/gin"
	nethttpmw "github.com/thanhbvha/go-common/v2/web/middleware/nethttp"
	echoresp "github.com/thanhbvha/go-common/v2/web/response/echo"
	fiberresp "github.com/thanhbvha/go-common/v2/web/response/fiber"
	ginresp "github.com/thanhbvha/go-common/v2/web/response/gin"
	nethttpresp "github.com/thanhbvha/go-common/v2/web/response/nethttp"
	"github.com/thanhbvha/go-common/v2/web/validator"
)

const address = ":3000"

type CreateUserRequest struct {
	Name  string `json:"name" validate:"required,min=3"`
	Email string `json:"email" validate:"required,email"`
}

func main() {
	stack := flag.String("stack", "nethttp", "HTTP stack: nethttp, fiber, gin, or echo")
	flag.Parse()

	l := logger.New(logger.Options{StdOut: true})
	logger.SetDefault(l)
	defer logger.Close()

	tel, err := telemetry.New(context.Background(), telemetry.Config{ServiceName: "demo-web-api", EnableTracing: false})
	if err != nil {
		fmt.Fprintf(os.Stderr, "telemetry initialization failed: %v\n", err)
		os.Exit(1)
	}
	tel.InstallGlobal()
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error { return tel.Shutdown(ctx) })

	switch *stack {
	case "nethttp":
		runNetHTTP(shutdown)
	case "fiber":
		runFiber(shutdown)
	case "gin":
		runGin(shutdown)
	case "echo":
		runEcho(shutdown)
	default:
		fmt.Fprintln(os.Stderr, "invalid -stack; use nethttp, fiber, gin, or echo")
		os.Exit(2)
	}
	if err := shutdown.Wait(); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
}

func createdUser(req CreateUserRequest, stack string) (map[string]any, error) {
	referenceID, err := str.Random(12)
	if err != nil {
		return nil, fmt.Errorf("generate reference ID: %w", err)
	}
	return map[string]any{"slug": str.Slugify(req.Name), "reference_id": referenceID, "email": req.Email, "stack": stack}, nil
}

func runNetHTTP(shutdown *graceful.Shutdown) {
	mux := http.NewServeMux()
	mux.Handle("POST /users", nethttpmw.ErrorHandler(func(w http.ResponseWriter, r *http.Request) error {
		var req CreateUserRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return nethttpresp.Error(w, r, http.StatusBadRequest, "Invalid input data")
		}
		if validationErrors, err := validator.Validate(&req); err != nil {
			return err
		} else if validationErrors != nil {
			return nethttpresp.ValidationError(w, r, validationErrors)
		}
		created, err := createdUser(req, "net/http")
		if err != nil {
			return err
		}
		return nethttpresp.Created(w, r, created)
	}))
	handler := nethttplog.RequestIDMiddleware(nethttplog.Middleware(nethttpmw.Recover(nethttpmw.Telemetry("HTTP Request")(mux))))
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	shutdown.Register(func(ctx context.Context) error { return server.Shutdown(ctx) })
	go listenHTTP("net/http", server)
}

func runFiber(shutdown *graceful.Shutdown) {
	app := fiber.New(fiber.Config{ErrorHandler: fibermw.ErrorHandler, DisableStartupMessage: true})
	app.Use(fiberlog.RequestIDMiddleware(), fiberlog.Middleware(), fibermw.Recover(), fibermw.Telemetry("HTTP Request"))
	app.Post("/users", func(c *fiber.Ctx) error {
		var req CreateUserRequest
		if err := c.BodyParser(&req); err != nil {
			return fiberresp.Error(c, fiber.StatusBadRequest, "Invalid input data")
		}
		if validationErrors, err := validator.Validate(&req); err != nil {
			return err
		} else if validationErrors != nil {
			return fiberresp.ValidationError(c, validationErrors)
		}
		created, err := createdUser(req, "fiber")
		if err != nil {
			return err
		}
		return fiberresp.Created(c, created)
	})
	shutdown.Register(func(context.Context) error { return app.Shutdown() })
	go func() { _ = app.Listen(address) }()
}

func runGin(shutdown *graceful.Shutdown) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(ginlog.RequestIDMiddleware(), ginlog.Middleware(), ginmw.Recover(), ginmw.ErrorHandler(), ginmw.Telemetry("HTTP Request"))
	router.POST("/users", func(c *gin.Context) {
		var req CreateUserRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			ginresp.Error(c, http.StatusBadRequest, "Invalid input data")
			return
		}
		if validationErrors, err := validator.Validate(&req); err != nil {
			ginresp.Error(c, http.StatusInternalServerError, "Validation failed")
			return
		} else if validationErrors != nil {
			ginresp.ValidationError(c, validationErrors)
			return
		}
		created, err := createdUser(req, "gin")
		if err != nil {
			ginresp.Error(c, http.StatusInternalServerError, "Could not create user")
			return
		}
		ginresp.Created(c, created)
	})
	server := &http.Server{Addr: address, Handler: router, ReadHeaderTimeout: 5 * time.Second}
	shutdown.Register(func(ctx context.Context) error { return server.Shutdown(ctx) })
	go listenHTTP("Gin", server)
}

func runEcho(shutdown *graceful.Shutdown) {
	e := echo.New()
	e.HideBanner = true
	e.HTTPErrorHandler = echomw.ErrorHandler
	e.Use(echolog.RequestIDMiddleware(), echolog.Middleware(), echomw.Recover(), echomw.Telemetry("HTTP Request"))
	e.POST("/users", func(c echo.Context) error {
		var req CreateUserRequest
		if err := c.Bind(&req); err != nil {
			return echoresp.Error(c, http.StatusBadRequest, "Invalid input data")
		}
		if validationErrors, err := validator.Validate(&req); err != nil {
			return err
		} else if validationErrors != nil {
			return echoresp.ValidationError(c, validationErrors)
		}
		created, err := createdUser(req, "echo")
		if err != nil {
			return err
		}
		return echoresp.Created(c, created)
	})
	shutdown.Register(func(ctx context.Context) error { return e.Shutdown(ctx) })
	go func() { _ = e.Start(address) }()
}

func listenHTTP(name string, server *http.Server) {
	fmt.Printf("%s web example listening on http://localhost%s\n", name, address)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server error", "stack", name, "error", err)
	}
}
