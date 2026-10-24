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

	"github.com/thanhbvha/go-common/auth"
	echoauth "github.com/thanhbvha/go-common/auth/middleware/echo"
	fiberauth "github.com/thanhbvha/go-common/auth/middleware/fiber"
	ginauth "github.com/thanhbvha/go-common/auth/middleware/gin"
	nethttpauth "github.com/thanhbvha/go-common/auth/middleware/nethttp"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/utils/graceful"
)

const address = ":3000"

func main() {
	stack := flag.String("stack", "nethttp", "HTTP stack: nethttp, fiber, gin, or echo")
	flag.Parse()

	manager, err := auth.NewManager(os.Getenv("AUTH_JWT_SECRET"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "AUTH_JWT_SECRET must contain at least 32 characters")
		os.Exit(1)
	}

	switch *stack {
	case "nethttp":
		runNetHTTP(manager)
	case "fiber":
		runFiber(manager)
	case "gin":
		runGin(manager)
	case "echo":
		runEcho(manager)
	default:
		fmt.Fprintln(os.Stderr, "invalid -stack; use nethttp, fiber, gin, or echo")
		os.Exit(2)
	}
}

func issueToken(manager *auth.Manager) (string, error) {
	return manager.GenerateToken(auth.UserInfo{
		ID:    "user-123",
		Role:  "admin",
		Email: "user@example.com",
	}, time.Hour)
}

func userFromContext(ctx context.Context) (*auth.UserInfo, bool) {
	value, ok := ctxkey.GetUserInfo(ctx)
	if !ok {
		return nil, false
	}
	user, ok := value.(*auth.UserInfo)
	return user, ok && user != nil
}

func runNetHTTP(manager *auth.Manager) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /login", func(w http.ResponseWriter, _ *http.Request) {
		token, err := issueToken(manager)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"access_token": token, "token_type": "Bearer"})
	})
	mux.Handle("GET /api/profile", nethttpauth.Middleware(manager)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := userFromContext(r.Context())
		if !ok {
			http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"user": user, "stack": "net/http"})
	})))

	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	runHTTPServer("net/http", server)
}

func runFiber(manager *auth.Manager) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	app.Post("/login", func(c *fiber.Ctx) error {
		token, err := issueToken(manager)
		if err != nil {
			return c.SendStatus(fiber.StatusInternalServerError)
		}
		return c.JSON(fiber.Map{"access_token": token, "token_type": "Bearer"})
	})
	app.Get("/api/profile", fiberauth.Middleware(manager), func(c *fiber.Ctx) error {
		user, ok := userFromContext(c.UserContext())
		if !ok {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		return c.JSON(fiber.Map{"user": user, "stack": "fiber"})
	})

	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(context.Context) error { return app.Shutdown() })
	go func() {
		fmt.Printf("Fiber auth example listening on http://localhost%s\n", address)
		if err := app.Listen(address); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	shutdown.Wait()
}

func runGin(manager *auth.Manager) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.POST("/login", func(c *gin.Context) {
		token, err := issueToken(manager)
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		c.JSON(http.StatusOK, gin.H{"access_token": token, "token_type": "Bearer"})
	})
	router.GET("/api/profile", ginauth.Middleware(manager), func(c *gin.Context) {
		user, ok := userFromContext(c.Request.Context())
		if !ok {
			c.Status(http.StatusUnauthorized)
			return
		}
		c.JSON(http.StatusOK, gin.H{"user": user, "stack": "gin"})
	})

	server := &http.Server{Addr: address, Handler: router, ReadHeaderTimeout: 5 * time.Second}
	runHTTPServer("Gin", server)
}

func runEcho(manager *auth.Manager) {
	e := echo.New()
	e.HideBanner = true
	e.POST("/login", func(c echo.Context) error {
		token, err := issueToken(manager)
		if err != nil {
			return c.NoContent(http.StatusInternalServerError)
		}
		return c.JSON(http.StatusOK, map[string]string{"access_token": token, "token_type": "Bearer"})
	})
	e.GET("/api/profile", func(c echo.Context) error {
		user, ok := userFromContext(c.Request().Context())
		if !ok {
			return c.NoContent(http.StatusUnauthorized)
		}
		return c.JSON(http.StatusOK, map[string]any{"user": user, "stack": "echo"})
	}, echoauth.Middleware(manager))

	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error { return e.Shutdown(ctx) })
	go func() {
		fmt.Printf("Echo auth example listening on http://localhost%s\n", address)
		if err := e.Start(address); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	shutdown.Wait()
}

func runHTTPServer(name string, server *http.Server) {
	shutdown := graceful.NewShutdown(10 * time.Second)
	shutdown.Register(func(ctx context.Context) error { return server.Shutdown(ctx) })
	go func() {
		fmt.Printf("%s auth example listening on http://localhost%s\n", name, address)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	shutdown.Wait()
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
