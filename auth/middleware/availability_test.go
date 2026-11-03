package middleware_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v4"
	"github.com/thanhbvha/go-common/v2/auth"
	echoauth "github.com/thanhbvha/go-common/v2/auth/middleware/echo"
	fiberauth "github.com/thanhbvha/go-common/v2/auth/middleware/fiber"
	ginauth "github.com/thanhbvha/go-common/v2/auth/middleware/gin"
	nethttpauth "github.com/thanhbvha/go-common/v2/auth/middleware/nethttp"
)

type availabilityChecker struct {
	revoked bool
	err     error
}

func (c *availabilityChecker) IsRevoked(context.Context, string) (bool, error) {
	return c.revoked, c.err
}

type availabilityClaims struct{ jwt.RegisteredClaims }

// The wrapper exercises the public validator fallback rather than the manager's
// internal policy-driven validation shortcut.
type fallbackManager struct {
	auth.GenericManager[*availabilityClaims]
	checker *availabilityChecker
}

func (m fallbackManager) IsRevoked(ctx context.Context, id string) (bool, error) {
	return m.checker.IsRevoked(ctx, id)
}

type authFixtures struct {
	standard  *auth.Manager
	encrypted *auth.EncryptedManager
	generic   auth.ContextTokenValidator[*availabilityClaims]
}
type authAdapter func(string, authFixtures, *http.Request) (*http.Response, error)

func TestAuthAvailabilityAdapterParity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	adapters := map[string]authAdapter{
		"nethttp": func(mode string, f authFixtures, r *http.Request) (*http.Response, error) {
			mw := nethttpauth.Middleware(f.standard)
			if mode == "encrypted" {
				mw = nethttpauth.EncryptedMiddleware(f.encrypted, nil)
			} else if strings.HasPrefix(mode, "generic") {
				mw = nethttpauth.GenericMiddleware(f.generic)
			}
			rec := httptest.NewRecorder()
			mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })).ServeHTTP(rec, r)
			return rec.Result(), nil
		},
		"gin": func(mode string, f authFixtures, r *http.Request) (*http.Response, error) {
			mw := ginauth.Middleware(f.standard)
			if mode == "encrypted" {
				mw = ginauth.EncryptedMiddleware(f.encrypted, nil)
			} else if strings.HasPrefix(mode, "generic") {
				mw = ginauth.GenericMiddleware(f.generic)
			}
			app := gin.New()
			app.Use(mw)
			app.GET("/", func(c *gin.Context) { c.Status(204) })
			rec := httptest.NewRecorder()
			app.ServeHTTP(rec, r)
			return rec.Result(), nil
		},
		"echo": func(mode string, f authFixtures, r *http.Request) (*http.Response, error) {
			mw := echoauth.Middleware(f.standard)
			if mode == "encrypted" {
				mw = echoauth.EncryptedMiddleware(f.encrypted, nil)
			} else if strings.HasPrefix(mode, "generic") {
				mw = echoauth.GenericMiddleware(f.generic)
			}
			app := echo.New()
			app.Use(mw)
			app.GET("/", func(c echo.Context) error { return c.NoContent(204) })
			rec := httptest.NewRecorder()
			app.ServeHTTP(rec, r)
			return rec.Result(), nil
		},
		"fiber": func(mode string, f authFixtures, r *http.Request) (*http.Response, error) {
			mw := fiberauth.Middleware(f.standard)
			if mode == "encrypted" {
				mw = fiberauth.EncryptedMiddleware(f.encrypted, nil)
			} else if strings.HasPrefix(mode, "generic") {
				mw = fiberauth.GenericMiddleware(f.generic)
			}
			app := fiber.New()
			app.Use(mw)
			app.Get("/", func(c *fiber.Ctx) error { return c.SendStatus(204) })
			return app.Test(r)
		},
	}
	cause := errors.New("redis password=do-not-disclose")
	for _, mode := range []string{"standard", "encrypted", "generic_policy", "generic_validator"} {
		t.Run(mode, func(t *testing.T) {
			checker := &availabilityChecker{}
			fixtures, token := makeFixtures(t, mode, checker)
			checker.err = cause
			var classified error
			switch mode {
			case "standard":
				_, classified = fixtures.standard.ValidateToken(token)
			case "encrypted":
				_, classified = fixtures.encrypted.ValidateToken(token, nil)
			default:
				_, classified = fixtures.generic.ValidateRequestToken(context.Background(), token)
			}
			if !errors.Is(classified, auth.ErrAuthBackendUnavailable) || !errors.Is(classified, cause) {
				t.Fatalf("validation lost availability/cause identity: %v", classified)
			}
			checker.err = nil
			for name, serve := range adapters {
				t.Run(name, func(t *testing.T) {
					for _, tc := range []struct {
						name    string
						err     error
						revoked bool
						token   string
						want    int
					}{
						{"available", nil, false, token, 204}, {"unavailable", cause, false, token, 503}, {"revoked", nil, true, token, 401}, {"malformed", cause, false, "invalid-token", 401},
					} {
						t.Run(tc.name, func(t *testing.T) {
							checker.err, checker.revoked = tc.err, tc.revoked
							r := httptest.NewRequest("GET", "/", nil)
							r.Header.Set("Authorization", "Bearer "+tc.token)
							response, err := serve(mode, fixtures, r)
							if err != nil {
								t.Fatal(err)
							}
							defer response.Body.Close()
							body, err := io.ReadAll(response.Body)
							if err != nil {
								t.Fatal(err)
							}
							if response.StatusCode != tc.want {
								t.Fatalf("status=%d want=%d body=%s", response.StatusCode, tc.want, body)
							}
							if strings.Contains(string(body), "do-not-disclose") || strings.Contains(string(body), "redis") {
								t.Fatalf("backend cause disclosed: %s", body)
							}
						})
					}
				})
			}
		})
	}
}

func makeFixtures(t *testing.T, mode string, checker *availabilityChecker) (authFixtures, string) {
	t.Helper()
	secret := strings.Repeat("s", 32)
	f := authFixtures{}
	var token string
	var err error
	switch mode {
	case "standard":
		f.standard, err = auth.NewManagerWithOptions(auth.ManagerOptions{SecretKey: secret, Revocation: checker})
		if err == nil {
			token, err = f.standard.GenerateToken(auth.UserInfo{ID: "u"}, time.Hour)
		}
	case "encrypted":
		f.encrypted, err = auth.NewEncryptedManagerWithOptions(auth.EncryptedManagerOptions{SecretKey: secret, AESKey: strings.Repeat("e", 32), Revocation: checker})
		if err == nil {
			token, err = f.encrypted.GenerateToken(auth.UserInfo{ID: "u"}, time.Hour, nil)
		}
	default:
		opts := auth.GenericManagerOptions[*availabilityClaims]{SecretKey: secret}
		extract := func(c *availabilityClaims) (string, error) { return c.ID, nil }
		if mode == "generic_policy" {
			opts.Revocation = checker
			opts.TokenID = extract
		}
		manager, buildErr := auth.NewGenericManagerWithOptions(opts)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		now := time.Now()
		claims := &availabilityClaims{RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)), IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), Subject: "u", ID: "jti"}}
		token, err = manager.GenerateTokenContext(context.Background(), claims)
		if err == nil {
			if mode == "generic_policy" {
				f.generic, err = auth.NewGenericTokenValidator(manager, func() *availabilityClaims { return &availabilityClaims{} }, nil)
			} else {
				f.generic, err = auth.NewGenericTokenValidator(fallbackManager{GenericManager: manager, checker: checker}, func() *availabilityClaims { return &availabilityClaims{} }, extract)
			}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return f, token
}
