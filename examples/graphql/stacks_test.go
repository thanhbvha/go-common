package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAllStacksPaginationAndDevelopment(t *testing.T) {
	old := development
	defer func() { development = old }()
	for _, dev := range []bool{false, true} {
		development = dev
		for _, stack := range []string{"nethttp", "fiber", "gin", "echo"} {
			t.Run(stack+map[bool]string{true: "/dev", false: "/production"}[dev], func(t *testing.T) {
				var send func(*http.Request) (int, []byte)
				switch stack {
				case "fiber":
					app := newFiberApp()
					send = func(r *http.Request) (int, []byte) {
						res, err := app.Test(r)
						if err != nil {
							t.Fatal(err)
						}
						defer res.Body.Close()
						body, err := io.ReadAll(res.Body)
						if err != nil {
							t.Fatal(err)
						}
						return res.StatusCode, body
					}
				default:
					var h http.Handler
					switch stack {
					case "nethttp":
						h = newNetHTTPHandler()
					case "gin":
						h = newGinRouter()
					case "echo":
						h = newEchoRouter()
					}
					send = func(r *http.Request) (int, []byte) {
						w := httptest.NewRecorder()
						h.ServeHTTP(w, r)
						return w.Code, w.Body.Bytes()
					}
				}
				for _, q := range []string{
					"{posts(first:2,offset:1){id author{id name}}}",
					"{users(first:2){id posts(first:1){id author{name}}}}",
				} {
					payload, _ := json.Marshal(map[string]string{"query": q})
					r := httptest.NewRequest("POST", "/query", strings.NewReader(string(payload)))
					r.Header.Set("Content-Type", "application/json")
					status, body := send(r)
					if status != 200 || strings.Contains(string(body), "\"errors\"") {
						t.Fatalf("%d %s", status, body)
					}
					var result struct {
						Data struct {
							Posts []struct{ ID int }
							Users []struct{ Posts []struct{ ID int } }
						}
					}
					if err := json.Unmarshal(body, &result); err != nil {
						t.Fatal(err)
					}
					if strings.HasPrefix(q, "{posts") {
						if len(result.Data.Posts) != 2 || result.Data.Posts[0].ID != 102 || result.Data.Posts[1].ID != 103 {
							t.Fatalf("pagination: %s", body)
						}
					} else {
						users := result.Data.Users
						if len(users) != 2 || len(users[0].Posts) != 1 || len(users[1].Posts) != 1 || users[0].Posts[0].ID != 101 || users[1].Posts[0].ID != 103 {
							t.Fatalf("nested pagination: %s", body)
						}
					}
				}
				intro := httptest.NewRequest("POST", "/query", strings.NewReader(`{"query":"{__schema{queryType{name}}}"}`))
				intro.Header.Set("Content-Type", "application/json")
				_, introBody := send(intro)
				if strings.Contains(string(introBody), "\"errors\"") == dev {
					t.Fatalf("introspection dev=%v: %s", dev, introBody)
				}
				r := httptest.NewRequest("POST", "/query", strings.NewReader(`{"query":"{posts(first:0){id}}"}`))
				r.Header.Set("Content-Type", "application/json")
				_, body := send(r)
				if !strings.Contains(string(body), "BAD_USER_INPUT") {
					t.Fatal(string(body))
				}
				status, _ := send(httptest.NewRequest("GET", "/", nil))
				if (dev && status != 200) || (!dev && status != 404) {
					t.Fatalf("playground dev=%v status=%d", dev, status)
				}
			})
		}
	}
}
