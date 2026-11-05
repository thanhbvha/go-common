//go:build integration

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/thanhbvha/go-common/v2/auth"
	"github.com/thanhbvha/go-common/v2/db/orm/backend/postgres"
	"github.com/thanhbvha/go-common/v2/internal/perftest"
)

func TestPerformanceHTTP(t *testing.T) {
	output := perftest.Require(t)
	dsn, redis := os.Getenv("PRODUCTION_TEST_POSTGRES_DSN"), os.Getenv("PRODUCTION_TEST_REDIS_ADDR")
	if dsn == "" || redis == "" {
		t.Fatal("dedicated PostgreSQL and Redis required")
	}
	svc, url := startReference(t, redis, postgres.New(dsn), 1000000000)
	token, err := svc.tokens.GenerateToken(auth.UserInfo{ID: "performance"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PERF_HTTP_TOKEN", token)
	var results []perftest.Result
	for _, n := range []string{"1", "16"} {
		cmd := exec.Command(os.Getenv("PERF_LOADGEN"), "-target", url+"/v1/me", "-concurrency", n)
		results = append(results, perftest.Child(t, "http-"+n, cmd))
	}
	if err := perftest.WriteJSON(filepath.Join(output, "metrics.json"), results); err != nil {
		t.Fatal(err)
	}
}
