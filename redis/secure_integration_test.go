//go:build integration

package redis

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func integrationRedisBinary(t *testing.T) string {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		if os.Getenv("REDIS_INTEGRATION_REQUIRED") == "1" {
			t.Fatal("redis-server required", err)
		}
		t.Skip("redis-server unavailable")
	}
	return binary
}
func reserveRedisPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}
func startRedisProcess(t *testing.T, args ...string) func() {
	t.Helper()
	binary := integrationRedisBinary(t)
	log, err := os.Create(filepath.Join(t.TempDir(), "redis.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = log.Close() }) }
	t.Cleanup(stop)
	return stop
}
func awaitRedis(t *testing.T, description string, condition func() bool) {
	t.Helper()
	until := time.Now().Add(20 * time.Second)
	for time.Now().Before(until) {
		if condition() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
func testRedisCertificate(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "redis-test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	encodedKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err = os.WriteFile(certPath, cert, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: encodedKey}), 0600); err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(cert) {
		t.Fatal("invalid test CA")
	}
	return certPath, keyPath, roots
}
func redisBaseArguments(t *testing.T, port int) []string {
	return []string{"--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no", "--dir", t.TempDir()}
}

func TestRedisTLSACLIntegration(t *testing.T) {
	integrationRedisBinary(t)
	cert, key, roots := testRedisCertificate(t)
	port := reserveRedisPort(t)
	acl := filepath.Join(t.TempDir(), "users.acl")
	if err := os.WriteFile(acl, []byte("user default off\nuser app on >fixture-password ~* +@all\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := append(redisBaseArguments(t, 0), "--tls-port", strconv.Itoa(port), "--tls-cert-file", cert, "--tls-key-file", key, "--tls-ca-cert-file", cert, "--tls-auth-clients", "no", "--aclfile", acl)
	startRedisProcess(t, args...)
	cfg := DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = port
	cfg.Username = "app"
	cfg.Password = "fixture-password"
	cfg.MaxConnRetries = 1
	cfg.MaxRetries = -1
	cfg.TLSConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	probe, err := New(cfg).buildClient()
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	awaitRedis(t, "TLS+ACL server", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return probe.Ping(ctx).Err() == nil
	})
	c := New(cfg)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.Set(ctx, "review:key", "safe", time.Minute); err != nil {
		t.Fatal(err)
	}
	bad := cfg
	bad.Password = "wrong-password"
	wrong := New(bad)
	defer wrong.Close()
	if err = wrong.Connect(ctx); err == nil {
		t.Fatal("wrong ACL password accepted")
	}
	untrusted := cfg
	untrusted.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	denied := New(untrusted)
	defer denied.Close()
	if err = denied.Connect(ctx); err == nil {
		t.Fatal("untrusted TLS certificate accepted")
	}
}

func TestRedisSentinelFailoverIntegration(t *testing.T) {
	integrationRedisBinary(t)
	masterPort, replicaPort, sentinelPort := reserveRedisPort(t), reserveRedisPort(t), reserveRedisPort(t)
	stopMaster := startRedisProcess(t, redisBaseArguments(t, masterPort)...)
	master := goredis.NewClient(&goredis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", masterPort), ContextTimeoutEnabled: true, MaxRetries: -1})
	defer master.Close()
	awaitRedis(t, "master", func() bool { return master.Ping(context.Background()).Err() == nil })
	startRedisProcess(t, append(redisBaseArguments(t, replicaPort), "--replicaof", "127.0.0.1", strconv.Itoa(masterPort))...)
	replica := goredis.NewClient(&goredis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", replicaPort), ContextTimeoutEnabled: true, MaxRetries: -1})
	defer replica.Close()
	awaitRedis(t, "replica sync", func() bool {
		info, err := replica.Info(context.Background(), "replication").Result()
		return err == nil && strings.Contains(info, "master_link_status:up") && !strings.Contains(info, "master_sync_in_progress:1")
	})
	sentinelDir := t.TempDir()
	path := filepath.Join(sentinelDir, "sentinel.conf")
	content := fmt.Sprintf("bind 127.0.0.1\nport %d\ndir %s\nsentinel monitor review 127.0.0.1 %d 1\nsentinel down-after-milliseconds review 500\nsentinel failover-timeout review 5000\nsentinel parallel-syncs review 1\n", sentinelPort, sentinelDir, masterPort)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	startRedisProcess(t, path, "--sentinel")
	cfg := DefaultConfig()
	cfg.Mode = ModeSentinel
	cfg.MasterName = "review"
	cfg.SentinelAddrs = []string{fmt.Sprintf("127.0.0.1:%d", sentinelPort)}
	cfg.MaxRetries = -1
	cfg.MaxConnRetries = 1
	client := New(cfg)
	defer client.Close()
	awaitRedis(t, "sentinel client", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		return client.Connect(ctx) == nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Set(ctx, "sentinel:before", "replicated", time.Minute); err != nil {
		t.Fatal(err)
	}
	awaitRedis(t, "replicated key", func() bool {
		value, err := replica.Get(context.Background(), "sentinel:before").Result()
		return err == nil && value == "replicated"
	})
	native := client.Native()
	stopMaster()
	awaitRedis(t, "sentinel promotion and client reconnect", func() bool {
		op, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		return client.Set(op, "sentinel:after", "promoted", time.Minute) == nil
	})
	if client.Native() != native {
		t.Fatal("failover replaced wrapper pool")
	}
	value, err := replica.Get(context.Background(), "sentinel:after").Result()
	if err != nil || value != "promoted" {
		t.Fatal("write did not reach promoted replica", value, err)
	}
}
