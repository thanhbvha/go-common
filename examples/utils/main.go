package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/thanhbvha/go-common/utils/crypt"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/utils/graceful"
	utilslice "github.com/thanhbvha/go-common/utils/slice"
	"github.com/thanhbvha/go-common/utils/str"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	fmt.Println("=== Utils Module Examples ===")
	if err := runCryptExample(); err != nil {
		return err
	}
	if err := runStringExample(); err != nil {
		return err
	}
	runContextExample()
	runSliceExample()
	return runGracefulExample()
}

func runCryptExample() error {
	fmt.Println("\n--- Cryptography ---")
	key, err := crypt.GenerateKey32()
	if err != nil {
		return fmt.Errorf("generate encryption key: %w", err)
	}
	plaintext := []byte("sensitive message")
	aad := []byte("tenant:example")

	ciphertext, err := crypt.EncryptAESGCM(key, plaintext, aad)
	if err != nil {
		return fmt.Errorf("encrypt AES-GCM: %w", err)
	}
	decrypted, err := crypt.DecryptAESGCM(key, ciphertext, aad)
	if err != nil {
		return fmt.Errorf("decrypt AES-GCM: %w", err)
	}
	fmt.Printf("AES-GCM ciphertext: %s\n", base64.StdEncoding.EncodeToString(ciphertext))
	fmt.Printf("AES-GCM plaintext: %s\n", decrypted)

	chachaCiphertext, err := crypt.EncryptChaCha20(key, plaintext, aad)
	if err != nil {
		return fmt.Errorf("encrypt ChaCha20-Poly1305: %w", err)
	}
	if _, err = crypt.DecryptChaCha20(key, chachaCiphertext, aad); err != nil {
		return fmt.Errorf("decrypt ChaCha20-Poly1305: %w", err)
	}

	passwordHash, err := crypt.HashPassword("correct horse battery staple")
	if err != nil {
		return fmt.Errorf("hash password: %w", err)
	}
	match, err := crypt.VerifyPassword("correct horse battery staple", passwordHash)
	if err != nil {
		return fmt.Errorf("verify password: %w", err)
	}
	fmt.Printf("Password matches: %v\n", match)
	fmt.Printf("SHA-256: %s\n", crypt.HashSHA256([]byte("hello world")))
	fmt.Printf("SHA3-256: %s\n", crypt.HashSHA3_256([]byte("hello world")))
	fmt.Printf("BLAKE2b-256: %s\n", crypt.HashBLAKE2b([]byte("hello world")))
	return nil
}

func runStringExample() error {
	fmt.Println("\n--- Strings ---")
	random, err := str.Random(16)
	if err != nil {
		return fmt.Errorf("generate reference ID: %w", err)
	}
	fmt.Printf("Random reference ID: %s\n", random)
	fmt.Printf("Slug: %s\n", str.Slugify("Xin chào Việt Nam 2026!"))
	return nil
}

func runContextExample() {
	fmt.Println("\n--- Context values ---")
	ctx := ctxkey.SetUserID(context.Background(), "user_999")
	ctx = ctxkey.SetRequestID(ctx, "req-abcd-1234")
	userID, ok := ctxkey.GetUserID(ctx)
	fmt.Printf("User ID: %s (found: %v)\n", userID, ok)
}

func runSliceExample() {
	fmt.Println("\n--- Slices ---")
	unique := utilslice.Unique([]int{1, 2, 2, 3, 4, 4, 5})
	even := utilslice.Filter(unique, func(value int) bool { return value%2 == 0 })
	labels := utilslice.Map(even, func(value int) string { return fmt.Sprintf("Num-%d", value) })
	fmt.Printf("Unique: %v; contains 3: %v; labels: %v\n", unique, slices.Contains(unique, 3), labels)
}

func runGracefulExample() error {
	fmt.Println("\n--- Graceful shutdown ---")
	fmt.Println("Press Ctrl+C to run cleanup hooks...")
	shutdown := graceful.NewShutdown(5 * time.Second)
	shutdown.Register(func(ctx context.Context) error {
		fmt.Println("Closing database...")
		return waitFor(ctx, 500*time.Millisecond)
	})
	shutdown.Register(func(ctx context.Context) error {
		fmt.Println("Stopping HTTP server...")
		return waitFor(ctx, 500*time.Millisecond)
	})
	return shutdown.Wait()
}

func waitFor(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
