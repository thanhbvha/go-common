package auth

import (
	"sync"
	"testing"
	"time"
)

func TestEncryptedManagerConcurrentUse(t *testing.T) {
	manager, err := NewEncryptedManagerWithOptions(EncryptedManagerOptions{
		SecretKey: "jwt-secret-key-that-is-at-least-32-bytes",
		AESKey:    "01234567890123456789012345678901",
		AADPolicy: RequireAAD,
	})
	if err != nil {
		t.Fatal(err)
	}
	aad := []byte("session-1")
	token, err := manager.GenerateToken(UserInfo{ID: "user-1"}, time.Hour, aad)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 16
	const iterations = 50
	errorsFound := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				if _, err := manager.ValidateToken(token, aad); err != nil {
					errorsFound <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Fatal(err)
	}
}
