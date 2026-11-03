package cache

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/redis/go-redis/v9"
)

type failedLockEntropy struct{ err error }

func (r failedLockEntropy) Read([]byte) (int, error) { return 0, r.err }

func TestRedisLockEntropyFailure(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	unavailable := errors.New("synthetic entropy unavailable")
	var typedNil *redis.Client
	for _, invalid := range []redis.UniversalClient{nil, typedNil} {
		if lock, err := newRedisLock(invalid, failedLockEntropy{unavailable}); lock != nil || !errors.Is(err, ErrInvalidLock) || errors.Is(err, ErrEntropyUnavailable) {
			t.Fatalf("client validation did not precede entropy: %v %v", lock, err)
		}
	}
	for _, reader := range []io.Reader{failedLockEntropy{unavailable}, io.MultiReader(bytes.NewReader([]byte{1, 2}), failedLockEntropy{unavailable})} {
		lock, err := newRedisLock(client, reader)
		if lock != nil || !errors.Is(err, ErrEntropyUnavailable) || !errors.Is(err, unavailable) {
			t.Fatalf("entropy failure: lock=%v err=%v", lock, err)
		}
	}
	// Initial creation succeeds, but a later ownership session must propagate a
	// subsequent entropy failure without publishing a partial lock.
	factory, err := newRedisLock(client, io.MultiReader(bytes.NewReader(make([]byte, 16)), failedLockEntropy{unavailable}))
	if err != nil {
		t.Fatal(err)
	}
	if session, err := factory.NewSession(); session != nil || !errors.Is(err, ErrEntropyUnavailable) || !errors.Is(err, unavailable) {
		t.Fatalf("session entropy failure: session=%v err=%v", session, err)
	}
	if lock, err := newRedisLock(client, bytes.NewReader([]byte{1})); lock != nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short entropy accepted: %v %v", lock, err)
	}
}
