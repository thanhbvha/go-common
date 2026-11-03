package auth

import (
	"context"
	"strings"
	"testing"
	"time"
)

func FuzzRejectMalformedTokens(f *testing.F) {
	opts := ManagerOptions{SecretKey: strings.Repeat("s", 32), Clock: func() time.Time { return time.Unix(1800000000, 0) }}
	standard, err := NewManagerWithOptions(opts)
	if err != nil {
		f.Fatal(err)
	}
	encrypted, err := NewEncryptedManagerWithOptions(EncryptedManagerOptions{SecretKey: opts.SecretKey, AESKey: strings.Repeat("e", 32), Clock: opts.Clock})
	if err != nil {
		f.Fatal(err)
	}
	signed, err := standard.GenerateToken(UserInfo{ID: "user"}, time.Hour)
	if err != nil {
		f.Fatal(err)
	}
	hidden, err := encrypted.GenerateToken(UserInfo{ID: "user"}, time.Hour, []byte("binding"))
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range []string{"", "not.a.jwt", signed, hidden, strings.Repeat("x", DefaultMaxTokenSize+1)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		_, _ = standard.ValidateTokenContext(context.Background(), input)
		_, _ = encrypted.ValidateTokenDetailsContext(context.Background(), input, []byte("binding"))
	})
}
