package auth

import (
	"testing"
	"time"
)

func TestManagerKeyRotation(t *testing.T) {
	keys := map[string][]byte{
		"v1": []byte("signing-key-v1-that-is-at-least-32-bytes"),
		"v2": []byte("signing-key-v2-that-is-at-least-32-bytes"),
	}
	v1, err := NewStaticKeyProvider("v1", keys)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := NewStaticKeyProvider("v2", keys)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := NewManagerWithOptions(ManagerOptions{SigningKeys: v1})
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := NewManagerWithOptions(ManagerOptions{SigningKeys: v2})
	if err != nil {
		t.Fatal(err)
	}

	oldToken, err := issuer.GenerateToken(UserInfo{ID: "user-1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	newToken, err := rotated.GenerateToken(UserInfo{ID: "user-1"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rotated.ValidateToken(oldToken); err != nil {
		t.Fatalf("rotated manager rejected old token: %v", err)
	}
	if _, err := issuer.ValidateToken(newToken); err != nil {
		t.Fatalf("old manager keyring rejected new token: %v", err)
	}
}

func TestEncryptedManagerKeyRotation(t *testing.T) {
	signing := map[string][]byte{
		"sig-v1": []byte("signing-key-v1-that-is-at-least-32-bytes"),
		"sig-v2": []byte("signing-key-v2-that-is-at-least-32-bytes"),
	}
	encryption := map[string][]byte{
		"enc-v1": []byte("01234567890123456789012345678901"),
		"enc-v2": []byte("12345678901234567890123456789012"),
	}
	sigV1, err := NewStaticKeyProvider("sig-v1", signing)
	if err != nil {
		t.Fatal(err)
	}
	sigV2, err := NewStaticKeyProvider("sig-v2", signing)
	if err != nil {
		t.Fatal(err)
	}
	encV1, err := NewStaticKeyProvider("enc-v1", encryption)
	if err != nil {
		t.Fatal(err)
	}
	encV2, err := NewStaticKeyProvider("enc-v2", encryption)
	if err != nil {
		t.Fatal(err)
	}

	issuer, err := NewEncryptedManagerWithOptions(EncryptedManagerOptions{SigningKeys: sigV1, EncryptionKeys: encV1})
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := NewEncryptedManagerWithOptions(EncryptedManagerOptions{SigningKeys: sigV2, EncryptionKeys: encV2})
	if err != nil {
		t.Fatal(err)
	}

	token, err := issuer.GenerateToken(UserInfo{ID: "user-1"}, time.Hour, []byte("session"))
	if err != nil {
		t.Fatal(err)
	}
	user, err := rotated.ValidateToken(token, []byte("session"))
	if err != nil || user.ID != "user-1" {
		t.Fatalf("rotated encrypted manager validation = %#v, %v", user, err)
	}
}

func TestStaticKeyProviderLegacyKey(t *testing.T) {
	keys := map[string][]byte{
		"v1": []byte("signing-key-v1-that-is-at-least-32-bytes"),
		"v2": []byte("signing-key-v2-that-is-at-least-32-bytes"),
	}
	provider, err := NewStaticKeyProviderWithLegacy("v2", "v1", keys)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := provider.Key(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if string(legacy) != string(keys["v1"]) {
		t.Fatal("legacy token did not resolve to the configured legacy key")
	}
}
