package crypt

import "testing"

func FuzzDecryptMalformedCiphertext(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("malformed encrypted payload"))
	key := []byte("01234567890123456789012345678901")
	f.Fuzz(func(t *testing.T, ciphertext []byte) {
		_, _ = DecryptAESGCM(key, ciphertext, []byte("test"))
		_, _ = DecryptChaCha20(key, ciphertext, []byte("test"))
	})
}
