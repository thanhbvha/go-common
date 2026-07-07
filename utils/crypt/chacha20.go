package crypt

import (
	"crypto/rand"
	"errors"
	"io"
	"golang.org/x/crypto/chacha20poly1305"
)

// EncryptChaCha20 encrypts the plaintext using ChaCha20-Poly1305.
// key must be exactly 32 bytes.
// additionalData (AAD) is optional and provides extra integrity.
// The returned ciphertext will have a 12-byte nonce prepended.
func EncryptChaCha20(key, plaintext, additionalData []byte) ([]byte, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, ErrInvalidKeySize
	}

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := aead.Seal(nonce, nonce, plaintext, additionalData)
	return ciphertext, nil
}

// DecryptChaCha20 decrypts the ciphertext using ChaCha20-Poly1305.
// key must be exactly 32 bytes.
// additionalData must match what was passed during encryption.
// The ciphertext is expected to have the 12-byte nonce prepended to it.
func DecryptChaCha20(key, ciphertext, additionalData []byte) ([]byte, error) {
	if len(key) != chacha20poly1305.KeySize {
		return nil, ErrInvalidKeySize
	}

	aead, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}

	nonceSize := aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, ErrCiphertextTooShort
	}

	nonce, encryptedMessage := ciphertext[:nonceSize], ciphertext[nonceSize:]
	
	plaintext, err := aead.Open(nil, nonce, encryptedMessage, additionalData)
	if err != nil {
		return nil, errors.New("crypt: chacha20poly1305 decryption failed")
	}

	return plaintext, nil
}
