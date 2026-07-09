package crypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

var (
	ErrInvalidKeySize     = errors.New("crypt: invalid key size, must be 32 bytes for AES-256")
	ErrCiphertextTooShort = errors.New("crypt: ciphertext too short")
)

// EncryptAESGCM encrypts the plaintext using AES-256 GCM.
// key must be exactly 32 bytes.
// additionalData (AAD) is optional and provides extra integrity without being encrypted.
// The returned ciphertext will have the 12-byte nonce prepended to it.
func EncryptAESGCM(key, plaintext, additionalData []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	// Never use more than 2^32 random nonces with a given key because of the risk of a repeat.
	nonce := make([]byte, aesgcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	// Seal appends the encrypted data to the first argument. We pass the nonce
	// as the destination to prepend it to the ciphertext.
	ciphertext := aesgcm.Seal(nonce, nonce, plaintext, additionalData)
	return ciphertext, nil
}

// DecryptAESGCM decrypts the ciphertext using AES-256 GCM.
// key must be exactly 32 bytes.
// additionalData must match what was passed during encryption.
// The ciphertext is expected to have the 12-byte nonce prepended to it.
func DecryptAESGCM(key, ciphertext, additionalData []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := aesgcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, ErrCiphertextTooShort
	}

	nonce, encryptedMessage := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := aesgcm.Open(nil, nonce, encryptedMessage, additionalData)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}
