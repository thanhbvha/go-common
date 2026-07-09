package crypt

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

var (
	ErrInvalidPadding = errors.New("crypt: invalid PKCS7 padding")
)

// EncryptAESCBC encrypts plaintext using AES-256 CBC with PKCS#7 padding.
// key must be exactly 32 bytes.
// The returned ciphertext has a 16-byte IV prepended to it.
func EncryptAESCBC(key, plaintext []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// PKCS#7 Padding
	blockSize := block.BlockSize() // 16 for AES
	paddedPlaintext := pkcs7Pad(plaintext, blockSize)

	ciphertext := make([]byte, blockSize+len(paddedPlaintext))
	iv := ciphertext[:blockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext[blockSize:], paddedPlaintext)

	return ciphertext, nil
}

// DecryptAESCBC decrypts ciphertext using AES-256 CBC with PKCS#7 unpadding.
// key must be exactly 32 bytes.
// The ciphertext must have the 16-byte IV prepended to it.
func DecryptAESCBC(key, ciphertext []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, ErrInvalidKeySize
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	blockSize := block.BlockSize()
	if len(ciphertext) < blockSize {
		return nil, ErrCiphertextTooShort
	}
	if len(ciphertext)%blockSize != 0 {
		return nil, errors.New("crypt: ciphertext is not a multiple of the block size")
	}

	iv := ciphertext[:blockSize]
	ciphertextMessage := ciphertext[blockSize:]

	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(ciphertextMessage))
	mode.CryptBlocks(plaintext, ciphertextMessage)

	// PKCS#7 Unpadding
	plaintext, err = pkcs7Unpad(plaintext, blockSize)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

func pkcs7Pad(data []byte, blockSize int) []byte {
	padding := blockSize - len(data)%blockSize
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(data, padText...)
}

func pkcs7Unpad(data []byte, blockSize int) ([]byte, error) {
	length := len(data)
	if length == 0 {
		return nil, ErrInvalidPadding
	}
	if length%blockSize != 0 {
		return nil, ErrInvalidPadding
	}

	padding := int(data[length-1])
	if padding == 0 || padding > blockSize {
		return nil, ErrInvalidPadding
	}

	for i := 0; i < padding; i++ {
		if data[length-padding+i] != byte(padding) {
			return nil, ErrInvalidPadding
		}
	}

	return data[:length-padding], nil
}
