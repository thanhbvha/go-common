package auth

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"sync"

	"github.com/thanhbvha/go-common/utils/crypt"
)

type cachedAEADKey struct {
	id     string
	digest [sha256.Size]byte
}

type aeadCache struct {
	values sync.Map
}

func (c *aeadCache) load(keyID string, key []byte) (cipher.AEAD, error) {
	cacheKey := cachedAEADKey{id: keyID, digest: sha256.Sum256(key)}
	if cached, ok := c.values.Load(cacheKey); ok {
		return cached.(cipher.AEAD), nil
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	actual, _ := c.values.LoadOrStore(cacheKey, aead)
	return actual.(cipher.AEAD), nil
}

func (c *aeadCache) encrypt(keyID string, key, plaintext, aad []byte) ([]byte, error) {
	aead, err := c.load(keyID, key)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return aead.Seal(nonce, nonce, plaintext, aad), nil
}

func (c *aeadCache) decrypt(keyID string, key, ciphertext, aad []byte) ([]byte, error) {
	aead, err := c.load(keyID, key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < aead.NonceSize() {
		return nil, crypt.ErrCiphertextTooShort
	}
	nonce, encrypted := ciphertext[:aead.NonceSize()], ciphertext[aead.NonceSize():]
	return aead.Open(nil, nonce, encrypted, aad)
}
