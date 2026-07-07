# utils

A comprehensive toolset utilizing Go Generics, context safety, and graceful shutdown.

```go
import (
    "github.com/thanhbvha/go-common/utils/str"
    "github.com/thanhbvha/go-common/utils/slice"
    "github.com/thanhbvha/go-common/utils/graceful"
    "github.com/thanhbvha/go-common/utils/crypt"
)

// str
random := str.Random(8)
slug := str.Slugify("Xin chào Việt Nam") // xin-chao-viet-nam

// slice (Generics)
uniqueInts := slice.Unique([]int{1, 2, 2, 3}) // [1, 2, 3]
isFound := slice.Contains([]string{"a", "b"}, "a") // true

// Graceful Shutdown
graceful.Register(func(ctx context.Context) error {
    log.Println("Closing database...")
    return db.Close()
})

graceful.Wait(10 * time.Second) // Blocks until SIGTERM/SIGINT

// --- crypt (Encryption, Hashing, Passwords) ---

// 1. Keys & Random Data
key, _ := crypt.GenerateKey32() // Secure 32-byte key
randomHex, _ := crypt.GenerateRandomString(16) // Secure random hex string

// 2. Symmetric Encryption (AEAD)
plaintext := []byte("secret message")
aad := []byte("metadata-id-123") // Additional Authenticated Data

// AES-256 GCM (Industry Standard)
ciphertext, _ := crypt.EncryptAESGCM(key, plaintext, aad)
decrypted, _ := crypt.DecryptAESGCM(key, ciphertext, aad)

// ChaCha20-Poly1305 (Modern, Fast)
chachaCipher, _ := crypt.EncryptChaCha20(key, plaintext, aad)
chachaPlain, _ := crypt.DecryptChaCha20(key, chachaCipher, aad)

// AES-256 CBC (Legacy, with PKCS#7)
cbcCipher, _ := crypt.EncryptAESCBC(key, plaintext)
cbcPlain, _ := crypt.DecryptAESCBC(key, cbcCipher)

// 3. Cryptographic Hashing
hashSHA256 := crypt.HashSHA256(plaintext)
hashSHA3 := crypt.HashSHA3_256(plaintext) // Modern standard
hashBLAKE2b, _ := crypt.HashBLAKE2b(plaintext) // Extremely fast and secure

// 4. Secure Password Hashing (Argon2id)
password := "my-secure-password"
encodedHash, _ := crypt.HashPasswordArgon2id(password)
// encodedHash looks like: $argon2id$v=19$m=65536,t=1,p=4$<salt>$<hash>
isValid, _ := crypt.VerifyPasswordArgon2id(password, encodedHash) // true
```
