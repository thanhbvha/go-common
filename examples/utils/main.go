package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/thanhbvha/go-common/utils/crypt"
	"github.com/thanhbvha/go-common/utils/ctxkey"
	"github.com/thanhbvha/go-common/utils/graceful"
	"github.com/thanhbvha/go-common/utils/maps"
	"github.com/thanhbvha/go-common/utils/slice"
	"github.com/thanhbvha/go-common/utils/str"
)

func main() {
	fmt.Println("=== Utils Module Examples ===")

	RunCryptExample()
	RunStrExample()
	RunCtxKeyExample()
	RunSliceExample()
	RunMapsExample()
	
	// Run graceful shutdown last as it blocks
	RunGracefulExample()
}

// =====================================================================
// 1. Cryptography Examples
// =====================================================================
func RunCryptExample() {
	fmt.Println("\n--- 1. Cryptography (utils/crypt) ---")

	// Generate a cryptographically secure 32-byte key
	secretKey, err := crypt.GenerateKey32()
	if err != nil {
		log.Fatalf("Failed to generate key: %v", err)
	}
	fmt.Printf("Generated 32-byte Key (Hex): %x\n", secretKey)

	plainText := []byte("This is a highly sensitive message.")
	
	// A. AES-256 GCM (Recommended for Symmetric Encryption)
	fmt.Println("\n--- A. AES-256 GCM (AEAD) ---")
	aad := []byte("optional-additional-authenticated-data")
	cipherGCM, _ := crypt.EncryptAESGCM(secretKey, plainText, aad)
	fmt.Printf("Encrypted (Base64): %s\n", base64.StdEncoding.EncodeToString(cipherGCM))

	decryptedGCM, _ := crypt.DecryptAESGCM(secretKey, cipherGCM, aad)
	fmt.Printf("Decrypted: %s\n", string(decryptedGCM))

	// B. ChaCha20-Poly1305 (Fast AEAD)
	fmt.Println("\n--- B. ChaCha20-Poly1305 (AEAD) ---")
	cipherChaCha, _ := crypt.EncryptChaCha20(secretKey, plainText, nil)
	fmt.Printf("Encrypted (Base64): %s\n", base64.StdEncoding.EncodeToString(cipherChaCha))

	decryptedChaCha, _ := crypt.DecryptChaCha20(secretKey, cipherChaCha, nil)
	fmt.Printf("Decrypted: %s\n", string(decryptedChaCha))

	// C. AES-256 CBC (Legacy)
	fmt.Println("\n--- C. AES-256 CBC (Legacy) ---")
	cipherCBC, _ := crypt.EncryptAESCBC(secretKey, plainText)
	fmt.Printf("Encrypted (Base64): %s\n", base64.StdEncoding.EncodeToString(cipherCBC))

	decryptedCBC, _ := crypt.DecryptAESCBC(secretKey, cipherCBC)
	fmt.Printf("Decrypted: %s\n", string(decryptedCBC))

	// D. Argon2id Password Hashing (OWASP Recommended)
	fmt.Println("\n--- D. Argon2id Password Hashing ---")
	userPassword := "SuperSecretPassword123!"
	encodedHash, _ := crypt.HashPasswordArgon2id(userPassword)
	fmt.Printf("Argon2id Encoded Hash: %s\n", encodedHash)

	match, _ := crypt.VerifyPasswordArgon2id(userPassword, encodedHash)
	fmt.Printf("Password Match? %v\n", match)

	// E. Fast Hashing (SHA256, SHA3, BLAKE2b)
	fmt.Println("\n--- E. Data Hashing ---")
	data := []byte("hello world")
	fmt.Printf("SHA-256: %s\n", crypt.HashSHA256(data))
	fmt.Printf("SHA3-256: %s\n", crypt.HashSHA3_256(data))
	blake2bHash, _ := crypt.HashBLAKE2b(data)
	fmt.Printf("BLAKE2b: %s\n", blake2bHash)
}

// =====================================================================
// 2. String Manipulation Examples
// =====================================================================
func RunStrExample() {
	fmt.Println("\n--- 2. String Utils (utils/str) ---")

	// Random String
	randStr := str.Random(16)
	fmt.Printf("Random 16-char string: %s\n", randStr)

	// Slugify
	title := "Xin chào Việt Nam 2026!"
	slug := str.Slugify(title)
	fmt.Printf("Slugified '%s' -> '%s'\n", title, slug)
}

// =====================================================================
// 3. Context Key Examples
// =====================================================================
func RunCtxKeyExample() {
	fmt.Println("\n--- 3. Context Keys (utils/ctxkey) ---")
	
	ctx := context.Background()
	
	// Set values safely without string collision
	ctx = ctxkey.SetUserID(ctx, "user_999")
	ctx = ctxkey.SetRequestID(ctx, "req-abcd-1234")

	// Retrieve safely
	userID, ok := ctxkey.GetUserID(ctx)
	fmt.Printf("Extracted UserID: %s (Found: %v)\n", userID, ok)
}

// =====================================================================
// 4. Slice Generics Examples
// =====================================================================
func RunSliceExample() {
	fmt.Println("\n--- 4. Slice Generics (utils/slice) ---")

	numbers := []int{1, 2, 2, 3, 4, 4, 5}
	fmt.Printf("Original slice: %v\n", numbers)

	// Unique
	uniqueNums := slice.Unique(numbers)
	fmt.Printf("Unique slice: %v\n", uniqueNums)

	// Contains
	hasThree := slice.Contains(uniqueNums, 3)
	fmt.Printf("Contains 3? %v\n", hasThree)

	// Filter
	evenNums := slice.Filter(uniqueNums, func(v int) bool { return v%2 == 0 })
	fmt.Printf("Filtered even numbers: %v\n", evenNums)

	// Map
	strNums := slice.Map(evenNums, func(v int) string { return fmt.Sprintf("Num-%d", v) })
	fmt.Printf("Mapped to strings: %v\n", strNums)
}

// =====================================================================
// 5. Map Generics Examples
// =====================================================================
func RunMapsExample() {
	fmt.Println("\n--- 5. Map Generics (utils/maps) ---")

	userAges := map[string]int{
		"Alice": 25,
		"Bob":   30,
		"Carol": 22,
	}

	keys := maps.Keys(userAges)
	values := maps.Values(userAges)

	fmt.Printf("Map Keys: %v\n", keys)
	fmt.Printf("Map Values: %v\n", values)
}

// =====================================================================
// 6. Graceful Shutdown Example
// =====================================================================
func RunGracefulExample() {
	fmt.Println("\n--- 6. Graceful Shutdown (utils/graceful) ---")
	fmt.Println("Press Ctrl+C to trigger graceful shutdown...")

	// Register a cleanup hook (e.g. closing DB connections)
	graceful.Register(func(ctx context.Context) error {
		fmt.Println("[Hook 1] Closing Database Connection...")
		time.Sleep(500 * time.Millisecond) // Simulate work
		return nil
	})

	// Register another hook (executed in LIFO order)
	graceful.Register(func(ctx context.Context) error {
		fmt.Println("[Hook 2] Stopping HTTP Server...")
		time.Sleep(500 * time.Millisecond) // Simulate work
		return nil
	})

	// Wait for OS Signal (SIGINT/SIGTERM), with 5 seconds timeout
	graceful.Wait(5 * time.Second)
}
