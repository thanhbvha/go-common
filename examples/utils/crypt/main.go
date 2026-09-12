package main

import (
	"encoding/base64"
	"fmt"
	"log"

	"github.com/thanhbvha/go-common/utils/crypt"
)

func main() {
	fmt.Println("=== go-common/utils/crypt Examples ===")

	// ==========================================
	// 1. Random Key Generation
	// ==========================================
	fmt.Println("\n--- 1. Random Key Generation ---")
	// Generate a cryptographically secure 32-byte key
	secretKey, err := crypt.GenerateKey32()
	if err != nil {
		log.Fatalf("Failed to generate key: %v", err)
	}
	fmt.Printf("Generated 32-byte Key (Hex): %x\n", secretKey)

	plainText := []byte("This is a highly sensitive message.")
	fmt.Printf("Original Text: %s\n", string(plainText))

	// ==========================================
	// 2. AES-256 GCM (Recommended for Symmetric Encryption)
	// ==========================================
	fmt.Println("\n--- 2. AES-256 GCM (AEAD) ---")
	// GCM provides both confidentiality and data authenticity.
	aad := []byte("optional-additional-authenticated-data")
	
	cipherGCM, err := crypt.EncryptAESGCM(secretKey, plainText, aad)
	if err != nil {
		log.Fatalf("AES-GCM Encryption failed: %v", err)
	}
	fmt.Printf("Encrypted (Base64): %s\n", base64.StdEncoding.EncodeToString(cipherGCM))

	decryptedGCM, err := crypt.DecryptAESGCM(secretKey, cipherGCM, aad)
	if err != nil {
		log.Fatalf("AES-GCM Decryption failed: %v", err)
	}
	fmt.Printf("Decrypted: %s\n", string(decryptedGCM))

	// ==========================================
	// 3. ChaCha20-Poly1305
	// ==========================================
	fmt.Println("\n--- 3. ChaCha20-Poly1305 (AEAD) ---")
	// Fast on devices without AES hardware acceleration (e.g., mobile devices).
	cipherChaCha, err := crypt.EncryptChaCha20(secretKey, plainText, nil)
	if err != nil {
		log.Fatalf("ChaCha20 Encryption failed: %v", err)
	}
	fmt.Printf("Encrypted (Base64): %s\n", base64.StdEncoding.EncodeToString(cipherChaCha))

	decryptedChaCha, err := crypt.DecryptChaCha20(secretKey, cipherChaCha, nil)
	if err != nil {
		log.Fatalf("ChaCha20 Decryption failed: %v", err)
	}
	fmt.Printf("Decrypted: %s\n", string(decryptedChaCha))

	// ==========================================
	// 4. AES-256 CBC (Legacy)
	// ==========================================
	fmt.Println("\n--- 4. AES-256 CBC (Legacy) ---")
	// WARNING: CBC does not provide data authenticity. Vulnerable to padding oracles
	// if decryption errors are exposed. Use GCM for new projects.
	cipherCBC, err := crypt.EncryptAESCBC(secretKey, plainText)
	if err != nil {
		log.Fatalf("AES-CBC Encryption failed: %v", err)
	}
	fmt.Printf("Encrypted (Base64): %s\n", base64.StdEncoding.EncodeToString(cipherCBC))

	decryptedCBC, err := crypt.DecryptAESCBC(secretKey, cipherCBC)
	if err != nil {
		log.Fatalf("AES-CBC Decryption failed: %v", err)
	}
	fmt.Printf("Decrypted: %s\n", string(decryptedCBC))

	// ==========================================
	// 5. Argon2id Password Hashing
	// ==========================================
	fmt.Println("\n--- 5. Argon2id Password Hashing (OWASP Recommended) ---")
	userPassword := "SuperSecretPassword123!"
	
	// Hash password (automatically generates salt and uses OWASP recommended params)
	encodedHash, err := crypt.HashPasswordArgon2id(userPassword)
	if err != nil {
		log.Fatalf("Password hashing failed: %v", err)
	}
	fmt.Printf("Argon2id Encoded Hash:\n%s\n", encodedHash)

	// Verify password
	match, err := crypt.VerifyPasswordArgon2id(userPassword, encodedHash)
	if err != nil {
		log.Fatalf("Password verification failed: %v", err)
	}
	fmt.Printf("Password Match? %v\n", match)

	// Verify WRONG password
	matchWrong, _ := crypt.VerifyPasswordArgon2id("WrongPassword", encodedHash)
	fmt.Printf("Wrong Password Match? %v\n", matchWrong)
}
