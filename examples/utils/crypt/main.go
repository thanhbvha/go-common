package main

import (
	"encoding/base64"
	"fmt"
	"log"

	"github.com/thanhbvha/go-common/utils/crypt"
)

func main() {
	fmt.Println("--- Cryptography (AES-CBC) Example ---")

	// ==========================================
	// 1. KEY REQUIREMENTS
	// ==========================================
	// The key must be exactly 16, 24, or 32 bytes to select AES-128, AES-192, or AES-256.
	// We use a 32-byte key for AES-256 encryption.
	secretKey := []byte("my-32-byte-ultra-secure-key-0000")
	if len(secretKey) != 32 {
		log.Fatalf("Key must be 32 bytes, got %d", len(secretKey))
	}

	plainText := []byte("This is a secret message that needs to be encrypted securely.")
	fmt.Printf("Original Text: %s\n", string(plainText))

	// ==========================================
	// 2. ENCRYPTION
	// ==========================================
	// EncryptAESCBC securely encrypts the plaintext and prepends a random IV.
	// It automatically applies PKCS7 padding to the plaintext to ensure it aligns
	// with AES block sizes.
	cipherText, err := crypt.EncryptAESCBC(secretKey, plainText)
	if err != nil {
		log.Fatalf("Encryption failed: %v", err)
	}
	
	// Convert to Base64 for safe storage or transmission
	b64CipherText := base64.StdEncoding.EncodeToString(cipherText)
	fmt.Printf("Encrypted (Base64): %s\n", b64CipherText)

	// ==========================================
	// 3. DECRYPTION
	// ==========================================
	// Decode from Base64 back to raw bytes before decrypting
	decodedCipher, err := base64.StdEncoding.DecodeString(b64CipherText)
	if err != nil {
		log.Fatalf("Base64 decode failed: %v", err)
	}

	// DecryptAESCBC expects raw bytes containing the IV + CipherText.
	// It safely verifies padding boundaries to prevent padding oracle vulnerabilities.
	decryptedText, err := crypt.DecryptAESCBC(secretKey, decodedCipher)
	if err != nil {
		log.Fatalf("Decryption failed: %v", err)
	}
	
	fmt.Printf("Decrypted Text: %s\n", string(decryptedText))

	if string(decryptedText) == string(plainText) {
		fmt.Println("Success: Decrypted text matches original text!")
	} else {
		fmt.Println("Error: Decrypted text does not match!")
	}
}
