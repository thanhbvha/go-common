package main

import (
	"fmt"
	"time"

	"github.com/thanhbvha/go-common/auth"
)

func runRotationDemo() error {
	signingV1, err := requiredEnv("AUTH_SIGNING_KEY_V1")
	if err != nil {
		return err
	}
	signingV2, err := requiredEnv("AUTH_SIGNING_KEY_V2")
	if err != nil {
		return err
	}
	encryptionV1, err := requiredEnv("AUTH_ENCRYPTION_KEY_V1")
	if err != nil {
		return err
	}
	encryptionV2, err := requiredEnv("AUTH_ENCRYPTION_KEY_V2")
	if err != nil {
		return err
	}

	signingKeys := map[string][]byte{
		"signing-v1": []byte(signingV1),
		"signing-v2": []byte(signingV2),
	}
	encryptionKeys := map[string][]byte{
		"encryption-v1": []byte(encryptionV1),
		"encryption-v2": []byte(encryptionV2),
	}
	oldSigning, err := auth.NewStaticKeyProvider("signing-v1", signingKeys)
	if err != nil {
		return fmt.Errorf("configure old signing keyring: %w", err)
	}
	newSigning, err := auth.NewStaticKeyProvider("signing-v2", signingKeys)
	if err != nil {
		return fmt.Errorf("configure rotated signing keyring: %w", err)
	}
	oldEncryption, err := auth.NewStaticKeyProvider("encryption-v1", encryptionKeys)
	if err != nil {
		return fmt.Errorf("configure old encryption keyring: %w", err)
	}
	newEncryption, err := auth.NewStaticKeyProvider("encryption-v2", encryptionKeys)
	if err != nil {
		return fmt.Errorf("configure rotated encryption keyring: %w", err)
	}

	oldManager, err := auth.NewManagerWithOptions(auth.ManagerOptions{
		SigningKeys: oldSigning, Issuer: exampleIssuer,
		Audience: []string{exampleAudience}, MaxLifetime: time.Hour,
	})
	if err != nil {
		return err
	}
	rotatedManager, err := auth.NewManagerWithOptions(auth.ManagerOptions{
		SigningKeys: newSigning, Issuer: exampleIssuer,
		Audience: []string{exampleAudience}, MaxLifetime: time.Hour,
	})
	if err != nil {
		return err
	}
	oldToken, err := oldManager.GenerateToken(auth.UserInfo{ID: "rotation-user", Role: "admin"}, time.Hour)
	if err != nil {
		return err
	}
	claims, err := rotatedManager.ValidateToken(oldToken)
	if err != nil {
		return fmt.Errorf("rotated signing keyring rejected v1 token: %w", err)
	}

	aad, err := (auth.AADContext{
		Issuer: exampleIssuer, Audience: exampleAudience, SessionID: "rotation-session",
	}).Bytes()
	if err != nil {
		return err
	}
	oldEncrypted, err := auth.NewEncryptedManagerWithOptions(auth.EncryptedManagerOptions{
		SigningKeys: oldSigning, EncryptionKeys: oldEncryption,
		Issuer: exampleIssuer, Audience: []string{exampleAudience},
		MaxLifetime: time.Hour, AADPolicy: auth.RequireAAD,
	})
	if err != nil {
		return err
	}
	rotatedEncrypted, err := auth.NewEncryptedManagerWithOptions(auth.EncryptedManagerOptions{
		SigningKeys: newSigning, EncryptionKeys: newEncryption,
		Issuer: exampleIssuer, Audience: []string{exampleAudience},
		MaxLifetime: time.Hour, AADPolicy: auth.RequireAAD,
	})
	if err != nil {
		return err
	}
	encryptedToken, err := oldEncrypted.GenerateToken(auth.UserInfo{ID: "rotation-user", Role: "admin"}, time.Hour, aad)
	if err != nil {
		return err
	}
	details, err := rotatedEncrypted.ValidateTokenDetails(encryptedToken, aad)
	if err != nil {
		return fmt.Errorf("rotated encryption keyring rejected v1 token: %w", err)
	}

	fmt.Printf("standard rotation OK: subject=%s signing key v1 remains verifiable\n", claims.Subject)
	fmt.Printf("encrypted rotation OK: subject=%s ekid=%s format=v%d\n",
		details.RegisteredClaims.Subject, details.EncryptionKeyID, details.FormatVersion)
	return nil
}
