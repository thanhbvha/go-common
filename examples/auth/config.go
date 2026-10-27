package main

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
)

const (
	exampleIssuer   = "go-common-example"
	exampleAudience = "go-common-example-api"
)

func requiredEnv(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", errors.New(name + " is required")
	}
	return value, nil
}

func randomID() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
