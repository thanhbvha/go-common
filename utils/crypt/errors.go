package crypt

import "errors"

var (
	ErrInvalidKeySize       = errors.New("crypt: key must be exactly 32 bytes")
	ErrCiphertextTooShort   = errors.New("crypt: ciphertext too short")
	ErrAuthenticationFailed = errors.New("crypt: authentication failed")
	ErrInvalidLength        = errors.New("crypt: length must not be negative")
	ErrInvalidPasswordHash  = errors.New("crypt: invalid password hash")
)
