// Package str provides utility functions for common string manipulation tasks.
//
// It includes helpers for generating random strings, converting cases, and other
// text processing utilities often needed in web applications.
package str

import (
	"crypto/rand"
	"errors"

	"github.com/gosimple/slug"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const unbiasedByteLimit = 256 - (256 % len(charset))

var ErrInvalidLength = errors.New("str: length must not be negative")

// Random returns a cryptographically secure alphanumeric string of length.
func Random(length int) (string, error) {
	if length < 0 {
		return "", ErrInvalidLength
	}
	result := make([]byte, length)
	var random [128]byte
	for offset := 0; offset < length; {
		if _, err := rand.Read(random[:]); err != nil {
			return "", err
		}
		for _, value := range random {
			if int(value) >= unbiasedByteLimit {
				continue
			}
			result[offset] = charset[int(value)%len(charset)]
			offset++
			if offset == length {
				break
			}
		}
	}
	return string(result), nil
}

// Slugify converts text, including Vietnamese text, to a URL-friendly slug.
func Slugify(text string) string {
	return slug.Make(text)
}
