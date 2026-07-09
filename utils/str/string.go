// Package str provides utility functions for common string manipulation tasks.
//
// It includes helpers for generating random strings, converting cases, and other
// text processing utilities often needed in web applications.
package str

import (
	"crypto/rand"
	"math/big"

	"github.com/gosimple/slug"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// Random generates a secure random string of a given length
func Random(length int) string {
	b := make([]byte, length)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			// Fallback if random fails (extremely rare)
			b[i] = charset[0]
			continue
		}
		b[i] = charset[n.Int64()]
	}
	return string(b)
}

// Slugify converts any string (including Vietnamese) to a URL-friendly slug
func Slugify(text string) string {
	return slug.Make(text)
}
