package auth

import (
	"github.com/golang-jwt/jwt/v5"
)

// UserInfo represents the standard user information embedded in a token.
type UserInfo struct {
	ID       string `json:"id"`
	Role     string `json:"role,omitempty"`
	Email    string `json:"email,omitempty"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// CustomClaims represents standard JWT claims plus our custom UserInfo payload.
type CustomClaims struct {
	UserInfo UserInfo `json:"user_info"`
	jwt.RegisteredClaims
}
