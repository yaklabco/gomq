// Package auth provides authentication and authorization for AMQP users.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
)

const saltSize = 4

// Password holds a salted SHA256 password hash.
// The hash field contains a 4-byte random salt followed by SHA256(salt + plaintext),
// totaling 36 bytes.
type Password struct {
	hash []byte // salt (4 bytes) + SHA256(salt + plaintext) (32 bytes) = 36 bytes
}

// HashPassword generates a new salted SHA256 password hash from plaintext.
func HashPassword(plaintext string) Password {
	salt := make([]byte, saltSize)
	if _, err := rand.Read(salt); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}

	buf := make([]byte, 0, saltSize+len(plaintext))
	buf = append(buf, salt...)
	buf = append(buf, []byte(plaintext)...)
	digest := sha256.Sum256(buf)

	hash := make([]byte, saltSize+sha256.Size)
	copy(hash[:saltSize], salt)
	copy(hash[saltSize:], digest[:])

	return Password{hash: hash}
}

// ParsePassword decodes a base64-encoded password hash.
func ParsePassword(encoded string) (Password, error) {
	hash, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return Password{}, fmt.Errorf("decode password hash: %w", err)
	}

	return Password{hash: hash}, nil
}

// Verify checks whether the given plaintext matches the stored hash.
func (p Password) Verify(plaintext string) bool {
	if len(p.hash) < saltSize {
		return false
	}

	salt := p.hash[:saltSize]
	stored := p.hash[saltSize:]

	buf := make([]byte, 0, saltSize+len(plaintext))
	buf = append(buf, salt...)
	buf = append(buf, []byte(plaintext)...)
	digest := sha256.Sum256(buf)

	return subtle.ConstantTimeCompare(stored, digest[:]) == 1
}

// String returns the base64-encoded representation of the password hash.
func (p Password) String() string {
	return base64.StdEncoding.EncodeToString(p.hash)
}

// MarshalJSON serializes the password as a base64 JSON string.
func (p Password) MarshalJSON() ([]byte, error) {
	encoded := p.String()
	out := make([]byte, 0, len(encoded)+len(`""`))
	out = append(out, '"')
	out = append(out, []byte(encoded)...)
	out = append(out, '"')

	return out, nil
}

// UnmarshalJSON deserializes a base64 JSON string into a password.
func (p *Password) UnmarshalJSON(data []byte) error {
	if len(data) < len(`""`) || data[0] != '"' || data[len(data)-1] != '"' {
		return errors.New("password hash must be a JSON string")
	}

	encoded := string(data[1 : len(data)-1])

	parsed, err := ParsePassword(encoded)
	if err != nil {
		return err
	}

	p.hash = parsed.hash

	return nil
}
