package auth

import (
	"encoding/base64"
	"testing"
)

func TestHashPasswordAndVerify(t *testing.T) {
	t.Parallel()

	pw := HashPassword("guest")

	if !pw.Verify("guest") {
		t.Error("Verify() returned false for correct password")
	}
}

func TestVerifyRejectsWrongPassword(t *testing.T) {
	t.Parallel()

	pw := HashPassword("correct")

	if pw.Verify("wrong") {
		t.Error("Verify() returned true for wrong password")
	}
}

func TestVerifyKnownGuestHash(t *testing.T) {
	t.Parallel()

	const knownHash = "+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+"

	pw, err := ParsePassword(knownHash)
	if err != nil {
		t.Fatalf("ParsePassword() error: %v", err)
	}

	if !pw.Verify("guest") {
		t.Error("Verify() returned false for known guest hash")
	}

	if pw.Verify("notguest") {
		t.Error("Verify() returned true for wrong password against known hash")
	}
}

func TestPasswordString(t *testing.T) {
	t.Parallel()

	const knownHash = "+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+"

	pw, err := ParsePassword(knownHash)
	if err != nil {
		t.Fatalf("ParsePassword() error: %v", err)
	}

	if got := pw.String(); got != knownHash {
		t.Errorf("String() = %q, want %q", got, knownHash)
	}
}

func TestHashPasswordProduces36Bytes(t *testing.T) {
	t.Parallel()

	pw := HashPassword("test")

	decoded, err := base64.StdEncoding.DecodeString(pw.String())
	if err != nil {
		t.Fatalf("base64 decode error: %v", err)
	}

	const expectedLen = 36 // 4 byte salt + 32 byte SHA256
	if len(decoded) != expectedLen {
		t.Errorf("decoded hash length = %d, want %d", len(decoded), expectedLen)
	}
}

func TestParsePasswordInvalidBase64(t *testing.T) {
	t.Parallel()

	_, err := ParsePassword("!!!not-valid-base64!!!")
	if err == nil {
		t.Error("ParsePassword() expected error for invalid base64, got nil")
	}
}

func TestHashPasswordRoundtripMultiple(t *testing.T) {
	t.Parallel()

	passwords := []string{"", "short", "a-longer-password-with-special-chars!@#$%"}
	for _, plain := range passwords {
		t.Run(plain, func(t *testing.T) {
			t.Parallel()

			pw := HashPassword(plain)
			if !pw.Verify(plain) {
				t.Errorf("Verify() returned false for %q", plain)
			}
		})
	}
}
