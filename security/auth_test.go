package security

import (
	"errors"
	"strings"
	"testing"

	"relay-gateway/db"
)

func initSecurityTestDB(t *testing.T) {
	t.Helper()
	if err := db.InitDB(t.TempDir() + "/security.db"); err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
}

func TestAdminLifecycleAndGatewayIsolation(t *testing.T) {
	initSecurityTestDB(t)
	session, gatewayToken, err := SetupAdmin("admin-user", "a-strong-password", "127.0.0.1:4000", "test")
	if err != nil {
		t.Fatal(err)
	}
	if !HasAdmin() || !ValidateGatewayToken(gatewayToken) {
		t.Fatal("setup did not create the administrator and gateway token")
	}
	var tokenRow db.GatewayTokenModel
	if err := db.DB.First(&tokenRow, 1).Error; err != nil {
		t.Fatal(err)
	}
	if tokenRow.TokenHash == gatewayToken || strings.Contains(tokenRow.TokenHash, gatewayToken) {
		t.Fatal("gateway token was stored in plaintext")
	}
	if _, err := Login("admin-user", "wrong-password", "127.0.0.1", "test"); !errors.Is(err, ErrInvalidLogin) {
		t.Fatalf("wrong password should fail, got %v", err)
	}
	csrf, err := CSRFToken(session.Token)
	if err != nil || ValidateCSRF(session.Token, csrf) != nil {
		t.Fatalf("CSRF lifecycle failed: tokenErr=%v validateErr=%v", err, ValidateCSRF(session.Token, csrf))
	}

	newSession, err := UpdateCredentials(session.User.ID, "a-strong-password", "renamed-admin", "a-new-strong-password", "127.0.0.1", "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateSession(session.Token); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("old session should be invalid, got %v", err)
	}
	if _, err := ValidateSession(newSession.Token); err != nil {
		t.Fatalf("new session should be valid: %v", err)
	}
	if _, err := Login("renamed-admin", "a-new-strong-password", "127.0.0.1", "test"); err != nil {
		t.Fatalf("new credentials should work: %v", err)
	}

	rotated, err := RotateGatewayToken()
	if err != nil {
		t.Fatal(err)
	}
	if ValidateGatewayToken(gatewayToken) || !ValidateGatewayToken(rotated) {
		t.Fatal("gateway token rotation did not invalidate the old token")
	}

	if err := ResetAdmin(); err != nil {
		t.Fatal(err)
	}
	if HasAdmin() {
		t.Fatal("administrator still exists after reset")
	}
	if ValidateGatewayToken(rotated) {
		t.Fatal("gateway token remained valid after administrator reset")
	}
	if _, _, err := SetupAdmin("admin-again", "another-strong-password", "127.0.0.1", "test"); err != nil {
		t.Fatalf("setup after CLI-style reset should replace the gateway token row: %v", err)
	}
}

func TestLoginRateLimitUsesIPWithoutSourcePort(t *testing.T) {
	username := "rate-limit-user"
	for i := 0; i < 5; i++ {
		RecordLoginResult("203.0.113.9:"+string(rune('1'+i)), username, false)
	}
	allowed, retry := LoginAllowed("203.0.113.9:9999", username)
	if allowed || retry <= 0 {
		t.Fatalf("expected login to be rate limited across source ports, allowed=%v retry=%v", allowed, retry)
	}
	RecordLoginResult("203.0.113.9:1234", username, true)
}
