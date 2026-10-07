package security

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"relay-gateway/db"
)

func createCSRFTestSession(t *testing.T) *Session {
	t.Helper()
	user := db.AdminUserModel{Username: "csrf-admin", PasswordHash: "unused", SessionVersion: 1}
	if err := db.DB.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	session, err := newSessionRecord(db.DB, user, "127.0.0.1", "csrf-test")
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestCSRFTokenSupportsConcurrentTabsWithoutSessionWrites(t *testing.T) {
	initSecurityTestDB(t)
	session := createCSRFTestSession(t)
	var before db.AdminSessionModel
	if err := db.DB.First(&before, "token_hash = ?", digest(session.Token)).Error; err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32
	const callback = "test:csrf-session-writes"
	if err := db.DB.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "admin_session_models" {
			writes.Add(1)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.DB.Callback().Update().Remove(callback) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	start := make(chan struct{})
	var workers sync.WaitGroup
	for tab := 0; tab < 32; tab++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for refresh := 0; refresh < 8; refresh++ {
				token, err := CSRFTokenContext(ctx, session.Token)
				if err != nil || token != session.CSRFToken {
					t.Errorf("tab token changed: equal=%v err=%v", token == session.CSRFToken, err)
					return
				}
				// One tab's refresh must not invalidate another tab's cached token,
				// including the token returned by the initial login/setup.
				if err := ValidateCSRFContext(ctx, session.Token, session.CSRFToken); err != nil {
					t.Errorf("another tab's token was revoked: %v", err)
					return
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	if writes.Load() != 0 {
		t.Fatalf("CSRF token retrieval/validation performed %d session writes", writes.Load())
	}
	var after db.AdminSessionModel
	if err := db.DB.First(&after, "token_hash = ?", digest(session.Token)).Error; err != nil {
		t.Fatal(err)
	}
	if before.CSRFHash != after.CSRFHash || !before.ExpiresAt.Equal(after.ExpiresAt) || !before.LastSeenAt.Equal(after.LastSeenAt) {
		t.Fatal("CSRF retrieval changed session state")
	}
}

func TestStableCSRFTokenPreservesExistingSessionTokens(t *testing.T) {
	initSecurityTestDB(t)
	session := createCSRFTestSession(t)
	legacyToken, err := randomToken(32)
	if err != nil {
		t.Fatal(err)
	}
	legacyHash := digest(legacyToken)
	if err := db.DB.Model(&db.AdminSessionModel{}).Where("token_hash = ?", digest(session.Token)).Update("csrf_hash", legacyHash).Error; err != nil {
		t.Fatal(err)
	}
	for refresh := 0; refresh < 8; refresh++ {
		token, err := CSRFToken(session.Token)
		if err != nil || token != session.CSRFToken {
			t.Fatalf("stable token retrieval: equal=%v err=%v", token == session.CSRFToken, err)
		}
		for _, tabToken := range []string{legacyToken, token} {
			if err := ValidateCSRF(session.Token, tabToken); err != nil {
				t.Fatalf("pre-upgrade and refreshed tabs cannot coexist: %v", err)
			}
		}
	}
	var row db.AdminSessionModel
	if err := db.DB.First(&row, "token_hash = ?", digest(session.Token)).Error; err != nil {
		t.Fatal(err)
	}
	if row.CSRFHash != legacyHash {
		t.Fatal("retrieving a stable token replaced the legacy verifier")
	}
	if err := Logout(session.Token); err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{legacyToken, session.CSRFToken} {
		if err := ValidateCSRF(session.Token, token); !errors.Is(err, ErrInvalidCSRF) {
			t.Fatalf("logged-out legacy/stable token remained usable: %v", err)
		}
	}
}

func TestCSRFTokenIsBoundToLiveSession(t *testing.T) {
	for _, test := range []struct {
		name   string
		revoke func(*Session) error
	}{
		{"logout", func(s *Session) error { return Logout(s.Token) }},
		{"expiry", func(s *Session) error {
			return db.DB.Model(&db.AdminSessionModel{}).Where("token_hash = ?", digest(s.Token)).Update("expires_at", time.Now().UTC().Add(-time.Second)).Error
		}},
		{"account version", func(s *Session) error {
			return db.DB.Model(&db.AdminUserModel{}).Where("id = ?", s.User.ID).Update("session_version", s.User.SessionVersion+1).Error
		}},
		{"deleted account", func(s *Session) error {
			return db.DB.Delete(&db.AdminUserModel{}, s.User.ID).Error
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			initSecurityTestDB(t)
			session := createCSRFTestSession(t)
			if err := test.revoke(session); err != nil {
				t.Fatal(err)
			}
			if token, err := CSRFToken(session.Token); !errors.Is(err, ErrInvalidSession) || token != "" {
				t.Fatalf("revoked session issued a token: token=%q err=%v", token, err)
			}
			if err := ValidateCSRF(session.Token, session.CSRFToken); !errors.Is(err, ErrInvalidCSRF) {
				t.Fatalf("revoked session accepted CSRF: %v", err)
			}
		})
	}
}

func TestCSRFTokenRejectsTamperingAndOtherSessions(t *testing.T) {
	initSecurityTestDB(t)
	first := createCSRFTestSession(t)
	second, err := newSessionRecord(db.DB, first.User, "127.0.0.1", "second-tab")
	if err != nil {
		t.Fatal(err)
	}
	if first.CSRFToken == first.Token || first.CSRFToken == second.CSRFToken {
		t.Fatal("CSRF token exposes the cookie or is shared across sessions")
	}
	for _, pair := range [][2]string{
		{first.Token, ""},
		{first.Token, first.CSRFToken + "x"},
		{first.Token, second.CSRFToken},
		{second.Token, first.CSRFToken},
		{first.Token, first.Token},
		{"", first.CSRFToken},
		{"unknown-session", sessionCSRFToken("unknown-session")},
	} {
		if err := ValidateCSRF(pair[0], pair[1]); !errors.Is(err, ErrInvalidCSRF) {
			t.Fatalf("invalid CSRF pair accepted: %v", err)
		}
	}
	if _, err := ValidateSession(first.CSRFToken); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("CSRF token authenticated as a session: %v", err)
	}
}

func TestCSRFChecksExpiryAfterConnectionPoolWait(t *testing.T) {
	initSecurityTestDB(t)
	session := createCSRFTestSession(t)
	sqlDB, err := db.DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []struct {
		name string
		run  func(context.Context) error
		want error
	}{
		{"retrieve", func(ctx context.Context) error {
			_, err := CSRFTokenContext(ctx, session.Token)
			return err
		}, ErrInvalidSession},
		{"validate", func(ctx context.Context) error {
			return ValidateCSRFContext(ctx, session.Token, session.CSRFToken)
		}, ErrInvalidCSRF},
	} {
		t.Run(call.name, func(t *testing.T) {
			if err := db.DB.Model(&db.AdminSessionModel{}).Where("token_hash = ?", digest(session.Token)).Update("expires_at", time.Now().UTC().Add(time.Hour)).Error; err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			reserved, err := sqlDB.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer reserved.Close()
			waitCount := sqlDB.Stats().WaitCount
			done := make(chan error, 1)
			go func() { done <- call.run(ctx) }()
			for sqlDB.Stats().WaitCount == waitCount && ctx.Err() == nil {
				time.Sleep(time.Millisecond)
			}
			if ctx.Err() != nil {
				t.Fatal("CSRF lookup did not wait for the reserved connection")
			}
			// Expire after the lookup started. A timestamp bound before its pool
			// wait would still consider this row live when the query is admitted.
			if _, err := reserved.ExecContext(ctx, "UPDATE admin_session_models SET expires_at = ? WHERE token_hash = ?", time.Now().UTC(), digest(session.Token)); err != nil {
				t.Fatal(err)
			}
			if err := reserved.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, call.want) {
					t.Fatalf("session expired during pool wait was accepted: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("CSRF lookup did not finish after releasing the connection")
			}
		})
	}
}
