package security

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"relay-gateway/db"
)

func TestConcurrentSetupCreatesSingleAdmin(t *testing.T) {
	initSecurityTestDB(t)
	type setupResult struct {
		session *Session
		token   string
		err     error
	}
	results := make(chan setupResult, 4)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			<-start
			session, token, err := SetupAdmin(fmt.Sprintf("concurrent-admin-%d", index), "correct horse battery", "203.0.113.10", "setup-test")
			results <- setupResult{session: session, token: token, err: err}
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)

	var winner setupResult
	successes := 0
	for result := range results {
		if result.err == nil {
			successes++
			winner = result
		} else if !errors.Is(result.err, ErrAlreadySetup) && !errors.Is(result.err, ErrAuthBusy) {
			t.Errorf("concurrent setup returned unexpected error: %v", result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful setup count = %d, want 1", successes)
	}
	assertCount := func(model any) {
		t.Helper()
		var count int64
		if err := db.DB.Model(model).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%T count = %d, want 1", model, count)
		}
	}
	assertCount(&db.AdminUserModel{})
	assertCount(&db.GatewayTokenModel{})
	assertCount(&db.AdminSessionModel{})
	if winner.session == nil || !ValidateGatewayToken(winner.token) {
		t.Fatal("winning setup did not return valid credentials")
	}
	if _, err := ValidateSession(winner.session.Token); err != nil {
		t.Fatalf("winning session is invalid: %v", err)
	}
	var before db.GatewayTokenModel
	if err := db.DB.First(&before, 1).Error; err != nil {
		t.Fatal(err)
	}
	if _, _, err := SetupAdmin("repeat-admin", "another strong password", "203.0.113.11", "setup-test"); !errors.Is(err, ErrAlreadySetup) {
		t.Fatalf("repeated setup error = %v, want ErrAlreadySetup", err)
	}
	var after db.GatewayTokenModel
	if err := db.DB.First(&after, 1).Error; err != nil {
		t.Fatal(err)
	}
	if before.TokenHash != after.TokenHash || !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatal("repeated setup changed the gateway key")
	}
	assertCount(&db.AdminUserModel{})
	assertCount(&db.GatewayTokenModel{})
	assertCount(&db.AdminSessionModel{})
	if !ValidateGatewayToken(winner.token) {
		t.Fatal("repeated setup invalidated the winning gateway key")
	}
	if _, err := ValidateSession(winner.session.Token); err != nil {
		t.Fatalf("repeated setup invalidated the winning session: %v", err)
	}
}
