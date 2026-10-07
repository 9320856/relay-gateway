package security

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"

	"relay-gateway/db"
)

func TestPasswordBudgetRejectsConcurrentWorkAndHonorsCancellation(t *testing.T) {
	budget := &passwordBudget{slots: make(chan struct{}, 2)}
	first, err := budget.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := budget.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := budget.acquire(context.Background())
			if err == nil {
				admitted.Add(1)
				release()
			} else if !errors.Is(err, ErrAuthBusy) {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 0 {
		t.Fatal("hash budget admitted work above its limit")
	}
	first()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := budget.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission: %v", err)
	}
	release, err := budget.acquire(context.Background())
	if err != nil {
		t.Fatalf("released capacity unavailable: %v", err)
	}
	release()
	second()
}

func TestConcurrentCredentialChangesRejectStaleVerifiedUser(t *testing.T) {
	initSecurityTestDB(t)
	session, _, err := SetupAdmin("concurrent-account", "a-strong-password", "127.0.0.1", "test")
	if err != nil {
		t.Fatal(err)
	}
	previousBudget := authBudget
	authBudget = &passwordBudget{slots: make(chan struct{}, 2)}
	t.Cleanup(func() { authBudget = previousBudget })
	// Force both calls to read and verify the same account version. The SQL
	// connection has been released before this after-query callback runs.
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var queries atomic.Int32
	const callback = "test:credential-read-barrier"
	if err := db.DB.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Schema != nil && tx.Statement.Schema.Name == "AdminUserModel" && queries.Add(1) <= 2 {
			ready <- struct{}{}
			<-release
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.DB.Callback().Query().Remove(callback) })
	type outcome struct {
		session *Session
		err     error
	}
	results := make(chan outcome, 2)
	for _, username := range []string{"changed-first", "changed-second"} {
		go func(username string) {
			updated, err := UpdateCredentials(session.User.ID, "a-strong-password", username, "", "127.0.0.1", "test")
			results <- outcome{updated, err}
		}(username)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-ready:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("both credential changes did not reach the stale-read barrier")
		}
	}
	close(release)
	var succeeded int
	var winner *Session
	for i := 0; i < 2; i++ {
		select {
		case result := <-results:
			if result.err == nil {
				succeeded++
				winner = result.session
			} else if !errors.Is(result.err, ErrCredentialsChanged) {
				t.Fatalf("unexpected concurrent credential failure: %v", result.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("credential change did not finish")
		}
	}
	if succeeded != 1 {
		t.Fatalf("%d credential changes accepted the same verified account version, want exactly one", succeeded)
	}
	if winner == nil || winner.User.SessionVersion != session.User.SessionVersion+1 {
		t.Fatalf("account version did not advance exactly once: %+v", winner)
	}
	if _, err := ValidateSession(winner.Token); err != nil {
		t.Fatalf("rejected stale change invalidated the winning session: %v", err)
	}
	if _, err := ValidateSession(session.Token); !errors.Is(err, ErrInvalidSession) {
		t.Fatalf("prior account session survived the successful credential change: %v", err)
	}
	var persisted db.AdminUserModel
	if err := db.DB.First(&persisted, session.User.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.Username != winner.User.Username || persisted.SessionVersion != winner.User.SessionVersion {
		t.Fatalf("rejected stale change replaced the winning account: persisted=%+v winner=%+v", persisted, winner.User)
	}
}

// Authentication helpers execute before LoginContext or the actual gateway
// handler, so cancellation must reach these initial lookups as well.
func TestAuthenticationLookupsCancelBeforeConnectionAdmission(t *testing.T) {
	initSecurityTestDB(t)
	sqlDB, err := db.DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := sqlDB.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()
	checks := []struct {
		name string
		call func(context.Context) error
	}{
		{"administrator lookup", func(ctx context.Context) error {
			if HasAdminContext(ctx) {
				return errors.New("canceled administrator lookup unexpectedly succeeded")
			}
			return nil
		}},
		{"gateway credential lookup", func(ctx context.Context) error {
			if ValidateGatewayTokenContext(ctx, "candidate-token") {
				return errors.New("canceled gateway credential lookup unexpectedly succeeded")
			}
			return nil
		}},
		{"gateway information lookup", func(ctx context.Context) error {
			if GatewayTokenInfoContext(ctx).Configured {
				return errors.New("canceled gateway information lookup unexpectedly succeeded")
			}
			return nil
		}},
		{"CSRF retrieval", func(ctx context.Context) error {
			_, err := CSRFTokenContext(ctx, "candidate-session")
			if !errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("canceled CSRF retrieval returned %v", err)
			}
			return nil
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- check.call(ctx) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Fatal("lookup returned before exercising its SQL connection wait")
				}
			case <-time.After(time.Second):
				t.Fatal("authentication lookup ignored context deadline while SQL connection was reserved")
			}
		})
	}
}

func TestConcurrentLoginReservationsAndIdempotentRelease(t *testing.T) {
	const username = "concurrent-reservations"
	var accepted atomic.Int32
	var wg sync.WaitGroup
	attempts := make(chan *LoginAttempt, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			attempt, _ := BeginLoginAttempt(fmt.Sprintf("203.0.113.42:%d", 4000+index), username)
			if attempt != nil {
				accepted.Add(1)
				attempts <- attempt
			}
		}(i)
	}
	wg.Wait()
	close(attempts)
	if accepted.Load() != 5 {
		t.Fatalf("admitted %d simultaneous attempts, want 5", accepted.Load())
	}
	for attempt := range attempts {
		attempt.Cancel()
		attempt.Cancel()
	}
	if allowed, _ := LoginAllowed("203.0.113.42:1234", username); !allowed {
		t.Fatal("canceled attempts consumed failure budget")
	}
	for i := 0; i < 5; i++ {
		attempt, _ := BeginLoginAttempt("203.0.113.42", username)
		if attempt == nil {
			t.Fatal("attempt rejected before fifth failure")
		}
		attempt.Finish(false)
		attempt.Cancel()
	}
	if allowed, retry := LoginAllowed("203.0.113.42", username); allowed || retry <= 0 {
		t.Fatal("completed failures did not lock login")
	}
	RecordLoginResult("203.0.113.42", username, true)
}

func TestActiveLoginReservationsCannotBeEvicted(t *testing.T) {
	loginAttempts.Lock()
	previous := loginAttempts.items
	loginAttempts.items = make(map[string]*loginBucket)
	for i := 0; i < maxLoginBuckets; i++ {
		loginAttempts.items[fmt.Sprint(i)] = &loginBucket{InFlight: 1}
	}
	loginAttempts.Unlock()
	t.Cleanup(func() { loginAttempts.Lock(); loginAttempts.items = previous; loginAttempts.Unlock() })
	if attempt, _ := BeginLoginAttempt("203.0.113.43", "full-reservations"); attempt != nil {
		attempt.Cancel()
		t.Fatal("evicted an active reservation")
	}
}

func TestAllCredentialOperationsSharePasswordBudget(t *testing.T) {
	initSecurityTestDB(t)
	session, _, err := SetupAdmin("budget-admin", "a-strong-password", "127.0.0.1", "test")
	if err != nil {
		t.Fatal(err)
	}
	var releases []func()
	for i := 0; i < cap(authBudget.slots); i++ {
		release, err := acquirePasswordWork(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	if _, err := LoginContext(context.Background(), "budget-admin", "a-strong-password", "127.0.0.1", "test"); !errors.Is(err, ErrAuthBusy) {
		t.Fatalf("login bypassed budget: %v", err)
	}
	if _, err := UpdateCredentials(session.User.ID, "a-strong-password", "changed-admin", "another-password", "127.0.0.1", "test"); !errors.Is(err, ErrAuthBusy) {
		t.Fatalf("credential update bypassed budget: %v", err)
	}
	if err := ResetAdmin(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := SetupAdmin("new-budget-admin", "a-strong-password", "127.0.0.1", "test"); !errors.Is(err, ErrAuthBusy) {
		t.Fatalf("setup bypassed budget: %v", err)
	}
	var count int64
	if err := db.DB.Model(&db.AdminUserModel{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("rejected setup changed administrator state: count=%d err=%v", count, err)
	}
}
