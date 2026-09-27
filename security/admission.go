package security

import (
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"time"
)

var ErrAuthBusy = errors.New("password verification capacity is busy")

// Password work has a separate, fail-fast budget: waiting HTTP requests must
// not reserve hundreds of Argon2 allocations or form an unbounded hash queue.
type passwordBudget struct {
	slots chan struct{}
}

func (b *passwordBudget) acquire(ctx context.Context) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case b.slots <- struct{}{}:
		return func() { <-b.slots }, nil
	default:
		return nil, ErrAuthBusy
	}
}

var authBudgetOnce sync.Once
var authBudget *passwordBudget

func acquirePasswordWork(ctx context.Context) (func(), error) {
	authBudgetOnce.Do(func() {
		limit := 2
		if configured, err := strconv.Atoi(os.Getenv("RELAY_AUTH_HASH_CONCURRENCY")); err == nil && configured >= 1 && configured <= 32 {
			limit = configured
		}
		authBudget = &passwordBudget{slots: make(chan struct{}, limit)}
	})
	return authBudget.acquire(ctx)
}

type loginBucket struct {
	Failures []time.Time
	LockedTo time.Time
	LastSeen time.Time
	InFlight int
}

var loginAttempts = struct {
	sync.Mutex
	items map[string]*loginBucket
}{items: make(map[string]*loginBucket)}

func loginBucketAllowed(b *loginBucket, now time.Time) (bool, time.Duration) {
	if b == nil {
		return true, 0
	}
	pruneLoginBucket(b, now)
	if b.LockedTo.After(now) {
		return false, b.LockedTo.Sub(now)
	}
	if len(b.Failures)+b.InFlight >= 5 {
		return false, time.Second
	}
	return true, 0
}

func LoginAllowed(clientIP, username string) (bool, time.Duration) {
	loginAttempts.Lock()
	defer loginAttempts.Unlock()
	return loginBucketAllowed(loginAttempts.items[loginAttemptKey(clientIP, username)], time.Now())
}

// BeginLoginAttempt atomically includes active attempts in the per-client
// limit. Finish records a result; Cancel releases requests rejected by the
// global password budget or canceled before they could be checked.
type LoginAttempt struct {
	key    string
	bucket *loginBucket
	once   sync.Once
}

func BeginLoginAttempt(clientIP, username string) (*LoginAttempt, time.Duration) {
	key, now := loginAttemptKey(clientIP, username), time.Now()
	loginAttempts.Lock()
	defer loginAttempts.Unlock()
	b := loginAttempts.items[key]
	if allowed, retry := loginBucketAllowed(b, now); !allowed {
		return nil, retry
	}
	if b == nil {
		b = makeLoginBucket(key, now)
		if b == nil {
			return nil, time.Second
		}
	}
	b.InFlight++
	b.LastSeen = now
	return &LoginAttempt{key: key, bucket: b}, 0
}

func (a *LoginAttempt) Finish(success bool) { a.finish(&success) }
func (a *LoginAttempt) Cancel()             { a.finish(nil) }

func (a *LoginAttempt) finish(success *bool) {
	if a == nil {
		return
	}
	a.once.Do(func() {
		loginAttempts.Lock()
		defer loginAttempts.Unlock()
		b := a.bucket
		b.InFlight--
		if success != nil {
			recordLoginBucket(b, *success, time.Now())
		}
		if b.InFlight == 0 && len(b.Failures) == 0 {
			delete(loginAttempts.items, a.key)
		}
	})
}

func RecordLoginResult(clientIP, username string, success bool) {
	key, now := loginAttemptKey(clientIP, username), time.Now()
	loginAttempts.Lock()
	defer loginAttempts.Unlock()
	b := loginAttempts.items[key]
	if b == nil {
		if success {
			return
		}
		b = makeLoginBucket(key, now)
		if b == nil {
			return
		}
	}
	recordLoginBucket(b, success, now)
	if b.InFlight == 0 && len(b.Failures) == 0 {
		delete(loginAttempts.items, key)
	}
}

func recordLoginBucket(b *loginBucket, success bool, now time.Time) {
	b.LastSeen = now
	if success {
		b.Failures = nil
		b.LockedTo = time.Time{}
		return
	}
	pruneLoginBucket(b, now)
	b.Failures = append(b.Failures, now)
	if len(b.Failures) >= 5 {
		b.LockedTo = now.Add(loginFailureWindow)
	}
}

// Called with loginAttempts locked. Active reservations are never evicted.
func makeLoginBucket(key string, now time.Time) *loginBucket {
	if len(loginAttempts.items) >= maxLoginBuckets {
		pruneLoginAttempts(now)
		if len(loginAttempts.items) >= maxLoginBuckets {
			evictOldestLoginBucket()
		}
		if len(loginAttempts.items) >= maxLoginBuckets {
			return nil
		}
	}
	b := &loginBucket{LastSeen: now}
	loginAttempts.items[key] = b
	return b
}

func pruneLoginBucket(b *loginBucket, now time.Time) {
	cutoff := now.Add(-loginFailureWindow)
	kept := b.Failures[:0]
	for _, at := range b.Failures {
		if at.After(cutoff) {
			kept = append(kept, at)
		}
	}
	b.Failures = kept
}

func pruneLoginAttempts(now time.Time) {
	for key, b := range loginAttempts.items {
		pruneLoginBucket(b, now)
		if b.InFlight == 0 && !b.LockedTo.After(now) && len(b.Failures) == 0 {
			delete(loginAttempts.items, key)
		}
	}
}

func evictOldestLoginBucket() {
	var oldestKey string
	var oldest time.Time
	for key, b := range loginAttempts.items {
		if b.InFlight == 0 && (oldestKey == "" || b.LastSeen.Before(oldest)) {
			oldestKey, oldest = key, b.LastSeen
		}
	}
	if oldestKey != "" {
		delete(loginAttempts.items, oldestKey)
	}
}
