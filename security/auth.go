package security

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/argon2"
	"gorm.io/gorm"

	"relay-gateway/db"
)

const (
	SessionCookieName  = "relay_admin_session"
	SessionLifetime    = 7 * 24 * time.Hour
	loginFailureWindow = 15 * time.Minute
	maxLoginBuckets    = 4096
)

var (
	ErrNotInitialized = errors.New("administrator is not initialized")
	ErrAlreadySetup   = errors.New("administrator already exists")
	ErrInvalidLogin   = errors.New("invalid username or password")
	ErrInvalidSession = errors.New("invalid or expired session")
	ErrInvalidCSRF    = errors.New("invalid CSRF token")
)

type Session struct {
	Token     string
	CSRFToken string
	User      db.AdminUserModel
	ExpiresAt time.Time
}

type TokenInfo struct {
	Configured bool      `json:"configured"`
	Prefix     string    `json:"prefix,omitempty"`
	UpdatedAt  time.Time `json:"updated_at,omitempty"`
}

func HasAdmin() bool {
	if db.DB == nil {
		return false
	}
	var count int64
	return db.DB.Model(&db.AdminUserModel{}).Count(&count).Error == nil && count > 0
}

func validateUsername(username string) error {
	username = strings.TrimSpace(username)
	if len([]rune(username)) < 3 || len([]rune(username)) > 64 {
		return errors.New("username must contain 3 to 64 characters")
	}
	return nil
}

func validatePassword(password string) error {
	if len([]byte(password)) < 12 || len([]byte(password)) > 128 {
		return errors.New("password must contain 12 to 128 bytes")
	}
	return nil
}

func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func hashPassword(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	parallelism := uint8(2)
	if runtime.NumCPU() == 1 {
		parallelism = 1
	}
	memory, iterations, keyLen := uint32(64*1024), uint32(3), uint32(32)
	key := argon2.IDKey([]byte(password), salt, iterations, memory, parallelism, keyLen)
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s", memory, iterations, parallelism, base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key)), nil
}

func verifyPassword(encoded, password string) bool {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" || parts[2] != "v=19" {
		return false
	}
	var memory, iterations uint32
	var parallelism uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &parallelism); err != nil {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, iterations, memory, parallelism, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}

func newSessionRecord(tx *gorm.DB, user db.AdminUserModel, clientIP, userAgent string) (*Session, error) {
	token, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	csrf, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	record := db.AdminSessionModel{TokenHash: digest(token), AdminUserID: user.ID, SessionVersion: user.SessionVersion, CSRFHash: digest(csrf), ClientIP: clientIP, UserAgent: truncate(userAgent, 512), CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(SessionLifetime)}
	if err := tx.Create(&record).Error; err != nil {
		return nil, err
	}
	return &Session{Token: token, CSRFToken: csrf, User: user, ExpiresAt: record.ExpiresAt}, nil
}

func generateGatewayToken() (string, error) {
	raw, err := randomToken(32)
	if err != nil {
		return "", err
	}
	return "sk-gw-" + raw, nil
}

func SetupAdmin(username, password, clientIP, userAgent string) (*Session, string, error) {
	return SetupAdminContext(context.Background(), username, password, clientIP, userAgent)
}

// SetupAdminContext uses the request transaction when the setup request is
// wrapped by the audit middleware, keeping the first administrator, gateway
// token, session, and admin_action record atomic.
func SetupAdminContext(ctx context.Context, username, password, clientIP, userAgent string) (*Session, string, error) {
	conn := db.DBForContext(ctx)
	if conn == nil {
		return nil, "", errors.New("database is not initialized")
	}
	username = strings.TrimSpace(username)
	if err := validateUsername(username); err != nil {
		return nil, "", err
	}
	if err := validatePassword(password); err != nil {
		return nil, "", err
	}
	passwordHash, err := hashPassword(password)
	if err != nil {
		return nil, "", err
	}
	gatewayToken, err := generateGatewayToken()
	if err != nil {
		return nil, "", err
	}
	var session *Session
	setupFn := func(tx *gorm.DB) error {
		var count int64
		if err := tx.Model(&db.AdminUserModel{}).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return ErrAlreadySetup
		}
		user := db.AdminUserModel{Username: username, PasswordHash: passwordHash, SessionVersion: 1}
		if err := tx.Create(&user).Error; err != nil {
			return err
		}
		if err := tx.Save(&db.GatewayTokenModel{ID: 1, TokenHash: digest(gatewayToken), Prefix: tokenPrefix(gatewayToken)}).Error; err != nil {
			return err
		}
		session, err = newSessionRecord(tx, user, clientIP, userAgent)
		return err
	}
	if conn == db.DB {
		err = conn.Transaction(setupFn)
	} else {
		err = setupFn(conn)
	}
	return session, gatewayToken, err
}

func Login(username, password, clientIP, userAgent string) (*Session, error) {
	if db.DB == nil {
		return nil, ErrNotInitialized
	}
	var user db.AdminUserModel
	if err := db.DB.First(&user, "username = ?", strings.TrimSpace(username)).Error; err != nil || !verifyPassword(user.PasswordHash, password) {
		return nil, ErrInvalidLogin
	}
	return newSessionRecord(db.DB, user, clientIP, userAgent)
}

func ValidateSession(token string) (*Session, error) {
	return ValidateSessionContext(context.Background(), token)
}

func ValidateSessionContext(ctx context.Context, token string) (*Session, error) {
	conn := db.DBForContext(ctx)
	if conn == nil || strings.TrimSpace(token) == "" {
		return nil, ErrInvalidSession
	}
	var row db.AdminSessionModel
	if err := conn.First(&row, "token_hash = ?", digest(token)).Error; err != nil {
		return nil, ErrInvalidSession
	}
	now := time.Now().UTC()
	if !row.ExpiresAt.After(now) {
		_ = conn.Delete(&row).Error
		return nil, ErrInvalidSession
	}
	var user db.AdminUserModel
	if err := conn.First(&user, row.AdminUserID).Error; err != nil || user.SessionVersion != row.SessionVersion {
		return nil, ErrInvalidSession
	}
	if now.Sub(row.LastSeenAt) >= time.Hour {
		row.LastSeenAt = now
		row.ExpiresAt = now.Add(SessionLifetime)
		_ = conn.Model(&row).Updates(map[string]any{"last_seen_at": row.LastSeenAt, "expires_at": row.ExpiresAt}).Error
	}
	return &Session{Token: token, User: user, ExpiresAt: row.ExpiresAt}, nil
}

func ValidateCSRF(sessionToken, csrfToken string) error {
	return ValidateCSRFContext(context.Background(), sessionToken, csrfToken)
}

func ValidateCSRFContext(ctx context.Context, sessionToken, csrfToken string) error {
	conn := db.DBForContext(ctx)
	if conn == nil || sessionToken == "" || csrfToken == "" {
		return ErrInvalidCSRF
	}
	var row db.AdminSessionModel
	if err := conn.Select("csrf_hash").First(&row, "token_hash = ?", digest(sessionToken)).Error; err != nil {
		return ErrInvalidCSRF
	}
	if subtle.ConstantTimeCompare([]byte(row.CSRFHash), []byte(digest(csrfToken))) != 1 {
		return ErrInvalidCSRF
	}
	return nil
}

func CSRFToken(sessionToken string) (string, error) {
	// CSRF values are deliberately not recoverable. Rotate it when /me is called and return the new raw token.
	csrf, err := randomToken(32)
	if err != nil {
		return "", err
	}
	result := db.DB.Model(&db.AdminSessionModel{}).Where("token_hash = ?", digest(sessionToken)).Update("csrf_hash", digest(csrf))
	if result.Error != nil || result.RowsAffected != 1 {
		return "", ErrInvalidSession
	}
	return csrf, nil
}

func Logout(token string) error {
	return LogoutContext(context.Background(), token)
}

func LogoutContext(ctx context.Context, token string) error {
	conn := db.DBForContext(ctx)
	if conn == nil || token == "" {
		return nil
	}
	return conn.Delete(&db.AdminSessionModel{}, "token_hash = ?", digest(token)).Error
}

func UpdateCredentials(adminID uint, currentPassword, newUsername, newPassword, clientIP, userAgent string) (*Session, error) {
	return UpdateCredentialsContext(context.Background(), adminID, currentPassword, newUsername, newPassword, clientIP, userAgent)
}

func UpdateCredentialsContext(ctx context.Context, adminID uint, currentPassword, newUsername, newPassword, clientIP, userAgent string) (*Session, error) {
	conn := db.DBForContext(ctx)
	if conn == nil {
		return nil, errors.New("database is not initialized")
	}
	var user db.AdminUserModel
	if err := conn.First(&user, adminID).Error; err != nil {
		return nil, err
	}
	if !verifyPassword(user.PasswordHash, currentPassword) {
		return nil, ErrInvalidLogin
	}
	newUsername = strings.TrimSpace(newUsername)
	if newUsername == "" {
		newUsername = user.Username
	}
	if err := validateUsername(newUsername); err != nil {
		return nil, err
	}
	newHash := user.PasswordHash
	if newPassword != "" {
		if err := validatePassword(newPassword); err != nil {
			return nil, err
		}
		var err error
		newHash, err = hashPassword(newPassword)
		if err != nil {
			return nil, err
		}
	}
	var session *Session
	updateFn := func(tx *gorm.DB) error {
		user.Username = newUsername
		user.PasswordHash = newHash
		user.SessionVersion++
		if err := tx.Save(&user).Error; err != nil {
			return err
		}
		if err := tx.Where("admin_user_id = ?", user.ID).Delete(&db.AdminSessionModel{}).Error; err != nil {
			return err
		}
		var err error
		session, err = newSessionRecord(tx, user, clientIP, userAgent)
		return err
	}
	var err error
	if conn == db.DB {
		err = conn.Transaction(updateFn)
	} else {
		err = updateFn(conn)
	}
	return session, err
}

func ResetAdmin() error {
	if db.DB == nil {
		return errors.New("database is not initialized")
	}
	return db.DB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&db.AdminSessionModel{}).Error; err != nil {
			return err
		}
		if err := tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&db.AdminUserModel{}).Error; err != nil {
			return err
		}
		return tx.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&db.GatewayTokenModel{}).Error
	})
}

func RotateGatewayToken() (string, error) {
	return RotateGatewayTokenContext(context.Background())
}

func RotateGatewayTokenContext(ctx context.Context) (string, error) {
	conn := db.DBForContext(ctx)
	if conn == nil {
		return "", errors.New("database is not initialized")
	}
	token, err := generateGatewayToken()
	if err != nil {
		return "", err
	}
	record := db.GatewayTokenModel{ID: 1, TokenHash: digest(token), Prefix: tokenPrefix(token)}
	return token, conn.Save(&record).Error
}

func ValidateGatewayToken(token string) bool {
	if db.DB == nil || strings.TrimSpace(token) == "" {
		return false
	}
	var record db.GatewayTokenModel
	if err := db.DB.First(&record, 1).Error; err != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(record.TokenHash), []byte(digest(strings.TrimSpace(token)))) == 1
}

func GatewayTokenInfo() TokenInfo {
	if db.DB == nil {
		return TokenInfo{}
	}
	var record db.GatewayTokenModel
	if err := db.DB.First(&record, 1).Error; err != nil {
		return TokenInfo{}
	}
	return TokenInfo{Configured: true, Prefix: record.Prefix, UpdatedAt: record.UpdatedAt}
}

func tokenPrefix(token string) string {
	if len(token) <= 12 {
		return token
	}
	return token[:12]
}

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

type loginBucket struct {
	Failures []time.Time
	LockedTo time.Time
	LastSeen time.Time
}

var loginAttempts = struct {
	sync.Mutex
	items map[string]*loginBucket
}{items: make(map[string]*loginBucket)}

func LoginAllowed(clientIP, username string) (bool, time.Duration) {
	key := loginAttemptKey(clientIP, username)
	now := time.Now()
	loginAttempts.Lock()
	defer loginAttempts.Unlock()
	b := loginAttempts.items[key]
	if b == nil {
		return true, 0
	}
	if b.LockedTo.After(now) {
		return false, time.Until(b.LockedTo)
	}
	pruneLoginBucket(b, now)
	if len(b.Failures) == 0 {
		delete(loginAttempts.items, key)
	}
	return true, 0
}

func RecordLoginResult(clientIP, username string, success bool) {
	key := loginAttemptKey(clientIP, username)
	loginAttempts.Lock()
	defer loginAttempts.Unlock()
	if success {
		delete(loginAttempts.items, key)
		return
	}
	now := time.Now()
	b := loginAttempts.items[key]
	if b == nil {
		if len(loginAttempts.items) >= maxLoginBuckets {
			pruneLoginAttempts(now)
			if len(loginAttempts.items) >= maxLoginBuckets {
				evictOldestLoginBucket()
			}
		}
		b = &loginBucket{}
		loginAttempts.items[key] = b
	}
	pruneLoginBucket(b, now)
	b.Failures = append(b.Failures, now)
	b.LastSeen = now
	if len(b.Failures) >= 5 {
		b.LockedTo = now.Add(loginFailureWindow)
	}
}

func pruneLoginBucket(bucket *loginBucket, now time.Time) {
	if bucket == nil {
		return
	}
	cutoff := now.Add(-loginFailureWindow)
	kept := bucket.Failures[:0]
	for _, at := range bucket.Failures {
		if at.After(cutoff) {
			kept = append(kept, at)
		}
	}
	bucket.Failures = kept
}

// pruneLoginAttempts is only called while the map is at capacity, avoiding an
// O(n) sweep on each login while ensuring failed-login keys cannot grow without
// bound in a long-running public deployment.
func pruneLoginAttempts(now time.Time) {
	for key, bucket := range loginAttempts.items {
		pruneLoginBucket(bucket, now)
		if !bucket.LockedTo.After(now) && len(bucket.Failures) == 0 {
			delete(loginAttempts.items, key)
		}
	}
}

func evictOldestLoginBucket() {
	var oldestKey string
	var oldest time.Time
	for key, bucket := range loginAttempts.items {
		if oldestKey == "" || bucket.LastSeen.Before(oldest) {
			oldestKey, oldest = key, bucket.LastSeen
		}
	}
	if oldestKey != "" {
		delete(loginAttempts.items, oldestKey)
	}
}

func loginAttemptKey(clientIP, username string) string {
	clientIP = strings.TrimSpace(clientIP)
	if host, _, err := net.SplitHostPort(clientIP); err == nil {
		clientIP = host
	}
	return strings.ToLower(clientIP) + "|" + digest(strings.ToLower(strings.TrimSpace(username)))
}

func RetryAfterSeconds(d time.Duration) string {
	seconds := int(d.Round(time.Second).Seconds())
	if seconds < 1 {
		seconds = 1
	}
	return strconv.Itoa(seconds)
}
