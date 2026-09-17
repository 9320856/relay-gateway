package db

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"relay-gateway/config"
)

// txContextKey carries a request-scoped transaction to persistence helpers.
// Keeping this in the db package avoids mutating the process-global DB pointer
// (which would be unsafe while other requests are running).
type txContextKey struct{}

// WithTx returns a context that uses tx for writes performed by context-aware
// helpers. The transaction must be committed/rolled back by the caller.
func WithTx(ctx context.Context, tx *gorm.DB) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, txContextKey{}, tx)
}

// DBForContext resolves the request transaction, falling back to the shared
// database for callers outside a transaction (tests, workers, and reads).
func DBForContext(ctx context.Context) *gorm.DB {
	if ctx != nil {
		if tx, ok := ctx.Value(txContextKey{}).(*gorm.DB); ok && tx != nil {
			return tx
		}
	}
	return DB
}

const SchemaVersion = "3"

const previousSchemaVersion = "2"

const (
	taskMappingExternalIDMaxBytes = 128
	// ImageTaskMappingLookupPrefix namespaces image job lookup keys so a raw
	// provider ID cannot collide with a video mapping. It is shared with the
	// router because these keys are internal, not public route IDs.
	ImageTaskMappingLookupPrefix = "imgjob_"
)

// ErrIncompatibleSchema means the database contains application data from an
// unsupported schema version.  Opening it must never discard that data; an
// explicit migration or restore is required instead.
var ErrIncompatibleSchema = errors.New("unsupported database schema")

// ErrTaskMappingChannelConflict means a provider task ID is already pinned to
// a different channel.  Callers must never overwrite that routing decision:
// doing so could send a later status or content request to the wrong upstream.
var ErrTaskMappingChannelConflict = errors.New("task mapping is pinned to a different channel")

// ErrDBEncryptionKeyRequired is returned when channel credentials are provided
// but RELAY_DB_ENCRYPTION_KEY is not configured in the environment. Plaintext
// credentials must never be written to SQLite.
var ErrDBEncryptionKeyRequired = errors.New("RELAY_DB_ENCRYPTION_KEY is required to store channel credentials")

var (
	DB                  *gorm.DB
	activeChannelsCache atomic.Pointer[[]config.UpstreamChannel]
	videoTaskCache      sync.Map
	OnChannelSaved      func(channelID string)
	// BeforeClose lets background services finish database work before the
	// shared connection is cleared and closed. It must return before Close can
	// proceed.
	BeforeClose       func()
	initMu            sync.Mutex
	databasePath      string
	memorySecretKey   []byte
	memorySecretKeyMu sync.RWMutex
)

type SchemaMeta struct {
	Key       string    `gorm:"primaryKey;size:64" json:"key"`
	Value     string    `gorm:"type:text;not null" json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (SchemaMeta) TableName() string { return "schema_meta" }

type ChannelModel struct {
	ID               string    `gorm:"primaryKey;size:64" json:"id"`
	Name             string    `gorm:"size:128;not null" json:"name"`
	Type             string    `gorm:"size:32;not null;index" json:"type"`
	BaseURL          string    `gorm:"size:512;not null" json:"base_url"`
	Enabled          bool      `gorm:"not null;index" json:"enabled"`
	Priority         int       `gorm:"not null;default:1;index" json:"priority"`
	Weight           int       `gorm:"not null;default:1" json:"weight"`
	FetchModels      bool      `gorm:"not null" json:"fetch_models"`
	AnthropicVersion string    `gorm:"size:32;default:'2023-06-01'" json:"anthropic_version"`
	ModelsRaw        string    `gorm:"type:text" json:"models_raw"`
	ModelMapRaw      string    `gorm:"type:text" json:"model_map_raw"`
	HeadersRaw       string    `gorm:"type:text" json:"headers_raw"`
	LastLatencyMs    int       `gorm:"not null;default:0" json:"last_latency_ms"`
	LastStatus       string    `gorm:"size:32;not null;default:'untested'" json:"last_status"`
	LastErrorMessage string    `gorm:"type:text" json:"last_error_message"`
	ModelsSyncedRaw  string    `gorm:"type:text" json:"models_synced_raw"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`

	APIKey     string   `gorm:"-" json:"api_key,omitempty"`
	APIKeysRaw string   `gorm:"-" json:"api_keys_raw,omitempty"`
	APIKeys    []string `gorm:"-" json:"api_keys,omitempty"`
	// APIKeyLoadError is populated when an encrypted credential cannot be
	// decrypted. It is intentionally not serialized; callers must fail closed
	// instead of treating a key-management error as an empty-key channel.
	APIKeyLoadError string `gorm:"-" json:"-"`
}

type ChannelKeyModel struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	ChannelID string    `gorm:"size:64;not null;uniqueIndex:idx_channel_key_position;index" json:"channel_id"`
	Position  int       `gorm:"not null;uniqueIndex:idx_channel_key_position" json:"position"`
	Secret    string    `gorm:"type:text;not null" json:"-"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ModelMappingModel stores incoming-to-upstream model aliases separately from
// the channel record.  ModelMapRaw remains as a compatibility/cache field,
// while this table is the canonical v2 representation.
type ModelMappingModel struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	ChannelID   string    `gorm:"size:64;not null;uniqueIndex:idx_model_mapping_source,priority:1" json:"channel_id"`
	SourceModel string    `gorm:"size:255;not null;uniqueIndex:idx_model_mapping_source,priority:2" json:"source_model"`
	TargetModel string    `gorm:"size:255;not null" json:"target_model"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (ModelMappingModel) TableName() string { return "model_mappings" }

type SettingModel struct {
	Key       string    `gorm:"primaryKey;size:64" json:"key"`
	Value     string    `gorm:"type:text;not null" json:"value"`
	UpdatedAt time.Time `json:"updated_at"`
}

type AdminUserModel struct {
	ID             uint      `gorm:"primaryKey" json:"id"`
	Username       string    `gorm:"size:64;not null;uniqueIndex" json:"username"`
	PasswordHash   string    `gorm:"type:text;not null" json:"-"`
	SessionVersion uint64    `gorm:"not null;default:1" json:"-"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type AdminSessionModel struct {
	TokenHash      string    `gorm:"primaryKey;size:64" json:"-"`
	AdminUserID    uint      `gorm:"not null;index" json:"admin_user_id"`
	SessionVersion uint64    `gorm:"not null" json:"-"`
	CSRFHash       string    `gorm:"size:64;not null" json:"-"`
	ClientIP       string    `gorm:"size:128" json:"client_ip"`
	UserAgent      string    `gorm:"size:512" json:"user_agent"`
	ExpiresAt      time.Time `gorm:"not null;index" json:"expires_at"`
	LastSeenAt     time.Time `gorm:"not null" json:"last_seen_at"`
	CreatedAt      time.Time `json:"created_at"`
}

type GatewayTokenModel struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	TokenHash string    `gorm:"size:64;not null;uniqueIndex" json:"-"`
	Prefix    string    `gorm:"size:16;not null" json:"prefix"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TaskMapping pins an asynchronous task (or one of its provider aliases) to
// the channel that created it. TaskID is always the lookup key. TaskAlias is
// the canonical external ID shown to callers, which lets image jobs retain
// their legacy imgjob_ lookup namespace without exposing that prefix in the
// audit UI.
//
// OriginRequestID points at the audit row that created the task. It is empty
// for legacy mappings and deliberately nullable-by-convention so existing
// databases and callers remain compatible.
type TaskMapping struct {
	// Image lookup keys include ImageTaskMappingLookupPrefix before an otherwise
	// valid 128-byte provider ID, so the internal key needs 135 bytes. TaskAlias
	// remains the public provider ID and is intentionally limited to 128 bytes.
	TaskID          string    `gorm:"primaryKey;size:135" json:"task_id"`
	ChannelID       string    `gorm:"size:64;not null;index" json:"channel_id"`
	OriginRequestID string    `gorm:"size:64;index" json:"origin_request_id,omitempty"`
	TaskKind        string    `gorm:"size:32;index" json:"task_kind,omitempty"`
	TaskAlias       string    `gorm:"size:128;index" json:"task_alias,omitempty"`
	CreatedAt       time.Time `gorm:"not null;index" json:"created_at"`
}

// Keep the old exported name for callers that only need task-to-channel
// routing. The table name stays stable across the rename.
type VideoTaskMapping = TaskMapping

func (TaskMapping) TableName() string { return "video_task_mappings" }

type RequestLogModel struct {
	ID                   string     `gorm:"primaryKey;size:64" json:"id"`
	Kind                 string     `gorm:"size:32;not null;index:idx_request_logs_kind_started,priority:1" json:"kind"`
	StartedAt            time.Time  `gorm:"not null;index;index:idx_request_logs_kind_started,priority:2,sort:desc" json:"started_at"`
	FinishedAt           *time.Time `json:"finished_at,omitempty"`
	DurationMs           int64      `gorm:"not null;default:0" json:"duration_ms"`
	Method               string     `gorm:"size:16;not null" json:"method"`
	Path                 string     `gorm:"size:512;not null;index" json:"path"`
	ClientIP             string     `gorm:"size:128" json:"client_ip"`
	RequestedModel       string     `gorm:"size:255;index:idx_request_logs_model_started,priority:1" json:"requested_model"`
	TargetModel          string     `gorm:"size:255" json:"target_model"`
	ChannelID            string     `gorm:"size:64;index:idx_request_logs_channel_started,priority:1" json:"channel_id"`
	ChannelName          string     `gorm:"size:128" json:"channel_name"`
	ChannelType          string     `gorm:"size:32" json:"channel_type"`
	StatusCode           int        `gorm:"not null;default:0;index:idx_request_logs_status_started,priority:1" json:"status_code"`
	Outcome              string     `gorm:"size:32;not null;default:'running';index" json:"outcome"`
	IsStream             bool       `gorm:"not null;default:false" json:"is_stream"`
	InputTokens          int        `gorm:"not null;default:0" json:"input_tokens"`
	OutputTokens         int        `gorm:"not null;default:0" json:"output_tokens"`
	RetryCount           int        `gorm:"not null;default:0" json:"retry_count"`
	AsyncTaskKind        string     `gorm:"size:32;index" json:"async_task_kind,omitempty"`
	AsyncTaskID          string     `gorm:"size:128;index" json:"async_task_id,omitempty"`
	AsyncTaskStatus      string     `gorm:"size:64;index" json:"async_task_status,omitempty"`
	AsyncPollCount       int        `gorm:"not null;default:0" json:"async_poll_count"`
	AsyncLastPolledAt    *time.Time `json:"async_last_polled_at,omitempty"`
	AsyncCompletedAt     *time.Time `json:"async_completed_at,omitempty"`
	AsyncResultBody      string     `gorm:"type:text" json:"async_result_body,omitempty"`
	AsyncResultTruncated bool       `gorm:"not null;default:false" json:"async_result_truncated"`
	AsyncTaskError       string     `gorm:"type:text" json:"async_task_error,omitempty"`
	RequestHeaders       string     `gorm:"type:text" json:"request_headers,omitempty"`
	RequestBody          string     `gorm:"type:text" json:"request_body,omitempty"`
	ResponseBody         string     `gorm:"type:text" json:"response_body,omitempty"`
	StreamText           string     `gorm:"type:text" json:"stream_text,omitempty"`
	ErrorMessage         string     `gorm:"type:text" json:"error_message,omitempty"`
	RequestTruncated     bool       `gorm:"not null;default:false" json:"request_truncated"`
	ResponseTruncated    bool       `gorm:"not null;default:false" json:"response_truncated"`
	StreamTruncated      bool       `gorm:"not null;default:false" json:"stream_truncated"`
}

type RequestEventModel struct {
	ID         uint      `gorm:"primaryKey" json:"id"`
	RequestID  string    `gorm:"size:64;not null;uniqueIndex:idx_request_event_sequence;index" json:"request_id"`
	Sequence   int       `gorm:"not null;uniqueIndex:idx_request_event_sequence" json:"sequence"`
	OccurredAt time.Time `gorm:"not null" json:"occurred_at"`
	ElapsedMs  int64     `gorm:"not null;default:0" json:"elapsed_ms"`
	Phase      string    `gorm:"size:64;not null" json:"phase"`
	ChannelID  string    `gorm:"size:64" json:"channel_id,omitempty"`
	TargetURL  string    `gorm:"size:1024" json:"target_url,omitempty"`
	StatusCode int       `gorm:"not null;default:0" json:"status_code,omitempty"`
	Message    string    `gorm:"type:text" json:"message,omitempty"`
	Data       string    `gorm:"type:text" json:"data,omitempty"`
}

type videoTaskCacheEntry struct {
	mapping  TaskMapping
	cachedAt time.Time
}

// videoTaskCacheTTL bounds only the in-memory cache. Task mappings in SQLite
// are durable routing records: a completed video's stable gateway URL must
// keep resolving to the channel that created it after this cache entry ages
// out or the process restarts.
const videoTaskCacheTTL = 7 * 24 * time.Hour

func InitDB(dbPath string) (err error) {
	initMu.Lock()
	defer initMu.Unlock()
	if strings.TrimSpace(dbPath) == "" {
		dbPath = "gateway.db"
	}
	// Empty worker queues legitimately return gorm.ErrRecordNotFound on each
	// poll; keep those expected misses out of the runtime log while preserving
	// warnings and errors for actual database failures.
	opened, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{Logger: logger.New(log.New(os.Stdout, "", log.LstdFlags), logger.Config{
		LogLevel:                  logger.Warn,
		IgnoreRecordNotFoundError: true,
	})})
	if err != nil {
		return err
	}
	initialized := false
	defer func() {
		if initialized {
			return
		}
		if sqlDB, dbErr := opened.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	}()
	// Validate before applying write-affecting SQLite pragmas. An incompatible
	// database must be left completely untouched so it can be backed up or
	// migrated with a deliberate offline procedure.
	if err := validateSchema(opened); err != nil {
		return err
	}
	for _, pragma := range []string{"PRAGMA journal_mode = WAL", "PRAGMA busy_timeout = 5000", "PRAGMA synchronous = NORMAL", "PRAGMA foreign_keys = ON"} {
		if err := opened.Exec(pragma).Error; err != nil {
			return fmt.Errorf("apply SQLite setting %q: %w", pragma, err)
		}
	}
	if sqlDB, err := opened.DB(); err == nil {
		// SQLite still has one writer. Keep one connection until audit writes
		// are moved behind a durable queue; multiple connections otherwise turn
		// concurrent audit updates into SQLITE_BUSY races even with WAL enabled.
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)
		sqlDB.SetConnMaxLifetime(time.Hour)
	}
	models := schemaModels()
	if err := migrateSchema(opened, models); err != nil {
		return err
	}
	if err := opened.AutoMigrate(models...); err != nil {
		return err
	}
	if err := opened.Save(&SchemaMeta{Key: "schema_version", Value: SchemaVersion}).Error; err != nil {
		return err
	}
	// SQLite contains administrator/session metadata and may contain legacy
	// plaintext channel credentials. Restrict the file where the platform
	// supports POSIX permissions; Windows ACLs remain the host's responsibility.
	_ = os.Chmod(dbPath, 0o600)
	var retentionCount int64
	if err := opened.Model(&SettingModel{}).Where("key = ?", "audit_retention_days").Count(&retentionCount).Error; err != nil {
		return err
	}
	if retentionCount == 0 {
		if err := opened.Create(&SettingModel{Key: "audit_retention_days", Value: "30"}).Error; err != nil {
			return err
		}
	}
	var encKey SettingModel
	if err := opened.Where("key = ?", "db_encryption_key").First(&encKey).Error; err == nil && strings.TrimSpace(encKey.Value) != "" {
		sum := sha256.Sum256([]byte(encKey.Value))
		memorySecretKeyMu.Lock()
		memorySecretKey = sum[:]
		memorySecretKeyMu.Unlock()
	} else {
		memorySecretKeyMu.Lock()
		memorySecretKey = nil
		memorySecretKeyMu.Unlock()
	}
	if len(externalEncryptionKey()) > 0 {
		if err := MigrateLegacyDatabaseEncryption(opened); err != nil {
			return fmt.Errorf("migrate legacy database encryption: %w", err)
		}
	}
	var storedChannelKeys []ChannelKeyModel
	if err := opened.Where("secret <> ''").Find(&storedChannelKeys).Error; err != nil {
		return fmt.Errorf("validate stored channel credentials: %w", err)
	}
	for _, storedKey := range storedChannelKeys {
		if _, err := decryptChannelSecret(storedKey.Secret); err != nil {
			return fmt.Errorf("channel %q credential is unavailable; configure RELAY_DB_ENCRYPTION_KEY: %w", storedKey.ChannelID, err)
		}
	}
	now := time.Now()
	_ = opened.Model(&RequestLogModel{}).Where("outcome = ?", "running").Updates(map[string]any{"outcome": "interrupted", "finished_at": now, "error_message": "process stopped before request completed"}).Error
	_ = opened.Where("expires_at < ?", now).Delete(&AdminSessionModel{}).Error
	var legacyMediaLogIDs []string
	if err := opened.Model(&RequestLogModel{}).Where("path LIKE '/v1/media/%' OR path LIKE '%/content' OR path LIKE '%/content.mp4' OR path LIKE '%/video-content/%'").Pluck("id", &legacyMediaLogIDs).Error; err == nil && len(legacyMediaLogIDs) > 0 {
		_ = opened.Where("request_id IN ?", legacyMediaLogIDs).Delete(&RequestEventModel{}).Error
		_ = opened.Where("id IN ?", legacyMediaLogIDs).Delete(&RequestLogModel{}).Error
	}
	_ = opened.Exec("PRAGMA optimize").Error
	oldDB := DB
	DB = opened
	databasePath = dbPath
	clearVideoTaskCache()
	activeChannelsCache.Store(nil)
	if oldDB != nil {
		if oldSQLDB, oldErr := oldDB.DB(); oldErr == nil {
			_ = oldSQLDB.Close()
		}
	}
	RefreshActiveChannelsCache()
	initialized = true
	return nil
}

func schemaModels() []any {
	return []any{&SchemaMeta{}, &ChannelModel{}, &ChannelKeyModel{}, &ModelMappingModel{}, &SettingModel{}, &AdminUserModel{}, &AdminSessionModel{}, &GatewayTokenModel{}, &VideoTaskMapping{}, &RequestLogModel{}, &RequestEventModel{}, &ProtocolProfile{}, &ProtocolProfileRevision{}, &ChannelProtocolBinding{}, &TaskRun{}, &TaskAlias{}, &TaskAttempt{}, &TaskEvent{}, &MediaAsset{}, &MediaObject{}, &MediaMaterializationJob{}, &MediaDeletionJob{}}
}

// migrateSchema applies only explicitly supported upgrades. Each migration is
// idempotent: the marker advances only after all schema changes succeed, so a
// failed or interrupted upgrade can be retried safely on the next start.
func migrateSchema(conn *gorm.DB, models []any) error {
	if !conn.Migrator().HasTable(&SchemaMeta{}) {
		return nil
	}
	var meta SchemaMeta
	if err := conn.First(&meta, "key = ?", "schema_version").Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("%w: schema marker is missing; back up and migrate the database before starting this version", ErrIncompatibleSchema)
		}
		return fmt.Errorf("read schema marker: %w", err)
	}
	if meta.Value == SchemaVersion {
		return nil
	}
	if meta.Value != previousSchemaVersion {
		return fmt.Errorf("%w: found version %q, need %q; back up and migrate the database before starting this version", ErrIncompatibleSchema, meta.Value, SchemaVersion)
	}
	if err := conn.AutoMigrate(models...); err != nil {
		return fmt.Errorf("migrate schema %s to %s: %w", previousSchemaVersion, SchemaVersion, err)
	}
	if err := conn.Model(&SchemaMeta{}).Where("key = ?", "schema_version").Update("value", SchemaVersion).Error; err != nil {
		return fmt.Errorf("record schema version %s: %w", SchemaVersion, err)
	}
	return nil
}

func validateSchema(conn *gorm.DB) error {
	if !conn.Migrator().HasTable(&SchemaMeta{}) {
		var tableCount int64
		if err := conn.Raw("SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table', 'view', 'trigger') AND name NOT LIKE 'sqlite_%'").Scan(&tableCount).Error; err != nil {
			return fmt.Errorf("inspect database schema: %w", err)
		}
		if tableCount == 0 {
			return nil
		}
		return fmt.Errorf("%w: database has %d existing schema object(s) but no schema marker; back it up and migrate it before starting this version", ErrIncompatibleSchema, tableCount)
	}
	var meta SchemaMeta
	err := conn.First(&meta, "key = ?", "schema_version").Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return fmt.Errorf("%w: schema marker is missing; back up and migrate the database before starting this version", ErrIncompatibleSchema)
	}
	if err != nil {
		return fmt.Errorf("read schema marker: %w", err)
	}
	if meta.Value != SchemaVersion && meta.Value != previousSchemaVersion {
		return fmt.Errorf("%w: found version %q, need %q; back up and migrate the database before starting this version", ErrIncompatibleSchema, meta.Value, SchemaVersion)
	}
	return nil
}

func parseList(raw string) []string {
	var out []string
	seen := map[string]bool{}
	for _, item := range strings.FieldsFunc(strings.ReplaceAll(raw, ",", "\n"), func(r rune) bool { return r == '\n' || r == '\r' }) {
		item = strings.TrimSpace(item)
		if item != "" && !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}

const encryptedSecretPrefix = "enc:v1:"

func externalEncryptionKey() []byte {
	value := strings.TrimSpace(os.Getenv("RELAY_DB_ENCRYPTION_KEY"))
	if value != "" {
		sum := sha256.Sum256([]byte(value))
		return sum[:]
	}
	return nil
}

func legacySecretKey() []byte {
	memorySecretKeyMu.RLock()
	defer memorySecretKeyMu.RUnlock()
	if len(memorySecretKey) > 0 {
		key := make([]byte, len(memorySecretKey))
		copy(key, memorySecretKey)
		return key
	}
	return nil
}

// channelSecretKey derives a stable AEAD key from the operator-configured
// RELAY_DB_ENCRYPTION_KEY.
func channelSecretKey() []byte {
	return externalEncryptionKey()
}

func encryptWithKey(plain string, key []byte) (string, error) {
	if len(key) == 0 {
		return "", errors.New("encryption key is required")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := aead.Seal(nil, nonce, []byte(plain), nil)
	encoded := append(nonce, ciphertext...)
	return encryptedSecretPrefix + base64.RawStdEncoding.EncodeToString(encoded), nil
}

func decryptWithKey(stored string, key []byte) (string, error) {
	if !strings.HasPrefix(stored, encryptedSecretPrefix) {
		return stored, nil
	}
	if len(key) == 0 {
		return "", errors.New("encryption key is required")
	}
	data, err := base64.RawStdEncoding.DecodeString(strings.TrimPrefix(stored, encryptedSecretPrefix))
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil || len(data) < aead.NonceSize() {
		if err == nil {
			err = errors.New("invalid encrypted channel key")
		}
		return "", err
	}
	plain, err := aead.Open(nil, data[:aead.NonceSize()], data[aead.NonceSize():], nil)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func encryptChannelSecret(secret string) (string, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return "", nil
	}
	if strings.HasPrefix(secret, encryptedSecretPrefix) {
		return secret, nil
	}
	key := externalEncryptionKey()
	if len(key) == 0 {
		return "", ErrDBEncryptionKeyRequired
	}
	return encryptWithKey(secret, key)
}

func decryptChannelSecret(stored string) (string, error) {
	if !strings.HasPrefix(stored, encryptedSecretPrefix) {
		return stored, nil
	}
	key := externalEncryptionKey()
	if len(key) > 0 {
		if plain, err := decryptWithKey(stored, key); err == nil {
			return plain, nil
		}
	}
	legacyKey := legacySecretKey()
	if len(legacyKey) > 0 {
		if plain, err := decryptWithKey(stored, legacyKey); err == nil {
			return plain, nil
		}
	}
	if len(key) == 0 && len(legacyKey) == 0 {
		return "", errors.New("encrypted channel key requires RELAY_DB_ENCRYPTION_KEY")
	}
	return "", errors.New("cannot decrypt channel key with configured encryption key")
}

// MigrateLegacyDatabaseEncryption re-encrypts historical channel credentials
// and recoverable media capabilities using the external RELAY_DB_ENCRYPTION_KEY.
// When all credentials and capabilities are successfully re-encrypted and verified,
// the legacy db_encryption_key is removed from the settings table. If any step fails,
// the entire transaction is rolled back, leaving legacy data and settings intact.
func MigrateLegacyDatabaseEncryption(conn *gorm.DB) error {
	if conn == nil {
		return errors.New("database is not initialized")
	}
	if !conn.Migrator().HasTable(&SettingModel{}) {
		return nil
	}
	extKey := externalEncryptionKey()
	if len(extKey) == 0 {
		return ErrDBEncryptionKeyRequired
	}

	var encSetting SettingModel
	hasLegacyKey := conn.Where("key = ?", "db_encryption_key").First(&encSetting).Error == nil && strings.TrimSpace(encSetting.Value) != ""

	var unencryptedCount int64
	_ = conn.Model(&ChannelKeyModel{}).Where("secret <> '' AND secret NOT LIKE 'enc:v1:%'").Count(&unencryptedCount).Error

	if !hasLegacyKey && unencryptedCount == 0 {
		return nil
	}

	var oldKey []byte
	if hasLegacyKey {
		sum := sha256.Sum256([]byte(encSetting.Value))
		oldKey = sum[:]
	}

	return conn.Transaction(func(tx *gorm.DB) error {
		// 1. Re-encrypt all channel keys
		var channelKeys []ChannelKeyModel
		if err := tx.Find(&channelKeys).Error; err != nil {
			return fmt.Errorf("find channel keys: %w", err)
		}
		for _, ck := range channelKeys {
			secret := strings.TrimSpace(ck.Secret)
			if secret == "" {
				continue
			}
			var plain string
			if strings.HasPrefix(secret, encryptedSecretPrefix) {
				decrypted := false
				if len(oldKey) > 0 {
					if p, err := decryptWithKey(secret, oldKey); err == nil {
						plain = p
						decrypted = true
					}
				}
				if !decrypted {
					if p, err := decryptWithKey(secret, extKey); err == nil {
						plain = p
						decrypted = true
					}
				}
				if !decrypted {
					return fmt.Errorf("channel key %d for channel %q cannot be decrypted with legacy or external key", ck.ID, ck.ChannelID)
				}
			} else {
				plain = secret
			}

			newCipher, err := encryptWithKey(plain, extKey)
			if err != nil {
				return fmt.Errorf("re-encrypt channel key %d: %w", ck.ID, err)
			}
			verified, err := decryptWithKey(newCipher, extKey)
			if err != nil || verified != plain {
				return fmt.Errorf("verify re-encrypted channel key %d failed", ck.ID)
			}
			if err := tx.Model(&ChannelKeyModel{}).Where("id = ?", ck.ID).Update("secret", newCipher).Error; err != nil {
				return fmt.Errorf("update channel key %d: %w", ck.ID, err)
			}
		}

		// 2. Re-encrypt recoverable MediaAsset capability ciphertexts
		if tx.Migrator().HasTable(&MediaAsset{}) {
			var assets []MediaAsset
			if err := tx.Where("capability_ciphertext <> ''").Find(&assets).Error; err != nil {
				return fmt.Errorf("find media assets for encryption migration: %w", err)
			}
			for _, asset := range assets {
				cipherText := strings.TrimSpace(asset.CapabilityCiphertext)
				if cipherText == "" {
					continue
				}
				var plainCap string
				decrypted := false
				if len(oldKey) > 0 {
					if p, err := decryptWithKey(cipherText, oldKey); err == nil {
						plainCap = p
						decrypted = true
					}
				}
				if !decrypted {
					if p, err := decryptWithKey(cipherText, extKey); err == nil {
						plainCap = p
						decrypted = true
					}
				}
				if !decrypted {
					return fmt.Errorf("media asset %d capability ciphertext cannot be decrypted with legacy or external key", asset.ID)
				}
				newCipher, err := encryptWithKey(plainCap, extKey)
				if err != nil {
					return fmt.Errorf("re-encrypt media asset %d capability: %w", asset.ID, err)
				}
				verified, err := decryptWithKey(newCipher, extKey)
				if err != nil || verified != plainCap {
					return fmt.Errorf("verify re-encrypted media asset %d capability failed", asset.ID)
				}
				if err := tx.Model(&MediaAsset{}).Where("id = ?", asset.ID).Update("capability_ciphertext", newCipher).Error; err != nil {
					return fmt.Errorf("update media asset %d capability: %w", asset.ID, err)
				}
			}
		}

		// 3. Delete old db_encryption_key from settings
		if hasLegacyKey {
			if err := tx.Where("key = ?", "db_encryption_key").Delete(&SettingModel{}).Error; err != nil {
				return fmt.Errorf("delete legacy db_encryption_key setting: %w", err)
			}
		}

		// Clear memorySecretKey since old key in settings is removed
		memorySecretKeyMu.Lock()
		memorySecretKey = nil
		memorySecretKeyMu.Unlock()

		return nil
	})
}

func hydrateChannelKeys(cm *ChannelModel) {
	if DB == nil || cm == nil || cm.ID == "" {
		return
	}
	var keys []ChannelKeyModel
	if DB.Where("channel_id = ?", cm.ID).Order("position asc").Find(&keys).Error != nil {
		return
	}
	cm.APIKeys = make([]string, 0, len(keys))
	cm.APIKeyLoadError = ""
	for _, item := range keys {
		if secret, err := decryptChannelSecret(item.Secret); err == nil {
			cm.APIKeys = append(cm.APIKeys, secret)
		} else if cm.APIKeyLoadError == "" {
			cm.APIKeyLoadError = fmt.Sprintf("cannot decrypt channel credential: %v", err)
		}
	}
	cm.APIKeysRaw = strings.Join(cm.APIKeys, "\n")
	if len(cm.APIKeys) > 0 {
		cm.APIKey = cm.APIKeys[0]
	}
	var mappings []ModelMappingModel
	if DB.Where("channel_id = ?", cm.ID).Order("source_model asc").Find(&mappings).Error == nil && len(mappings) > 0 {
		modelMap := make(map[string]string, len(mappings))
		for _, mapping := range mappings {
			if strings.TrimSpace(mapping.SourceModel) != "" && strings.TrimSpace(mapping.TargetModel) != "" {
				modelMap[mapping.SourceModel] = mapping.TargetModel
			}
		}
		if encoded, err := json.Marshal(modelMap); err == nil {
			cm.ModelMapRaw = string(encoded)
		}
	}
}

func (m *ChannelModel) ToUpstreamChannel() config.UpstreamChannel {
	if len(m.APIKeys) == 0 {
		hydrateChannelKeys(m)
	}
	modelMap := make(map[string]string)
	if m.ModelMapRaw != "" {
		_ = json.Unmarshal([]byte(m.ModelMapRaw), &modelMap)
	}
	headers := make(map[string]string)
	if m.HeadersRaw != "" {
		_ = json.Unmarshal([]byte(m.HeadersRaw), &headers)
	}
	priority, weight := m.Priority, m.Weight
	if priority <= 0 {
		priority = 1
	}
	if weight <= 0 {
		weight = 1
	}
	return config.UpstreamChannel{ID: m.ID, Type: m.Type, BaseURL: m.BaseURL, APIKey: m.APIKey, APIKeys: append([]string(nil), m.APIKeys...), Enabled: m.Enabled, Priority: priority, Weight: weight, FetchModels: m.FetchModels, AnthropicVersion: m.AnthropicVersion, Headers: headers, Models: parseList(m.ModelsRaw), ModelMap: modelMap}
}

func RefreshActiveChannelsCache() {
	if DB == nil {
		return
	}
	var cms []ChannelModel
	if err := DB.Where("enabled = ?", true).Order("priority asc, weight desc, created_at asc").Find(&cms).Error; err != nil {
		return
	}
	res := make([]config.UpstreamChannel, 0, len(cms))
	for i := range cms {
		hydrateChannelKeys(&cms[i])
		if cms[i].APIKeyLoadError != "" {
			continue
		}
		res = append(res, cms[i].ToUpstreamChannel())
	}
	activeChannelsCache.Store(&res)
}

// NotifyChannelsChanged refreshes in-memory routing state after a committed
// channel transaction. It must not be called while that transaction is open.
func NotifyChannelsChanged(ids ...string) {
	RefreshActiveChannelsCache()
	if OnChannelSaved == nil {
		return
	}
	for _, id := range ids {
		if strings.TrimSpace(id) != "" {
			OnChannelSaved(id)
		}
	}
}

func GetActiveUpstreamChannels() []config.UpstreamChannel {
	if DB == nil {
		return config.Global.Channels
	}
	if ptr := activeChannelsCache.Load(); ptr != nil {
		return *ptr
	}
	RefreshActiveChannelsCache()
	if ptr := activeChannelsCache.Load(); ptr != nil {
		return *ptr
	}
	return nil
}

func GetAllChannelModels() ([]ChannelModel, error) {
	if DB == nil {
		return nil, nil
	}
	var cms []ChannelModel
	err := DB.Order("priority asc, created_at asc").Find(&cms).Error
	if err == nil {
		for i := range cms {
			hydrateChannelKeys(&cms[i])
		}
	}
	return cms, err
}

func GetChannelModel(id string) (*ChannelModel, error) {
	return GetChannelModelContext(context.Background(), id)
}

func GetChannelModelContext(ctx context.Context, id string) (*ChannelModel, error) {
	conn := DBForContext(ctx)
	if conn == nil {
		return nil, gorm.ErrRecordNotFound
	}
	var cm ChannelModel
	if err := conn.First(&cm, "id = ?", id).Error; err != nil {
		return nil, err
	}
	// Hydrate against the same connection while a transaction is open.
	if conn == DB {
		hydrateChannelKeys(&cm)
		if cm.APIKeyLoadError != "" {
			return nil, errors.New(cm.APIKeyLoadError)
		}
	} else {
		var keys []ChannelKeyModel
		if err := conn.Where("channel_id = ?", cm.ID).Order("position asc").Find(&keys).Error; err == nil {
			for _, item := range keys {
				if secret, decryptErr := decryptChannelSecret(item.Secret); decryptErr == nil {
					cm.APIKeys = append(cm.APIKeys, secret)
				} else if cm.APIKeyLoadError == "" {
					cm.APIKeyLoadError = fmt.Sprintf("cannot decrypt channel credential: %v", decryptErr)
				}
			}
			cm.APIKeysRaw = strings.Join(cm.APIKeys, "\n")
			if len(cm.APIKeys) > 0 {
				cm.APIKey = cm.APIKeys[0]
			}
		}
		if cm.APIKeyLoadError != "" {
			return nil, errors.New(cm.APIKeyLoadError)
		}
	}
	return &cm, nil
}

func ToggleChannel(id string) (bool, error) {
	return ToggleChannelContext(context.Background(), id)
}

// ToggleChannelContext updates a channel using the request transaction when
// one is present. The cache refresh is intentionally deferred until commit by
// the caller (the in-memory cache is not part of the SQLite transaction).
func ToggleChannelContext(ctx context.Context, id string) (bool, error) {
	cm, err := GetChannelModelContext(ctx, id)
	if err != nil {
		return false, err
	}
	cm.Enabled = !cm.Enabled
	conn := DBForContext(ctx)
	err = conn.Model(&ChannelModel{}).Where("id = ?", id).Update("enabled", cm.Enabled).Error
	if err == nil {
		invalidateVideoTaskCacheForChannel(id)
		if conn == DB {
			RefreshActiveChannelsCache()
			if OnChannelSaved != nil {
				OnChannelSaved(id)
			}
		}
	}
	return cm.Enabled, err
}

func DeleteChannel(id string) error {
	return DeleteChannelContext(context.Background(), id)
}

func DeleteChannelContext(ctx context.Context, id string) error {
	conn := DBForContext(ctx)
	if conn == nil {
		return nil
	}
	deleteFn := func(tx *gorm.DB) error {
		if err := tx.Where("channel_id = ?", id).Delete(&ChannelKeyModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("channel_id = ?", id).Delete(&ModelMappingModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("channel_id = ?", id).Delete(&VideoTaskMapping{}).Error; err != nil {
			return err
		}
		return tx.Delete(&ChannelModel{}, "id = ?", id).Error
	}
	var err error
	if conn == DB {
		err = conn.Transaction(deleteFn)
	} else {
		err = deleteFn(conn)
	}
	if err == nil {
		// The request audit middleware may be wrapping this operation in an
		// outer transaction. In that case it invalidates the cache only after
		// commit; invalidating here would make a rollback observable.
		if conn == DB {
			invalidateVideoTaskCacheForChannel(id)
			RefreshActiveChannelsCache()
		}
	}
	return err
}

func SaveChannelModel(cm *ChannelModel) error {
	return SaveChannelModelContext(context.Background(), cm)
}

func SaveChannelModelContext(ctx context.Context, cm *ChannelModel) error {
	conn := DBForContext(ctx)
	if conn == nil {
		return errors.New("database is not initialized")
	}
	if cm == nil {
		return errors.New("channel is required")
	}
	cm.ID = strings.TrimSpace(cm.ID)
	cm.Name = strings.TrimSpace(cm.Name)
	cm.Type = strings.ToLower(strings.TrimSpace(cm.Type))
	cm.BaseURL = strings.TrimSpace(cm.BaseURL)
	if cm.ID == "" || cm.BaseURL == "" {
		return errors.New("channel id and base URL are required")
	}
	// The DB package deliberately does not import adapter (adapter implementations
	// depend on audit/db), so keep persistence validation decoupled from the
	// runtime registry. HTTP/configuration paths validate against the registry;
	// this guard remains a final storage boundary for known protocol types.
	allowed := map[string]bool{"openai": true, "anthropic": true, "newapi": true, "sub2api": true}
	if !allowed[cm.Type] {
		return fmt.Errorf("unsupported adapter type %q", cm.Type)
	}
	if cm.Name == "" {
		cm.Name = cm.ID
	}
	if cm.Priority <= 0 {
		cm.Priority = 1
	}
	if cm.Weight <= 0 {
		cm.Weight = 1
	}
	if cm.AnthropicVersion == "" {
		cm.AnthropicVersion = "2023-06-01"
	}
	// This value is discovered from the current upstream, never user input.
	// Clearing it on every save prevents an edited channel from routing against
	// capabilities learned from a previous URL, type, or credential set.
	cm.ModelsSyncedRaw = ""
	keys := append([]string(nil), cm.APIKeys...)
	if len(keys) == 0 {
		keys = parseList(cm.APIKeysRaw)
	}
	if len(keys) == 0 && strings.TrimSpace(cm.APIKey) != "" {
		keys = []string{strings.TrimSpace(cm.APIKey)}
	}
	for _, secret := range keys {
		if strings.ContainsAny(secret, "\r\n") {
			return errors.New("invalid channel key")
		}
	}
	hasSecrets := false
	for _, secret := range keys {
		if strings.TrimSpace(secret) != "" {
			hasSecrets = true
			break
		}
	}
	if hasSecrets && len(externalEncryptionKey()) == 0 {
		return ErrDBEncryptionKeyRequired
	}
	if rawHeaders := strings.TrimSpace(cm.HeadersRaw); rawHeaders != "" {
		var headers map[string]string
		if err := json.Unmarshal([]byte(rawHeaders), &headers); err != nil || headers == nil {
			return errors.New("invalid headers JSON object")
		}
		for name, value := range headers {
			if strings.TrimSpace(name) == "" || strings.ContainsAny(name+value, "\r\n") {
				return errors.New("invalid header name or value")
			}
		}
	}
	saveFn := func(tx *gorm.DB) error {
		var existing ChannelModel
		found := tx.First(&existing, "id = ?", cm.ID).Error == nil
		if found {
			cm.CreatedAt = existing.CreatedAt
			if cm.LastStatus == "" {
				cm.LastStatus = existing.LastStatus
			}
			if cm.LastLatencyMs == 0 {
				cm.LastLatencyMs = existing.LastLatencyMs
			}
			if cm.LastErrorMessage == "" {
				cm.LastErrorMessage = existing.LastErrorMessage
			}
			if err := tx.Save(cm).Error; err != nil {
				return err
			}
		} else if err := tx.Create(cm).Error; err != nil {
			return err
		}
		if err := tx.Where("channel_id = ?", cm.ID).Delete(&ChannelKeyModel{}).Error; err != nil {
			return err
		}
		for i, secret := range keys {
			storedSecret, encErr := encryptChannelSecret(strings.TrimSpace(secret))
			if encErr != nil {
				return fmt.Errorf("encrypt channel key: %w", encErr)
			}
			if err := tx.Create(&ChannelKeyModel{ChannelID: cm.ID, Position: i, Secret: storedSecret}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("channel_id = ?", cm.ID).Delete(&ModelMappingModel{}).Error; err != nil {
			return err
		}
		modelMap := make(map[string]string)
		if strings.TrimSpace(cm.ModelMapRaw) != "" {
			if err := json.Unmarshal([]byte(cm.ModelMapRaw), &modelMap); err != nil {
				return fmt.Errorf("invalid model map: %w", err)
			}
		}
		for source, target := range modelMap {
			source, target = strings.TrimSpace(source), strings.TrimSpace(target)
			if source == "" || target == "" {
				return errors.New("model mapping source and target cannot be empty")
			}
			if err := tx.Create(&ModelMappingModel{ChannelID: cm.ID, SourceModel: source, TargetModel: target}).Error; err != nil {
				return err
			}
		}
		return nil
	}
	var err error
	if conn == DB {
		err = conn.Transaction(saveFn)
	} else {
		err = saveFn(conn)
	}
	if err == nil {
		if conn == DB {
			RefreshActiveChannelsCache()
			if OnChannelSaved != nil {
				OnChannelSaved(cm.ID)
			}
		}
	}
	return err
}

func UpdateChannelHealth(id, status string, latencyMs int, errMsg string, syncedModels []string) {
	_ = UpdateChannelHealthContext(context.Background(), id, status, latencyMs, errMsg, syncedModels)
}

// UpdateChannelHealthContext persists the result of a channel probe using the
// transaction carried by ctx when one is present. Probe callers deliberately
// perform the upstream request before opening a transaction; this keeps a
// network timeout from holding SQLite's single writer connection while still
// allowing the health row and its audit event to commit atomically.
func UpdateChannelHealthContext(ctx context.Context, id, status string, latencyMs int, errMsg string, syncedModels []string) error {
	conn := DBForContext(ctx)
	if conn == nil {
		return errors.New("database is not initialized")
	}
	updates := map[string]any{"last_status": status, "last_latency_ms": latencyMs, "last_error_message": errMsg, "updated_at": time.Now()}
	if syncedModels != nil {
		b, _ := json.Marshal(syncedModels)
		updates["models_synced_raw"] = string(b)
	}
	return conn.Model(&ChannelModel{}).Where("id = ?", id).Updates(updates).Error
}

func GetSetting(key, defaultVal string) string {
	if DB == nil {
		return defaultVal
	}
	var s SettingModel
	if err := DB.First(&s, "key = ?", key).Error; err != nil {
		return defaultVal
	}
	return s.Value
}

func SetSetting(key, value string) error {
	return SetSettingContext(context.Background(), key, value)
}

func SetSettingContext(ctx context.Context, key, value string) error {
	conn := DBForContext(ctx)
	if conn == nil {
		return errors.New("database is not initialized")
	}
	return conn.Save(&SettingModel{Key: key, Value: value}).Error
}

func HasSetting(key string) bool {
	if DB == nil {
		return false
	}
	var count int64
	_ = DB.Model(&SettingModel{}).Where("key = ?", key).Count(&count).Error
	return count > 0
}

// RecordTaskMapping persists a complete asynchronous task mapping outside a
// request transaction. Most request handlers should use
// RecordTaskMappingContext so a task mapping and its creation audit remain
// atomic.
func RecordTaskMapping(mapping TaskMapping) error {
	return RecordTaskMappingContext(context.Background(), mapping)
}

// RecordTaskMappingContext persists an asynchronous task mapping using the
// transaction carried by ctx when present. The in-memory cache is published
// only for non-transactional callers; transactional callers must publish it
// after their transaction commits so a later rollback cannot leave a stale
// routing entry in memory.
//
// TaskID is the routing lookup key. When a provider returns multiple aliases,
// write one mapping per lookup key and use the same TaskAlias for each row.
func RecordTaskMappingContext(ctx context.Context, mapping TaskMapping) error {
	mapping.TaskID = strings.TrimSpace(mapping.TaskID)
	mapping.ChannelID = strings.TrimSpace(mapping.ChannelID)
	mapping.OriginRequestID = strings.TrimSpace(mapping.OriginRequestID)
	mapping.TaskKind = strings.ToLower(strings.TrimSpace(mapping.TaskKind))
	mapping.TaskAlias = strings.TrimSpace(mapping.TaskAlias)
	if mapping.TaskID == "" || mapping.ChannelID == "" {
		return errors.New("task id and channel id are required")
	}
	if !IsValidTaskMappingLookupID(mapping.TaskID, mapping.TaskKind) {
		return errors.New("invalid task id")
	}
	if mapping.TaskAlias == "" {
		mapping.TaskAlias = defaultTaskMappingAlias(mapping.TaskID, mapping.TaskKind)
	}
	if !isValidTaskMappingID(mapping.TaskAlias) {
		return errors.New("invalid task alias")
	}
	if mapping.CreatedAt.IsZero() {
		mapping.CreatedAt = time.Now().UTC()
	}
	conn := DBForContext(ctx)
	if conn == nil {
		return errors.New("database is not initialized")
	}
	if err := conn.Save(&mapping).Error; err != nil {
		return err
	}
	if conn == DB {
		PublishTaskMappingCache(mapping)
	}
	return nil
}

// EnsureTaskMappings records asynchronous task lookup IDs without ever
// repointing an existing ID.  It is intentionally stricter than the legacy
// RecordTaskMappingContext helper: retries after an upstream task has already
// been created must be idempotent, while a cross-channel collision must remain
// visible for manual investigation instead of silently changing routing.
//
// The function owns a short transaction and publishes only rows that it
// inserted after that transaction commits. Existing mappings for the same
// channel and task kind are treated as successful no-ops and retain their
// original audit provenance and creation timestamp. A task key is the table's
// primary key, so a same-channel mapping of a different kind is a conflict:
// silently accepting it would leave the requested kind without a route.
func EnsureTaskMappings(mappings ...TaskMapping) error {
	return EnsureTaskMappingsContext(context.Background(), mappings...)
}

// EnsureTaskMappingsContext is the context-aware form of EnsureTaskMappings.
// Request handlers use a short deadline after an upstream task is accepted so
// a busy SQLite database cannot turn a successful create into a client timeout.
func EnsureTaskMappingsContext(ctx context.Context, mappings ...TaskMapping) error {
	if DB == nil {
		return errors.New("database is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	unique := make([]TaskMapping, 0, len(mappings))
	byTaskID := make(map[string]int, len(mappings))
	for _, mapping := range mappings {
		mapping.TaskID = strings.TrimSpace(mapping.TaskID)
		mapping.ChannelID = strings.TrimSpace(mapping.ChannelID)
		mapping.OriginRequestID = strings.TrimSpace(mapping.OriginRequestID)
		mapping.TaskKind = strings.ToLower(strings.TrimSpace(mapping.TaskKind))
		mapping.TaskAlias = strings.TrimSpace(mapping.TaskAlias)
		if mapping.TaskID == "" || mapping.ChannelID == "" {
			return errors.New("task id and channel id are required")
		}
		if !IsValidTaskMappingLookupID(mapping.TaskID, mapping.TaskKind) {
			return errors.New("invalid task id")
		}
		if !isValidTaskMappingID(mapping.TaskAlias) {
			mapping.TaskAlias = defaultTaskMappingAlias(mapping.TaskID, mapping.TaskKind)
		}
		if !isValidTaskMappingID(mapping.TaskAlias) {
			return errors.New("invalid task alias")
		}
		if mapping.CreatedAt.IsZero() {
			mapping.CreatedAt = time.Now().UTC()
		}
		if index, ok := byTaskID[mapping.TaskID]; ok {
			if unique[index].ChannelID != mapping.ChannelID || effectiveTaskMappingKind(unique[index].TaskKind) != effectiveTaskMappingKind(mapping.TaskKind) {
				return fmt.Errorf("%w: task %s", ErrTaskMappingChannelConflict, mapping.TaskID)
			}
			continue
		}
		byTaskID[mapping.TaskID] = len(unique)
		unique = append(unique, mapping)
	}
	if len(unique) == 0 {
		return nil
	}

	inserted := make([]TaskMapping, 0, len(unique))
	if err := DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, mapping := range unique {
			var existing TaskMapping
			result := tx.Where("task_id = ?", mapping.TaskID).Limit(1).Find(&existing)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected > 0 {
				if existing.ChannelID != mapping.ChannelID || effectiveTaskMappingKind(existing.TaskKind) != effectiveTaskMappingKind(mapping.TaskKind) {
					return fmt.Errorf("%w: task %s", ErrTaskMappingChannelConflict, mapping.TaskID)
				}
				if mapping.TaskAlias != "" && mapping.TaskAlias != existing.TaskAlias && (existing.TaskAlias == "" || existing.TaskAlias == existing.TaskID) {
					if err := tx.Model(&TaskMapping{}).Where("task_id = ?", existing.TaskID).Update("task_alias", mapping.TaskAlias).Error; err != nil {
						return err
					}
					existing.TaskAlias = mapping.TaskAlias
					inserted = append(inserted, existing)
				}
				continue
			}
			if err := tx.Create(&mapping).Error; err != nil {
				return err
			}
			inserted = append(inserted, mapping)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, mapping := range inserted {
		PublishTaskMappingCache(mapping)
	}
	return nil
}

func isValidTaskMappingID(id string) bool {
	if len(id) == 0 || len(id) > taskMappingExternalIDMaxBytes {
		return false
	}
	if strings.Contains(id, "..") || strings.ContainsAny(id, "/\\?#& \r\n\t") {
		return false
	}
	for _, r := range id {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

// IsValidTaskID is the shared public task-ID validator used by HTTP routes and
// persistence code. Keeping one implementation avoids subtle drift in path
// traversal and length checks.
func IsValidTaskID(id string) bool { return isValidTaskMappingID(id) }

// IsValidTaskMappingLookupID validates an internal task-mapping lookup key.
// Public IDs and aliases remain bounded by 128 bytes. The only larger key is
// an image mapping in the legacy imgjob_ namespace, whose suffix must still be
// a valid public task ID. Do not use this for route parameters or response IDs.
func IsValidTaskMappingLookupID(id, taskKind string) bool {
	id = strings.TrimSpace(id)
	taskKind = strings.ToLower(strings.TrimSpace(taskKind))
	if taskKind == "image" && strings.HasPrefix(id, ImageTaskMappingLookupPrefix) {
		return isValidTaskMappingID(strings.TrimPrefix(id, ImageTaskMappingLookupPrefix))
	}
	return isValidTaskMappingID(id)
}

func defaultTaskMappingAlias(taskID, taskKind string) string {
	taskID = strings.TrimSpace(taskID)
	if strings.EqualFold(strings.TrimSpace(taskKind), "image") && strings.HasPrefix(taskID, ImageTaskMappingLookupPrefix) {
		return strings.TrimPrefix(taskID, ImageTaskMappingLookupPrefix)
	}
	return taskID
}

// RecordVideoTask keeps the old task-to-channel API available to callers that
// do not need audit provenance. New asynchronous routes should use
// RecordTaskMappingContext instead.
// Deprecated: use RecordTaskMappingContext.
func RecordVideoTask(taskID, channelID string) {
	_ = RecordVideoTaskContext(context.Background(), taskID, channelID)
}

func RecordVideoTaskContext(ctx context.Context, taskID, channelID string) error {
	return RecordTaskMappingContext(ctx, TaskMapping{TaskID: taskID, ChannelID: channelID, TaskKind: "video"})
}

// PublishTaskMappingCache makes a complete mapping visible to the fast lookup
// path after the caller has successfully committed its database transaction.
func PublishTaskMappingCache(mapping TaskMapping) {
	mapping.TaskID = strings.TrimSpace(mapping.TaskID)
	mapping.ChannelID = strings.TrimSpace(mapping.ChannelID)
	if mapping.TaskID == "" || mapping.ChannelID == "" {
		return
	}
	if mapping.CreatedAt.IsZero() {
		mapping.CreatedAt = time.Now().UTC()
	}
	videoTaskCache.Store(mapping.TaskID, videoTaskCacheEntry{mapping: mapping, cachedAt: time.Now().UTC()})
}

// PublishVideoTaskCache is the compatibility wrapper for callers that only
// know task and channel IDs.
// Deprecated: use PublishTaskMappingCache.
func PublishVideoTaskCache(taskID, channelID string, createdAt ...time.Time) {
	mapping := TaskMapping{TaskID: taskID, ChannelID: channelID, TaskKind: "video"}
	if len(createdAt) > 0 {
		mapping.CreatedAt = createdAt[0]
	}
	PublishTaskMappingCache(mapping)
}

func clearVideoTaskCache() {
	videoTaskCache.Range(func(key, _ any) bool {
		videoTaskCache.Delete(key)
		return true
	})
}

func invalidateVideoTaskCacheForChannel(channelID string) {
	if strings.TrimSpace(channelID) == "" {
		return
	}
	videoTaskCache.Range(func(key, value any) bool {
		if entry, ok := value.(videoTaskCacheEntry); ok && entry.mapping.ChannelID == channelID {
			videoTaskCache.Delete(key)
		}
		return true
	})
}

// InvalidateTaskCacheForChannel is called by transactional callers after a
// channel deletion has committed. It is exported to keep the cache lifecycle
// explicit without exposing the sync.Map itself.
func InvalidateTaskCacheForChannel(channelID string) {
	invalidateVideoTaskCacheForChannel(channelID)
}

// ClearTaskMappingCache drops all in-memory task mappings, forcing subsequent
// lookups to reload from the database.
func ClearTaskMappingCache() {
	clearVideoTaskCache()
}

// GetTaskMapping resolves a complete asynchronous task mapping. It returns
// nil for unknown mappings, so callers can safely preserve their normal
// standalone audit path. Cache expiry never makes a durable mapping invalid;
// it simply causes a reload from SQLite. For backward compatibility, a raw
// image-job ID also falls back to the legacy imgjob_ lookup namespace.
//
// This compatibility lookup is intentionally type-agnostic. Callers that
// authorize or route a known task kind must use GetTaskMappingForKind instead,
// so an image mapping cannot be reused as a video mapping (or vice versa).
func GetTaskMapping(taskID string) *TaskMapping {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil
	}
	if mapping := getTaskMappingExact(taskID); mapping != nil {
		return mapping
	}
	if !strings.HasPrefix(taskID, ImageTaskMappingLookupPrefix) {
		legacyImageTaskID := ImageTaskMappingLookupPrefix + taskID
		return getTaskMappingExact(legacyImageTaskID)
	}
	return nil
}

// GetTaskMappingForKind resolves a mapping only when its task kind matches the
// requested route. Image lookups retain the historical raw-ID -> imgjob_ raw-ID
// fallback, while video lookups never use that namespace. This keeps old image
// job URLs working without allowing an image mapping to authorize or route a
// video request.
//
// Mappings created before task kinds were introduced are legacy video mappings:
// a blank TaskKind therefore matches video only. New image mappings must carry
// TaskKind == "image".
func GetTaskMappingForKind(taskID, taskKind string) *TaskMapping {
	taskID = strings.TrimSpace(taskID)
	taskKind = strings.ToLower(strings.TrimSpace(taskKind))
	if taskID == "" || taskKind == "" {
		return nil
	}

	if mapping := getTaskMappingExact(taskID); taskMappingMatchesKind(mapping, taskKind) {
		return mapping
	}
	if taskKind != "image" || strings.HasPrefix(taskID, ImageTaskMappingLookupPrefix) {
		return nil
	}

	legacyImageTaskID := ImageTaskMappingLookupPrefix + taskID
	if mapping := getTaskMappingExact(legacyImageTaskID); taskMappingMatchesKind(mapping, taskKind) {
		return mapping
	}
	return nil
}

func taskMappingMatchesKind(mapping *TaskMapping, taskKind string) bool {
	if mapping == nil {
		return false
	}
	taskKind = strings.ToLower(strings.TrimSpace(taskKind))
	return effectiveTaskMappingKind(mapping.TaskKind) == taskKind
}

// effectiveTaskMappingKind maps pre-TaskKind rows to video. Those legacy rows
// were created exclusively by the video routes, so treating a blank kind as
// video preserves their compatibility while preventing a new image mapping
// from silently sharing their primary-key task ID.
func effectiveTaskMappingKind(taskKind string) string {
	taskKind = strings.ToLower(strings.TrimSpace(taskKind))
	if taskKind == "" {
		return "video"
	}
	return taskKind
}

func getTaskMappingExact(taskID string) *TaskMapping {
	if mapping := getCachedTaskMapping(taskID); mapping != nil {
		return mapping
	}
	return loadTaskMapping(taskID)
}

func getCachedTaskMapping(taskID string) *TaskMapping {
	value, ok := videoTaskCache.Load(taskID)
	if !ok {
		return nil
	}
	entry, ok := value.(videoTaskCacheEntry)
	if !ok || entry.cachedAt.IsZero() || time.Since(entry.cachedAt) > videoTaskCacheTTL {
		videoTaskCache.Delete(taskID)
		return nil
	}
	mapping := entry.mapping
	return &mapping
}

func loadTaskMapping(taskID string) *TaskMapping {
	if DB == nil {
		return nil
	}
	var mapping TaskMapping
	if err := DB.First(&mapping, "task_id = ?", taskID).Error; err != nil {
		return nil
	}
	PublishTaskMappingCache(mapping)
	return &mapping
}

func GetVideoTaskChannel(taskID string) string {
	return GetTaskChannelForKind(taskID, "video")
}

// GetImageTaskChannel resolves a task only through image mappings, including
// the legacy imgjob_ lookup namespace for raw image-job IDs.
func GetImageTaskChannel(taskID string) string {
	return GetTaskChannelForKind(taskID, "image")
}

// GetTaskChannelForKind is the routing counterpart to GetTaskMappingForKind.
// It is safe for type-specific authorization and upstream channel selection.
func GetTaskChannelForKind(taskID, taskKind string) string {
	if mapping := GetTaskMappingForKind(taskID, taskKind); mapping != nil {
		return mapping.ChannelID
	}
	return ""
}

// GetCanonicalTaskIDForKind returns the provider-facing ID for a task alias
// when one of the later status responses revealed it. The public alias remains
// valid for status lookups, while content endpoints can use the provider ID
// that the upstream actually accepts.
func GetCanonicalTaskIDForKind(taskID, taskKind string) string {
	mapping := GetTaskMappingForKind(taskID, taskKind)
	if mapping == nil || strings.TrimSpace(mapping.ChannelID) == "" {
		return strings.TrimSpace(taskID)
	}
	taskAlias := strings.TrimSpace(mapping.TaskAlias)
	if taskAlias == "" {
		return mapping.TaskID
	}
	if mapping.TaskID != taskAlias {
		if parent := GetTaskMappingForKind(taskAlias, mapping.TaskKind); parent != nil && parent.ChannelID == mapping.ChannelID && parent.TaskID == taskAlias {
			return mapping.TaskID
		}
		return taskAlias
	}
	if DB == nil {
		return mapping.TaskID
	}
	var aliases []TaskMapping
	if err := DB.Where("channel_id = ? AND task_kind = ? AND task_alias = ?", mapping.ChannelID, mapping.TaskKind, mapping.TaskAlias).
		Order("created_at asc").Find(&aliases).Error; err != nil {
		return mapping.TaskID
	}
	for _, alias := range aliases {
		if alias.TaskID != mapping.TaskID && alias.TaskID != mapping.TaskAlias {
			return alias.TaskID
		}
	}
	return mapping.TaskID
}

// DatabasePath returns the SQLite path currently owned by InitDB.  It is used
// by small recovery sidecars that need to live beside the durable database,
// rather than beside the process working directory.
func DatabasePath() string {
	initMu.Lock()
	defer initMu.Unlock()
	return databasePath
}

func Close() error {
	// Do not hold initMu while waiting: the closing service may still need the
	// database in order to finish an in-flight operation.
	if BeforeClose != nil {
		BeforeClose()
	}
	initMu.Lock()
	defer initMu.Unlock()
	if DB == nil {
		return nil
	}
	memorySecretKeyMu.Lock()
	memorySecretKey = nil
	memorySecretKeyMu.Unlock()
	sqlDB, err := DB.DB()
	if err != nil {
		return err
	}
	DB = nil
	databasePath = ""
	activeChannelsCache.Store(nil)
	clearVideoTaskCache()
	return sqlDB.Close()
}
