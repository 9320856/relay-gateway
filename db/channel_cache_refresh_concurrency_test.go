package db

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"

	"relay-gateway/config"
)

func TestActiveChannelCacheOverlappingRefreshCannotRestoreDisabledChannel(t *testing.T) {
	if err := InitDB(t.TempDir() + "/overlapping-refresh.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	channel := &ChannelModel{ID: "disable-me", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true}
	if err := SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	pool, err := DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	// WAL permits a committed writer alongside the refresher's read snapshot.
	pool.SetMaxOpenConns(2)
	defer pool.SetMaxOpenConns(1)

	oldSnapshotRead := make(chan struct{})
	newSnapshotRead := make(chan struct{})
	resumeOldRefresh := make(chan struct{})
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(resumeOldRefresh) }) }
	defer resume()
	var reads atomic.Int32
	const callback = "test:pause_channel_refresh"
	if err := DB.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table != "channel_models" {
			return
		}
		switch reads.Add(1) {
		case 1:
			close(oldSnapshotRead)
			<-resumeOldRefresh
		case 2:
			close(newSnapshotRead)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer DB.Callback().Query().Remove(callback)
	oldRefreshFinished := make(chan struct{})
	go func() {
		RefreshActiveChannelsCache()
		close(oldRefreshFinished)
	}()
	select {
	case <-oldSnapshotRead:
	case <-time.After(5 * time.Second):
		t.Fatal("first refresh did not load its snapshot")
	}
	if err := DB.Model(&ChannelModel{}).Where("id = ?", channel.ID).Update("enabled", false).Error; err != nil {
		t.Fatal(err)
	}
	newRefreshFinished := make(chan struct{})
	go func() {
		RefreshActiveChannelsCache()
		close(newRefreshFinished)
	}()
	// If refreshes overlap, finish publishing the newer snapshot before the
	// first resumes. Ordered refreshes defer the newer load until the first
	// has published. Both schedules must end with the disabled channel absent.
	select {
	case <-newSnapshotRead:
		select {
		case <-newRefreshFinished:
		case <-time.After(5 * time.Second):
			t.Fatal("new refresh did not finish")
		}
	case <-time.After(100 * time.Millisecond):
	}

	// A slow refresh must not block requests reading the current snapshot.
	activeRead := make(chan []config.UpstreamChannel, 1)
	go func() { activeRead <- GetActiveUpstreamChannels() }()
	select {
	case <-activeRead:
	case <-time.After(time.Second):
		t.Fatal("routing read blocked behind a cache refresh")
	}
	resume()
	for _, done := range []chan struct{}{oldRefreshFinished, newRefreshFinished} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("refresh did not finish")
		}
	}
	if active := GetActiveUpstreamChannels(); len(active) != 0 {
		t.Fatalf("an older refresh restored a disabled channel: %+v", active)
	}
}

func TestActiveChannelCacheRefreshKeepsEndpointCredentialsAndMappingsTogether(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "snapshot-test-encryption-key")
	if err := InitDB(t.TempDir() + "/consistent-channel-snapshot.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	old := &ChannelModel{ID: "snapshot", Type: "openai", BaseURL: "https://old.example.invalid/v1", Enabled: true, APIKeys: []string{"old-first", "old-second"}, ModelMapRaw: `{"incoming":"old-target"}`}
	if err := SaveChannelModel(old); err != nil {
		t.Fatal(err)
	}
	pool, err := DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(2)
	defer pool.SetMaxOpenConns(1)

	oldRowsRead, latestRowsRead := make(chan struct{}), make(chan struct{})
	resumeOld, resumeLatest := make(chan struct{}), make(chan struct{})
	var oldOnce, latestOnce sync.Once
	releaseOld := func() { oldOnce.Do(func() { close(resumeOld) }) }
	releaseLatest := func() { latestOnce.Do(func() { close(resumeLatest) }) }
	defer releaseOld()
	defer releaseLatest()
	var reads atomic.Int32
	const callback = "test:pause_channel_snapshot_rows"
	if err := DB.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table != "channel_models" {
			return
		}
		if _, refresh := tx.Statement.Dest.(*[]ChannelModel); !refresh {
			return
		}
		switch reads.Add(1) {
		case 1:
			close(oldRowsRead)
			<-resumeOld
		case 2:
			close(latestRowsRead)
			<-resumeLatest
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer DB.Callback().Query().Remove(callback)
	oldFinished := make(chan struct{})
	go func() {
		RefreshActiveChannelsCache()
		close(oldFinished)
	}()
	select {
	case <-oldRowsRead:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not read the old endpoint")
	}
	const newBaseURL = "https://new.example.invalid/v1"
	newChannel := &ChannelModel{ID: old.ID, Type: "openai", BaseURL: newBaseURL, Enabled: true, APIKeys: []string{"new-first", "new-second"}, ModelMapRaw: `{"incoming":"new-target"}`}
	newSaved := make(chan error, 1)
	go func() { newSaved <- SaveChannelModel(newChannel) }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		var committed ChannelModel
		if err := DB.WithContext(ctx).Select("base_url").First(&committed, "id = ?", old.ID).Error; err != nil {
			t.Fatalf("read concurrent committed save: %v", err)
		}
		if committed.BaseURL == newBaseURL {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("concurrent save did not commit while the snapshot was open")
		case <-time.After(time.Millisecond):
		}
	}
	releaseOld()
	select {
	case <-latestRowsRead:
	case <-time.After(5 * time.Second):
		t.Fatal("post-commit refresh did not start")
	}
	// The next refresher is paused, exposing the snapshot the earlier one
	// published after the save committed. Each complete configuration is safe;
	// an old endpoint paired with new credentials or mappings is not.
	active := GetActiveUpstreamChannels()
	if len(active) != 1 {
		t.Errorf("published snapshot channel count = %d", len(active))
	} else {
		wantKeys, wantTarget := old.APIKeys, "old-target"
		if active[0].BaseURL == newChannel.BaseURL {
			wantKeys, wantTarget = newChannel.APIKeys, "new-target"
		} else if active[0].BaseURL != old.BaseURL {
			t.Errorf("published snapshot has an unknown endpoint: %s", active[0].BaseURL)
		}
		if !reflect.DeepEqual(active[0].APIKeys, wantKeys) || active[0].APIKey != wantKeys[0] || active[0].ModelMap["incoming"] != wantTarget {
			t.Errorf("refresh mixed endpoint, credentials, or mappings from different commits: %+v", active[0])
		}
	}
	releaseLatest()
	select {
	case <-oldFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("old refresh did not finish")
	}
	select {
	case err := <-newSaved:
		if err != nil {
			t.Fatalf("concurrent save: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("save's post-commit refresh did not finish")
	}
	active = GetActiveUpstreamChannels()
	if len(active) != 1 || active[0].BaseURL != newChannel.BaseURL || !reflect.DeepEqual(active[0].APIKeys, newChannel.APIKeys) || active[0].ModelMap["incoming"] != "new-target" {
		t.Fatalf("final cache did not publish the latest complete configuration: %+v", active)
	}
}

func TestActiveChannelCacheSnapshotHandlesEmptyMultipleAndDamagedKeys(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "snapshot-test-encryption-key")
	if err := InitDB(t.TempDir() + "/snapshot-keys.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	for _, channel := range []*ChannelModel{
		{ID: "empty", Type: "openai", BaseURL: "https://empty.example.invalid/v1", Enabled: true},
		{ID: "multiple", Type: "openai", BaseURL: "https://multiple.example.invalid/v1", Enabled: true, APIKeys: []string{"first-key", "second-key"}},
	} {
		if err := SaveChannelModel(channel); err != nil {
			t.Fatal(err)
		}
	}
	wrongKeyCiphertext, err := encryptWithKey("wrong-key-secret", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	for id, secret := range map[string]string{"malformed": "enc:v1:not-valid-base64!", "wrong-key": wrongKeyCiphertext} {
		if err := DB.Create(&ChannelModel{ID: id, Type: "openai", BaseURL: "https://damaged.example.invalid/v1", Enabled: true}).Error; err != nil {
			t.Fatal(err)
		}
		if err := DB.Create(&ChannelKeyModel{ChannelID: id, Secret: secret}).Error; err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	go func() {
		RefreshActiveChannelsCache()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot refresh deadlocked hydrating empty or damaged credentials")
	}
	byID := make(map[string]config.UpstreamChannel)
	for _, channel := range GetActiveUpstreamChannels() {
		byID[channel.ID] = channel
	}
	if len(byID) != 2 || byID["empty"].ID == "" || len(byID["empty"].APIKeys) != 0 || byID["empty"].APIKey != "" {
		t.Fatalf("empty-key channel was lost or damaged credentials became active: %+v", byID)
	}
	if multiple := byID["multiple"]; multiple.APIKey != "first-key" || !reflect.DeepEqual(multiple.APIKeys, []string{"first-key", "second-key"}) {
		t.Fatalf("snapshot changed credential ordering: %+v", multiple)
	}
}

func TestActiveChannelSnapshotCancellationDoesNotPublishPartialConfiguration(t *testing.T) {
	t.Setenv("RELAY_DB_ENCRYPTION_KEY", "snapshot-test-encryption-key")
	if err := InitDB(t.TempDir() + "/snapshot-cancel.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := SaveChannelModel(&ChannelModel{ID: "cancel", Type: "openai", BaseURL: "https://cancel.example.invalid/v1", Enabled: true, APIKey: "valid-key"}); err != nil {
		t.Fatal(err)
	}
	before := activeChannelsCache.Load()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const callback = "test:cancel_channel_snapshot"
	if err := DB.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "channel_key_models" {
			cancel()
		}
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := loadActiveChannelsSnapshot(DB.WithContext(ctx))
	_ = DB.Callback().Query().Remove(callback)
	if !errors.Is(err, context.Canceled) || snapshot != nil {
		t.Fatalf("canceled hydration returned a partial snapshot: snapshot=%+v err=%v", snapshot, err)
	}
	if activeChannelsCache.Load() != before {
		t.Fatal("snapshot loading changed the globally published cache")
	}
	// The canceled read transaction must release the single connection so the
	// next refresh can complete normally.
	RefreshActiveChannelsCache()
	if active := GetActiveUpstreamChannels(); len(active) != 1 || active[0].APIKey != "valid-key" {
		t.Fatalf("refresh did not recover after canceled snapshot: %+v", active)
	}
}

func TestActiveChannelCacheRefreshRetainsSnapshotOnHydrationQueryError(t *testing.T) {
	for _, table := range []string{"channel_key_models", "model_mappings"} {
		t.Run(table, func(t *testing.T) {
			t.Setenv("RELAY_DB_ENCRYPTION_KEY", "snapshot-test-encryption-key")
			if err := InitDB(t.TempDir() + "/snapshot-error.db"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = Close() })
			channel := &ChannelModel{ID: "retained", Type: "openai", BaseURL: "https://old.example.invalid/v1", Enabled: true, APIKey: "valid-key", ModelMapRaw: `{"incoming":"target"}`}
			if err := SaveChannelModel(channel); err != nil {
				t.Fatal(err)
			}
			before := activeChannelsCache.Load()
			const changedURL = "https://changed.example.invalid/v1"
			if err := DB.Model(&ChannelModel{}).Where("id = ?", channel.ID).Update("base_url", changedURL).Error; err != nil {
				t.Fatal(err)
			}
			const callback = "test:fail_channel_snapshot_hydration"
			if err := DB.Callback().Query().Before("gorm:query").Register(callback, func(tx *gorm.DB) {
				if tx.Statement.Table == table {
					tx.AddError(errors.New("injected hydration query failure"))
				}
			}); err != nil {
				t.Fatal(err)
			}
			RefreshActiveChannelsCache()
			_ = DB.Callback().Query().Remove(callback)
			if activeChannelsCache.Load() != before {
				t.Fatal("failed hydration replaced the previous complete cache snapshot")
			}
			RefreshActiveChannelsCache()
			active := GetActiveUpstreamChannels()
			if len(active) != 1 || active[0].BaseURL != changedURL || active[0].APIKey != "valid-key" || active[0].ModelMap["incoming"] != "target" {
				t.Fatalf("refresh did not recover after query failure: %+v", active)
			}
		})
	}
}

func TestActiveChannelCacheRefreshWaitsForCommittedChannelChanges(t *testing.T) {
	if err := InitDB(t.TempDir() + "/transaction-cache.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	channel := &ChannelModel{ID: "transactional", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true}
	if err := SaveChannelModel(channel); err != nil {
		t.Fatal(err)
	}
	owner := DB.Begin()
	if owner.Error != nil {
		t.Fatal(owner.Error)
	}
	defer owner.Rollback()
	if _, err := ToggleChannelContext(WithTx(context.Background(), owner), channel.ID); err != nil {
		t.Fatal(err)
	}
	if active := GetActiveUpstreamChannels(); len(active) != 1 || active[0].ID != channel.ID {
		t.Fatalf("uncommitted toggle changed routing cache: %+v", active)
	}
	refreshFinished := make(chan struct{})
	go func() {
		RefreshActiveChannelsCache()
		close(refreshFinished)
	}()
	// The sole SQL connection is occupied, but both transaction work and
	// atomic routing reads must stay usable while the refresh is waiting.
	if err := SetSettingContext(WithTx(context.Background(), owner), "transaction-alive", "true"); err != nil {
		t.Fatal(err)
	}
	if err := owner.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	select {
	case <-refreshFinished:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh deadlocked with the caller-owned transaction")
	}
	if active := GetActiveUpstreamChannels(); len(active) != 1 || active[0].ID != channel.ID {
		t.Fatalf("refresh published rolled-back channel changes: %+v", active)
	}
	if enabled, err := ToggleChannel(channel.ID); err != nil || enabled {
		t.Fatalf("committed toggle failed: enabled=%v err=%v", enabled, err)
	}
	if active := GetActiveUpstreamChannels(); len(active) != 0 {
		t.Fatalf("committed toggle was not refreshed: %+v", active)
	}
}
