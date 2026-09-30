package db

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestParseSelectedModels(t *testing.T) {
	for _, tt := range []struct {
		raw     string
		want    []string
		invalid bool
	}{
		{raw: "", want: nil},
		{raw: "[]", want: []string{}},
		{raw: `[" a ","b","a"]`, want: []string{"a", "b"}},
		{raw: "null", invalid: true},
		{raw: `{}`, invalid: true},
		{raw: `[1]`, invalid: true},
		{raw: `[null]`, invalid: true},
		{raw: `[""]`, invalid: true},
		{raw: `["*"]`, invalid: true},
		{raw: `["gpt-?"]`, invalid: true},
		{raw: `["gpt-[45]"]`, invalid: true},
	} {
		t.Run(tt.raw, func(t *testing.T) {
			got, err := ParseSelectedModels(tt.raw)
			if (err != nil) != tt.invalid || (!tt.invalid && !reflect.DeepEqual(got, tt.want)) {
				t.Fatalf("ParseSelectedModels(%q) = %#v, %v; want %#v invalid=%v", tt.raw, got, err, tt.want, tt.invalid)
			}
		})
	}
}

func TestChannelModelSelectionPersistsAndOlderClientsCannotRemoveIt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "selected-models.db")
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	row := &ChannelModel{ID: "selected", Type: "openai", BaseURL: "https://example.invalid/v1", Enabled: true, FetchModels: true, SelectedModelsRaw: `[" a ","b","a"]`}
	if err := SaveChannelModel(row); err != nil {
		t.Fatal(err)
	}
	if row.SelectedModelsRaw != `["a","b"]` {
		t.Fatalf("selection was not normalized: %q", row.SelectedModelsRaw)
	}
	// The discovery cache retains candidates that can be selected later.
	if err := UpdateChannelHealthContext(context.Background(), row.ID, "healthy", 1, "", []string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
	}
	olderClient := &ChannelModel{ID: row.ID, Name: "renamed", Type: row.Type, BaseURL: row.BaseURL, Enabled: true, FetchModels: true, ModelsSyncedRaw: `["fake-client-model"]`}
	if err := SaveChannelModel(olderClient); err != nil {
		t.Fatal(err)
	}
	if olderClient.SelectedModelsRaw != `["a","b"]` || olderClient.ModelsSyncedRaw != `["a","b","c"]` {
		t.Fatalf("old client changed selection/discovery: %+v", olderClient)
	}
	// Explicit [] is a durable decision to disable every model.
	olderClient.SelectedModelsRaw = "[]"
	if err := SaveChannelModel(olderClient); err != nil {
		t.Fatal(err)
	}
	if err := Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	stored, err := GetChannelModel(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SelectedModelsRaw != "[]" || stored.ModelsSyncedRaw != `["a","b","c"]` {
		t.Fatalf("selection or candidates lost after restart: %+v", stored)
	}
	if got := stored.ToUpstreamChannel().SelectedModels; got == nil || len(got) != 0 {
		t.Fatalf("empty selection became unrestricted: %#v", got)
	}
	active := GetActiveUpstreamChannels()
	if len(active) != 1 || active[0].SelectedModels == nil || len(active[0].SelectedModels) != 0 {
		t.Fatalf("active cache lost explicit empty selection: %#v", active)
	}
}

func TestSaveChannelSelectionRejectsInvalidRawAndFailsClosedOnCorruption(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "invalid-selection.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	for _, raw := range []string{"null", "{}", `[false]`, `[""]`, `["gpt-*"]`} {
		row := &ChannelModel{ID: "invalid", Type: "openai", BaseURL: "https://example.invalid/v1", SelectedModelsRaw: raw}
		if err := SaveChannelModel(row); err == nil {
			t.Fatalf("SaveChannelModel accepted invalid selection %q", raw)
		}
		got := row.ToUpstreamChannel().SelectedModels
		if got == nil || len(got) != 0 {
			t.Fatalf("corrupt selection %q failed open: %#v", raw, got)
		}
	}
}

func TestChannelSelectionSavePreservesDiscoveryCacheOnlyForSameUpstream(t *testing.T) {
	for _, tt := range []struct {
		name     string
		change   func(*ChannelModel)
		preserve bool
	}{
		{"selection", func(row *ChannelModel) { row.SelectedModelsRaw = `["a"]` }, true},
		{"name", func(row *ChannelModel) { row.Name = "renamed" }, true},
		{"priority", func(row *ChannelModel) { row.Priority = 8; row.Weight = 2 }, true},
		{"manual-models", func(row *ChannelModel) { row.ModelsRaw = "extra" }, true},
		{"mapping", func(row *ChannelModel) { row.ModelMapRaw = `{"alias":"a"}` }, true},
		{"equivalent-headers", func(row *ChannelModel) { row.HeadersRaw = ` { "X-B" : "b", "X-A" : "a" } ` }, true},
		{"url", func(row *ChannelModel) { row.BaseURL = "https://other.invalid/v1" }, false},
		{"type", func(row *ChannelModel) { row.Type = "newapi" }, false},
		{"keys", func(row *ChannelModel) { row.APIKeys = []string{"new-secret"} }, false},
		{"headers", func(row *ChannelModel) { row.HeadersRaw = `{"X-A":"changed"}` }, false},
		{"version", func(row *ChannelModel) { row.AnthropicVersion = "2024-01-01" }, false},
		{"fetch-models", func(row *ChannelModel) { row.FetchModels = false }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := InitDB(filepath.Join(t.TempDir(), "cache.db")); err != nil {
				t.Fatal(err)
			}
			defer Close()
			row := &ChannelModel{ID: "cache", Type: "openai", BaseURL: "https://example.invalid/v1", FetchModels: true, APIKeys: []string{"secret"}, HeadersRaw: `{"X-A":"a","X-B":"b"}`, SelectedModelsRaw: `["a","b"]`}
			if err := SaveChannelModel(row); err != nil {
				t.Fatal(err)
			}
			UpdateChannelHealth(row.ID, "healthy", 1, "", []string{"a", "b", "c"})
			row.ModelsSyncedRaw = `["fake-model"]`
			tt.change(row)
			// Use a request transaction to verify key comparisons do not hydrate
			// through the global DB connection.
			if err := DB.Transaction(func(tx *gorm.DB) error {
				return SaveChannelModelContext(WithTx(context.Background(), tx), row)
			}); err != nil {
				t.Fatal(err)
			}
			stored, err := GetChannelModel(row.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := ""
			if tt.preserve {
				want = `["a","b","c"]`
			}
			if stored.ModelsSyncedRaw != want {
				t.Fatalf("cache = %q, want %q", stored.ModelsSyncedRaw, want)
			}
		})
	}
}

func TestChannelProbeAndHealthNeverChangeSelection(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "probe-selection.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	row := &ChannelModel{ID: "probe", Type: "openai", BaseURL: "https://example.invalid/v1", FetchModels: true, SelectedModelsRaw: `["a"]`}
	if err := SaveChannelModel(row); err != nil {
		t.Fatal(err)
	}
	snapshot, err := SnapshotChannelProbe(context.Background(), row.ToUpstreamChannel())
	if err != nil {
		t.Fatal(err)
	}
	if committed, err := CommitChannelProbe(context.Background(), snapshot, "healthy", 1, "", []string{"a", "b"}); err != nil || !committed {
		t.Fatalf("probe committed=%v, err=%v", committed, err)
	}
	UpdateChannelHealth(row.ID, "healthy", 2, "", []string{"a", "b", "c"})
	stored, err := GetChannelModel(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.SelectedModelsRaw != `["a"]` || stored.ModelsSyncedRaw != `["a","b","c"]` {
		t.Fatalf("probe or health changed selection: %+v", stored)
	}
}

func TestInitDBMigratesRealV3ChannelSelectionColumn(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "v3-selection.db")
	legacy, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.AutoMigrate(schemaModels()...); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Migrator().DropColumn(&ChannelModel{}, "SelectedModelsRaw"); err != nil {
		t.Fatal(err)
	}
	row := &ChannelModel{ID: "v3", Type: "openai", BaseURL: "https://v3.invalid/v1", Enabled: true, ModelsRaw: "a,b", ModelsSyncedRaw: `["a","b","c"]`}
	if err := legacy.Omit("SelectedModelsRaw").Create(row).Error; err != nil {
		t.Fatal(err)
	}
	if err := legacy.Create(&SchemaMeta{Key: "schema_version", Value: "3"}).Error; err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := legacy.DB()
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := InitDB(dbPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	for i := 0; i < 2; i++ {
		stored, err := GetChannelModel(row.ID)
		if err != nil {
			t.Fatal(err)
		}
		if stored.SelectedModelsRaw != "" || stored.ToUpstreamChannel().SelectedModels != nil || stored.ModelsRaw != row.ModelsRaw || stored.ModelsSyncedRaw != row.ModelsSyncedRaw {
			t.Fatalf("v3 channel changed on migration/restart: %+v", stored)
		}
		var marker SchemaMeta
		if err := DB.First(&marker, "key = ?", "schema_version").Error; err != nil || marker.Value != SchemaVersion {
			t.Fatalf("marker=%+v err=%v", marker, err)
		}
		if i == 0 {
			if err := Close(); err != nil {
				t.Fatal(err)
			}
			if err := InitDB(dbPath); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestInspectDatabaseAcceptsV2V3AndV4(t *testing.T) {
	for _, version := range []string{"2", "3", SchemaVersion} {
		path := makeBackupTestDB(t, version)
		report, err := InspectDatabase(context.Background(), path)
		if err != nil || report.SchemaVersion != version {
			t.Fatalf("InspectDatabase(%s)=%+v, %v", version, report, err)
		}
	}
}
