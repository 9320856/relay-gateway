package db

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestChannelProbeCommitRejectsChangedConfiguration(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "probe.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	row := &ChannelModel{ID: "probe", Name: "probe", Type: "openai", BaseURL: "http://old.example/v1", Enabled: true, FetchModels: true}
	if err := SaveChannelModel(row); err != nil {
		t.Fatal(err)
	}
	snapshot, err := SnapshotChannelProbe(context.Background(), row.ToUpstreamChannel())
	if err != nil {
		t.Fatal(err)
	}
	row.BaseURL = "http://new.example/v1"
	if err = SaveChannelModel(row); err != nil {
		t.Fatal(err)
	}
	committed, err := CommitChannelProbe(context.Background(), snapshot, "healthy", 1, "", []string{"old-model"})
	if err != nil || committed {
		t.Fatalf("stale commit=%v err=%v", committed, err)
	}
	current, err := GetChannelModel(row.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ModelsSyncedRaw != "" {
		t.Fatalf("old model persisted: %s", current.ModelsSyncedRaw)
	}
	snapshot, err = SnapshotChannelProbe(context.Background(), current.ToUpstreamChannel())
	if err != nil {
		t.Fatal(err)
	}
	committed, err = CommitChannelProbe(context.Background(), snapshot, "healthy", 1, "", []string{"new-model"})
	if err != nil || !committed {
		t.Fatalf("current commit=%v err=%v", committed, err)
	}
}

func TestChannelProbeCommitHonorsCanceledContext(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "probe-context.db")); err != nil {
		t.Fatal(err)
	}
	defer Close()
	row := &ChannelModel{ID: "probe", Name: "probe", Type: "openai", BaseURL: "http://old.example/v1", Enabled: true, FetchModels: true}
	if err := SaveChannelModel(row); err != nil {
		t.Fatal(err)
	}
	snapshot, err := SnapshotChannelProbe(context.Background(), row.ToUpstreamChannel())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	committed, err := CommitChannelProbe(ctx, snapshot, "healthy", 1, "", []string{"ignored"})
	if committed || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled commit=%v error=%v", committed, err)
	}
}

func TestChannelConfigurationTimestampIsStrictlyMonotonic(t *testing.T) {
	if err := InitDB(filepath.Join(t.TempDir(), "channel-revisions.db")); err != nil {
		t.Fatal(err)
	}
	defer Close()
	row := &ChannelModel{ID: "probe", Name: "probe", Type: "openai", BaseURL: "http://old.example/v1", Enabled: true, FetchModels: true}
	if err := SaveChannelModel(row); err != nil {
		t.Fatal(err)
	}
	previous := row.UpdatedAt
	for i := 0; i < 20; i++ {
		if err := SaveChannelModel(row); err != nil {
			t.Fatal(err)
		}
		if !row.UpdatedAt.After(previous) {
			t.Fatalf("configuration revision did not increase: %v <= %v", row.UpdatedAt, previous)
		}
		previous = row.UpdatedAt
		if _, err := ToggleChannel(row.ID); err != nil {
			t.Fatal(err)
		}
		current, err := GetChannelModel(row.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !current.UpdatedAt.After(previous) {
			t.Fatalf("toggle revision did not increase: %v <= %v", current.UpdatedAt, previous)
		}
		previous = current.UpdatedAt
	}
}
