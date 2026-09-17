package db

import (
	"errors"
	"testing"

	"gorm.io/gorm"
)

func TestDeleteProfileDetachesCompletedTasks(t *testing.T) {
	if err := InitDB(t.TempDir() + "/profile-delete.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "delete-me", Name: "Delete me", Source: ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := SaveProtocolProfileRevision(&ProtocolProfileRevision{ProfileID: "delete-me", Revision: 1, SchemaVersion: 1, ContentJSON: `{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/chat"},"response":{}}]}`, ContentDigest: "x", State: ProfileRevisionRetired}); err != nil {
		t.Fatal(err)
	}
	if err := CreateTaskRun(&TaskRun{ID: "done", ProfileID: "delete-me", ProfileRevision: 1, TaskStatus: "completed"}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteProtocolProfile("delete-me"); err != nil {
		t.Fatal(err)
	}
	run, err := GetTaskRun("done")
	if err != nil {
		t.Fatal(err)
	}
	if run.ProfileID != "" || run.ProfileRevision != 0 {
		t.Fatalf("task still references deleted profile: %#v", run)
	}
}

func TestDeleteProfileRejectsActiveTasks(t *testing.T) {
	if err := InitDB(t.TempDir() + "/profile-delete-active.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "keep", Name: "Keep", Source: ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := CreateTaskRun(&TaskRun{ID: "active", ProfileID: "keep", ProfileRevision: 1, TaskStatus: "processing"}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteProtocolProfile("keep"); err == nil {
		t.Fatal("active task allowed profile deletion")
	}
}

func TestDeleteProfileRevisionDetachesCompletedTasks(t *testing.T) {
	if err := InitDB(t.TempDir() + "/profile-revision-delete.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "revision", Name: "Revision", Source: ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := SaveProtocolProfileRevision(&ProtocolProfileRevision{ProfileID: "revision", Revision: 1, SchemaVersion: 1, ContentJSON: `{}`, ContentDigest: "digest", State: ProfileRevisionPublished}); err != nil {
		t.Fatal(err)
	}
	if err := CreateTaskRun(&TaskRun{ID: "revision-done", ProfileID: "revision", ProfileRevision: 1, TaskStatus: "completed"}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteProtocolProfileRevision("revision", 1); err != nil {
		t.Fatal(err)
	}
	run, err := GetTaskRun("revision-done")
	if err != nil {
		t.Fatal(err)
	}
	if run.ProfileID != "" || run.ProfileRevision != 0 {
		t.Fatalf("completed task still references deleted revision: %#v", run)
	}
	if _, err := GetProtocolProfileRevision("revision", 1); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted revision still exists: err=%v", err)
	}
}

func TestDeleteProfileRevisionRejectsActiveTasks(t *testing.T) {
	if err := InitDB(t.TempDir() + "/profile-revision-delete-active.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = Close() })
	if err := CreateProtocolProfile(&ProtocolProfile{ID: "revision-active", Name: "Revision active", Source: ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := SaveProtocolProfileRevision(&ProtocolProfileRevision{ProfileID: "revision-active", Revision: 1, SchemaVersion: 1, ContentJSON: `{}`, ContentDigest: "digest", State: ProfileRevisionRetired}); err != nil {
		t.Fatal(err)
	}
	if err := CreateTaskRun(&TaskRun{ID: "revision-active-task", ProfileID: "revision-active", ProfileRevision: 1, TaskStatus: "processing"}); err != nil {
		t.Fatal(err)
	}
	if err := DeleteProtocolProfileRevision("revision-active", 1); !errors.Is(err, ErrRevisionHasActiveTasks) {
		t.Fatalf("active task deletion error=%v, want ErrRevisionHasActiveTasks", err)
	}
	if _, err := GetProtocolProfileRevision("revision-active", 1); err != nil {
		t.Fatalf("active task deletion removed revision: %v", err)
	}
}
