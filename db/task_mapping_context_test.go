package db

import (
	"context"
	"testing"
)

func TestTaskMappingLookupHonorsCancellationAndTransactionVisibility(t *testing.T) {
	if err := InitDB(t.TempDir() + "/lookup-context.db"); err != nil {
		t.Fatal(err)
	}
	defer Close()
	if err := RecordTaskMapping(TaskMapping{TaskID: "committed", ChannelID: "channel", TaskKind: "video"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if GetTaskMappingForKindContext(ctx, "committed", "video") != nil {
		t.Fatal("canceled request returned a cache hit")
	}
	tx := DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	txCtx := WithTx(context.Background(), tx)
	if err := RecordTaskMappingContext(txCtx, TaskMapping{TaskID: "uncommitted", ChannelID: "channel", TaskKind: "video"}); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if got := GetTaskMappingForKindContext(txCtx, "uncommitted", "video"); got == nil || got.ChannelID != "channel" {
		tx.Rollback()
		t.Fatal("lookup ignored caller transaction")
	}
	if getCachedTaskMapping("uncommitted") != nil {
		tx.Rollback()
		t.Fatal("lookup published uncommitted mapping")
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if GetTaskMapping("uncommitted") != nil {
		t.Fatal("rolled back mapping leaked")
	}
}
