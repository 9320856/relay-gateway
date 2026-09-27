package audit

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"

	"relay-gateway/db"
)

func TestRequestAuditWritesHonorCancellationAndCompletionSurvivesDisconnect(t *testing.T) {
	initAuditTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	entry, err := StartContext(ctx, "api_call", "test", "POST", "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	entry.SetReqBody([]byte(`{"model":"should-not-persist"}`), "should-not-persist")
	if !errors.Is(entry.WriteError(), context.Canceled) {
		t.Fatalf("request-stage audit write ignored cancellation: %v", entry.WriteError())
	}
	entry.RecordResult(0, nil, context.Canceled)
	detail, err := GetDetail(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.Outcome != "cancelled" || detail.Log.RequestedModel != "" {
		t.Fatalf("cancellation or detached completion was lost: %+v", detail.Log)
	}
	if len(detail.Events) != 2 || detail.Events[1].Phase != "request_cancelled" {
		t.Fatalf("completion did not retain the cancellation event: %+v", detail.Events)
	}
}

func TestIndependentAuditOperationsBoundConnectionWaits(t *testing.T) {
	checks := []struct {
		name string
		run  func(*AuditEntry) error
	}{
		{"event", func(entry *AuditEntry) error { return entry.AddEvent("test", EventData{}) }},
		{"body", func(entry *AuditEntry) error { entry.SetReqBody(nil, "test"); return entry.WriteError() }},
		{"dispatch", func(entry *AuditEntry) error {
			entry.RecordDispatch("unknown", "test", "", "test")
			return entry.WriteError()
		}},
		{"failover", func(entry *AuditEntry) error { entry.RecordFailover("test"); return entry.WriteError() }},
		{"completion", func(entry *AuditEntry) error { entry.RecordResult(http.StatusOK, nil, nil); return entry.WriteError() }},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			initAuditTestDB(t)
			entry, err := Start("api_call", "test", "GET", "/v1/models", nil)
			if err != nil {
				t.Fatal(err)
			}
			// Per-entry budget injection avoids mutating the production deadline
			// or a shared global while exercising the exact SQL path.
			entry.writeBudget = 25 * time.Millisecond
			pool, err := db.DB.DB()
			if err != nil {
				t.Fatal(err)
			}
			reserved, err := pool.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer reserved.Close()
			done := make(chan error, 1)
			go func() { done <- check.run(entry) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("audit persistence ignored its independent deadline: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("audit operation still waits for sole database connection")
			}
		})
	}
}

func TestQueuedAuditWritesSpendTheirBudgetBeforeTakingEntryLock(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "test", "GET", "/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	entry.writeBudget = 40 * time.Millisecond
	pool, err := db.DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()
	const writers = 32
	ready, begin := make(chan struct{}, writers), make(chan struct{})
	results := make(chan error, writers)
	for writer := 0; writer < writers; writer++ {
		go func() {
			ready <- struct{}{}
			<-begin
			results <- entry.AddEvent("queued", EventData{})
		}()
	}
	for writer := 0; writer < writers; writer++ {
		<-ready
	}
	close(begin)
	deadline := time.After(500 * time.Millisecond)
	for writer := 0; writer < writers; writer++ {
		select {
		case err := <-results:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("queued audit write ignored its deadline: %v", err)
			}
		case <-deadline:
			t.Fatal("queued writers each restarted the SQL budget after acquiring the entry lock")
		}
	}
}

func TestLogicalAuditWriteSharesDeadlineAcrossQueries(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "test", "POST", "/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	var deadlines []time.Time
	var mu sync.Mutex
	observe := func(tx *gorm.DB) {
		deadline, ok := tx.Statement.Context.Deadline()
		if !ok {
			t.Error("audit SQL has no persistence deadline")
			return
		}
		mu.Lock()
		deadlines = append(deadlines, deadline)
		mu.Unlock()
	}
	const callback = "test:audit-deadline"
	if err := db.DB.Callback().Query().Before("gorm:query").Register(callback, observe); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Callback().Update().Before("gorm:update").Register(callback, observe); err != nil {
		t.Fatal(err)
	}
	if err := db.DB.Callback().Create().Before("gorm:create").Register(callback, observe); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.DB.Callback().Query().Remove(callback)
		_ = db.DB.Callback().Update().Remove(callback)
		_ = db.DB.Callback().Create().Remove(callback)
	})
	entry.RecordDispatch("missing-channel", "openai", "https://example.invalid/v1", "model")
	if err := entry.WriteError(); err != nil {
		t.Fatal(err)
	}
	if len(deadlines) != 3 {
		t.Fatalf("expected lookup, update, and event SQL, observed %d", len(deadlines))
	}
	for _, deadline := range deadlines[1:] {
		if !deadline.Equal(deadlines[0]) {
			t.Fatal("logical audit operation reset its deadline between SQL statements")
		}
	}
}

func TestCleanupWorkerCancellationReleasesDatabaseWait(t *testing.T) {
	initAuditTestDB(t)
	pool, err := db.DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()
	previousWaits := pool.Stats().WaitCount
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := StartCleanupWorker(ctx)
	deadline := time.Now().Add(time.Second)
	for pool.Stats().WaitCount == previousWaits {
		if time.Now().After(deadline) {
			t.Fatal("cleanup worker did not reach its setting-query connection wait")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup worker ignored cancellation while connection was occupied")
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cleanupCancel()
	if err := CleanupContext(cleanupCtx, 30); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanup transaction ignored cancellation: %v", err)
	}
}

func TestPollMarkingCancelsOnUncachedTaskLookup(t *testing.T) {
	initAuditTestDB(t)
	entry, err := Start("api_call", "test", "GET", "/v1/videos/missing", nil)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()
	ctx, cancel := context.WithTimeout(WithAudit(context.Background(), entry), 25*time.Millisecond)
	defer cancel()
	done := make(chan bool, 1)
	go func() { done <- MarkAsyncTaskPoll(ctx, "missing", "video", "processing", nil, nil) }()
	select {
	case marked := <-done:
		if marked || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatal("task marking bypassed its deadline or marked an unknown task")
		}
	case <-time.After(time.Second):
		t.Fatal("poll marking still waits for sole database connection")
	}
}

func TestDetachedCompletionDoesNotEscapeCanceledTransaction(t *testing.T) {
	initAuditTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	tx := db.DB.WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	entry, err := StartContext(db.WithTx(ctx, tx), "api_call", "test", "GET", "/v1/models", nil)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = tx.Rollback().Error
	entry.RecordResult(0, nil, context.Canceled)
	if entry.WriteError() == nil {
		t.Fatal("completion of a canceled transaction unexpectedly succeeded")
	}
	if _, err := GetDetail(entry.ID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("detached audit completion escaped its rolled-back transaction: %v", err)
	}
}

func TestPollCoalescingBudgetRetainsChildAndAllowsLaterRetry(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "coalesce-budget", "video", "queued")
	child, err := Start("api_call", "test", "GET", "/v1/videos/coalesce-budget", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !MarkAsyncTaskPoll(WithAudit(context.Background(), child), "coalesce-budget", "video", "processing", nil, nil) {
		t.Fatal("known poll could not be marked")
	}
	child.RecordResult(http.StatusOK, nil, nil)
	child.writeBudget = 25 * time.Millisecond
	pool, err := db.DB.DB()
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()
	if err := CoalesceMarkedAsyncTaskPoll(child); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("coalescing did not respect independent write budget: %v", err)
	}
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := GetDetail(child.ID); err != nil {
		t.Fatalf("timed-out coalescing discarded the independent child: %v", err)
	}
	if err := CoalesceMarkedAsyncTaskPoll(child); err != nil {
		t.Fatalf("deadline retained an in-flight flag and blocked a later retry: %v", err)
	}
	detail, err := GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncPollCount != 1 {
		t.Fatalf("failed coalescing partially advanced parent poll count: %d", detail.Log.AsyncPollCount)
	}
	if _, err := GetDetail(child.ID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("successful retry did not remove its independent child: %v", err)
	}
}

func TestPollCoalescingUsesCallerTransactionAndRollsBackTogether(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "coalesce-transaction", "video", "queued")
	tx := db.DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	child, err := StartWithDB(tx, "api_call", "test", "GET", "/v1/videos/coalesce-transaction", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithAudit(db.WithTx(context.Background(), tx), child)
	if !MarkAsyncTaskPoll(ctx, "coalesce-transaction", "video", "processing", nil, nil) {
		t.Fatal("transactional poll could not be marked")
	}
	child.RecordResult(http.StatusOK, nil, nil)
	if err := CoalesceMarkedAsyncTaskPoll(child); err != nil {
		t.Fatalf("transaction-owned coalescing waited for the global connection: %v", err)
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	detail, err := GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncPollCount != 0 || detail.Log.AsyncTaskStatus != "queued" {
		t.Fatalf("transaction rollback leaked coalesced parent progress: %+v", detail.Log)
	}
}

func TestTimedOutNestedCoalescingRollsBackItsSavepoint(t *testing.T) {
	initAuditTestDB(t)
	parent := createAsyncTaskParent(t, "nested-coalesce-budget", "video", "queued")
	tx := db.DB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	child, err := StartWithDB(tx, "api_call", "test", "GET", "/v1/videos/nested-coalesce-budget", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !MarkAsyncTaskPoll(WithAudit(db.WithTx(context.Background(), tx), child), "nested-coalesce-budget", "video", "processing", nil, nil) {
		t.Fatal("transactional poll could not be marked")
	}
	child.RecordResult(http.StatusOK, nil, nil)
	child.writeBudget = 25 * time.Millisecond
	var delayed bool
	const callback = "test:expire-coalesce-after-update"
	if err := db.DB.Callback().Update().After("gorm:update").Register(callback, func(query *gorm.DB) {
		if !delayed && query.Statement.Schema != nil && query.Statement.Schema.Name == "RequestLogModel" {
			delayed = true
			<-query.Statement.Context.Done()
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.DB.Callback().Update().Remove(callback) })
	if err := CoalesceMarkedAsyncTaskPoll(child); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("coalescing did not time out after its first parent update: %v", err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	detail, err := GetDetail(parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Log.AsyncPollCount != 0 || detail.Log.AsyncTaskStatus != "queued" {
		t.Fatalf("timed-out nested transaction committed partial parent progress: %+v", detail.Log)
	}
	if _, err := GetDetail(child.ID); err != nil {
		t.Fatalf("timed-out nested transaction lost the independent child: %v", err)
	}
}
