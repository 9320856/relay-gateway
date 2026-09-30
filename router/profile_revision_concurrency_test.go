package router

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"relay-gateway/db"
)

func revisionCreateBody(path string, revision int) []byte {
	return []byte(fmt.Sprintf(`{"revision":%d,"profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":%q,"body_encoding":"json"}}]}}`, revision, path))
}

func TestProfileRevisionPOSTCannotOverwriteDraft(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/draft-post-conflict.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "draft"}); err != nil {
		t.Fatal(err)
	}
	engine := profileTestEngine()
	first := httptest.NewRecorder()
	engine.ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/profiles/draft/revisions", bytes.NewReader(revisionCreateBody("/original", 1))))
	if first.Code != http.StatusCreated {
		t.Fatalf("initial create=%d %s", first.Code, first.Body.String())
	}
	second := httptest.NewRecorder()
	engine.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/profiles/draft/revisions", bytes.NewReader(revisionCreateBody("/replacement", 1))))
	stored, err := db.GetProtocolProfileRevision("draft", 1)
	if err != nil || second.Code != http.StatusConflict || !bytes.Contains([]byte(stored.ContentJSON), []byte("/original")) {
		t.Fatalf("duplicate create=%d stored=%+v err=%v", second.Code, stored, err)
	}
}

func TestProfileRevisionConcurrentPOSTCreatesDistinctDrafts(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/concurrent-draft-post.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "concurrent"}); err != nil {
		t.Fatal(err)
	}
	engine := profileTestEngine()
	const count = 12
	start := make(chan struct{})
	results := make(chan *httptest.ResponseRecorder, count)
	var workers sync.WaitGroup
	for i := 0; i < count; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			<-start
			result := httptest.NewRecorder()
			engine.ServeHTTP(result, httptest.NewRequest(http.MethodPost, "/profiles/concurrent/revisions", bytes.NewReader(revisionCreateBody(fmt.Sprintf("/draft-%d", i), 0))))
			results <- result
		}(i)
	}
	close(start)
	workers.Wait()
	close(results)
	seen := make(map[int]bool)
	for result := range results {
		var payload struct{ Revision db.ProtocolProfileRevision }
		if err := json.Unmarshal(result.Body.Bytes(), &payload); err != nil || result.Code != http.StatusCreated {
			t.Fatalf("concurrent create=%d body=%s err=%v", result.Code, result.Body.String(), err)
		}
		if seen[payload.Revision.Revision] {
			t.Fatalf("draft overwritten: revision %d allocated twice", payload.Revision.Revision)
		}
		seen[payload.Revision.Revision] = true
	}
	profile, err := db.GetProtocolProfile("concurrent")
	if err != nil || len(seen) != count || profile.LatestRevision != count {
		t.Fatalf("created=%d profile=%+v err=%v", len(seen), profile, err)
	}
}
