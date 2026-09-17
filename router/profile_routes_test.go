package router

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/db"
)

func profileTestEngine() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/profiles", handleListProtocolProfiles)
	r.GET("/profiles/:id", handleGetProtocolProfile)
	r.GET("/profiles/:id/revisions/:revision/references", handleGetProtocolProfileRevisionReferences)
	r.GET("/profiles/:id/revisions/:revision/diff", handleGetProtocolProfileRevisionDiff)
	r.POST("/profiles", handleCreateProtocolProfile)
	r.POST("/profiles/:id/revisions/:revision/publish", handlePublishProtocolProfileRevision)
	r.POST("/profiles/:id/revisions/:revision/retire", handleRetireProtocolProfileRevision)
	r.DELETE("/profiles/:id/revisions/:revision", handleDeleteProtocolProfileRevision)
	r.POST("/profiles/:id/revisions", handleCreateProtocolProfileRevision)
	r.PUT("/profiles/:id/revisions/:revision", handleUpdateProtocolProfileRevision)
	r.DELETE("/profiles/:id", handleDeleteProtocolProfile)
	return r
}

func TestProfileRevisionDiffAndReferences(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles-diff.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "diff-profile", Name: "Diff Profile", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	revisionOne := `{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/chat","body_encoding":"json"},"response":{"result_paths":["choices"]}}]}`
	revisionTwo := `{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/chat/completions","body_encoding":"json"},"response":{"result_paths":["choices"]}}]}`
	for number, content := range map[int]string{1: revisionOne, 2: revisionTwo} {
		if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "diff-profile", Revision: number, SchemaVersion: 1, ContentJSON: content, ContentDigest: content, State: db.ProfileRevisionPublished}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: "diff-channel-a", Operation: "chat", ModelPattern: "*", ProfileID: "diff-profile", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: "diff-channel-b", Operation: "chat", ModelPattern: "model-*", ProfileID: "diff-profile", ProfileRevision: 1, Precedence: 1, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTaskRun(&db.TaskRun{ID: "diff-active", TaskKind: "video", Operation: "video.create", ChannelID: "diff-channel-a", Engine: "profile", ProfileID: "diff-profile", ProfileRevision: 1, TaskStatus: "queued"}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTaskRun(&db.TaskRun{ID: "diff-done", TaskKind: "video", Operation: "video.create", ChannelID: "diff-channel-a", Engine: "profile", ProfileID: "diff-profile", ProfileRevision: 1, TaskStatus: "completed"}); err != nil {
		t.Fatal(err)
	}
	r := profileTestEngine()
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/profiles/diff-profile/revisions/1/references", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("references status=%d body=%s", resp.Code, resp.Body.String())
	}
	var references struct {
		References db.ProfileRevisionReferences `json:"references"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &references); err != nil {
		t.Fatal(err)
	}
	if references.References.Bindings != 2 || references.References.EnabledBindings != 1 || references.References.TaskRuns != 2 || references.References.NonTerminalTaskRuns != 1 {
		t.Fatalf("unexpected revision references: %+v", references.References)
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/profiles/diff-profile/revisions/2/diff", nil))
	if resp.Code != http.StatusOK || !bytes.Contains(resp.Body.Bytes(), []byte("$.operations[0].submit.path")) || !bytes.Contains(resp.Body.Bytes(), []byte("/chat/completions")) {
		t.Fatalf("diff status=%d body=%s", resp.Code, resp.Body.String())
	}
}

func TestProfileDraftRevisionCreateAndUpdate(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles-revisions.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := profileTestEngine()
	create := []byte(`{"id":"draft-profile","name":"Draft","profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat","body_encoding":"json"},"response":{"result_paths":["id"]}}]}}`)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles", bytes.NewReader(create)))
	if resp.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", resp.Code, resp.Body.String())
	}
	update := []byte(`{"profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat-updated","body_encoding":"json"},"response":{"result_paths":["id"]}}]}}`)
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPut, "/profiles/draft-profile/revisions/1", bytes.NewReader(update)))
	if resp.Code != http.StatusOK || !bytes.Contains(resp.Body.Bytes(), []byte("chat-updated")) {
		t.Fatalf("update status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles/draft-profile/revisions", bytes.NewReader(update)))
	if resp.Code != http.StatusCreated || !bytes.Contains(resp.Body.Bytes(), []byte(`"revision":2`)) {
		t.Fatalf("new revision status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles/draft-profile/revisions/1/publish", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("publish status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPut, "/profiles/draft-profile/revisions/1", bytes.NewReader(update)))
	if resp.Code != http.StatusConflict {
		t.Fatalf("published update status=%d body=%s", resp.Code, resp.Body.String())
	}
}

func TestPublishedProfileCanBeClonedIntoEditableDraft(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles-published-clone.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := profileTestEngine()
	create := []byte(`{"id":"published-clone","name":"Published clone","profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat","body_encoding":"json"},"response":{"result_paths":["id"]}}]}}`)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles", bytes.NewReader(create)))
	if resp.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles/published-clone/revisions/1/publish", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("publish status=%d body=%s", resp.Code, resp.Body.String())
	}
	clone := []byte(`{"profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat-fixed","body_encoding":"json"},"response":{"result_paths":["id"]}}]}}`)
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles/published-clone/revisions", bytes.NewReader(clone)))
	if resp.Code != http.StatusCreated || !bytes.Contains(resp.Body.Bytes(), []byte(`"revision":2`)) {
		t.Fatalf("clone status=%d body=%s", resp.Code, resp.Body.String())
	}
	var original db.ProtocolProfileRevision
	if err := db.DB.Where("profile_id = ? AND revision = ?", "published-clone", 1).First(&original).Error; err != nil {
		t.Fatal(err)
	}
	if original.State != db.ProfileRevisionPublished || !bytes.Contains([]byte(original.ContentJSON), []byte("/v1/chat")) {
		t.Fatalf("published revision was changed while cloning: %+v", original)
	}
	var draft db.ProtocolProfileRevision
	if err := db.DB.Where("profile_id = ? AND revision = ?", "published-clone", 2).First(&draft).Error; err != nil {
		t.Fatal(err)
	}
	if draft.State != db.ProfileRevisionDraft || !bytes.Contains([]byte(draft.ContentJSON), []byte("/v1/chat-fixed")) {
		t.Fatalf("clone did not create editable draft: %+v", draft)
	}
}

func TestUnboundProfileRevisionCanBeDeletedIncludingPublished(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles-revision-delete.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := profileTestEngine()
	create := []byte(`{"id":"revision-delete","name":"Revision delete","profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat","body_encoding":"json"},"response":{"result_paths":["id"]}}]}}`)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles", bytes.NewReader(create)))
	if resp.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", resp.Code, resp.Body.String())
	}
	clone := []byte(`{"profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat-draft","body_encoding":"json"},"response":{"result_paths":["id"]}}]}}`)
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles/revision-delete/revisions", bytes.NewReader(clone)))
	if resp.Code != http.StatusCreated {
		t.Fatalf("draft create status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodDelete, "/profiles/revision-delete/revisions/2", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("draft delete status=%d body=%s", resp.Code, resp.Body.String())
	}
	profile, err := db.GetProtocolProfile("revision-delete")
	if err != nil {
		t.Fatal(err)
	}
	if profile.LatestRevision != 1 {
		t.Fatalf("latest revision pointer was not recalculated: %+v", profile)
	}
	if _, err := db.GetProtocolProfileRevision("revision-delete", 2); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted draft still exists: err=%v", err)
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles/revision-delete/revisions/1/publish", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("publish status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodDelete, "/profiles/revision-delete/revisions/1", nil))
	if resp.Code != http.StatusOK || !bytes.Contains(resp.Body.Bytes(), []byte("Revision 已删除")) {
		t.Fatalf("published delete status=%d body=%s", resp.Code, resp.Body.String())
	}
	profile, err = db.GetProtocolProfile("revision-delete")
	if err != nil {
		t.Fatal(err)
	}
	if profile.LatestRevision != 0 {
		t.Fatalf("latest revision pointer was not cleared after deleting the only revision: %+v", profile)
	}
	if _, err := db.GetProtocolProfileRevision("revision-delete", 1); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted published revision still exists: err=%v", err)
	}
}

func TestBoundProfileRevisionCannotBeDeleted(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles-bound-revision-delete.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := profileTestEngine()
	create := []byte(`{"id":"bound-revision-delete","name":"Bound revision","profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat","body_encoding":"json"},"response":{"result_paths":["id"]}}]}}`)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles", bytes.NewReader(create)))
	if resp.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles/bound-revision-delete/revisions/1/publish", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("publish status=%d body=%s", resp.Code, resp.Body.String())
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: "bound-channel", Operation: "chat", ModelPattern: "*", ProfileID: "bound-revision-delete", ProfileRevision: 1, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodDelete, "/profiles/bound-revision-delete/revisions/1", nil))
	if resp.Code != http.StatusConflict || !bytes.Contains(resp.Body.Bytes(), []byte("已有渠道绑定")) {
		t.Fatalf("bound published delete status=%d body=%s", resp.Code, resp.Body.String())
	}
}

func TestProfileRoutesLifecycle(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := profileTestEngine()
	body := map[string]any{
		"id": "custom-1", "name": "Custom", "profile": map[string]any{
			"schema_version": 1,
			"operations": []any{map[string]any{
				"operation": "chat", "execution_mode": "direct", "polling_mode": "off",
				"submit":   map[string]any{"method": "POST", "path": "/v1/chat", "body_encoding": "json"},
				"response": map[string]any{"result_paths": []string{"id"}},
			}},
		},
	}
	data, _ := json.Marshal(body)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles", bytes.NewReader(data)))
	if resp.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/profiles", nil))
	if resp.Code != http.StatusOK || !bytes.Contains(resp.Body.Bytes(), []byte("custom-1")) {
		t.Fatalf("list status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles/custom-1/revisions/1/publish", nil))
	if resp.Code != http.StatusOK || !bytes.Contains(resp.Body.Bytes(), []byte(`"published"`)) {
		t.Fatalf("publish status=%d body=%s", resp.Code, resp.Body.String())
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles/custom-1/revisions/1/retire", nil))
	if resp.Code != http.StatusOK || !bytes.Contains(resp.Body.Bytes(), []byte(`"retired"`)) {
		t.Fatalf("retire status=%d body=%s", resp.Code, resp.Body.String())
	}
}

func TestCreateProtocolProfileGeneratesIDWhenOmitted(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles-generated-id.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := profileTestEngine()
	create := func(body []byte) (db.ProtocolProfile, db.ProtocolProfileRevision) {
		t.Helper()
		resp := httptest.NewRecorder()
		r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles", bytes.NewReader(body)))
		if resp.Code != http.StatusCreated {
			t.Fatalf("create status=%d body=%s", resp.Code, resp.Body.String())
		}
		var response struct {
			Profile  db.ProtocolProfile         `json:"profile"`
			Revision db.ProtocolProfileRevision `json:"revision"`
		}
		if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response.Profile, response.Revision
	}

	profileBody := `{"name":"Generated","profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat","body_encoding":"json"},"response":{"result_paths":["id"]}}]}}`
	first, firstRevision := create([]byte(profileBody))
	second, _ := create([]byte(profileBody))
	if first.ID == "" || second.ID == "" || first.ID == second.ID {
		t.Fatalf("generated IDs must be non-empty and unique: first=%q second=%q", first.ID, second.ID)
	}
	if firstRevision.ProfileID != first.ID || firstRevision.Revision != 1 {
		t.Fatalf("unexpected returned initial revision: %+v", firstRevision)
	}
	persistedRevision, err := db.GetProtocolProfileRevision(first.ID, 1)
	if err != nil || persistedRevision.ProfileID != first.ID {
		t.Fatalf("generated profile revision was not persisted with its profile ID: revision=%+v err=%v", persistedRevision, err)
	}

	explicit, explicitRevision := create([]byte(`{"id":"copied-profile","name":"Copied","profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat","body_encoding":"json"},"response":{"result_paths":["id"]}}]}}`))
	if explicit.ID != "copied-profile" || explicitRevision.ProfileID != "copied-profile" {
		t.Fatalf("explicit profile ID was not preserved: profile=%+v revision=%+v", explicit, explicitRevision)
	}
}

func TestProfileRouteRejectsSensitiveHeaders(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles-sensitive.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := profileTestEngine()
	body := []byte(`{"id":"sensitive","profile":{"schema_version":1,"operations":[{"operation":"chat","execution_mode":"direct","polling_mode":"off","submit":{"method":"POST","path":"/v1/chat","headers":{"Authorization":"sk-secret"}},"response":{}}]}}`)
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/profiles", bytes.NewReader(body)))
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("sensitive header status=%d body=%s", resp.Code, resp.Body.String())
	}
}

func TestDeleteProtocolProfile(t *testing.T) {
	if err := db.InitDB(t.TempDir() + "/profiles-delete.db"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := profileTestEngine()

	// 1. Cannot delete built-in profile
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "builtin-test", Name: "Builtin", Source: db.ProfileSourceBuiltin}); err != nil {
		t.Fatal(err)
	}
	resp := httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodDelete, "/profiles/builtin-test", nil))
	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden for builtin profile, got %d body=%s", resp.Code, resp.Body.String())
	}

	// 2. Cannot delete profile with bindings
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "bound-custom", Name: "Bound", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveChannelProtocolBinding(&db.ChannelProtocolBinding{ChannelID: "ch-1", Operation: "chat", ModelPattern: "*", ProfileID: "bound-custom", ProfileRevision: 1, Enabled: true}); err != nil {
		t.Fatal(err)
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodDelete, "/profiles/bound-custom", nil))
	if resp.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for bound profile, got %d body=%s", resp.Code, resp.Body.String())
	}

	// 3. Successfully delete unreferenced custom profile
	if err := db.CreateProtocolProfile(&db.ProtocolProfile{ID: "unbound-custom", Name: "Unbound", Source: db.ProfileSourceCustom}); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveProtocolProfileRevision(&db.ProtocolProfileRevision{ProfileID: "unbound-custom", Revision: 1, SchemaVersion: 1, ContentJSON: "{}", ContentDigest: "d", State: db.ProfileRevisionRetired}); err != nil {
		t.Fatal(err)
	}
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodDelete, "/profiles/unbound-custom", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for unbound profile deletion, got %d body=%s", resp.Code, resp.Body.String())
	}

	// Verify profile and revisions are gone
	resp = httptest.NewRecorder()
	r.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/profiles/unbound-custom", nil))
	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after deletion, got %d", resp.Code)
	}
}
