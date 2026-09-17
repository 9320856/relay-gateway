package router

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/protocol"
)

const profileIDRandomBytes = 12

// handleListProtocolProfiles returns profiles and their latest revision metadata.
func handleListProtocolProfiles(c *gin.Context) {
	database := db.DBForContext(c.Request.Context())
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库尚未初始化"})
		return
	}
	var profiles []db.ProtocolProfile
	if err := database.WithContext(c.Request.Context()).Order("id ASC").Find(&profiles).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 列表失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": profiles})
}

func handleGetProtocolProfile(c *gin.Context) {
	id := strings.TrimSpace(c.Param("id"))
	profile, err := db.GetProtocolProfileContext(c.Request.Context(), id)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 失败"})
		return
	}
	result := gin.H{"profile": profile}
	if rev := strings.TrimSpace(c.Query("revision")); rev != "" {
		n, parseErr := strconv.Atoi(rev)
		if parseErr != nil || n <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "版本号无效"})
			return
		}
		item, getErr := db.GetProtocolProfileRevisionContext(c.Request.Context(), id, n)
		if errors.Is(getErr, gorm.ErrRecordNotFound) {
			c.Status(http.StatusNotFound)
			return
		}
		if getErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 版本失败"})
			return
		}
		result["revision"] = item
	}
	c.JSON(http.StatusOK, result)
}

// handleGetProtocolProfileRevisionReferences exposes the live references that
// keep a revision in use. The endpoint is read-only and intentionally counts
// both enabled/disabled bindings and active TaskRuns.
func handleGetProtocolProfileRevisionReferences(c *gin.Context) {
	profileID := strings.TrimSpace(c.Param("id"))
	revision, ok := parseProfileRevisionParam(c)
	if !ok {
		return
	}
	if _, err := db.GetProtocolProfileRevisionContext(c.Request.Context(), profileID, revision); errors.Is(err, gorm.ErrRecordNotFound) {
		c.Status(http.StatusNotFound)
		return
	} else if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 版本失败"})
		return
	}
	references, err := db.GetProtocolProfileRevisionReferencesContext(c.Request.Context(), profileID, revision)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询版本引用失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"profile_id": profileID, "revision": revision, "references": references})
}

type profileRevisionDiffEntry struct {
	Path   string `json:"path"`
	Before any    `json:"before"`
	After  any    `json:"after"`
}

// handleGetProtocolProfileRevisionDiff compares a revision with the revision
// named by ?against=. When omitted, the immediately preceding revision is
// used. The response is a deterministic list of changed JSON paths so the UI
// can render a compact review without embedding a second diff engine.
func handleGetProtocolProfileRevisionDiff(c *gin.Context) {
	profileID := strings.TrimSpace(c.Param("id"))
	toRevision, ok := parseProfileRevisionParam(c)
	if !ok {
		return
	}
	to, err := db.GetProtocolProfileRevisionContext(c.Request.Context(), profileID, toRevision)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 版本失败"})
		return
	}
	against := 0
	if raw := strings.TrimSpace(c.Query("against")); raw != "" {
		against, err = strconv.Atoi(raw)
		if err != nil || against <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "对比版本号无效"})
			return
		}
	} else if toRevision > 1 {
		against = toRevision - 1
	}
	changes := make([]profileRevisionDiffEntry, 0)
	if against > 0 {
		from, getErr := db.GetProtocolProfileRevisionContext(c.Request.Context(), profileID, against)
		if errors.Is(getErr, gorm.ErrRecordNotFound) {
			c.Status(http.StatusNotFound)
			return
		}
		if getErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "查询对比版本失败"})
			return
		}
		var before, after any
		if err := json.Unmarshal([]byte(from.ContentJSON), &before); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "对比版本内容无效"})
			return
		}
		if err := json.Unmarshal([]byte(to.ContentJSON), &after); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Profile 版本内容无效"})
			return
		}
		appendProfileRevisionDiff(&changes, "$", before, after)
	}
	c.JSON(http.StatusOK, gin.H{"profile_id": profileID, "from_revision": against, "to_revision": toRevision, "changes": changes})
}

func parseProfileRevisionParam(c *gin.Context) (int, bool) {
	revision, err := strconv.Atoi(strings.TrimSpace(c.Param("revision")))
	if err != nil || revision <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "版本号无效"})
		return 0, false
	}
	return revision, true
}

func appendProfileRevisionDiff(changes *[]profileRevisionDiffEntry, path string, before, after any) {
	beforeMap, beforeIsMap := before.(map[string]any)
	afterMap, afterIsMap := after.(map[string]any)
	if beforeIsMap && afterIsMap {
		keys := make(map[string]struct{}, len(beforeMap)+len(afterMap))
		for key := range beforeMap {
			keys[key] = struct{}{}
		}
		for key := range afterMap {
			keys[key] = struct{}{}
		}
		sorted := make([]string, 0, len(keys))
		for key := range keys {
			sorted = append(sorted, key)
		}
		sort.Strings(sorted)
		for _, key := range sorted {
			left, leftOK := beforeMap[key]
			right, rightOK := afterMap[key]
			switch {
			case leftOK && rightOK:
				appendProfileRevisionDiff(changes, path+"."+key, left, right)
			case leftOK:
				*changes = append(*changes, profileRevisionDiffEntry{Path: path + "." + key, Before: left})
			case rightOK:
				*changes = append(*changes, profileRevisionDiffEntry{Path: path + "." + key, After: right})
			}
		}
		return
	}
	beforeList, beforeIsList := before.([]any)
	afterList, afterIsList := after.([]any)
	if beforeIsList && afterIsList {
		limit := len(beforeList)
		if len(afterList) > limit {
			limit = len(afterList)
		}
		for i := 0; i < limit; i++ {
			itemPath := path + "[" + strconv.Itoa(i) + "]"
			switch {
			case i < len(beforeList) && i < len(afterList):
				appendProfileRevisionDiff(changes, itemPath, beforeList[i], afterList[i])
			case i < len(beforeList):
				*changes = append(*changes, profileRevisionDiffEntry{Path: itemPath, Before: beforeList[i]})
			default:
				*changes = append(*changes, profileRevisionDiffEntry{Path: itemPath, After: afterList[i]})
			}
		}
		return
	}
	if !reflect.DeepEqual(before, after) {
		*changes = append(*changes, profileRevisionDiffEntry{Path: path, Before: before, After: after})
	}
}

type createProfileRequest struct {
	ID       string           `json:"id"`
	Name     string           `json:"name"`
	Source   string           `json:"source"`
	Revision int              `json:"revision"`
	Profile  protocol.Profile `json:"profile"`
}

type profileRevisionRequest struct {
	Revision int              `json:"revision"`
	Profile  protocol.Profile `json:"profile"`
}

// handleCreateProtocolProfile creates a profile and its first draft revision.
// The protocol compiler rejects sensitive headers (including Authorization), so
// credentials are never persisted in profile content.
func handleCreateProtocolProfile(c *gin.Context) {
	var input createProfileRequest
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 请求格式无效"})
		return
	}
	input.ID = strings.TrimSpace(input.ID)
	if input.ID == "" {
		id, generateErr := generateProfileID()
		if generateErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "生成 Profile 标识失败"})
			return
		}
		input.ID = id
	}
	if len(input.ID) > 64 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 标识长度必须有效"})
		return
	}
	compiled, err := protocol.Compile(input.Profile)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 配置无效"})
		return
	}
	if input.Source == "" {
		input.Source = db.ProfileSourceCustom
	}
	if input.Source != db.ProfileSourceCustom {
		c.JSON(http.StatusBadRequest, gin.H{"error": "只能创建自定义 Profile"})
		return
	}
	revision := input.Revision
	if revision <= 0 {
		revision = 1
	}
	profile := &db.ProtocolProfile{ID: input.ID, Name: strings.TrimSpace(input.Name), Source: input.Source}
	rev := &db.ProtocolProfileRevision{ProfileID: input.ID, Revision: revision, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionDraft}
	if err := db.CreateProtocolProfileWithInitialRevisionContext(c.Request.Context(), profile, rev); err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			status = http.StatusConflict
		}
		c.JSON(status, gin.H{"error": "创建 Profile 失败"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"profile": profile, "revision": rev})
}

// generateProfileID returns an opaque identifier that fits the persisted
// profile ID limit. Explicit IDs remain supported by the create endpoint for
// imports and copy flows.
func generateProfileID() (string, error) {
	randomBytes := make([]byte, profileIDRandomBytes)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", err
	}
	return "profile-" + hex.EncodeToString(randomBytes), nil
}

// handleCreateProtocolProfileRevision creates the next draft revision for an
// existing profile. A caller may provide an explicit revision number when
// importing a draft; otherwise the profile's latest revision is incremented.
func handleCreateProtocolProfileRevision(c *gin.Context) {
	profileID := strings.TrimSpace(c.Param("id"))
	profile, err := db.GetProtocolProfileContext(c.Request.Context(), profileID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 失败"})
		return
	}
	var input profileRevisionRequest
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "版本请求格式无效"})
		return
	}
	compiled, err := protocol.Compile(input.Profile)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 配置无效"})
		return
	}
	revision := input.Revision
	if revision <= 0 {
		revision = profile.LatestRevision + 1
	}
	rev := &db.ProtocolProfileRevision{ProfileID: profileID, Revision: revision, SchemaVersion: compiled.Profile().SchemaVersion, ContentJSON: string(compiled.CanonicalJSON()), ContentDigest: compiled.Digest(), State: db.ProfileRevisionDraft}
	if err := db.SaveProtocolProfileRevisionContext(c.Request.Context(), rev); err != nil {
		if errors.Is(err, db.ErrPublishedRevisionImmutable) || errors.Is(err, db.ErrRetiredRevisionImmutable) || strings.Contains(strings.ToLower(err.Error()), "unique") {
			c.JSON(http.StatusConflict, gin.H{"error": "版本已存在或不可修改"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建 Profile 版本失败"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"revision": rev})
}

// handleUpdateProtocolProfileRevision updates a draft revision in place. The
// database layer remains the final guard against published/retired mutation.
func handleUpdateProtocolProfileRevision(c *gin.Context) {
	profileID := strings.TrimSpace(c.Param("id"))
	revisionNumber, err := strconv.Atoi(c.Param("revision"))
	if err != nil || revisionNumber <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "版本号无效"})
		return
	}
	existing, err := db.GetProtocolProfileRevisionContext(c.Request.Context(), profileID, revisionNumber)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 版本失败"})
		return
	}
	if existing.State != db.ProfileRevisionDraft {
		c.JSON(http.StatusConflict, gin.H{"error": "只能编辑草稿版本"})
		return
	}
	var input profileRevisionRequest
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "版本请求格式无效"})
		return
	}
	compiled, err := protocol.Compile(input.Profile)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 配置无效"})
		return
	}
	existing.SchemaVersion = compiled.Profile().SchemaVersion
	existing.ContentJSON = string(compiled.CanonicalJSON())
	existing.ContentDigest = compiled.Digest()
	if err := db.SaveProtocolProfileRevisionContext(c.Request.Context(), existing); err != nil {
		if errors.Is(err, db.ErrPublishedRevisionImmutable) || errors.Is(err, db.ErrRetiredRevisionImmutable) {
			c.JSON(http.StatusConflict, gin.H{"error": "该版本不可修改"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "更新 Profile 版本失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"revision": existing})
}

func handlePublishProtocolProfileRevision(c *gin.Context) {
	handleProfileRevisionStateChange(c, true)
}

func handleRetireProtocolProfileRevision(c *gin.Context) {
	handleProfileRevisionStateChange(c, false)
}

func handleDeleteProtocolProfileRevision(c *gin.Context) {
	profileID := strings.TrimSpace(c.Param("id"))
	revision, err := strconv.Atoi(strings.TrimSpace(c.Param("revision")))
	if profileID == "" || revision <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "版本号无效"})
		return
	}
	err = db.DeleteProtocolProfileRevisionContext(c.Request.Context(), profileID, revision)
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		c.Status(http.StatusNotFound)
	case errors.Is(err, db.ErrBuiltinProfileImmutable):
		c.JSON(http.StatusForbidden, gin.H{"error": "内置 Profile 版本不允许删除"})
	case errors.Is(err, db.ErrRevisionHasBindings):
		c.JSON(http.StatusConflict, gin.H{"error": "该版本已有渠道绑定，不能删除", "detail": err.Error()})
	case errors.Is(err, db.ErrRevisionHasActiveTasks):
		c.JSON(http.StatusConflict, gin.H{"error": "该版本仍有未完成任务，不能删除", "detail": err.Error()})
	case errors.Is(err, db.ErrProfileHasTaskRuns):
		c.JSON(http.StatusConflict, gin.H{"error": "该版本仍有任务记录，不能删除", "detail": err.Error()})
	case errors.Is(err, db.ErrInvalidRevisionState):
		c.JSON(http.StatusConflict, gin.H{"error": "Profile 版本状态不允许删除"})
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "删除 Profile 版本失败"})
	default:
		c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "Profile Revision 已删除", "profile_id": profileID, "revision": revision})
	}
}

func handleProfileRevisionStateChange(c *gin.Context, publish bool) {
	revision, err := strconv.Atoi(c.Param("revision"))
	if err != nil || revision <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "版本号无效"})
		return
	}
	profileID := strings.TrimSpace(c.Param("id"))
	if publish {
		err = db.PublishProtocolProfileRevisionContext(c.Request.Context(), profileID, revision)
	} else {
		err = db.RetireProtocolProfileRevisionContext(c.Request.Context(), profileID, revision)
	}
	if errors.Is(err, db.ErrRevisionHasActiveBindings) {
		c.JSON(http.StatusConflict, gin.H{"error": "该版本仍有启用中的渠道绑定，请在下方渠道绑定列表禁用或迁移后再停用。渠道类型与 Profile 绑定是独立配置。"})
		return
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "Profile 版本状态无法切换"})
		return
	}
	item, getErr := db.GetProtocolProfileRevisionContext(c.Request.Context(), profileID, revision)
	if getErr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 版本失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"revision": item})
}

func handleDeleteProtocolProfile(c *gin.Context) {
	profileID := strings.TrimSpace(c.Param("id"))
	if profileID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 标识不能为空"})
		return
	}
	err := db.DeleteProtocolProfileContext(c.Request.Context(), profileID)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if errors.Is(err, db.ErrBuiltinProfileImmutable) {
		c.JSON(http.StatusForbidden, gin.H{"error": "内置 Profile 不允许删除"})
		return
	}
	if errors.Is(err, db.ErrProfileHasBindings) || errors.Is(err, db.ErrProfileHasTaskRuns) {
		c.JSON(http.StatusConflict, gin.H{"error": "该 Profile 仍有未完成任务或渠道绑定，不能删除。请先等待任务结束并禁用或删除渠道绑定。", "detail": err.Error()})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "删除 Profile 失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "message": "Profile 已删除", "profile_id": profileID})
}
