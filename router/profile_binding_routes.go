package router

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"relay-gateway/db"
	"relay-gateway/protocol"
)

type bindChannelProfileRequest struct {
	ProfileID       string `json:"profile_id"`
	ProfileRevision int    `json:"profile_revision"`
}

type profileBindingRequest struct {
	ChannelID       string `json:"channel_id"`
	Operation       string `json:"operation"`
	ModelPattern    string `json:"model_pattern"`
	ProfileID       string `json:"profile_id"`
	ProfileRevision int    `json:"profile_revision"`
	Precedence      int    `json:"precedence"`
	Enabled         *bool  `json:"enabled"`
}

func handleListChannelProtocolBindings(c *gin.Context) {
	database := db.DBForContext(c.Request.Context())
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库尚未初始化"})
		return
	}
	query := database.WithContext(c.Request.Context()).Model(&db.ChannelProtocolBinding{})
	for key, column := range map[string]string{"channel_id": "channel_id", "profile_id": "profile_id", "operation": "operation"} {
		if value := strings.TrimSpace(c.Query(key)); value != "" {
			query = query.Where(column+" = ?", value)
		}
	}
	var bindings []db.ChannelProtocolBinding
	if err := query.Order("precedence DESC, id ASC").Find(&bindings).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询渠道绑定失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": bindings})
}

// handleBindChannelProfile applies every operation from one published Profile
// revision to a channel using a wildcard model rule. The channel's adapter type
// remains independent: it still controls model discovery and credentials,
// while this binding controls how each operation is sent upstream.
func handleBindChannelProfile(c *gin.Context) {
	channelID := strings.TrimSpace(c.Param("id"))
	if channelID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "渠道标识不能为空"})
		return
	}
	var input bindChannelProfileRequest
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 绑定请求格式无效"})
		return
	}
	input.ProfileID = strings.TrimSpace(input.ProfileID)
	if len(input.ProfileID) > 64 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 标识长度超出限制"})
		return
	}
	ctx := c.Request.Context()
	database := db.DBForContext(ctx)
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库尚未初始化"})
		return
	}
	var channel db.ChannelModel
	if err := database.WithContext(ctx).Where("id = ?", channelID).First(&channel).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "渠道不存在"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询渠道失败"})
		return
	}

	// When profile_id is empty, unbind custom profile bindings for this channel
	// to revert cleanly to built-in default precedence.
	if input.ProfileID == "" {
		unbind := func(txCtx context.Context) error {
			txDB := db.DBForContext(txCtx)
			return txDB.WithContext(txCtx).
				Where("channel_id = ? AND precedence = 0 AND model_pattern = ?", channelID, "*").
				Delete(&db.ChannelProtocolBinding{}).Error
		}
		if database == db.DB {
			if err := database.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return unbind(db.WithTx(ctx, tx)) }); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "清除 Profile 绑定失败"})
				return
			}
		} else if err := unbind(ctx); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "清除 Profile 绑定失败"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"channel_id": channelID, "profile_id": "", "bindings": []db.ChannelProtocolBinding{}})
		return
	}

	var profile db.ProtocolProfile
	if err := database.WithContext(ctx).Where("id = ?", input.ProfileID).First(&profile).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 不存在"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 失败"})
		return
	}
	var revision db.ProtocolProfileRevision
	revisionQuery := database.WithContext(ctx).Where("profile_id = ? AND state = ?", input.ProfileID, db.ProfileRevisionPublished)
	if input.ProfileRevision > 0 {
		revisionQuery = revisionQuery.Where("revision = ?", input.ProfileRevision)
	} else {
		revisionQuery = revisionQuery.Order("revision DESC")
	}
	if err := revisionQuery.First(&revision).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if input.ProfileRevision > 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 版本必须先发布"})
			} else {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 尚无已发布版本"})
			}
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询 Profile 版本失败"})
		return
	}
	var source protocol.Profile
	if err := json.Unmarshal([]byte(revision.ContentJSON), &source); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 版本内容无效"})
		return
	}
	compiled, err := protocol.Compile(source)
	if err != nil || len(compiled.Profile().Operations) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Profile 版本内容无效"})
		return
	}

	bind := func(txCtx context.Context) error {
		txDB := db.DBForContext(txCtx)
		newOpList := make([]string, 0, len(compiled.Profile().Operations))
		for _, op := range compiled.Profile().Operations {
			newOpList = append(newOpList, op.Operation)
		}
		// Delete any existing precedence 0 wildcard bindings for operations
		// not in the new profile revision to prevent orphan bindings from lingering.
		if len(newOpList) > 0 {
			if err := txDB.WithContext(txCtx).
				Where("channel_id = ? AND precedence = 0 AND model_pattern = ? AND operation NOT IN ?", channelID, "*", newOpList).
				Delete(&db.ChannelProtocolBinding{}).Error; err != nil {
				return err
			}
		}
		for _, operation := range compiled.Profile().Operations {
			var existing db.ChannelProtocolBinding
			lookup := txDB.WithContext(txCtx).
				Where("channel_id = ? AND operation = ? AND model_pattern = ? AND precedence = ?", channelID, operation.Operation, "*", 0).
				First(&existing)
			switch {
			case lookup.Error == nil:
				existing.ProfileID = input.ProfileID
				existing.ProfileRevision = revision.Revision
				existing.Enabled = true
				if err := db.SaveChannelProtocolBindingContext(txCtx, &existing); err != nil {
					return err
				}
			case errors.Is(lookup.Error, gorm.ErrRecordNotFound):
				if err := db.SaveChannelProtocolBindingContext(txCtx, &db.ChannelProtocolBinding{
					ChannelID: channelID, Operation: operation.Operation, ModelPattern: "*",
					ProfileID: input.ProfileID, ProfileRevision: revision.Revision,
					Precedence: 0, Enabled: true,
				}); err != nil {
					return err
				}
			default:
				return lookup.Error
			}
		}
		return nil
	}
	if database == db.DB {
		if err := database.WithContext(ctx).Transaction(func(tx *gorm.DB) error { return bind(db.WithTx(ctx, tx)) }); err != nil {
			if errors.Is(err, db.ErrBindingConflict) {
				c.JSON(http.StatusConflict, gin.H{"error": "渠道已有同优先级的 Profile 绑定规则，请先调整或删除后重试"})
				return
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "应用 Profile 绑定失败"})
			return
		}
	} else if err := bind(ctx); err != nil {
		if errors.Is(err, db.ErrBindingConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "渠道已有同优先级的 Profile 绑定规则，请先调整或删除后重试"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "应用 Profile 绑定失败"})
		return
	}
	var bindings []db.ChannelProtocolBinding
	if err := db.DBForContext(ctx).WithContext(ctx).
		Where("channel_id = ? AND profile_id = ? AND profile_revision = ? AND model_pattern = ? AND precedence = ?", channelID, input.ProfileID, revision.Revision, "*", 0).
		Order("operation ASC").Find(&bindings).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取 Profile 绑定失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"channel_id": channelID, "profile_id": input.ProfileID, "profile_revision": revision.Revision, "bindings": bindings})
}

func handleGetChannelProtocolBinding(c *gin.Context) {
	id, ok := parseBindingID(c)
	if !ok {
		return
	}
	database := db.DBForContext(c.Request.Context())
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库尚未初始化"})
		return
	}
	var binding db.ChannelProtocolBinding
	err := database.WithContext(c.Request.Context()).First(&binding, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c.Status(http.StatusNotFound)
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询渠道绑定失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"binding": binding})
}

func handleCreateChannelProtocolBinding(c *gin.Context) {
	var input profileBindingRequest
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "渠道绑定请求格式无效"})
		return
	}
	binding, status, err := validateBindingRequest(c, input)
	if err != nil {
		c.JSON(status, gin.H{"error": bindingRequestErrorMessage(err)})
		return
	}
	if input.Enabled != nil {
		binding.Enabled = *input.Enabled
	} else {
		binding.Enabled = true
	}
	if err := db.SaveChannelProtocolBindingContext(c.Request.Context(), binding); err != nil {
		if errors.Is(err, db.ErrBindingConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "渠道绑定规则冲突"})
			return
		}
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			c.JSON(http.StatusConflict, gin.H{"error": "渠道绑定已存在"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "创建渠道绑定失败"})
		return
	}
	c.JSON(http.StatusCreated, gin.H{"binding": binding})
}

func handleUpdateChannelProtocolBinding(c *gin.Context) {
	id, ok := parseBindingID(c)
	if !ok {
		return
	}
	database := db.DBForContext(c.Request.Context())
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库尚未初始化"})
		return
	}
	var existing db.ChannelProtocolBinding
	if err := database.WithContext(c.Request.Context()).First(&existing, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.Status(http.StatusNotFound)
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询渠道绑定失败"})
		return
	}
	var input profileBindingRequest
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "渠道绑定请求格式无效"})
		return
	}
	binding, status, err := validateBindingRequest(c, input)
	if err != nil {
		c.JSON(status, gin.H{"error": bindingRequestErrorMessage(err)})
		return
	}
	binding.ID, binding.CreatedAt, binding.Enabled = existing.ID, existing.CreatedAt, existing.Enabled
	if input.Enabled != nil {
		binding.Enabled = *input.Enabled
	}
	if err := db.SaveChannelProtocolBindingContext(c.Request.Context(), binding); err != nil {
		if errors.Is(err, db.ErrBindingConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "渠道绑定规则冲突"})
			return
		}
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			c.JSON(http.StatusConflict, gin.H{"error": "渠道绑定已存在"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "更新渠道绑定失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"binding": binding})
}

func handleToggleChannelProtocolBinding(c *gin.Context) {
	id, ok := parseBindingID(c)
	if !ok {
		return
	}
	database := db.DBForContext(c.Request.Context())
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库尚未初始化"})
		return
	}
	database = database.WithContext(c.Request.Context())
	var binding db.ChannelProtocolBinding
	if err := database.First(&binding, id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.Status(http.StatusNotFound)
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询渠道绑定失败"})
		return
	}
	updated, err := db.SetChannelProtocolBindingEnabledContext(c.Request.Context(), binding.ID, !binding.Enabled)
	if err != nil {
		if errors.Is(err, db.ErrBindingConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "渠道绑定规则冲突"})
			return
		}
		if errors.Is(err, db.ErrRetiredRevisionImmutable) {
			c.JSON(http.StatusConflict, gin.H{"error": "该 Profile 版本已停用，不能重新启用绑定"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "更新渠道绑定失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"binding": updated})
}

func handleDeleteChannelProtocolBinding(c *gin.Context) {
	id, ok := parseBindingID(c)
	if !ok {
		return
	}
	database := db.DBForContext(c.Request.Context())
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "数据库尚未初始化"})
		return
	}
	result := database.WithContext(c.Request.Context()).Delete(&db.ChannelProtocolBinding{}, id)
	if result.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "删除渠道绑定失败"})
		return
	}
	if result.RowsAffected == 0 {
		c.Status(http.StatusNotFound)
		return
	}
	c.Status(http.StatusNoContent)
}

func parseBindingID(c *gin.Context) (uint, bool) {
	n, err := strconv.ParseUint(strings.TrimSpace(c.Param("id")), 10, 32)
	if err != nil || n == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "渠道绑定编号无效"})
		return 0, false
	}
	return uint(n), true
}

func validateBindingRequest(c *gin.Context, input profileBindingRequest) (*db.ChannelProtocolBinding, int, error) {
	input.ChannelID, input.Operation, input.ModelPattern, input.ProfileID = strings.TrimSpace(input.ChannelID), strings.TrimSpace(input.Operation), strings.TrimSpace(input.ModelPattern), strings.TrimSpace(input.ProfileID)
	if input.ChannelID == "" || len(input.ChannelID) > 64 || input.Operation == "" || len(input.Operation) > 64 || input.ModelPattern == "" || len(input.ModelPattern) > 255 || input.ProfileID == "" || len(input.ProfileID) > 64 || input.ProfileRevision <= 0 {
		return nil, http.StatusBadRequest, errors.New("渠道绑定字段不能为空且长度必须有效")
	}
	ctx := c.Request.Context()
	if _, err := db.GetChannelModelContext(ctx, input.ChannelID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, http.StatusBadRequest, errors.New("渠道不存在")
		}
		return nil, http.StatusInternalServerError, errors.New("查询渠道失败")
	}
	if _, err := db.GetProtocolProfileContext(ctx, input.ProfileID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, http.StatusBadRequest, errors.New("Profile 不存在")
		}
		return nil, http.StatusInternalServerError, errors.New("查询 Profile 失败")
	}
	revision, err := db.GetProtocolProfileRevisionContext(ctx, input.ProfileID, input.ProfileRevision)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, http.StatusBadRequest, errors.New("Profile 版本不存在")
	}
	if err != nil {
		return nil, http.StatusInternalServerError, errors.New("查询 Profile 版本失败")
	}
	if revision.State != db.ProfileRevisionPublished {
		return nil, http.StatusBadRequest, errors.New("Profile 版本必须先发布")
	}
	var source protocol.Profile
	if err := json.Unmarshal([]byte(revision.ContentJSON), &source); err != nil {
		return nil, http.StatusBadRequest, errors.New("Profile 版本内容无效")
	}
	compiled, err := protocol.Compile(source)
	if err != nil {
		return nil, http.StatusBadRequest, errors.New("Profile 版本内容无效")
	}
	for _, operation := range compiled.Profile().Operations {
		if operation.Operation == input.Operation {
			return &db.ChannelProtocolBinding{ChannelID: input.ChannelID, Operation: input.Operation, ModelPattern: input.ModelPattern, ProfileID: input.ProfileID, ProfileRevision: input.ProfileRevision, Precedence: input.Precedence}, http.StatusOK, nil
		}
	}
	return nil, http.StatusBadRequest, errors.New("该 Profile 版本未定义所选操作")
}

func bindingRequestErrorMessage(err error) string {
	if err == nil {
		return "渠道绑定请求无效"
	}
	return err.Error()
}

func handleReconcileChannelBindings(c *gin.Context) {
	channelID := strings.TrimSpace(c.Param("id"))
	if channelID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "渠道标识不能为空"})
		return
	}
	pruneMismatched := true
	if pruneParam := c.Query("prune"); pruneParam == "false" || pruneParam == "0" {
		pruneMismatched = false
	}
	if err := ReconcileChannelProfileBindings(c.Request.Context(), channelID, pruneMismatched); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "同步渠道默认绑定失败"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "渠道默认绑定已同步", "channel_id": channelID})
}
