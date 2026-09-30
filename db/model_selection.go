package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

// ParseSelectedModels distinguishes legacy channels (nil) from an explicit
// empty selection ([]). Only concrete upstream model names can be selected.
func ParseSelectedModels(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var models []string
	if err := json.Unmarshal([]byte(raw), &models); err != nil || models == nil {
		return nil, errors.New("selected_models must be a JSON string array")
	}
	selected := make([]string, 0, len(models))
	seen := make(map[string]bool, len(models))
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" || strings.ContainsAny(model, "*?[") {
			return nil, errors.New("selected_models must contain non-empty concrete model names")
		}
		if !seen[model] {
			seen[model] = true
			selected = append(selected, model)
		}
	}
	return selected, nil
}

// Compare credentials on the caller's transaction. ToUpstreamChannel would
// hydrate via the global database and deadlock SQLite's single connection.
func channelDiscoveryInputsUnchanged(tx *gorm.DB, existing, next *ChannelModel, keys []string) (bool, error) {
	if existing.Type != next.Type || existing.BaseURL != next.BaseURL || existing.AnthropicVersion != next.AnthropicVersion || existing.FetchModels != next.FetchModels {
		return false, nil
	}
	var previousHeaders, nextHeaders map[string]string
	if strings.TrimSpace(existing.HeadersRaw) != "" {
		if err := json.Unmarshal([]byte(existing.HeadersRaw), &previousHeaders); err != nil {
			return false, nil
		}
	}
	if strings.TrimSpace(next.HeadersRaw) != "" {
		if err := json.Unmarshal([]byte(next.HeadersRaw), &nextHeaders); err != nil {
			return false, err
		}
	}
	if len(previousHeaders) != len(nextHeaders) || (len(previousHeaders) > 0 && !reflect.DeepEqual(previousHeaders, nextHeaders)) {
		return false, nil
	}
	var storedKeys []ChannelKeyModel
	if err := tx.Where("channel_id = ?", existing.ID).Order("position asc").Find(&storedKeys).Error; err != nil {
		return false, err
	}
	previousKeys := make([]string, 0, len(storedKeys))
	for _, key := range storedKeys {
		secret, err := decryptChannelSecret(key.Secret)
		if err != nil {
			return false, fmt.Errorf("cannot compare channel credential: %w", err)
		}
		if secret = strings.TrimSpace(secret); secret != "" {
			previousKeys = append(previousKeys, secret)
		}
	}
	nextKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if key = strings.TrimSpace(key); key != "" {
			nextKeys = append(nextKeys, key)
		}
	}
	return reflect.DeepEqual(previousKeys, nextKeys), nil
}
