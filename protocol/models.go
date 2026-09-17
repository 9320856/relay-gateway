package protocol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxModelDiscoveryResponse = 16 << 20

// DiscoverModels performs the conventional OpenAI-compatible GET /models
// request without consulting the Legacy Adapter registry. A profile's model
// defaults are used only when the upstream explicitly reports that /models is
// unavailable (404/405); transport failures and 5xx responses remain errors.
// Profiles can supply fallback model defaults while sharing one discovery
// implementation across all migrated channels.
func DiscoverModels(ctx context.Context, baseURL string, apiKeys []string, headers map[string]string, defaults []string) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	base, err := parseBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	endpoint := *base
	endpoint.Path = strings.TrimRight(base.Path, "/") + "/models"
	endpoint.RawPath = ""
	client := &http.Client{Timeout: 15 * time.Second}
	keys := normalizedKeys(apiKeys)
	if len(keys) == 0 {
		keys = []string{""}
	}
	var lastErr error
	for _, key := range keys {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
		if reqErr != nil {
			return nil, reqErr
		}
		for name, value := range headers {
			req.Header.Set(name, value)
		}
		if strings.TrimSpace(key) != "" {
			req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(key))
		}
		resp, doErr := client.Do(req)
		if doErr != nil {
			return nil, executorError("models", 0, false, false, doErr)
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxModelDiscoveryResponse+1))
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, executorError("models", resp.StatusCode, false, false, readErr)
		}
		if len(body) > maxModelDiscoveryResponse {
			return nil, ErrResponseTooLarge
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			lastErr = executorError("models", resp.StatusCode, false, true, errors.New("upstream rejected model discovery credentials"))
			continue
		}
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
			if models := normalizeModelList(defaults); len(models) > 0 {
				return models, nil
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, executorError("models", resp.StatusCode, resp.StatusCode >= 500, false, fmt.Errorf("upstream returned status %d", resp.StatusCode))
		}
		models, parseErr := parseModelList(body)
		if parseErr != nil {
			return nil, parseErr
		}
		return models, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return normalizeModelList(defaults), nil
}

func parseModelList(body []byte) ([]string, error) {
	var envelope struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("decode models response: %w", err)
	}
	models := make([]string, 0, len(envelope.Data))
	for _, raw := range envelope.Data {
		var item struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &item) == nil && strings.TrimSpace(item.ID) != "" {
			models = append(models, item.ID)
			continue
		}
		var id string
		if json.Unmarshal(raw, &id) == nil && strings.TrimSpace(id) != "" {
			models = append(models, id)
		}
	}
	return normalizeModelList(models), nil
}

func normalizeModelList(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := strings.ToLower(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

// ProfileModelDefaults returns a defensive copy of the built-in fallback list
// for a channel type. Unknown/custom types deliberately return no defaults.
func ProfileModelDefaults(channelType string) []string {
	profile, err := BuiltinPreset(strings.ToLower(strings.TrimSpace(channelType)))
	if err != nil {
		return nil
	}
	return normalizeModelList(profile.ModelDefaults)
}
