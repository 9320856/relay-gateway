package adapter

import (
	"context"
	"net/http"

	"relay-gateway/config"
)

// Responses forwards the OpenAI Responses API without attempting to convert
// its item/event format.  The API is intentionally passed through as JSON/SSE
// because Responses contains provider-specific fields (tools, output items,
// reasoning summaries, etc.) that should not be lossy-decoded by the gateway.
func (a *OpenAIAdapter) Responses(ctx context.Context, channel *config.UpstreamChannel, rawBody []byte, targetModel string, isStream bool, w http.ResponseWriter) error {
	if mapped, ok := channel.ModelMap[targetModel]; ok && mapped != "" {
		targetModel = mapped
	}
	bodyToSend := RewriteJSONModel(rawBody, targetModel)
	targetURL := a.NormalizeURL(channel.BaseURL, "/responses")
	return a.ForwardHTTPRequest(ctx, channel, http.MethodPost, targetURL, bodyToSend, nil, isStream, w)
}
