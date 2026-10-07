package adapter

import (
	"context"
	"errors"
	"net/http"
	"relay-gateway/config"
)

// VideoContentOpener is optional: existing Adapter implementations remain
// source compatible. The caller owns and closes the successful response body.
type VideoContentOpener interface {
	OpenVideoContent(context.Context, *config.UpstreamChannel, string, *http.Request) (*http.Response, error)
}

func (a *OpenAIAdapter) OpenVideoContent(ctx context.Context, channel *config.UpstreamChannel, taskID string, req *http.Request) (*http.Response, error) {
	return a.openVideoContentRequest(ctx, channel, a.NormalizeURL(channel.BaseURL, "/videos/"+taskID+"/content"), req)
}

func (a *NewAPIAdapter) OpenVideoContent(ctx context.Context, channel *config.UpstreamChannel, taskID string, req *http.Request) (*http.Response, error) {
	return a.openVideoContentRequest(ctx, channel, a.NormalizeURL(channel.BaseURL, "/videos/generations/"+taskID+"/content"), req)
}

func (a *OpenAIAdapter) openVideoContentRequest(ctx context.Context, channel *config.UpstreamChannel, targetURL string, req *http.Request) (*http.Response, error) {
	method := http.MethodGet
	headers := map[string]string{}
	if req != nil {
		if req.Method == http.MethodHead {
			method = http.MethodHead
		}
		if value := req.Header.Get("Range"); value != "" {
			headers["Range"] = value
		}
	}
	response, err := a.openVideoContentURL(ctx, channel, method, targetURL, headers)
	if method == http.MethodHead && shouldFallbackVideoHEAD(ctx, err) {
		return a.openVideoContentURL(ctx, channel, http.MethodGet, targetURL, headers)
	}
	return response, err
}

func (a *OpenAIAdapter) openVideoContentURL(ctx context.Context, channel *config.UpstreamChannel, method, targetURL string, headers map[string]string) (*http.Response, error) {
	var opened *http.Response
	err := a.ExecuteWithKeyRotation(ctx, channel, func(key string) (*http.Request, error) {
		request, err := http.NewRequestWithContext(ctx, method, targetURL, nil)
		if err != nil {
			return nil, err
		}
		a.SetHeadersWithKey(request, channel, key)
		for name, value := range headers {
			request.Header.Set(name, value)
		}
		return request, nil
	}, func(response *http.Response, _ string) error {
		if response.Body == nil {
			return errors.New("upstream content body is missing")
		}
		if isVideoContentRedirect(response.StatusCode) {
			if _, err := resolveVideoContentRedirect(response); err != nil {
				response.Body.Close()
				return err
			}
		} else if response.StatusCode < 200 || response.StatusCode >= 300 {
			defer response.Body.Close()
			body, err := readUpstreamErrorBody(response.Body)
			if err != nil {
				return err
			}
			return &UpstreamHTTPError{StatusCode: response.StatusCode, ContentType: response.Header.Get("Content-Type"), Body: string(body), RetryAfter: response.Header.Get("Retry-After")}
		}
		opened = response
		return nil
	})
	return opened, err
}
