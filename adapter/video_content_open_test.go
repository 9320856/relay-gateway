package adapter

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"relay-gateway/config"
)

type videoOpenTrackedBody struct {
	reader io.Reader
	reads  int
	closes int
}

func (body *videoOpenTrackedBody) Read(buffer []byte) (int, error) {
	body.reads++
	return body.reader.Read(buffer)
}

func (body *videoOpenTrackedBody) Close() error {
	body.closes++
	return nil
}

func newVideoOpenTrackedBody(content string) *videoOpenTrackedBody {
	return &videoOpenTrackedBody{reader: strings.NewReader(content)}
}

func newVideoContentOpenerForTest(kind string, client *http.Client) VideoContentOpener {
	base := &OpenAIAdapter{Client: client}
	if kind == "newapi" {
		return &NewAPIAdapter{OpenAIAdapter: base}
	}
	return base
}

func videoOpenTestResponse(request *http.Request, status int, body io.ReadCloser) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: body, Request: request}
}

func TestOpenVideoContentUsesAdapterPathAndReturnsUnreadCallerOwnedBody(t *testing.T) {
	for _, kind := range []string{"openai", "newapi"} {
		for _, method := range []string{"worker", http.MethodGet, http.MethodHead} {
			t.Run(kind+"/"+method, func(t *testing.T) {
				body := newVideoOpenTrackedBody("streamed-video-content")
				channel := &config.UpstreamChannel{
					ID: t.Name(), Type: kind, BaseURL: "https://provider.example/v1/", APIKey: "provider-key",
					Headers: map[string]string{"X-Provider-Account": "account-1", "Authorization": "must-not-override", "Cookie": "must-not-forward"},
				}
				var attempts int
				opener := newVideoContentOpenerForTest(kind, &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					attempts++
					wantPath := "/v1/videos/provider-task/content"
					if kind == "newapi" {
						wantPath = "/v1/videos/generations/provider-task/content"
					}
					if request.URL.Host != "provider.example" || request.URL.Path != wantPath || request.URL.RawQuery != "" {
						t.Fatalf("upstream URL = %s, want pinned adapter path %s", request.URL, wantPath)
					}
					wantMethod, wantRange := http.MethodGet, ""
					if method != "worker" {
						wantMethod, wantRange = method, "bytes=4-7"
					}
					if request.Method != wantMethod || request.Header.Get("Range") != wantRange {
						t.Fatalf("upstream method/range = %s %q, want %s %q", request.Method, request.Header.Get("Range"), wantMethod, wantRange)
					}
					if request.Body != nil || request.Header.Get("Content-Type") != "" {
						t.Fatal("content GET/HEAD must not advertise a request body")
					}
					if request.Header.Get("Authorization") != "Bearer provider-key" || request.Header.Get("X-Provider-Account") != "account-1" {
						t.Fatalf("provider authentication/custom headers were not preserved: %v", request.Header)
					}
					if request.Header.Get("Cookie") != "" || request.Header.Get("X-Client-Secret") != "" {
						t.Fatal("client or protected custom credentials leaked upstream")
					}
					response := videoOpenTestResponse(request, http.StatusPartialContent, body)
					response.Header.Set("Content-Type", "video/webm")
					response.Header.Set("Content-Range", "bytes 4-7/100")
					response.Header.Set("ETag", `"video-version"`)
					return response, nil
				})})
				var incoming *http.Request
				if method != "worker" {
					incoming = httptest.NewRequest(method, "https://gateway.example/v1/videos/untrusted-task/content?url=https://other.example", nil)
					incoming.Header.Set("Range", "bytes=4-7")
					incoming.Header.Set("Authorization", "Bearer gateway-key")
					incoming.Header.Set("Cookie", "gateway-session")
					incoming.Header.Set("X-Client-Secret", "client-secret")
				}
				response, err := opener.OpenVideoContent(context.Background(), channel, "provider-task", incoming)
				if err != nil {
					t.Fatal(err)
				}
				if attempts != 1 || response == nil || response.StatusCode != http.StatusPartialContent {
					t.Fatalf("opened response = %v, attempts = %d", response, attempts)
				}
				if response.Header.Get("Content-Range") != "bytes 4-7/100" || response.Header.Get("ETag") != `"video-version"` || response.Header.Get("Content-Type") != "video/webm" {
					t.Fatalf("upstream media response metadata lost: %v", response.Header)
				}
				if body.reads != 0 || body.closes != 0 {
					t.Fatalf("opening buffered or closed media: reads=%d closes=%d", body.reads, body.closes)
				}
				if response.Body != body {
					t.Fatal("the caller did not receive the original upstream stream")
				}
				if method != http.MethodHead {
					content, readErr := io.ReadAll(response.Body)
					if readErr != nil || string(content) != "streamed-video-content" {
						t.Fatalf("caller read = %q, %v", content, readErr)
					}
				}
				if err := response.Body.Close(); err != nil || body.closes != 1 {
					t.Fatalf("caller close = %v, closes=%d", err, body.closes)
				}
			})
		}
	}
}

func TestOpenVideoContentPreservesHTTPErrorsAndClosesRejectedBody(t *testing.T) {
	for _, kind := range []string{"openai", "newapi"} {
		for _, status := range []int{400, 401, 403, 404, 405, 429, 500, 501, 502} {
			t.Run(kind+"/"+http.StatusText(status), func(t *testing.T) {
				body := newVideoOpenTrackedBody(`{"error":"content unavailable"}`)
				attempts := 0
				opener := newVideoContentOpenerForTest(kind, &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					attempts++
					response := videoOpenTestResponse(request, status, body)
					response.Header.Set("Content-Type", "application/json")
					response.Header.Set("Retry-After", "17")
					return response, nil
				})})
				response, err := opener.OpenVideoContent(context.Background(), &config.UpstreamChannel{ID: t.Name(), BaseURL: "https://provider.example", APIKey: "key"}, "task", nil)
				var upstreamErr *UpstreamHTTPError
				if response != nil || !errors.As(err, &upstreamErr) || upstreamErr.StatusCode != status || upstreamErr.Body != `{"error":"content unavailable"}` || upstreamErr.ContentType != "application/json" || upstreamErr.RetryAfter != "17" {
					t.Fatalf("HTTP %d = response %v, error %#v", status, response, err)
				}
				if attempts != 1 || body.closes != 1 || body.reads == 0 {
					t.Fatalf("error handling attempts=%d reads=%d closes=%d", attempts, body.reads, body.closes)
				}
			})
		}
	}
}

func TestOpenVideoContentRotatesRejectedKeysBeforeReturningStream(t *testing.T) {
	for _, kind := range []string{"openai", "newapi"} {
		for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests} {
			t.Run(kind+"/"+http.StatusText(status), func(t *testing.T) {
				channel := &config.UpstreamChannel{ID: t.Name(), BaseURL: "https://provider.example", APIKeys: []string{"rejected-key", "working-key"}}
				RemoveChannelCounter(channel.ID)
				t.Cleanup(func() { RemoveChannelCounter(channel.ID) })
				rejected, success := newVideoOpenTrackedBody("rejected"), newVideoOpenTrackedBody("video")
				var keys []string
				opener := newVideoContentOpenerForTest(kind, &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					keys = append(keys, request.Header.Get("Authorization"))
					if len(keys) == 1 {
						return videoOpenTestResponse(request, status, rejected), nil
					}
					return videoOpenTestResponse(request, http.StatusOK, success), nil
				})})
				response, err := opener.OpenVideoContent(context.Background(), channel, "task", nil)
				if err != nil || response == nil {
					t.Fatalf("rotated content = %v, %v", response, err)
				}
				defer response.Body.Close()
				if !reflect.DeepEqual(keys, []string{"Bearer rejected-key", "Bearer working-key"}) || rejected.closes != 1 || success.reads != 0 || success.closes != 0 {
					t.Fatalf("rotation keys=%v rejected closes=%d success reads/closes=%d/%d", keys, rejected.closes, success.reads, success.closes)
				}
			})
		}
	}
}

func TestOpenVideoContentHEADFallbackRemainsSelective(t *testing.T) {
	for _, kind := range []string{"openai", "newapi"} {
		for _, status := range []int{404, 405, 501, 400, 401, 403, 429, 502} {
			t.Run(kind+"/"+http.StatusText(status), func(t *testing.T) {
				var methods, ranges []string
				rejected, success := newVideoOpenTrackedBody("unsupported"), newVideoOpenTrackedBody("fallback-video")
				opener := newVideoContentOpenerForTest(kind, &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					methods = append(methods, request.Method)
					ranges = append(ranges, request.Header.Get("Range"))
					if request.Method == http.MethodHead {
						return videoOpenTestResponse(request, status, rejected), nil
					}
					return videoOpenTestResponse(request, http.StatusPartialContent, success), nil
				})})
				incoming := httptest.NewRequest(http.MethodHead, "/content", nil)
				incoming.Header.Set("Range", "bytes=5-")
				response, err := opener.OpenVideoContent(context.Background(), &config.UpstreamChannel{ID: t.Name(), BaseURL: "https://provider.example", APIKey: "key"}, "task", incoming)
				fallback := status == 404 || status == 405 || status == 501
				if fallback {
					if err != nil || response == nil || response.StatusCode != http.StatusPartialContent || !reflect.DeepEqual(methods, []string{"HEAD", "GET"}) || !reflect.DeepEqual(ranges, []string{"bytes=5-", "bytes=5-"}) {
						t.Fatalf("HEAD fallback response=%v error=%v methods=%v ranges=%v", response, err, methods, ranges)
					}
					defer response.Body.Close()
					if success.reads != 0 || success.closes != 0 {
						t.Fatal("HEAD fallback consumed the caller's GET stream")
					}
				} else {
					var upstreamErr *UpstreamHTTPError
					if response != nil || !errors.As(err, &upstreamErr) || upstreamErr.StatusCode != status || !reflect.DeepEqual(methods, []string{"HEAD"}) {
						t.Fatalf("unexpected HEAD fallback response=%v error=%v methods=%v", response, err, methods)
					}
				}
				if rejected.closes != 1 {
					t.Fatalf("rejected HEAD body closes=%d, want 1", rejected.closes)
				}
			})
		}
	}
}

func TestOpenVideoContentHEADDoesNotFallbackOnTransportFailureOrCancellation(t *testing.T) {
	for _, kind := range []string{"openai", "newapi"} {
		t.Run(kind, func(t *testing.T) {
			attempts := 0
			transportErr := errors.New("content connection failed")
			opener := newVideoContentOpenerForTest(kind, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				attempts++
				return nil, transportErr
			})})
			channel := &config.UpstreamChannel{ID: t.Name(), BaseURL: "https://provider.example", APIKey: "key"}
			incoming := httptest.NewRequest(http.MethodHead, "/content", nil)
			response, err := opener.OpenVideoContent(context.Background(), channel, "task", incoming)
			if response != nil || !errors.Is(err, transportErr) || attempts != 1 {
				t.Fatalf("transport failure response=%v error=%v attempts=%d", response, err, attempts)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			response, err = opener.OpenVideoContent(ctx, channel, "task", incoming)
			if response != nil || !errors.Is(err, context.Canceled) || attempts != 1 {
				t.Fatalf("cancelled HEAD response=%v error=%v attempts=%d", response, err, attempts)
			}
		})
	}
}

func TestOpenVideoContentReturnsRedirectWithoutFollowingOrLeakingCredentials(t *testing.T) {
	for _, kind := range []string{"openai", "newapi"} {
		for _, status := range []int{301, 302, 303, 307, 308} {
			t.Run(kind+"/"+http.StatusText(status), func(t *testing.T) {
				body := newVideoOpenTrackedBody("redirect-body")
				attempts, redirectCallbacks := 0, 0
				opener := newVideoContentOpenerForTest(kind, &http.Client{
					CheckRedirect: func(*http.Request, []*http.Request) error { redirectCallbacks++; return nil },
					Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
						attempts++
						if request.URL.Host != "provider.example" {
							t.Fatalf("redirect caused a second request to %s with headers %v", request.URL.Host, request.Header)
						}
						if request.Header.Get("Authorization") != "Bearer provider-key" || request.Header.Get("X-Provider-Secret") != "provider-secret" {
							t.Fatal("first upstream request lost provider credentials")
						}
						response := videoOpenTestResponse(request, status, body)
						response.Header.Set("Location", "https://cdn.example/video.mp4?signature=fresh")
						return response, nil
					}),
				})
				channel := &config.UpstreamChannel{ID: t.Name(), BaseURL: "https://provider.example", APIKey: "provider-key", Headers: map[string]string{"X-Provider-Secret": "provider-secret"}}
				response, err := opener.OpenVideoContent(context.Background(), channel, "task", nil)
				if err != nil || response == nil || response.StatusCode != status || response.Header.Get("Location") != "https://cdn.example/video.mp4?signature=fresh" {
					t.Fatalf("redirect response=%v error=%v", response, err)
				}
				defer response.Body.Close()
				if attempts != 1 || redirectCallbacks != 0 || body.reads != 0 || body.closes != 0 {
					t.Fatalf("redirect attempts=%d callbacks=%d reads=%d closes=%d", attempts, redirectCallbacks, body.reads, body.closes)
				}
			})
		}
	}
}

func TestOpenVideoContentRejectsUnsafeRedirectAndClosesBody(t *testing.T) {
	for _, kind := range []string{"openai", "newapi"} {
		for _, location := range []string{"", "javascript:alert(1)", "https://user:secret@cdn.example/video", "http://127.0.0.1/video", "http://10.0.0.1/video", "https://localhost/video", "https://cdn.example/%zz"} {
			t.Run(kind+"/"+location, func(t *testing.T) {
				body := newVideoOpenTrackedBody("redirect-body")
				attempts := 0
				opener := newVideoContentOpenerForTest(kind, &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
					attempts++
					response := videoOpenTestResponse(request, http.StatusTemporaryRedirect, body)
					response.Header.Set("Location", location)
					return response, nil
				})})
				response, err := opener.OpenVideoContent(context.Background(), &config.UpstreamChannel{ID: t.Name(), BaseURL: "https://provider.example", APIKey: "key"}, "task", nil)
				if err == nil || response != nil || attempts != 1 || body.closes != 1 {
					t.Fatalf("unsafe redirect %q response=%v error=%v attempts=%d closes=%d", location, response, err, attempts, body.closes)
				}
			})
		}
	}
}
