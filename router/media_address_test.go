package router

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"relay-gateway/security"
)

func TestMediaExternalAddress(t *testing.T) {
	tests := []struct {
		name, remote, trust, host, proto, want string
		tls                                    bool
	}{
		{name: "direct HTTP", remote: "203.0.113.10:1234", want: "http://gateway.test:8000"},
		{name: "direct TLS", remote: "203.0.113.10:1234", tls: true, want: "https://gateway.test:8000"},
		{name: "loopback proxy", remote: "127.0.0.1:1234", host: "ai.example.com", proto: "https", want: "https://ai.example.com"},
		{name: "IPv6 loopback", remote: "[::1]:1234", host: "ai.example.com:8443", proto: "https", want: "https://ai.example.com:8443"},
		{name: "10 network", remote: "10.2.3.4:1234", host: "ai.example.com", proto: "https", want: "https://ai.example.com"},
		{name: "172 network", remote: "172.16.0.2:1234", host: "ai.example.com", proto: "https", want: "https://ai.example.com"},
		{name: "192 network", remote: "192.168.1.2:1234", host: "ai.example.com", proto: "http", want: "http://ai.example.com"},
		{name: "IPv6 private", remote: "[fd00::2]:1234", host: "[2001:db8::5]:8443", proto: "https", want: "https://[2001:db8::5]:8443"},
		{name: "IPv4 mapped loopback", remote: "[::ffff:127.0.0.1]:1234", host: "ai.example.com", proto: "https", want: "https://ai.example.com"},
		{name: "bare private peer", remote: "10.2.3.4", host: "ai.example.com", proto: "https", want: "https://ai.example.com"},
		{name: "public ignores forwarding", remote: "203.0.113.10:1234", host: "attacker.test", proto: "https", want: "http://gateway.test:8000"},
		{name: "IPv6 public ignores forwarding", remote: "[2001:db8::10]:1234", host: "attacker.test", proto: "https", want: "http://gateway.test:8000"},
		{name: "adjacent public range", remote: "172.32.0.2:1234", host: "attacker.test", proto: "https", want: "http://gateway.test:8000"},
		{name: "link local is not trusted", remote: "169.254.1.2:1234", host: "attacker.test", proto: "https", want: "http://gateway.test:8000"},
		{name: "invalid peer", remote: "proxy.invalid:1234", host: "attacker.test", proto: "https", want: "http://gateway.test:8000"},
		{name: "explicit trust public peer", remote: "203.0.113.10:1234", trust: "1", host: "ai.example.com", proto: "https", want: "https://ai.example.com"},
		{name: "explicit distrust local peer", remote: "127.0.0.1:1234", trust: "0", host: "attacker.test", proto: "https", want: "http://gateway.test:8000"},
		{name: "unknown flag fails closed", remote: "127.0.0.1:1234", trust: "true", host: "attacker.test", proto: "https", want: "http://gateway.test:8000"},
		{name: "missing host", remote: "127.0.0.1:1234", proto: "https", want: "https://gateway.test:8000"},
		{name: "missing protocol", remote: "127.0.0.1:1234", host: "ai.example.com", want: "http://ai.example.com"},
		{name: "invalid protocol", remote: "127.0.0.1:1234", host: "ai.example.com", proto: "ftp", want: "http://ai.example.com"},
		{name: "TLS overrides forwarded HTTP", remote: "127.0.0.1:1234", host: "ai.example.com", proto: "http", tls: true, want: "https://ai.example.com"},
		{name: "first forwarded values", remote: "127.0.0.1:1234", host: " ai.example.com:8443 , internal.test", proto: " HTTPS , http", want: "https://ai.example.com:8443"},
		{name: "empty first host", remote: "127.0.0.1:1234", host: ", attacker.test", proto: "https", want: "https://gateway.test:8000"},
		{name: "empty first protocol", remote: "127.0.0.1:1234", host: "ai.example.com", proto: ", https", want: "http://ai.example.com"},
		{name: "host with scheme", remote: "127.0.0.1:1234", host: "https://attacker.test", proto: "https", want: "https://gateway.test:8000"},
		{name: "host with user info", remote: "127.0.0.1:1234", host: "user@attacker.test", proto: "https", want: "https://gateway.test:8000"},
		{name: "host with path", remote: "127.0.0.1:1234", host: "attacker.test/path", proto: "https", want: "https://gateway.test:8000"},
		{name: "host with query", remote: "127.0.0.1:1234", host: "attacker.test?next=foo", proto: "https", want: "https://gateway.test:8000"},
		{name: "host with fragment", remote: "127.0.0.1:1234", host: "attacker.test#foo", proto: "https", want: "https://gateway.test:8000"},
		{name: "host with whitespace", remote: "127.0.0.1:1234", host: "bad host.test", proto: "https", want: "https://gateway.test:8000"},
		{name: "host with backslash", remote: "127.0.0.1:1234", host: "attacker.test\\path", proto: "https", want: "https://gateway.test:8000"},
		{name: "invalid port", remote: "127.0.0.1:1234", host: "ai.example.com:abc", proto: "https", want: "https://gateway.test:8000"},
		{name: "out of range port", remote: "127.0.0.1:1234", host: "ai.example.com:65536", proto: "https", want: "https://gateway.test:8000"},
		{name: "empty port", remote: "127.0.0.1:1234", host: "ai.example.com:", proto: "https", want: "https://gateway.test:8000"},
		{name: "invalid IPv6 authority", remote: "127.0.0.1:1234", host: "[not-an-ip]:8443", proto: "https", want: "https://gateway.test:8000"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("RELAY_TRUST_PROXY", tt.trust)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodGet, "http://gateway.test:8000/", nil)
			c.Request.RemoteAddr = tt.remote
			c.Request.Header.Set("X-Forwarded-Host", tt.host)
			c.Request.Header.Set("X-Forwarded-Proto", tt.proto)
			// A forwarded client address must not change transport-peer trust.
			c.Request.Header.Set("X-Forwarded-For", "127.0.0.1")
			if tt.tls {
				c.Request.TLS = &tls.ConnectionState{}
			}
			if got := mediaPublicURL(c, "asset", "cap"); got != tt.want+"/v1/media/asset/cap" {
				t.Fatalf("media URL = %q, want origin %q", got, tt.want)
			}
			path := "/v1/videos/task/content"
			if got := resolveAbsoluteURL(c, path); got != tt.want+path {
				t.Fatalf("video URL = %q, want %q", got, tt.want+path)
			}
			if !isGatewayStableVideoContentURL(c, tt.want+path) {
				t.Fatalf("generated video URL not recognized as gateway content")
			}
			if isGatewayStableVideoContentURL(c, "https://unrelated.example"+path) {
				t.Fatal("unrelated provider URL recognized as gateway content")
			}
			if got := resolveAbsoluteURL(c, "https://provider.example/video.mp4?signature=original"); got != "https://provider.example/video.mp4?signature=original" {
				t.Fatalf("absolute provider URL changed: %q", got)
			}
		})
	}
}

func TestMediaPublicURLPreservesEscapedPath(t *testing.T) {
	t.Setenv("RELAY_TRUST_PROXY", "")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "http://gateway.test/", nil)
	if got, want := mediaPublicURL(c, "asset/one", "cap?two"), "http://gateway.test/v1/media/asset%2Fone/cap%3Ftwo"; got != want {
		t.Fatalf("media URL = %q, want %q", got, want)
	}
}

func TestAutomaticMediaProxyRecognitionDoesNotChangeAuthentication(t *testing.T) {
	t.Setenv("RELAY_TRUST_PROXY", "")
	t.Setenv("RELAY_SETUP_SECRET", "")
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "http://gateway.test/", nil)
	c.Request.RemoteAddr = "127.0.0.1:1234"
	c.Request.Header.Set("X-Forwarded-Proto", "https")
	c.Request.Header.Set("X-Forwarded-Host", "ai.example.com")
	if got := mediaPublicURL(c, "asset", "cap"); got != "https://ai.example.com/v1/media/asset/cap" {
		t.Fatalf("automatic media recognition failed: %q", got)
	}
	setSessionCookie(c, &security.Session{Token: "session", ExpiresAt: time.Now().Add(time.Hour)})
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Secure || trustedForwardedHTTPS(c) {
		t.Fatalf("automatic media trust leaked into authentication: cookies=%v", cookies)
	}
	c.Request.RemoteAddr = "10.2.3.4:1234"
	t.Setenv("RELAY_SETUP_SECRET", "separate-initialization-secret")
	if setupRequestAllowed(c) {
		t.Fatal("automatic media trust bypassed the configured initialization secret")
	}
}
