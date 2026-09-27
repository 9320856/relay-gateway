package router

import (
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/gin-gonic/gin"
)

// externalMediaAddress reconstructs the current public origin for media links.
// Automatic proxy recognition is deliberately separate from authentication's
// explicit proxy trust policy (cookies and first-time setup).
func externalMediaAddress(c *gin.Context) (scheme, host string) {
	scheme = "http"
	if c == nil || c.Request == nil {
		return scheme, ""
	}
	r := c.Request
	host = r.Host
	if r.TLS != nil {
		scheme = "https"
	}
	if !mediaForwardedHeadersAllowed(r) {
		return scheme, host
	}
	if forwardedHost := firstForwardedValue(r.Header.Get("X-Forwarded-Host")); validMediaForwardedHost(forwardedHost) {
		host = forwardedHost
	}
	if r.TLS == nil {
		switch proto := strings.ToLower(firstForwardedValue(r.Header.Get("X-Forwarded-Proto"))); proto {
		case "http", "https":
			scheme = proto
		}
	}
	return scheme, host
}

func mediaForwardedHeadersAllowed(r *http.Request) bool {
	switch strings.TrimSpace(os.Getenv("RELAY_TRUST_PROXY")) {
	case "1":
		return true
	case "":
		// Use the transport peer, never ClientIP or X-Forwarded-For: those
		// describe the end user and may themselves be client-controlled.
		peer, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			peer = r.RemoteAddr
		}
		ip := net.ParseIP(strings.Trim(peer, "[]"))
		return ip != nil && (ip.IsLoopback() || ip.IsPrivate())
	default:
		// "0" explicitly disables forwarding; unknown values fail closed.
		return false
	}
}

func firstForwardedValue(value string) string {
	first, _, _ := strings.Cut(value, ",")
	return strings.TrimSpace(first)
}

func validMediaForwardedHost(host string) bool {
	if host == "" || strings.ContainsAny(host, "/\\?#@") || strings.IndexFunc(host, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) >= 0 {
		return false
	}
	parsed, err := url.Parse("//" + host)
	if err != nil || parsed.Host != host || parsed.Hostname() == "" || parsed.User != nil {
		return false
	}
	hostname := parsed.Hostname()
	if strings.ContainsAny(host, "[]") {
		// Brackets are reserved for IPv6 literals, not arbitrary domain names.
		ip := net.ParseIP(hostname)
		if !strings.HasPrefix(host, "[") || ip == nil || !strings.Contains(hostname, ":") {
			return false
		}
	} else if strings.Contains(hostname, ":") {
		return false // IPv6 authorities must be bracketed.
	}
	if port := parsed.Port(); port != "" {
		value, err := strconv.Atoi(port)
		return err == nil && value > 0 && value <= 65535
	}
	return !strings.HasSuffix(host, ":")
}
