// Package upstreamhttp shares connection and redirect policy for provider APIs.
// Media URL downloads use their own address validation and redirect policy.
package upstreamhttp

import (
	"net"
	"net/http"
	"time"
)

const DefaultTimeout = 10 * time.Minute

var Transport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          1000,
	MaxIdleConnsPerHost:   200,
	MaxConnsPerHost:       400,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
	ResponseHeaderTimeout: 180 * time.Second,
}

// NewClient copies an injected client's transport, timeout and cookie policy.
// API redirects are always returned to the caller so a request body or custom
// credential header cannot be replayed against another endpoint. The original
// client is never mutated, including when it is already shared by other users.
func NewClient(base *http.Client) *http.Client {
	client := http.Client{Timeout: DefaultTimeout}
	if base != nil {
		client = *base
	}
	if client.Transport == nil {
		client.Transport = Transport
	}
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &client
}
