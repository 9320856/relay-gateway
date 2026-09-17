//go:build windows

package router

import (
	"net"
	"strings"
	"time"

	"golang.org/x/sys/windows/registry"
)

func detectSystemProxy() string {
	// 1. Check Windows Internet Settings registry
	k, err := registry.OpenKey(registry.CURRENT_USER, `Software\Microsoft\Windows\CurrentVersion\Internet Settings`, registry.QUERY_VALUE)
	if err == nil {
		defer k.Close()
		server, _, err := k.GetStringValue("ProxyServer")
		if err == nil && server != "" {
			parts := strings.Split(server, ";")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if strings.HasPrefix(p, "https=") {
					return strings.TrimPrefix(p, "https=")
				}
				if strings.HasPrefix(p, "http=") {
					return strings.TrimPrefix(p, "http=")
				}
			}
			return parts[0]
		}
	}

	// 2. Check active local proxy ports (Clash / Mihomo / V2Ray)
	for _, addr := range []string{"127.0.0.1:7897", "127.0.0.1:7890", "127.0.0.1:10808", "127.0.0.1:10809"} {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return addr
		}
	}
	return ""
}
