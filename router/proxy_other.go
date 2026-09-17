//go:build !windows

package router

import (
	"net"
	"time"
)

func detectSystemProxy() string {
	for _, addr := range []string{"127.0.0.1:7890", "127.0.0.1:7897", "127.0.0.1:10808", "127.0.0.1:1080"} {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return addr
		}
	}
	return ""
}
