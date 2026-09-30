//go:build !windows && !linux

package noreplace

import "os"

// Rename uses a hard link when the platform has no no-replace rename syscall.
func Rename(source, destination string) error {
	if err := os.Link(source, destination); err != nil {
		return err
	}
	return os.Remove(source)
}
