//go:build linux

package noreplace

import "golang.org/x/sys/unix"

// Rename moves source to destination only when destination does not exist.
func Rename(source, destination string) error {
	return unix.Renameat2(unix.AT_FDCWD, source, unix.AT_FDCWD, destination, unix.RENAME_NOREPLACE)
}
