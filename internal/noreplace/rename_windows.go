//go:build windows

package noreplace

import "golang.org/x/sys/windows"

// Rename moves source to destination only when destination does not exist.
func Rename(source, destination string) error {
	from, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := windows.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(from, to, 0)
}
