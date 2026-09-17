package media

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

// FileEntry describes a regular file found below a local object store root.
// Key uses slash separators so it can be compared with MediaObject.StorageKey.
type FileEntry struct {
	Key     string
	Size    int64
	ModTime time.Time
}

// ListFiles returns regular files below the store root. It is intentionally
// read-only; callers decide whether an unreferenced file is safe to remove.
func (s *LocalObjectStore) ListFiles(ctx context.Context) ([]FileEntry, error) {
	if s == nil {
		return nil, os.ErrInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var files []FileEntry
	err := filepath.WalkDir(s.root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		files = append(files, FileEntry{Key: filepath.ToSlash(rel), Size: info.Size(), ModTime: info.ModTime()})
		return nil
	})
	return files, err
}
