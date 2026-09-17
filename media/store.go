// Package media contains storage and logical asset primitives used by the
// media pipeline. It deliberately has no database or HTTP dependencies.
package media

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	ErrNotFound     = os.ErrNotExist
	ErrTooLarge     = errors.New("media object exceeds size limit")
	ErrInvalidKey   = errors.New("invalid media object key")
	ErrInvalidRange = errors.New("invalid byte range")
)

type PutMeta struct {
	Key         string
	ContentType string
	Filename    string
	MaxBytes    int64
}

type ObjectInfo struct {
	Key         string
	Size        int64
	SHA256      string
	ContentType string
	ModTime     time.Time
}

// ByteRange is an optional half-open byte interval. Length < 0 means to EOF.
type ByteRange struct{ Start, Length int64 }

type ObjectStore interface {
	PutAtomic(context.Context, io.Reader, PutMeta) (ObjectInfo, error)
	Open(context.Context, string, *ByteRange) (io.ReadCloser, ObjectInfo, error)
	Delete(context.Context, string) error
	Stat(context.Context, string) (ObjectInfo, error)
}

type LocalObjectStore struct {
	root     string
	maxBytes int64
}

func NewLocalObjectStore(root string, maxBytes ...int64) (*LocalObjectStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("media store root is required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(abs, 0700); err != nil {
		return nil, err
	}
	limit := int64(0)
	if len(maxBytes) > 0 {
		limit = maxBytes[0]
	}
	if limit < 0 {
		return nil, errors.New("max bytes must be non-negative")
	}
	return &LocalObjectStore{root: abs, maxBytes: limit}, nil
}

func (s *LocalObjectStore) path(key string) (string, error) {
	if key == "" || filepath.IsAbs(key) || filepath.VolumeName(key) != "" || strings.ContainsAny(key, `\\`) {
		return "", ErrInvalidKey
	}
	clean := filepath.Clean(key)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
		return "", ErrInvalidKey
	}
	full := filepath.Join(s.root, clean)
	if rel, err := filepath.Rel(s.root, full); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", ErrInvalidKey
	}
	return full, nil
}

func (s *LocalObjectStore) withinResolved(path string) error {
	root, err := filepath.EvalSymlinks(s.root)
	if err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return ErrInvalidKey
	}
	return nil
}

func (s *LocalObjectStore) PutAtomic(ctx context.Context, src io.Reader, meta PutMeta) (ObjectInfo, error) {
	if src == nil {
		return ObjectInfo{}, errors.New("media source is nil")
	}
	path, err := s.path(meta.Key)
	if err != nil {
		return ObjectInfo{}, err
	}
	limit := s.maxBytes
	if meta.MaxBytes > 0 && (limit == 0 || meta.MaxBytes < limit) {
		limit = meta.MaxBytes
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return ObjectInfo{}, err
	}
	if err = s.withinResolved(filepath.Dir(path)); err != nil {
		return ObjectInfo{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".media-*.part")
	if err != nil {
		return ObjectInfo{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	h := sha256.New()
	n := int64(0)
	buf := make([]byte, 32*1024)
	for {
		if err := ctx.Err(); err != nil {
			tmp.Close()
			return ObjectInfo{}, err
		}
		read, rerr := src.Read(buf)
		if read > 0 {
			n += int64(read)
			if limit > 0 && n > limit {
				tmp.Close()
				return ObjectInfo{}, ErrTooLarge
			}
			if _, err = tmp.Write(buf[:read]); err != nil {
				tmp.Close()
				return ObjectInfo{}, err
			}
			_, _ = h.Write(buf[:read])
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			tmp.Close()
			return ObjectInfo{}, rerr
		}
	}
	if err = tmp.Sync(); err != nil {
		tmp.Close()
		return ObjectInfo{}, err
	}
	if err = tmp.Close(); err != nil {
		return ObjectInfo{}, err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return ObjectInfo{}, err
	}
	if dir, e := os.Open(filepath.Dir(path)); e == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	st, err := os.Stat(path)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{Key: meta.Key, Size: n, SHA256: hex.EncodeToString(h.Sum(nil)), ContentType: meta.ContentType, ModTime: st.ModTime()}, nil
}

func (s *LocalObjectStore) Open(ctx context.Context, key string, r *ByteRange) (io.ReadCloser, ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, ObjectInfo{}, err
	}
	path, err := s.path(key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	f, err := os.Open(path)
	if err != nil && os.IsNotExist(err) {
		altKey := ""
		if strings.HasSuffix(key, ".media") {
			altKey = strings.TrimSuffix(key, ".media") + ".mp4"
		} else if strings.HasSuffix(key, ".mp4") {
			altKey = strings.TrimSuffix(key, ".mp4") + ".media"
		}
		if altKey != "" {
			if altPath, altErr := s.path(altKey); altErr == nil {
				if altF, altOpenErr := os.Open(altPath); altOpenErr == nil {
					f = altF
					path = altPath
					err = nil
				}
			}
		}
	}
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if err = s.withinResolved(path); err != nil {
		f.Close()
		return nil, ObjectInfo{}, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, ObjectInfo{}, err
	}
	info := ObjectInfo{Key: key, Size: st.Size(), ModTime: st.ModTime()}
	if r == nil {
		return f, info, nil
	}
	if r.Start < 0 || r.Start > st.Size() || r.Length < -1 || (r.Length >= 0 && r.Start+r.Length > st.Size()) {
		f.Close()
		return nil, ObjectInfo{}, ErrInvalidRange
	}
	if _, err = f.Seek(r.Start, io.SeekStart); err != nil {
		f.Close()
		return nil, ObjectInfo{}, err
	}
	length := r.Length
	if length < 0 {
		length = st.Size() - r.Start
	}
	return structReadCloser{Reader: io.LimitReader(f, length), closer: f}, info, nil
}

type structReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r structReadCloser) Close() error { return r.closer.Close() }

func (s *LocalObjectStore) Stat(ctx context.Context, key string) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	p, e := s.path(key)
	if e != nil {
		return ObjectInfo{}, e
	}
	st, e := os.Stat(p)
	if e != nil && os.IsNotExist(e) {
		altKey := ""
		if strings.HasSuffix(key, ".media") {
			altKey = strings.TrimSuffix(key, ".media") + ".mp4"
		} else if strings.HasSuffix(key, ".mp4") {
			altKey = strings.TrimSuffix(key, ".mp4") + ".media"
		}
		if altKey != "" {
			if altPath, altErr := s.path(altKey); altErr == nil {
				if altSt, altStatErr := os.Stat(altPath); altStatErr == nil {
					st = altSt
					p = altPath
					e = nil
				}
			}
		}
	}
	if e != nil {
		return ObjectInfo{}, e
	}
	if e = s.withinResolved(p); e != nil {
		return ObjectInfo{}, e
	}
	return ObjectInfo{Key: key, Size: st.Size(), ModTime: st.ModTime()}, nil
}
func (s *LocalObjectStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p, e := s.path(key)
	if e != nil {
		return e
	}
	e = os.Remove(p)
	altKey := ""
	if strings.HasSuffix(key, ".media") {
		altKey = strings.TrimSuffix(key, ".media") + ".mp4"
	} else if strings.HasSuffix(key, ".mp4") {
		altKey = strings.TrimSuffix(key, ".mp4") + ".media"
	}
	if altKey != "" {
		if altPath, altErr := s.path(altKey); altErr == nil {
			_ = os.Remove(altPath)
		}
	}
	if os.IsNotExist(e) {
		return nil
	}
	return e
}
func (s *LocalObjectStore) String() string { return fmt.Sprintf("LocalObjectStore(%s)", s.root) }
