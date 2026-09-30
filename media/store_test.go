package media

import (
	"bytes"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestPutAtomicHashAndRange(t *testing.T) {
	dir := t.TempDir()
	s, err := NewLocalObjectStore(dir, 100)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("0123456789")
	info, err := s.PutAtomic(context.Background(), bytes.NewReader(data), PutMeta{Key: "nested/object.bin", ContentType: "application/octet-stream"})
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len(data)) || info.SHA256 == "" {
		t.Fatalf("bad info: %+v", info)
	}
	f, gotInfo, err := s.Open(context.Background(), "nested/object.bin", &ByteRange{Start: 2, Length: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	got, _ := io.ReadAll(f)
	if string(got) != "2345" || gotInfo.Size != 10 {
		t.Fatalf("range=%q info=%+v", got, gotInfo)
	}
}

func TestPutAtomicTooLargeLeavesNoPart(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalObjectStore(dir, 3)
	if _, err := s.PutAtomic(context.Background(), bytes.NewReader([]byte("1234")), PutMeta{Key: "x"}); err != ErrTooLarge {
		t.Fatalf("err=%v", err)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".part" {
			t.Fatalf("temporary file left: %s", e.Name())
		}
	}
}

func TestDeleteIdempotentAndKeyValidation(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewLocalObjectStore(dir)
	if _, err := s.PutAtomic(context.Background(), bytes.NewReader([]byte("x")), PutMeta{Key: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(context.Background(), "../x"); err != ErrInvalidKey {
		t.Fatalf("validation err=%v", err)
	}
}

func TestLegacyMediaKeyCompatibility(t *testing.T) {
	for _, storedKey := range []string{"video.mp4", "video.media"} {
		t.Run(storedKey, func(t *testing.T) {
			s, err := NewLocalObjectStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.PutAtomic(context.Background(), bytes.NewBufferString("video"), PutMeta{Key: storedKey}); err != nil {
				t.Fatal(err)
			}
			lookupKey := AlternateObjectKey(storedKey)
			if info, err := s.Stat(context.Background(), lookupKey); err != nil || info.Size != 5 {
				t.Fatalf("stat legacy key: info=%+v err=%v", info, err)
			}
			body, _, err := s.Open(context.Background(), lookupKey, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, readErr := io.ReadAll(body)
			_ = body.Close()
			if readErr != nil || string(got) != "video" {
				t.Fatalf("open legacy key: body=%q err=%v", got, readErr)
			}
			if err := s.Delete(context.Background(), lookupKey); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Stat(context.Background(), storedKey); !os.IsNotExist(err) {
				t.Fatalf("legacy object still exists: %v", err)
			}
		})
	}
}

func TestOpenRejectsOverflowingRange(t *testing.T) {
	s, err := NewLocalObjectStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutAtomic(context.Background(), bytes.NewBufferString("123"), PutMeta{Key: "short"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Open(context.Background(), "short", &ByteRange{Start: 1, Length: math.MaxInt64}); err != ErrInvalidRange {
		t.Fatalf("overflowing range error = %v", err)
	}
}

func TestDeleteRejectsSymlinkedParentOutsideStore(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "victim"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Skipf("directory symlinks unavailable: %v", err)
	}
	s, err := NewLocalObjectStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(context.Background(), "linked/victim"); err == nil {
		t.Fatalf("delete through external symlink: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "victim")); err != nil {
		t.Fatalf("external file removed: %v", err)
	}
}
