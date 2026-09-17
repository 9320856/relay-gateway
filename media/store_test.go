package media

import (
	"bytes"
	"context"
	"io"
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
