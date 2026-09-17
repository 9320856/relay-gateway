package media

import (
	"context"
	"encoding/base64"
	"strings"
	"testing"
)

func TestMaterializationWorkerStreamsInlineBinaryAndBase64(t *testing.T) {
	store, err := NewLocalObjectStore(t.TempDir(), 64)
	if err != nil {
		t.Fatal(err)
	}
	worker := &MaterializationWorker{Store: store, Fetcher: InlineSourceFetcher{}}
	info, err := worker.Materialize(context.Background(), MediaResult{SourceKind: SourceBinary, Body: strings.NewReader("hello"), ContentType: "text/plain"}, "asset.bin")
	if err != nil || info.Size != 5 {
		t.Fatalf("binary materialization = %+v, %v", info, err)
	}
	encoded := base64.RawStdEncoding.EncodeToString([]byte("world"))
	info, err = worker.Materialize(context.Background(), MediaResult{SourceKind: SourceBase64, Locator: encoded}, "asset-2.bin")
	if err != nil || info.Size != 5 {
		t.Fatalf("base64 materialization = %+v, %v", info, err)
	}
}

func TestMaterializationWorkerRejectsUnsupportedInlineSource(t *testing.T) {
	store, _ := NewLocalObjectStore(t.TempDir())
	worker := &MaterializationWorker{Store: store, Fetcher: InlineSourceFetcher{}}
	if _, err := worker.Materialize(context.Background(), MediaResult{SourceKind: SourceURL, Locator: "https://example.com/x"}, "asset"); err == nil {
		t.Fatal("URL source should require an explicit network fetcher")
	}
}
