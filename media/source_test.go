package media

import "testing"

func TestNormalizeMediaSourceClassifiesInlineImages(t *testing.T) {
	for _, tc := range []struct {
		name        string
		input       string
		contentType string
	}{
		{name: "data URL", input: "data:image/png;base64,iVBORw0KGgo=", contentType: "image/png"},
		{name: "raw PNG", input: "iVBORw0KGgo=", contentType: "image/png"},
		{name: "raw JPEG", input: "/9j/AAAA", contentType: "image/jpeg"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := NormalizeMediaSource(tc.input)
			if source.SourceKind != SourceBase64 || source.ContentType != tc.contentType {
				t.Fatalf("source = %#v", source)
			}
			if got := MediaSourceDataURL(source); got[:5] != "data:" {
				t.Fatalf("data URL = %q", got)
			}
		})
	}
}

func TestNormalizeMediaSourceDoesNotTreatArbitraryDataAsMedia(t *testing.T) {
	for _, input := range []string{"https://cdn.example/image.png", "/relative/image.png", "data:text/html;base64,PGgxPk5vPC9oMT4="} {
		if source := NormalizeMediaSource(input); source.SourceKind != SourceURL {
			t.Fatalf("%q classified as %#v", input, source)
		}
	}
}
