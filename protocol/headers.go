package protocol

import (
	"net/http"
	"sort"
	"strings"
)

// MergeRequestHeaders returns an independent, canonicalized map. Later layers
// override earlier layers regardless of spelling/case, avoiding random map
// iteration deciding which provider or forwarded client header reaches upstream.
func MergeRequestHeaders(layers ...map[string]string) map[string]string {
	merged := make(map[string]string)
	for _, layer := range layers {
		keys := make([]string, 0, len(layer))
		for key := range layer {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			name := http.CanonicalHeaderKey(strings.TrimSpace(key))
			if name != "" {
				merged[name] = layer[key]
			}
		}
	}
	return merged
}
