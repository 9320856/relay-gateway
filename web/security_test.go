package web

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// Model IDs and upstream error strings are untrusted provider data.  The UI
// renders both, so keep a regression guard against reintroducing the dynamic
// HTML sinks that previously made payloads such as <img onerror=...> execute.
func TestEmbeddedUIRejectsDynamicXSSSinks(t *testing.T) {
	script := string(AppJS)
	for _, forbidden := range []string{".innerHTML", ".outerHTML", "insertAdjacentHTML", "document.write", "eval("} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("shared UI script contains unsafe dynamic sink %q", forbidden)
		}
	}
	if !strings.Contains(script, ".textContent") {
		t.Fatal("shared UI script must render untrusted model/error text with textContent")
	}

	inlineHandler := regexp.MustCompile(`(?i)\son[a-z]+\s*=`)
	for name, page := range map[string][]byte{
		"setup": SetupHTML, "login": LoginHTML, "dashboard": DashboardHTML, "logs": LogsHTML, "media": MediaHTML, "profiles": ProfilesHTML,
	} {
		text := string(page)
		if inlineHandler.MatchString(text) {
			t.Fatalf("%s page contains an inline event handler", name)
		}
		if strings.Contains(text, "localStorage") {
			t.Fatalf("%s page must not persist credentials in localStorage", name)
		}
	}
}

func TestDashboardIncludesChannelPlaygroundAndSettingsWorkflows(t *testing.T) {
	page := string(DashboardHTML)
	for _, required := range []string{"fetch-channel-models", "mapping-list", "data-playground-kind=\"image\"", "data-playground-kind=\"video\"", "user-menu-button", "settings-dialog", "copy-token"} {
		if !strings.Contains(page, required) {
			t.Fatalf("dashboard is missing %q", required)
		}
	}
	script := string(AppJS)
	for _, required := range []string{"/api/playground/run", "serializeModelMappings", "pollPlaygroundImage", "/api/playground/image-status", "pollPlaygroundVideo", "/models?refresh=true"} {
		if !strings.Contains(script, required) {
			t.Fatalf("dashboard script is missing %q", required)
		}
	}
}

func TestVideoPreviewUsesOnePlaybackLayerAndLogRefreshHasOneLifecycle(t *testing.T) {
	script := string(AppJS)
	mediaStart := strings.Index(script, "function createMediaFigure")
	if mediaStart < 0 {
		t.Fatal("video media renderer could not be located")
	}
	mediaEnd := strings.Index(script[mediaStart:], "function renderPlaygroundImages")
	if mediaEnd < 0 {
		t.Fatal("video media renderer end could not be located")
	}
	mediaRenderer := script[mediaStart : mediaStart+mediaEnd]
	for _, required := range []string{
		`video.controls = true`,
		`video.preload = "metadata"`,
		`video.src = mediaSrc`,
		`reloadVideo = () =>`,
		`video.pause()`,
		`video.removeAttribute("src")`,
		`video.load()`,
	} {
		if !strings.Contains(mediaRenderer, required) {
			t.Fatalf("first-frame video renderer is missing %q", required)
		}
	}
	for _, forbidden := range []string{"media-video-overlay", "media-video-load", `loadButton.addEventListener("click"`} {
		if strings.Contains(mediaRenderer, forbidden) {
			t.Fatalf("video renderer still contains duplicate playback layer %q", forbidden)
		}
	}
	logsPage := string(LogsHTML)
	for _, required := range []string{"visual-card visual-response-card", `id="visual-response-body"`, `id="visual-media-view"`} {
		if !strings.Contains(logsPage, required) {
			t.Fatalf("log response card is missing integrated media structure %q", required)
		}
	}

	for _, required := range []string{
		"let activeLogRefreshTimer = null",
		"clearTimeout(activeLogRefreshTimer)",
		"scheduleActiveLogRefresh(id, requestToken)",
		`if (id !== activeLogID || requestToken !== activeLogRequestToken) return`,
		`!["completed", "failed"].includes(normalizedAsyncTaskStatus(row.async_task_status))`,
		`document.addEventListener("visibilitychange", handleActiveLogVisibilityChange)`,
		"isPermanentLogRefreshError(err)",
		"logDetailMaxRetryMs",
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("async log refresh lifecycle is missing %q", required)
		}
	}
}

func TestAppRuntimeBehavior(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	cmd := exec.Command(node, "app_runtime_test.mjs")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("app runtime behavior test failed: %v\n%s", err, output)
	}
}
