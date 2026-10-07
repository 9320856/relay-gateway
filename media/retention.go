package media

import "fmt"

const (
	RetentionDisabled   = "disabled"
	RetentionBestEffort = "best_effort"
	RetentionRequired   = "required"
)

// NormalizeRetention validates the channel policy. Empty channel settings use
// the default disabled policy; callers retaining historical task metadata must
// preserve its empty value until they resolve the task's captured definition.
func NormalizeRetention(policy string) (string, error) {
	switch policy {
	case "", RetentionDisabled:
		return RetentionDisabled, nil
	case RetentionBestEffort, RetentionRequired:
		return policy, nil
	default:
		return "", fmt.Errorf("invalid media_retention %q", policy)
	}
}
