package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"relay-gateway/media"
)

// TaskMediaRetention resolves the frozen channel policy for new tasks. Older
// Profile tasks instead use the immutable operation captured at creation;
// missing or invalid snapshots fail closed rather than consulting a current
// channel binding. Historical legacy tasks default to disabled.
func TaskMediaRetention(ctx context.Context, run *TaskRun) (string, error) {
	if run == nil {
		return "", errors.New("task run is required")
	}
	if run.MediaRetention != "" {
		return media.NormalizeRetention(run.MediaRetention)
	}
	if !strings.EqualFold(strings.TrimSpace(run.Engine), "profile") {
		return media.RetentionDisabled, nil
	}
	compiled, err := LoadTaskProfileContext(ctx, run)
	if err != nil {
		return "", err
	}
	for _, operation := range compiled.Profile().Operations {
		if operation.Operation == run.Operation {
			return media.NormalizeRetention(operation.EffectiveMediaRetention())
		}
	}
	return "", fmt.Errorf("%w: captured operation %q is missing", ErrProfileSnapshotInvalid, run.Operation)
}
