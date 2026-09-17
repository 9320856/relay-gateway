package router

import (
	"time"

	"relay-gateway/protocol"
)

const profileSyncImageTimeout = 5 * time.Minute

// Synchronous image operations include the generation time in the submit
// request. Async submits and other operations keep the executor's default.
func newProfileOperationExecutor(op protocol.Operation) *protocol.HTTPExecutor {
	executor := protocol.NewHTTPExecutor(nil)
	if op.ExecutionMode == protocol.ExecutionDirect {
		switch op.Operation {
		case "image.create", "images.create", "images.edits":
			executor.Client.Timeout = profileSyncImageTimeout
		}
	}
	return executor
}
