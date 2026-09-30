package router

import (
	"fmt"

	"relay-gateway/config"
	"relay-gateway/service"
)

// Pinned requests do not pass through the dispatcher candidate pool. Apply the
// same selection check before any new upstream submission, while existing task
// polling and content reads keep using their durable channel mapping.
func requireSelectedChannelModel(channel *config.UpstreamChannel, modelName string) error {
	if channel != nil && !channel.AllowsModel(modelName) {
		return fmt.Errorf("%w：渠道未保存模型 %q，请先勾选该模型并保存渠道", service.ErrModelNotSelected, modelName)
	}
	return nil
}
