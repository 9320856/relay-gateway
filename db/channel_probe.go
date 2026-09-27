package db

import (
	"context"
	"encoding/json"
	"time"

	"relay-gateway/config"
)

// ChannelProbeSnapshot binds discovery to the exact persisted channel revision.
// It is read before queuing network work, never inferred from a later generation.
type ChannelProbeSnapshot struct {
	Channel   config.UpstreamChannel
	UpdatedAt time.Time
	Persisted bool
}

func channelConfigurationTime(previous time.Time) time.Time {
	now := time.Now()
	if !now.After(previous) {
		return previous.Add(time.Nanosecond)
	}
	return now
}

func SnapshotChannelProbe(ctx context.Context, channel config.UpstreamChannel) (ChannelProbeSnapshot, error) {
	if DB == nil {
		return ChannelProbeSnapshot{Channel: channel}, nil
	}
	row, err := GetChannelModelContext(ctx, channel.ID)
	if err != nil {
		return ChannelProbeSnapshot{}, err
	}
	return ChannelProbeSnapshot{Channel: row.ToUpstreamChannel(), UpdatedAt: row.UpdatedAt, Persisted: true}, nil
}

// CommitChannelProbe uses one conditional UPDATE, so a save between validation
// and commit cannot attach the old provider's models or health to a new config.
func CommitChannelProbe(ctx context.Context, snapshot ChannelProbeSnapshot, status string, latency int, message string, models []string) (bool, error) {
	if !snapshot.Persisted {
		return true, nil
	}
	conn := SQLDBForContext(ctx)
	if conn == nil {
		return false, nil
	}
	updates := map[string]any{"last_status": status, "last_latency_ms": latency, "last_error_message": message}
	if models != nil {
		encoded, err := json.Marshal(models)
		if err != nil {
			return false, err
		}
		updates["models_synced_raw"] = string(encoded)
	}
	// Health refreshes do not change the configuration revision used by probes.
	result := conn.Model(&ChannelModel{}).Where("id = ? AND updated_at = ?", snapshot.Channel.ID, snapshot.UpdatedAt).UpdateColumns(updates)
	return result.RowsAffected == 1, result.Error
}
