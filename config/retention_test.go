package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestUpstreamChannelMediaRetentionRoundTrip(t *testing.T) {
	channel := UpstreamChannel{ID: "media", MediaRetention: "best_effort"}
	encoded, err := yaml.Marshal(channel)
	if err != nil {
		t.Fatal(err)
	}
	var loaded UpstreamChannel
	if err := yaml.Unmarshal(encoded, &loaded); err != nil {
		t.Fatal(err)
	}
	if loaded.ID != channel.ID || loaded.MediaRetention != channel.MediaRetention {
		t.Fatalf("retention did not survive channel conversion: %+v", loaded)
	}
}
