package protocol

import "testing"

func TestMergeRequestHeadersUsesLayerPriorityAcrossCaseVariants(t *testing.T) {
	channel := map[string]string{"Anthropic-Version": "channel-version", "X-Tenant": "tenant", "X-Operation": "channel"}
	client := map[string]string{"anthropic-version": "client-version"}
	operation := map[string]string{"x-operation": "operation"}
	merged := MergeRequestHeaders(channel, client, operation)
	if len(merged) != 3 || merged["Anthropic-Version"] != "client-version" || merged["X-Tenant"] != "tenant" || merged["X-Operation"] != "operation" {
		t.Fatalf("merged headers=%v", merged)
	}
	merged["X-Tenant"] = "mutated"
	if channel["X-Tenant"] != "tenant" {
		t.Fatal("merge mutated channel headers")
	}
}
