package config

import (
	"os"
	"strings"
	"testing"
)

func TestGenerateAPIKey(t *testing.T) {
	key1 := GenerateAPIKey()
	key2 := GenerateAPIKey()

	if !strings.HasPrefix(key1, "sk-") {
		t.Fatalf("expected key1 to have 'sk-' prefix, got %s", key1)
	}
	if !strings.HasPrefix(key2, "sk-") {
		t.Fatalf("expected key2 to have 'sk-' prefix, got %s", key2)
	}

	if len(key1) != 51 {
		t.Fatalf("expected key length 51, got %d (%s)", len(key1), key1)
	}

	if key1 == key2 {
		t.Fatalf("expected two generated keys to be unique, got both %s", key1)
	}
}

func TestGetSetAPIKeyAndPort(t *testing.T) {
	oldKey := GetAPIKey()
	defer SetAPIKey(oldKey)

	oldPort := GetPort()
	defer SetPort(oldPort)

	testKey := "sk-test-unique-key-12345"
	SetAPIKey(testKey)
	if GetAPIKey() != testKey {
		t.Fatalf("expected APIKey %s, got %s", testKey, GetAPIKey())
	}

	testPort := 9090
	SetPort(testPort)
	if GetPort() != testPort {
		t.Fatalf("expected Port %d, got %d", testPort, GetPort())
	}
}

func TestLoadMissingConfigFile(t *testing.T) {
	err := Load("non-existent-config-file.yaml")
	if err != nil {
		t.Fatalf("expected nil error for missing config file, got %v", err)
	}
}

func TestLoadCorruptConfigFile(t *testing.T) {
	tmpFile := t.TempDir() + "/corrupt-config.yaml"
	if err := os.WriteFile(tmpFile, []byte("port: not-a-number: [invalid"), 0644); err != nil {
		t.Fatal(err)
	}
	err := Load(tmpFile)
	if err == nil {
		t.Fatal("expected error for corrupt config file, got nil")
	}
}

func TestLoadInvalidPort(t *testing.T) {
	tmpFile := t.TempDir() + "/invalid-port.yaml"
	if err := os.WriteFile(tmpFile, []byte("port: 99999\n"), 0644); err != nil {
		t.Fatal(err)
	}
	err := Load(tmpFile)
	if err == nil {
		t.Fatal("expected error for out-of-range port, got nil")
	}
}
