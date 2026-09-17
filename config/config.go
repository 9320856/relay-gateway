package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

type UpstreamChannel struct {
	ID               string            `yaml:"id"`
	Type             string            `yaml:"type"` // one of: openai, anthropic, newapi, sub2api
	BaseURL          string            `yaml:"base_url"`
	APIKey           string            `yaml:"api_key"`
	APIKeys          []string          `yaml:"api_keys,omitempty"` // 单渠道多 Key 列表
	Enabled          bool              `yaml:"enabled"`            // 是否启用该渠道
	Priority         int               `yaml:"priority"`           // 优先级，数字越小越优先 (1 为主用，2 为备用)
	Weight           int               `yaml:"weight"`             // 负载均衡权重
	FetchModels      bool              `yaml:"fetch_models"`       // 自动向上游 /models 拉取可用模型列表
	AnthropicVersion string            `yaml:"anthropic_version"`  // Anthropic 协议版本头，默认 2023-06-01
	Headers          map[string]string `yaml:"headers"`            // 自定义附加请求头
	Models           []string          `yaml:"models"`
	ModelMap         map[string]string `yaml:"model_map"` // mapping from incoming model to upstream model
}

func (c *UpstreamChannel) GetEffectiveKeys() []string {
	var keys []string
	for _, k := range c.APIKeys {
		trimmed := strings.TrimSpace(k)
		if trimmed != "" {
			keys = append(keys, trimmed)
		}
	}
	if len(keys) == 0 && strings.TrimSpace(c.APIKey) != "" {
		keys = append(keys, strings.TrimSpace(c.APIKey))
	}
	return keys
}

type Config struct {
	Port         int    `yaml:"port"`
	DatabasePath string `yaml:"database_path,omitempty"`

	// APIKey and Channels remain runtime-only for backwards-compatible tests.
	// v2 never reads or writes credentials or channels through YAML.
	APIKey   string            `yaml:"-"`
	Channels []UpstreamChannel `yaml:"-"`
}

const (
	DefaultPort = 8000
	MinPort     = 1
	MaxPort     = 65535
)

var (
	Global = &Config{
		Port:         DefaultPort,
		DatabasePath: "gateway.db",
	}
	configMu sync.RWMutex
)

// GetAPIKey 线程安全地读取网关鉴权密钥
func GetAPIKey() string {
	configMu.RLock()
	defer configMu.RUnlock()
	return Global.APIKey
}

func IsValidPort(port int) bool {
	return port >= MinPort && port <= MaxPort
}

// SetAPIKey 线程安全地更新网关鉴权密钥
func SetAPIKey(key string) {
	configMu.Lock()
	defer configMu.Unlock()
	Global.APIKey = key
}

// GenerateAPIKey 生成高安全度的标准 sk- 前缀 API 密钥（192 位加密随机熵）
func GenerateAPIKey() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("sk-gateway-%d", time.Now().UnixNano())
	}
	return "sk-" + hex.EncodeToString(b)
}

// GetPort 线程安全地获取网关监听端口
func GetPort() int {
	configMu.RLock()
	defer configMu.RUnlock()
	return Global.Port
}

// SetPort 线程安全地设置网关监听端口
func SetPort(port int) {
	configMu.Lock()
	defer configMu.Unlock()
	Global.Port = port
}

func GetDatabasePath() string {
	configMu.RLock()
	defer configMu.RUnlock()
	if strings.TrimSpace(Global.DatabasePath) == "" {
		return "gateway.db"
	}
	return strings.TrimSpace(Global.DatabasePath)
}

func Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var loaded Config
	if err := yaml.Unmarshal(data, &loaded); err != nil {
		return err
	}
	if loaded.Port != 0 && !IsValidPort(loaded.Port) {
		return fmt.Errorf("port must be between %d and %d", MinPort, MaxPort)
	}
	configMu.Lock()
	defer configMu.Unlock()
	if loaded.Port != 0 {
		Global.Port = loaded.Port
	}
	if strings.TrimSpace(loaded.DatabasePath) != "" {
		Global.DatabasePath = strings.TrimSpace(loaded.DatabasePath)
	}
	return nil
}

// Save persists the legacy YAML configuration. New channel/settings writes go
// through SQLite-backed router handlers.
// Deprecated: retained for external integrations only.
func Save(path string) error {
	configMu.Lock()
	defer configMu.Unlock()
	data, err := yaml.Marshal(Global)
	if err != nil {
		return err
	}
	// 原子写入：先写临时文件再重命名，防止并发调用导致文件损坏
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
