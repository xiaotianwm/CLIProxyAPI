package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigOptionalMigratesLegacyPanelGitHubRepository(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	raw := strings.Join([]string{
		"remote-management:",
		"  panel-github-repository: \"https://github.com/router-for-me/Cli-Proxy-API-Management-Center\"",
		"",
	}, "\n")
	if err := os.WriteFile(configPath, []byte(raw), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfigOptional(configPath, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if got := cfg.RemoteManagement.PanelGitHubRepository; got != DefaultPanelGitHubRepository {
		t.Fatalf("panel repo = %q, want %q", got, DefaultPanelGitHubRepository)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(string(data), DefaultPanelGitHubRepository) {
		t.Fatalf("config file was not migrated, got:\n%s", string(data))
	}
}
