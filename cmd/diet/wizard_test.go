package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAtoi(t *testing.T) {
	tests := []struct {
		input string
		def   int
		want  int
	}{
		{"10", 0, 10},
		{"0", 5, 5},     // zero is treated as invalid (n <= 0)
		{"-1", 5, 5},    // negative is invalid
		{"abc", 5, 5},   // non-numeric
		{"", 5, 5},      // empty string
		{"100", 0, 100},
		{"6", 0, 6},
	}
	for _, tt := range tests {
		got := atoi(tt.input, tt.def)
		if got != tt.want {
			t.Errorf("atoi(%q, %d) = %d, want %d", tt.input, tt.def, got, tt.want)
		}
	}
}

func TestProfileArchiveFormat(t *testing.T) {
	tests := []struct {
		format string
		want   string
	}{
		{"", "zstd"},
		{"zstd", "zstd"},
		{"zip", "zip"},
	}
	for _, tt := range tests {
		p := profile{Format: tt.format}
		got := p.archiveFormat()
		if got != tt.want {
			t.Errorf("profile{Format:%q}.archiveFormat() = %q, want %q", tt.format, got, tt.want)
		}
	}
}

func TestProfileClientOptions(t *testing.T) {
	p := profile{
		Concurrency: 8,
		Timeout:     30,
		BatchSize:   50,
		RetryPasses: 3,
	}
	opts := p.clientOptions()
	if opts.Concurrency != 8 {
		t.Errorf("Concurrency = %d, want 8", opts.Concurrency)
	}
	if opts.Timeout != 30 {
		t.Errorf("Timeout = %d, want 30", opts.Timeout)
	}
	if opts.BatchSize != 50 {
		t.Errorf("BatchSize = %d, want 50", opts.BatchSize)
	}
	if opts.RetryPasses != 3 {
		t.Errorf("RetryPasses = %d, want 3", opts.RetryPasses)
	}
}

func TestProfileClientOptions_Defaults(t *testing.T) {
	p := profile{}
	opts := p.clientOptions()
	if opts.Concurrency != 0 {
		t.Errorf("Concurrency = %d, want 0 (use default)", opts.Concurrency)
	}
}

func TestSortedProfileNames(t *testing.T) {
	cfg := dietConfig{
		Profiles: map[string]profile{
			"charlie": {},
			"alpha":   {},
			"bravo":   {},
		},
	}
	names := sortedProfileNames(cfg)
	if len(names) != 3 {
		t.Fatalf("expected 3 names, got %d", len(names))
	}
	if names[0] != "alpha" || names[1] != "bravo" || names[2] != "charlie" {
		t.Errorf("got %v, want [alpha bravo charlie]", names)
	}
}

func TestSortedProfileNames_Empty(t *testing.T) {
	cfg := dietConfig{Profiles: map[string]profile{}}
	names := sortedProfileNames(cfg)
	if len(names) != 0 {
		t.Errorf("expected empty, got %v", names)
	}
}

func TestConfigPathDisplay(t *testing.T) {
	display := configPathDisplay()
	if display == "" {
		t.Error("configPathDisplay returned empty string")
	}
	// Should start with ~ if home dir is resolvable.
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		if display[0] != '~' {
			t.Errorf("configPathDisplay = %q, expected to start with ~", display)
		}
	}
}

func TestSaveAndLoadConfig(t *testing.T) {
	dir := t.TempDir()
	origXDG := os.Getenv("XDG_CONFIG_HOME")
	t.Setenv("XDG_CONFIG_HOME", dir)
	defer os.Setenv("XDG_CONFIG_HOME", origXDG)

	cfg := dietConfig{
		Profiles: map[string]profile{
			"test": {URL: "https://example.com", Token: "secret123", Concurrency: 8},
		},
	}
	if err := saveConfig(cfg); err != nil {
		t.Fatalf("saveConfig: %v", err)
	}

	// Verify file permissions.
	path := filepath.Join(dir, "diet", "config.yml")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("config file not created: %v", err)
	}
	perm := info.Mode().Perm()
	if perm != 0o600 {
		t.Errorf("config file permissions = %o, want 0600", perm)
	}

	// Verify directory permissions.
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("config dir not created: %v", err)
	}
	dirPerm := dirInfo.Mode().Perm()
	if dirPerm != 0o700 {
		t.Errorf("config dir permissions = %o, want 0700", dirPerm)
	}

	// Load and verify.
	loaded := loadConfig()
	if len(loaded.Profiles) != 1 {
		t.Fatalf("expected 1 profile, got %d", len(loaded.Profiles))
	}
	p := loaded.Profiles["test"]
	if p.URL != "https://example.com" {
		t.Errorf("URL = %q", p.URL)
	}
	if p.Token != "secret123" {
		t.Errorf("Token = %q", p.Token)
	}
	if p.Concurrency != 8 {
		t.Errorf("Concurrency = %d, want 8", p.Concurrency)
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	cfg := loadConfig()
	if cfg.Profiles == nil {
		t.Error("expected non-nil Profiles map")
	}
	if len(cfg.Profiles) != 0 {
		t.Errorf("expected empty profiles, got %d", len(cfg.Profiles))
	}
}
