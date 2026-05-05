package main

import (
	"runtime/debug"
	"testing"
)

// TestVersionFromBuildInfo covers each branch of the version resolver:
// real module version (go install @vX.Y.Z), pseudo-version, devel +
// VCS commit, devel + dirty commit, and the no-info fallback.
func TestVersionFromBuildInfo(t *testing.T) {
	tests := []struct {
		name string
		info *debug.BuildInfo
		want string
	}{
		{
			name: "tagged release: prefer Main.Version",
			info: &debug.BuildInfo{
				Main: debug.Module{Version: "v1.2.3"},
			},
			want: "v1.2.3",
		},
		{
			name: "pseudo-version: still preferred over VCS settings",
			info: &debug.BuildInfo{
				Main: debug.Module{Version: "v0.1.2-0.20260505122312-642420cad2b7"},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "ignored"},
				},
			},
			want: "v0.1.2-0.20260505122312-642420cad2b7",
		},
		{
			name: "(devel) main: fall back to short VCS revision with dev- prefix",
			info: &debug.BuildInfo{
				Main: debug.Module{Version: "(devel)"},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "abcdef1234567890"},
					{Key: "vcs.modified", Value: "false"},
				},
			},
			want: "dev-abcdef1",
		},
		{
			name: "dirty working tree: append -dirty suffix",
			info: &debug.BuildInfo{
				Main: debug.Module{Version: "(devel)"},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "abcdef1234567890"},
					{Key: "vcs.modified", Value: "true"},
				},
			},
			want: "dev-abcdef1-dirty",
		},
		{
			name: "short revision: don't truncate below 7 chars",
			info: &debug.BuildInfo{
				Main: debug.Module{Version: ""},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "abc"},
				},
			},
			want: "dev-abc",
		},
		{
			name: "empty Main.Version + no VCS info: literal dev",
			info: &debug.BuildInfo{
				Main: debug.Module{Version: ""},
			},
			want: "dev",
		},
		{
			name: "(devel) + no VCS info: literal dev",
			info: &debug.BuildInfo{
				Main: debug.Module{Version: "(devel)"},
			},
			want: "dev",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := versionFromBuildInfo(tt.info); got != tt.want {
				t.Errorf("versionFromBuildInfo() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestVersionPackageGlobal_NotZeroValue — sanity that the package-level
// `version` is populated (by init() → resolveVersion() → debug info).
// The exact value depends on how the test binary was built, so we just
// assert non-empty and not the legacy hardcoded "0.1.0" sentinel.
func TestVersionPackageGlobal_NotZeroValue(t *testing.T) {
	if version == "" {
		t.Errorf("package-level version is empty; init() didn't run or fell through")
	}
	if version == "0.1.0" {
		t.Errorf("version is the legacy hardcoded '0.1.0' — fix didn't take effect")
	}
}
