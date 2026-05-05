package main

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/spf13/cobra"
)

// version is resolved at startup by resolveVersion(). Declared as `var`
// (not const) so a `-ldflags "-X main.version=..."` build can pin a
// specific value — useful for release tags where the import path
// `@v1.2.3` semantics from `go install` aren't available (e.g.
// homebrew/docker recipes).
var version = ""

func init() {
	if version == "" {
		version = resolveVersion()
	}
}

// resolveVersion picks the most accurate version string available:
//
//  1. ldflags-provided value (already non-empty by the time init runs;
//     we won't get here in that case — see init())
//  2. module Version from `go install github.com/swchck/diet/cmd/diet@vX.Y.Z`
//  3. VCS revision from `go build` inside the repo, prefixed `dev-`
//  4. literal "dev" as last resort
//
// Why not just `const version = "0.1.0"`: the original hardcode never
// updated, so every archive's manifest claimed `diet_version: "0.1.0"`
// regardless of what binary actually wrote it — useless for diagnosing
// "which build produced this archive" after the fact.
func resolveVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	return versionFromBuildInfo(info)
}

// versionFromBuildInfo is the pure-logic half of resolveVersion: maps a
// debug.BuildInfo (which the runtime fills) to our preferred string.
// Pulled out so tests can synthesize BuildInfo values directly without
// having to actually compile-and-exec a binary in each scenario.
func versionFromBuildInfo(info *debug.BuildInfo) string {
	if v := info.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	var revision, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if revision != "" {
		short := revision
		if len(short) > 7 {
			short = short[:7]
		}
		out := "dev-" + short
		if modified == "true" {
			out += "-dirty"
		}
		return out
	}
	return "dev"
}

func main() {
	root := &cobra.Command{
		Use:     "diet",
		Short:   "DIET — Directus Import Export Tool",
		Long:    "Full-fidelity export and import of Directus collections with all metadata, validation, and field configuration. API-only, no database connection required.",
		Version: version,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runWizard()
		},
	}

	// Global flags.
	root.PersistentFlags().String("url", "", "Source Directus URL")
	root.PersistentFlags().String("token", "", "Source Directus static token")
	root.PersistentFlags().Bool("plain", false, "Plain text output for scripting (no TUI)")

	// HTTP client tuning. Zero means "use the package default" — see
	// clientOptions in client.go. Mirrored in YAML profile fields, so both
	// CLI flags and config can drive them.
	root.PersistentFlags().Int("concurrency", 0, "Parallel HTTP workers (default 6)")
	root.PersistentFlags().Int("timeout", 0, "HTTP timeout in seconds (default 60)")
	root.PersistentFlags().Int("batch-size", 0, "Items per batch POST (default 100)")
	root.PersistentFlags().Int("retry-passes", 0, "Max retry passes for FK failures (default 5)")

	root.AddCommand(newExportCmd())
	root.AddCommand(newImportCmd())
	root.AddCommand(newCleanCmd())
	root.AddCommand(newDiffCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
