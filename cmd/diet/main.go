package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const version = "0.1.0"

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
