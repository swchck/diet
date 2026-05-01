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

	root.AddCommand(newExportCmd())
	root.AddCommand(newImportCmd())
	root.AddCommand(newCleanCmd())
	root.AddCommand(newDiffCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
