package main

import (
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
)

func newImportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import collections from an archive into Directus",
		RunE:  runImport,
	}
	cmd.Flags().StringP("input", "i", "", "Input archive path (required)")
	cmd.Flags().String("target-url", "", "Target Directus URL (required)")
	cmd.Flags().String("target-token", "", "Target Directus static token (required)")
	cmd.Flags().String("email", "", "Admin email for token refresh")
	cmd.Flags().String("password", "", "Admin password for token refresh")
	cmd.Flags().Bool("data", true, "Also import item data")
	_ = cmd.MarkFlagRequired("input")
	_ = cmd.MarkFlagRequired("target-url")
	_ = cmd.MarkFlagRequired("target-token")
	return cmd
}

func runImport(cmd *cobra.Command, args []string) error {
	input, _ := cmd.Flags().GetString("input")
	targetURL, _ := cmd.Flags().GetString("target-url")
	targetToken, _ := cmd.Flags().GetString("target-token")
	email, _ := cmd.Flags().GetString("email")
	password, _ := cmd.Flags().GetString("password")
	importData, _ := cmd.Flags().GetBool("data")
	simpleUI, _ := cmd.Flags().GetBool("simpleui")

	client := newClient(targetURL, targetToken)
	client.email = email
	client.password = password

	return executeImport(client, input, importData, !simpleUI)
}

func executeImport(client *apiClient, inputFile string, importData, useTUI bool) error {
	fmt.Println("Reading archive:", inputFile)
	manifest, schema, data, systemData, err := extractArchive(inputFile)
	if err != nil {
		return fmt.Errorf("extract archive: %w", err)
	}

	fmt.Printf("  Source: %s (Directus %s)\n", manifest.SourceURL, manifest.DirectusVersion)
	fmt.Printf("  Collections: %d, Items: %d\n", len(manifest.Collections), sumCounts(manifest.ItemCounts))

	tracker := newTracker()
	logFn := func(msg string) {
		tracker.log(msg)
		if !useTUI {
			fmt.Println("  " + msg)
		}
	}

	var program *tea.Program
	if useTUI {
		m := newProgressModel(tracker)
		program = tea.NewProgram(m, tea.WithAltScreen())
		go func() {
			if _, err := program.Run(); err != nil {
				fmt.Fprintln(os.Stderr, "TUI error:", err)
			}
		}()
	}

	killTUI := func() {
		if program != nil {
			program.Send(progressDoneMsg{})
			program.Wait()
		}
	}

	// Count total steps for progress bar.
	steps := 3 // schema: collections + fields + relations
	if importData && len(data) > 0 {
		steps++ // data insertion
	}
	for _, name := range systemImportOrder {
		if items, ok := systemData[name]; ok && len(items) > 0 {
			steps++
		}
	}
	tracker.setTotal(steps)

	tracker.setPhase("Creating collections")
	if err := createCollections(client, schema.Collections, schema.Fields, logFn); err != nil {
		killTUI()
		return err
	}
	tracker.advance()

	tracker.setPhase("Creating fields")
	if err := createFields(client, schema.Fields, logFn); err != nil {
		killTUI()
		return err
	}
	tracker.advance()

	tracker.setPhase("Creating relations")
	if err := createRelations(client, schema.Relations, logFn); err != nil {
		killTUI()
		return err
	}
	tracker.advance()

	if importData && len(data) > 0 {
		tracker.setPhase("Inserting data")
		aliasFields := buildAliasFields(schema.Fields)
		insertOrder := buildInsertOrder(manifest.Collections, schema.Relations)
		progress := applyData(client, insertOrder, data, aliasFields, logFn)
		inserted := int(progress.inserted.Load())
		logFn(fmt.Sprintf("Data: %d/%d items inserted", inserted, progress.total))
		tracker.advance()
	}

	if len(systemData) > 0 {
		tracker.setPhase("Importing system entities")
		for _, name := range systemImportOrder {
			items, ok := systemData[name]
			if !ok || len(items) == 0 {
				continue
			}
			entity, ok := systemEntityByName(name)
			if !ok {
				continue
			}
			ins, fail := insertSystemItems(client, entity.Endpoint, items)
			logFn(fmt.Sprintf("System %s: %d inserted, %d failed", name, ins, fail))
			tracker.advance()
		}
	}

	tracker.setPhase("Complete")
	killTUI()

	if !useTUI {
		fmt.Printf("\n%s Import complete\n", okStyle.Render("✓"))
	}

	return nil
}

func sumCounts(m map[string]int) int {
	total := 0
	for _, v := range m {
		total += v
	}
	return total
}
