package main

import (
	"encoding/json"
	"fmt"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
)

func newExportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export collections from Directus to an archive",
		RunE:  runExport,
	}
	cmd.Flags().StringP("output", "o", "", "Output archive path (default: auto-generated)")
	cmd.Flags().String("format", "zstd", "Archive format: zstd or zip")
	cmd.Flags().Bool("all", false, "Export all collections without interactive picker")
	return cmd
}

func runExport(cmd *cobra.Command, args []string) error {
	url, _ := cmd.Flags().GetString("url")
	token, _ := cmd.Flags().GetString("token")
	output, _ := cmd.Flags().GetString("output")
	format, _ := cmd.Flags().GetString("format")
	plain, _ := cmd.Flags().GetBool("plain")
	all, _ := cmd.Flags().GetBool("all")

	if url == "" || token == "" {
		return fmt.Errorf("--url and --token are required")
	}

	client := newClient(url, token)

	if plain || all {
		return runSimpleExport(client, url, output, format, all)
	}

	// Full TUI mode — picker opens immediately, loads async.
	picker := newPicker(client, url, "", format, output, modeExport)
	p := tea.NewProgram(picker, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}

	pm := finalModel.(pickerModel)
	if pm.errMsg != "" {
		return fmt.Errorf("%s", pm.errMsg)
	}
	if pm.quitting && !pm.done {
		fmt.Println("Cancelled.")
	}

	return nil
}

// runSimpleExport handles --plain and --all modes (no TUI).
func runSimpleExport(client *apiClient, sourceURL, output, format string, all bool) error {
	fmt.Println("Connecting to", sourceURL)
	collections, err := fetchCollections(client)
	if err != nil {
		return fmt.Errorf("fetch collections: %w", err)
	}

	var selected []string
	for _, c := range collections {
		isFolder := string(c.Schema) == "null" || len(c.Schema) == 0
		if !isFolder {
			selected = append(selected, c.Collection)
		}
	}

	if !all {
		fmt.Printf("Found %d table collections, exporting all\n", len(selected))
	}

	selectedSet := make(map[string]bool)
	for _, s := range selected {
		selectedSet[s] = true
	}

	var exportCollections []CollectionInfo
	for _, c := range collections {
		if selectedSet[c.Collection] || string(c.Schema) == "null" || len(c.Schema) == 0 {
			exportCollections = append(exportCollections, c)
		}
	}

	fmt.Println("Fetching schema...")
	allFields, err := fetchAllFields(client, selected)
	if err != nil {
		return fmt.Errorf("fetch fields: %w", err)
	}

	allRelations, err := fetchRelations(client)
	if err != nil {
		return fmt.Errorf("fetch relations: %w", err)
	}
	var exportRelations []RelationInfo
	for _, r := range allRelations {
		if selectedSet[r.Collection] || selectedSet[r.RelatedCollection] {
			exportRelations = append(exportRelations, r)
		}
	}

	fmt.Printf("  %d collections, %d fields, %d relations\n",
		len(exportCollections), len(allFields), len(exportRelations))

	fmt.Println("Pulling data...")
	dataMap := pullAllData(client, selected, func(msg string) {
		fmt.Println(msg)
	})

	directusVersion := ""
	var si struct {
		Data struct{ Version string `json:"version"` } `json:"data"`
	}
	if body, err := client.get("/server/info"); err == nil {
		json.Unmarshal(body, &si)
		directusVersion = si.Data.Version
	}

	itemCounts := make(map[string]int)
	totalItems := 0
	for col, items := range dataMap {
		itemCounts[col] = len(items)
		totalItems += len(items)
	}

	manifest := Manifest{
		DietVersion:     version,
		DirectusVersion: directusVersion,
		SourceURL:       sourceURL,
		ExportedAt:      time.Now().UTC().Format(time.RFC3339),
		Format:          format,
		Collections:     selected,
		ItemCounts:      itemCounts,
	}

	schema := SchemaBundle{
		Collections: exportCollections,
		Fields:      allFields,
		Relations:   exportRelations,
	}

	if output == "" {
		ext := ".tar.zst"
		if format == "zip" {
			ext = ".zip"
		}
		output = fmt.Sprintf("diet-export-%s%s", time.Now().Format("20060102-150405"), ext)
	}

	fmt.Printf("Packing archive (%s)...\n", format)
	if err := createArchive(format, output, manifest, schema, dataMap, nil); err != nil {
		return fmt.Errorf("create archive: %w", err)
	}

	fmt.Printf("\n%s Export complete\n", okStyle.Render("✓"))
	fmt.Printf("  Archive: %s (%s)\n", output, archiveSize(output))
	fmt.Printf("  Collections: %d, Fields: %d, Relations: %d, Items: %d\n",
		len(selected), len(allFields), len(exportRelations), totalItems)

	return nil
}
