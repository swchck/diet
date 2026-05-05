package main

import (
	"encoding/json"
	"fmt"
	"strings"
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
	cmd.Flags().Bool("system", false, "Also export system entities (flows, dashboards, roles, users, etc.). TUI exports them via the picker; this flag enables them in --plain/--all mode where there is no picker.")
	cmd.Flags().StringSlice("collections", nil, "Comma-separated list of user collections to export (no TUI). Mutually exclusive with --all and the TUI. Use this for non-interactive single-collection exports.")
	return cmd
}

func runExport(cmd *cobra.Command, args []string) error {
	url, _ := cmd.Flags().GetString("url")
	token, _ := cmd.Flags().GetString("token")
	output, _ := cmd.Flags().GetString("output")
	format, _ := cmd.Flags().GetString("format")
	plain, _ := cmd.Flags().GetBool("plain")
	all, _ := cmd.Flags().GetBool("all")
	includeSystem, _ := cmd.Flags().GetBool("system")
	collections, _ := cmd.Flags().GetStringSlice("collections")

	if url == "" || token == "" {
		return fmt.Errorf("--url and --token are required")
	}

	if len(collections) > 0 && all {
		return fmt.Errorf("--collections is mutually exclusive with --all")
	}

	client := newClientWithOptions(url, token, clientOptionsFromFlags(cmd))

	if len(collections) > 0 {
		return runFilteredExport(client, url, output, format, includeSystem, collections)
	}

	if plain || all {
		return runSimpleExport(client, url, output, format, all, includeSystem)
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
//
// includeSystem mirrors the TUI's system-entity picker — when true, every
// flow/dashboard/role/user/translation/webhook is captured along with its
// dependents (operations, panels, presets). When false (the historical
// default), the archive contains only user collections + their data, which
// matches what `diet export --all` has always done.
func runSimpleExport(client *apiClient, sourceURL, output, format string, all, includeSystem bool) error {
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

	// Pull custom fields on system collections (directus_users, etc.) that
	// our relations reference — otherwise the relation would land in the
	// archive but the field it points at would not.
	systemFields, err := fetchSystemCustomFields(client, exportRelations, selectedSet)
	if err != nil {
		return fmt.Errorf("fetch system custom fields: %w", err)
	}
	if len(systemFields) > 0 {
		fmt.Printf("  System custom fields: %d (on %s)\n",
			len(systemFields), summarizeSystemFieldCollections(systemFields))
		allFields = append(allFields, systemFields...)
	}

	fmt.Printf("  %d collections, %d fields, %d relations\n",
		len(exportCollections), len(allFields), len(exportRelations))

	fmt.Println("Pulling data...")
	dataMap := pullAllData(client, selected, func(msg string) {
		fmt.Println(msg)
	})

	var systemData map[string][]json.RawMessage
	var sysNames []string
	if includeSystem {
		fmt.Println("Pulling system entities...")
		systemData = fetchAllSystemEntities(client, selectedSet, func(msg string) {
			fmt.Println("  " + msg)
		})
		for name, items := range systemData {
			sysNames = append(sysNames, name)
			fmt.Printf("  %s: %d\n", name, len(items))
		}
	}

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
		SystemEntities:  sysNames,
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
	if err := createArchive(format, output, manifest, schema, dataMap, systemData); err != nil {
		return fmt.Errorf("create archive: %w", err)
	}

	fmt.Printf("\n%s Export complete\n", okStyle.Render("✓"))
	fmt.Printf("  Archive: %s (%s)\n", output, archiveSize(output))
	fmt.Printf("  Collections: %d, Fields: %d, Relations: %d, Items: %d\n",
		len(selected), len(allFields), len(exportRelations), totalItems)
	if len(systemData) > 0 {
		totalSys := 0
		for _, items := range systemData {
			totalSys += len(items)
		}
		fmt.Printf("  System: %d types, %d items\n", len(systemData), totalSys)
	}

	return nil
}

// runFilteredExport handles `--collections=foo,bar` mode: validate the
// names against the live source first (so a typo errors out before we
// build a half-empty archive), then drive the same fetch path as
// runSimpleExport but seeded with the user-supplied list instead of
// "everything".
//
// Equivalent in spirit to runSimpleExport(--all=false), with the
// collection list explicitly constrained. We don't share a code path
// with runSimpleExport because validation has to happen up front and
// the messaging is different (the user picked a specific subset, not
// "all").
func runFilteredExport(client *apiClient, sourceURL, output, format string, includeSystem bool, names []string) error {
	fmt.Println("Connecting to", sourceURL)
	collections, err := fetchCollections(client)
	if err != nil {
		return fmt.Errorf("fetch collections: %w", err)
	}

	available := make(map[string]bool, len(collections))
	for _, c := range collections {
		isFolder := string(c.Schema) == "null" || len(c.Schema) == 0
		if !isFolder {
			available[c.Collection] = true
		}
	}
	var missing []string
	for _, n := range names {
		if !available[n] {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("--collections: not found on source: %v", missing)
	}

	selectedSet := make(map[string]bool, len(names))
	for _, n := range names {
		selectedSet[n] = true
	}

	var exportCollections []CollectionInfo
	for _, c := range collections {
		isFolder := string(c.Schema) == "null" || len(c.Schema) == 0
		if isFolder || selectedSet[c.Collection] {
			exportCollections = append(exportCollections, c)
		}
	}

	fmt.Printf("Fetching schema for %d collections...\n", len(names))
	allFields, err := fetchAllFields(client, names)
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

	systemFields, err := fetchSystemCustomFields(client, exportRelations, selectedSet)
	if err != nil {
		return fmt.Errorf("fetch system custom fields: %w", err)
	}
	if len(systemFields) > 0 {
		allFields = append(allFields, systemFields...)
	}

	fmt.Printf("  %d collections, %d fields, %d relations\n",
		len(exportCollections), len(allFields), len(exportRelations))

	fmt.Println("Pulling data...")
	dataMap := pullAllData(client, names, func(msg string) {
		fmt.Println(msg)
	})

	var systemData map[string][]json.RawMessage
	var sysNames []string
	if includeSystem {
		fmt.Println("Pulling system entities...")
		systemData = fetchAllSystemEntities(client, selectedSet, func(msg string) {
			fmt.Println("  " + msg)
		})
		for name := range systemData {
			sysNames = append(sysNames, name)
		}
	}

	directusVersion := ""
	var si struct {
		Data struct {
			Version string `json:"version"`
		} `json:"data"`
	}
	if body, err := client.get("/server/info"); err == nil {
		json.Unmarshal(body, &si)
		directusVersion = si.Data.Version
	}

	itemCounts := make(map[string]int, len(dataMap))
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
		Collections:     names,
		ItemCounts:      itemCounts,
		SystemEntities:  sysNames,
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
	if err := createArchive(format, output, manifest, schema, dataMap, systemData); err != nil {
		return fmt.Errorf("create archive: %w", err)
	}

	fmt.Printf("\n%s Export complete\n", okStyle.Render("✓"))
	fmt.Printf("  Archive: %s (%s)\n", output, archiveSize(output))
	fmt.Printf("  Collections: %s\n", strings.Join(names, ", "))
	fmt.Printf("  Items: %d\n", totalItems)
	if len(systemData) > 0 {
		totalSys := 0
		for _, items := range systemData {
			totalSys += len(items)
		}
		fmt.Printf("  System: %d types, %d items\n", len(systemData), totalSys)
	}

	return nil
}

// summarizeSystemFieldCollections renders a comma-joined list of distinct
// system collections that ended up contributing custom fields. Callers use
// it for a one-line export-progress hint.
func summarizeSystemFieldCollections(fields []FieldInfo) string {
	seen := map[string]bool{}
	var names []string
	for _, f := range fields {
		if seen[f.Collection] {
			continue
		}
		seen[f.Collection] = true
		names = append(names, f.Collection)
	}
	return strings.Join(names, ", ")
}
