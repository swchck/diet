package main

import (
	"context"
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
	cmd.Flags().Bool("bulk-schema", true, "Use Directus /schema/apply for schema (10-40× faster). Falls back to per-field on error.")
	cmd.Flags().Bool("strip-accountability", false, "Set meta.accountability=null on imported collections (skip activity + revisions logging — ~2-3× faster data import)")
	cmd.Flags().String("db-dsn", "", "Postgres DSN for direct COPY-protocol data load, bypassing the Directus REST API (e.g. postgres://user:pass@host:5432/db?sslmode=disable). Schema still goes through Directus. UNSAFE: skips ACL/hooks/cache — local/CI/manual pipelines only.")
	cmd.Flags().Bool("strict", false, "Exit non-zero if any data row or system entity failed to import. By default partial failures are logged but the command exits 0 (legacy behavior). CI/automation should set --strict to detect silent breakage.")
	cmd.Flags().StringSlice("collections", nil, "Comma-separated user collections to import from the archive (default: all). Relations crossing the boundary are dropped with a warning.")
	cmd.Flags().StringSlice("system-entities", nil, "Comma-separated system entity types to import (flows, dashboards, roles, users, translations, webhooks, operations, panels, presets). Default: all entity types present in the archive.")
	cmd.Flags().Bool("pick", false, "Open an interactive picker to choose collections and system entities from the archive before importing. Mutually exclusive with --collections / --system-entities / --plain.")
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
	bulkSchema, _ := cmd.Flags().GetBool("bulk-schema")
	stripAcc, _ := cmd.Flags().GetBool("strip-accountability")
	dbDSN, _ := cmd.Flags().GetString("db-dsn")
	strict, _ := cmd.Flags().GetBool("strict")
	collectionsFilter, _ := cmd.Flags().GetStringSlice("collections")
	systemEntitiesFilter, _ := cmd.Flags().GetStringSlice("system-entities")
	pick, _ := cmd.Flags().GetBool("pick")
	plain, _ := cmd.Flags().GetBool("plain")

	if pick && (len(collectionsFilter) > 0 || len(systemEntitiesFilter) > 0 || plain) {
		return fmt.Errorf("--pick is mutually exclusive with --collections, --system-entities, --plain")
	}

	client := newClientWithOptions(targetURL, targetToken, clientOptionsFromFlags(cmd))
	client.email = email
	client.password = password

	if pick {
		picked, err := runImportPicker(input)
		if err != nil {
			return err
		}
		if picked == nil {
			fmt.Println("Cancelled.")
			return nil
		}
		collectionsFilter = picked.collections
		systemEntitiesFilter = picked.systemEntities
	}

	return executeImport(client, input, importOpts{
		Data:                importData,
		UseTUI:              !plain && !pick,
		BulkSchema:          bulkSchema,
		StripAccountability: stripAcc,
		DBDSN:               dbDSN,
		Strict:              strict,
		Collections:         collectionsFilter,
		SystemEntities:      systemEntitiesFilter,
	})
}

// importOpts groups per-import toggles. Easier to extend than a positional
// argument list as more flags accrete (and at this point they will).
type importOpts struct {
	Data                bool
	UseTUI              bool
	BulkSchema          bool
	StripAccountability bool
	// DBDSN, when non-empty, switches the data-import phase to a
	// direct-to-Postgres COPY load instead of the REST /items/<col>
	// path. Schema still goes through Directus. CLI-only — never set
	// from the wizard.
	DBDSN string
	// Strict turns "logged but tolerated" partial failures into a
	// non-zero exit. Off by default for backward compatibility — many
	// existing pipelines treat a few FK-orphan rows as expected. CI
	// jobs that care about exact item counts should turn it on.
	Strict bool
	// Collections, when non-empty, restricts the import to the named
	// user collections plus their direct dependencies. Relations and
	// data referencing dropped collections are filtered out before
	// import. Empty = no filter (legacy behavior, import everything).
	Collections []string
	// SystemEntities, when non-empty, restricts the import to the
	// named system entity types ("flows", "dashboards", ...). Empty =
	// no filter.
	SystemEntities []string
}

func executeImport(client *apiClient, inputFile string, opts importOpts) error {
	fmt.Println("Reading archive:", inputFile)
	manifest, schema, data, systemData, err := extractArchive(inputFile)
	if err != nil {
		return fmt.Errorf("extract archive: %w", err)
	}

	if len(opts.Collections) > 0 {
		var report filterReport
		manifest, schema, data, report = filterArchiveSubset(manifest, schema, data, opts.Collections)
		if len(report.missingFromKeep) > 0 {
			return fmt.Errorf("--collections: not in archive: %v (available: %v)",
				report.missingFromKeep, manifest.Collections)
		}
		fmt.Printf("  Filter: %d collections kept; dropped %d cross-boundary relations, %d orphaned system fields\n",
			len(manifest.Collections), report.droppedRelations, report.droppedSystemFields)
		if len(manifest.Collections) == 0 {
			return fmt.Errorf("--collections produced empty selection — refusing to send empty schema/data to target")
		}
	}
	if len(opts.SystemEntities) > 0 {
		manifest, systemData = filterSystemSubset(manifest, systemData, opts.SystemEntities)
		fmt.Printf("  Filter: %d system entity types kept\n", len(systemData))
	}

	fmt.Printf("  Source: %s (Directus %s)\n", manifest.SourceURL, manifest.DirectusVersion)
	fmt.Printf("  Collections: %d, Items: %d\n", len(manifest.Collections), sumCounts(manifest.ItemCounts))

	tracker := newTracker()
	logFn := func(msg string) {
		tracker.log(msg)
		if !opts.UseTUI {
			fmt.Println("  " + msg)
		}
	}

	var program *tea.Program
	if opts.UseTUI {
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

	// Count total steps for progress bar. Bulk schema collapses the three
	// per-phase steps (collections + fields + relations) into one.
	schemaSteps := 3
	if opts.BulkSchema {
		schemaSteps = 1
	}
	steps := schemaSteps
	if opts.Data && len(data) > 0 {
		steps++ // data insertion
	}
	for _, name := range systemImportOrder {
		if items, ok := systemData[name]; ok && len(items) > 0 {
			steps++
		}
	}
	tracker.setTotal(steps)

	if opts.StripAccountability {
		n := stripAccountability(&schema)
		logFn(fmt.Sprintf("Stripped accountability from %d collections (no audit log on import)", n))
	}

	if opts.BulkSchema {
		tracker.setPhase("Applying schema (bulk)")
		if err := schemaApplyBulk(client, schema, logFn); err != nil {
			logFn(fmt.Sprintf("WARN: bulk schema failed (%v) — falling back to per-field", err))
			if err := runPerFieldSchema(client, schema, tracker, logFn); err != nil {
				killTUI()
				return err
			}
		} else {
			tracker.advance()
		}
	} else {
		if err := runPerFieldSchema(client, schema, tracker, logFn); err != nil {
			killTUI()
			return err
		}
	}

	// Aggregate counters across data + system phases so we can report a
	// single bottom-line and decide whether --strict should turn this
	// into a non-zero exit.
	var (
		dataInserted, dataTotal int
		sysInserted, sysFailed  int
	)

	if opts.Data && len(data) > 0 {
		aliasFields := buildAliasFields(schema.Fields)
		insertOrder := buildInsertOrder(manifest.Collections, schema.Relations)

		var progress *dataProgress
		if opts.DBDSN != "" {
			tracker.setPhase("Inserting data (direct DB)")
			logFn("Direct-DB mode: bypassing Directus REST for item inserts")
			p, err := applyDataDirect(
				context.Background(),
				opts.DBDSN,
				insertOrder,
				data,
				schema.Fields,
				aliasFields,
				client.Concurrency,
				client.RetryPasses,
				logFn,
			)
			if err != nil {
				logFn(fmt.Sprintf("WARN: direct-DB load failed (%v) — falling back to REST", err))
				tracker.setPhase("Inserting data (fallback REST)")
				progress = applyData(client, insertOrder, data, aliasFields, logFn)
			} else {
				progress = p
			}
		} else {
			tracker.setPhase("Inserting data")
			progress = applyData(client, insertOrder, data, aliasFields, logFn)
		}

		dataInserted = int(progress.inserted.Load())
		dataTotal = progress.total
		logFn(fmt.Sprintf("Data: %d/%d items inserted", dataInserted, dataTotal))
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
			sysInserted += ins
			sysFailed += fail
			logFn(fmt.Sprintf("System %s: %d inserted, %d failed", name, ins, fail))
			tracker.advance()
		}
	}

	tracker.setPhase("Complete")
	killTUI()

	if !opts.UseTUI {
		fmt.Printf("\n%s Import complete\n", okStyle.Render("✓"))
	}

	return classifyImportOutcome(opts.Strict, dataInserted, dataTotal, sysInserted, sysFailed)
}

// classifyImportOutcome turns the aggregate counters into either nil
// (success), a hard error (catastrophic — nothing landed when something
// should have), or a strict-mode error (any partial loss when --strict
// is set). Pulled out so it's straightforward to unit test the policy
// independently from the import driver.
func classifyImportOutcome(strict bool, dataInserted, dataTotal, sysInserted, sysFailed int) error {
	dataLoss := dataTotal - dataInserted

	// Catastrophic — we tried to import data and 0 of N landed. No
	// reasonable caller wants exit 0 here regardless of --strict.
	if dataTotal > 0 && dataInserted == 0 {
		return fmt.Errorf("import failed: 0 of %d data items inserted (target unreachable, FK chain broken, or schema mismatch)", dataTotal)
	}

	if !strict {
		return nil
	}

	if dataLoss > 0 || sysFailed > 0 {
		return fmt.Errorf("strict mode: %d/%d data items lost, %d system items failed (set --strict=false to ignore partial failures)",
			dataLoss, dataTotal, sysFailed)
	}
	return nil
}

// runPerFieldSchema runs the legacy per-field schema path: createCollections,
// createFields, createRelations. Used as fallback when /schema/apply fails
// (e.g. payload limit, version mismatch) or when --bulk-schema=false.
func runPerFieldSchema(client *apiClient, schema SchemaBundle, tracker *progressTracker, logFn func(string)) error {
	tracker.setPhase("Creating collections")
	if err := createCollections(client, schema.Collections, schema.Fields, logFn); err != nil {
		return err
	}
	tracker.advance()

	tracker.setPhase("Creating fields")
	if err := createFields(client, schema.Fields, logFn); err != nil {
		return err
	}
	tracker.advance()

	tracker.setPhase("Creating relations")
	if err := createRelations(client, schema.Relations, logFn); err != nil {
		return err
	}
	tracker.advance()
	return nil
}

func sumCounts(m map[string]int) int {
	total := 0
	for _, v := range m {
		total += v
	}
	return total
}
