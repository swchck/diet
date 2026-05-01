package main

import (
	"encoding/json"
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
)

func newCleanCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clean",
		Short: "Delete collections and system entities from Directus",
		RunE:  runClean,
	}
	cmd.Flags().Bool("all", false, "Select all collections for deletion")
	cmd.Flags().Bool("system", false, "Also include system entities (flows, dashboards, etc.)")
	return cmd
}

func runClean(cmd *cobra.Command, args []string) error {
	url, _ := cmd.Flags().GetString("url")
	token, _ := cmd.Flags().GetString("token")
	simpleUI, _ := cmd.Flags().GetBool("simpleui")
	all, _ := cmd.Flags().GetBool("all")
	system, _ := cmd.Flags().GetBool("system")

	if url == "" || token == "" {
		return fmt.Errorf("--url and --token are required")
	}

	client := newClient(url, token)

	if simpleUI || all {
		return runSimpleClean(client, url, all, system)
	}

	// TUI mode.
	picker := newPicker(client, url, "", "", "", modeClean)
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

func runSimpleClean(client *apiClient, sourceURL string, all, system bool) error {
	fmt.Println("Connecting to", sourceURL)

	collections, err := fetchCollections(client)
	if err != nil {
		return fmt.Errorf("fetch collections: %w", err)
	}

	var userCols []string
	var folders []string
	for _, c := range collections {
		isFolder := string(c.Schema) == "null" || len(c.Schema) == 0
		if isFolder {
			folders = append(folders, c.Collection)
		} else {
			userCols = append(userCols, c.Collection)
		}
	}

	relations, err := fetchRelations(client)
	if err != nil {
		return fmt.Errorf("fetch relations: %w", err)
	}

	// Reverse topological order for deletion.
	insertOrder := buildInsertOrder(userCols, relations)
	deleteOrder := make([]string, len(insertOrder))
	for i, c := range insertOrder {
		deleteOrder[len(insertOrder)-1-i] = c
	}

	fmt.Printf("\nCollections to delete: %d\n", len(deleteOrder))
	for _, c := range deleteOrder {
		fmt.Printf("  - %s\n", c)
	}
	if len(folders) > 0 {
		fmt.Printf("Folders: %d\n", len(folders))
		for _, f := range folders {
			fmt.Printf("  - %s\n", f)
		}
	}

	// System entities.
	type sysInfo struct {
		name  string
		count int
	}
	var sysToDelete []sysInfo
	if system {
		fmt.Println("System entities:")
		for _, name := range systemDeleteOrder {
			entity, ok := systemEntityByName(name)
			if !ok {
				continue
			}
			count := countSystemItems(client, entity.Endpoint)
			if count > 0 {
				sysToDelete = append(sysToDelete, sysInfo{name: entity.Name, count: count})
				fmt.Printf("  - %s (%d items)\n", entity.Name, count)
			}
		}
	}

	if len(deleteOrder) == 0 && len(folders) == 0 && len(sysToDelete) == 0 {
		fmt.Println("Nothing to delete.")
		return nil
	}

	// Confirmation.
	fmt.Print("\nType 'yes' to confirm deletion: ")
	var confirm string
	fmt.Scanln(&confirm)
	if confirm != "yes" {
		fmt.Println("Cancelled.")
		return nil
	}

	// Delete system entities first.
	if len(sysToDelete) > 0 {
		sysSet := make(map[string]bool)
		for _, s := range sysToDelete {
			sysSet[s.name] = true
		}
		fmt.Println("\nDeleting system entities...")
		for _, name := range systemDeleteOrder {
			if !sysSet[name] {
				continue
			}
			entity, _ := systemEntityByName(name)
			items, err := fetchSystemItems(client, entity.Endpoint)
			if err != nil {
				fmt.Printf("  WARN: fetch %s: %v\n", name, err)
				continue
			}
			del, fail := deleteSystemItems(client, entity.Endpoint, items)
			fmt.Printf("  %s: %d deleted, %d failed\n", name, del, fail)
		}
	}

	// Delete collections.
	fmt.Println("\nDeleting collections...")
	for _, col := range deleteOrder {
		if err := deleteCollection(client, col); err != nil {
			fmt.Printf("  WARN: %s: %v\n", col, err)
		} else {
			fmt.Printf("  Deleted: %s\n", col)
		}
	}

	// Delete orphan folders.
	if len(folders) > 0 {
		fmt.Println("Deleting folders...")
		// Re-fetch to see what's left.
		remaining, _ := fetchCollections(client)
		remainingSet := make(map[string]bool)
		for _, c := range remaining {
			remainingSet[c.Collection] = true
		}
		for _, f := range folders {
			if !remainingSet[f] {
				continue
			}
			// Check if folder has remaining children.
			hasChildren := false
			for _, c := range remaining {
				var meta struct{ Group string `json:"group"` }
				json.Unmarshal(c.Meta, &meta)
				if meta.Group == f && c.Collection != f {
					hasChildren = true
					break
				}
			}
			if !hasChildren {
				if err := deleteCollection(client, f); err != nil {
					fmt.Printf("  WARN: %s: %v\n", f, err)
				} else {
					fmt.Printf("  Deleted folder: %s\n", f)
				}
			}
		}
	}

	fmt.Printf("\n%s Clean complete\n", okStyle.Render("✓"))
	return nil
}
