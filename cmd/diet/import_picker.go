package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// pickedSubset is what runImportPicker returns to the import driver.
// nil = user cancelled (the import command should exit cleanly without
// running anything). Empty slices = user confirmed but unselected
// everything; valid but the import driver may want to refuse to send
// nothing to target.
type pickedSubset struct {
	collections    []string
	systemEntities []string
}

// runImportPicker opens the archive at inputPath and presents an
// interactive picker so the user can choose which collections and
// system entity types to import. Returns nil on cancel.
//
// The picker is intentionally simpler than the export-side TUI — there
// the user has to pick from a *live* server with full schema info,
// here all we need is a checkable list of names parsed from the
// archive's manifest.
func runImportPicker(inputPath string) (*pickedSubset, error) {
	manifest, _, _, _, err := extractArchive(inputPath)
	if err != nil {
		return nil, fmt.Errorf("read archive: %w", err)
	}

	cols := append([]string{}, manifest.Collections...)
	sort.Strings(cols)
	sys := append([]string{}, manifest.SystemEntities...)
	sort.Strings(sys)

	if len(cols) == 0 && len(sys) == 0 {
		return nil, fmt.Errorf("archive contains nothing importable: no user collections, no system entities")
	}

	model := newImportPickerModel(cols, sys, inputPath)
	p := tea.NewProgram(model, tea.WithAltScreen())
	final, err := p.Run()
	if err != nil {
		return nil, fmt.Errorf("TUI: %w", err)
	}
	m := final.(importPickerModel)
	if m.cancelled {
		return nil, nil
	}
	return &pickedSubset{
		collections:    m.selected(itemKindCollection),
		systemEntities: m.selected(itemKindSystem),
	}, nil
}

type itemKind int

const (
	itemKindCollection itemKind = iota
	itemKindSystem
)

type pickerItem struct {
	kind     itemKind
	name     string
	selected bool
}

type importPickerModel struct {
	inputPath string
	items     []pickerItem
	cursor    int
	width     int
	height    int
	confirmed bool
	cancelled bool
}

func newImportPickerModel(cols, sys []string, inputPath string) importPickerModel {
	items := make([]pickerItem, 0, len(cols)+len(sys))
	for _, c := range cols {
		items = append(items, pickerItem{kind: itemKindCollection, name: c, selected: true})
	}
	for _, s := range sys {
		items = append(items, pickerItem{kind: itemKindSystem, name: s, selected: true})
	}
	return importPickerModel{inputPath: inputPath, items: items}
}

func (m importPickerModel) selected(kind itemKind) []string {
	var out []string
	for _, it := range m.items {
		if it.kind == kind && it.selected {
			out = append(out, it.name)
		}
	}
	return out
}

func (m importPickerModel) Init() tea.Cmd { return nil }

func (m importPickerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		switch {
		case key.Matches(msg, importPickerKeys.up):
			if m.cursor > 0 {
				m.cursor--
			}
		case key.Matches(msg, importPickerKeys.down):
			if m.cursor < len(m.items)-1 {
				m.cursor++
			}
		case key.Matches(msg, importPickerKeys.toggle):
			if len(m.items) > 0 {
				m.items[m.cursor].selected = !m.items[m.cursor].selected
			}
		case key.Matches(msg, importPickerKeys.toggleAll):
			// If anything is unselected, select all. Else unselect all.
			anyOff := false
			for _, it := range m.items {
				if !it.selected {
					anyOff = true
					break
				}
			}
			for i := range m.items {
				m.items[i].selected = anyOff
			}
		case key.Matches(msg, importPickerKeys.confirm):
			m.confirmed = true
			return m, tea.Quit
		case key.Matches(msg, importPickerKeys.cancel):
			m.cancelled = true
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m importPickerModel) View() string {
	if m.confirmed || m.cancelled {
		return ""
	}

	var b strings.Builder
	title := lipgloss.NewStyle().Bold(true).Render(
		fmt.Sprintf("Import from %s", m.inputPath))
	b.WriteString(title + "\n\n")

	currentKind := itemKind(-1)
	for i, it := range m.items {
		if it.kind != currentKind {
			currentKind = it.kind
			label := "Collections"
			if it.kind == itemKindSystem {
				label = "System Entities"
			}
			b.WriteString(lipgloss.NewStyle().
				Foreground(lipgloss.Color("39")).Bold(true).
				Render("● "+label) + "\n")
		}
		cursor := "  "
		if i == m.cursor {
			cursor = "▸ "
		}
		check := "[ ]"
		if it.selected {
			check = "[x]"
		}
		b.WriteString(fmt.Sprintf("  %s%s %s\n", cursor, check, it.name))
	}

	selCols := len(m.selected(itemKindCollection))
	selSys := len(m.selected(itemKindSystem))
	b.WriteString("\n")
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render(
		fmt.Sprintf("Selected: %d collections, %d system entity types", selCols, selSys),
	))
	b.WriteString("\n\n")
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render(
		"↑/↓ move  space toggle  a toggle-all  enter confirm  q cancel"))
	return b.String()
}

type importPickerKeyMap struct {
	up        key.Binding
	down      key.Binding
	toggle    key.Binding
	toggleAll key.Binding
	confirm   key.Binding
	cancel    key.Binding
}

var importPickerKeys = importPickerKeyMap{
	up:        key.NewBinding(key.WithKeys("up", "k")),
	down:      key.NewBinding(key.WithKeys("down", "j")),
	toggle:    key.NewBinding(key.WithKeys(" ", "x")),
	toggleAll: key.NewBinding(key.WithKeys("a")),
	confirm:   key.NewBinding(key.WithKeys("enter")),
	cancel:    key.NewBinding(key.WithKeys("q", "esc", "ctrl+c")),
}
