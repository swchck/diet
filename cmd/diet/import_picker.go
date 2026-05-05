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
	// options carries the safety toggles set on the parameters page.
	// The CLI path uses individual flags directly; the TUI funnels
	// through this struct so the same import driver works for both.
	options pickedOptions
}

// pickedOptions holds the safety toggles surfaced on the picker's
// parameters page. Defaults are chosen to minimise side-effects on the
// target when the user is doing a paranoid partial import (e.g. taking
// one new collection from staging to prod). Toggle off if you want the
// non-conservative behavior.
type pickedOptions struct {
	// NoFolders drops folder (schema-less) collections from the archive
	// before /schema/apply, so the target's admin-UI hierarchy never
	// shifts.
	NoFolders bool
	// SkipData skips the data-import phase entirely (schema-only run).
	// Useful when you want to land a new collection's structure first
	// and load rows separately, or when the archive itself has zero
	// rows for the kept collections.
	SkipData bool
	// StripAccountability rewrites meta.accountability=null on imported
	// collections — faster import but mutates the meta of every
	// collection in the archive, including ones the user kept. Off by
	// default in the TUI.
	StripAccountability bool
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
		options:        m.collectOptions(),
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

// pickerPage is the two-step wizard state. Page 1 (items) is the original
// behavior; page 2 (params) was added so users on a "ship one collection
// to prod" path can review safety toggles before triggering the apply.
type pickerPage int

const (
	pagePickItems pickerPage = iota
	pagePickParams
)

// paramItem is a row on the parameters page. `key` identifies which option
// the row drives; `on` is the current value; `defaultOn` lets us render
// "(default)" hints. `desc` is the inline help line shown on the next row.
type paramItem struct {
	key       string
	label     string
	desc      string
	on        bool
	defaultOn bool
}

type importPickerModel struct {
	page        pickerPage
	inputPath   string
	items       []pickerItem
	params      []paramItem
	cursor      int // active row on current page
	width       int
	height      int
	confirmed   bool
	cancelled   bool
}

// defaultParamItems returns the safety-first defaults shown on the params
// page. ORDER matters — it's the rendering order. Keys MUST stay stable
// because collectOptions matches on them.
func defaultParamItems() []paramItem {
	return []paramItem{
		{
			key:       "no-folders",
			label:     "Skip folder collections",
			desc:      "Don't change the admin-UI hierarchy on target. Recommended for prod imports.",
			on:        true,
			defaultOn: true,
		},
		{
			key:       "skip-data",
			label:     "Skip data import (schema-only)",
			desc:      "Apply schema changes but don't INSERT any rows. Useful for prod-first schema rollouts.",
			on:        false,
			defaultOn: false,
		},
		{
			key:       "strip-accountability",
			label:     "Strip accountability (faster, mutates meta)",
			desc:      "Sets meta.accountability=null on every imported collection. ~2-3× faster but overwrites the target's audit-log setting.",
			on:        false,
			defaultOn: false,
		},
	}
}

func newImportPickerModel(cols, sys []string, inputPath string) importPickerModel {
	items := make([]pickerItem, 0, len(cols)+len(sys))
	for _, c := range cols {
		items = append(items, pickerItem{kind: itemKindCollection, name: c, selected: true})
	}
	for _, s := range sys {
		items = append(items, pickerItem{kind: itemKindSystem, name: s, selected: true})
	}
	return importPickerModel{
		inputPath: inputPath,
		items:     items,
		params:    defaultParamItems(),
	}
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

// collectOptions reads the params page state and returns the typed struct
// the import driver consumes. Pulled out so it's directly unit-testable
// without spinning up bubbletea.
func (m importPickerModel) collectOptions() pickedOptions {
	var opts pickedOptions
	for _, p := range m.params {
		switch p.key {
		case "no-folders":
			opts.NoFolders = p.on
		case "skip-data":
			opts.SkipData = p.on
		case "strip-accountability":
			opts.StripAccountability = p.on
		}
	}
	return opts
}

func (m importPickerModel) Init() tea.Cmd { return nil }

func (m importPickerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

// handleKey dispatches a keypress depending on the active page. Each page
// has its own cursor bounds and confirm semantics:
//   - Items page enter advances to params page (does NOT confirm import).
//   - Params page enter confirms and quits.
//   - Params page back/b returns to items.
//   - cancel (q/esc/ctrl+c) always aborts the whole picker.
func (m importPickerModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if key.Matches(msg, importPickerKeys.cancel) {
		m.cancelled = true
		return m, tea.Quit
	}
	switch m.page {
	case pagePickItems:
		return m.handleItemsKey(msg)
	case pagePickParams:
		return m.handleParamsKey(msg)
	}
	return m, nil
}

func (m importPickerModel) handleItemsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
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
		// Advance to params page rather than confirming. cursor reset so
		// the user lands on the first toggle.
		m.page = pagePickParams
		m.cursor = 0
	}
	return m, nil
}

func (m importPickerModel) handleParamsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, importPickerKeys.up):
		if m.cursor > 0 {
			m.cursor--
		}
	case key.Matches(msg, importPickerKeys.down):
		if m.cursor < len(m.params)-1 {
			m.cursor++
		}
	case key.Matches(msg, importPickerKeys.toggle):
		if len(m.params) > 0 {
			m.params[m.cursor].on = !m.params[m.cursor].on
		}
	case key.Matches(msg, importPickerKeys.back):
		// Return to items page, restoring its cursor to the top.
		m.page = pagePickItems
		m.cursor = 0
	case key.Matches(msg, importPickerKeys.confirm):
		m.confirmed = true
		return m, tea.Quit
	}
	return m, nil
}

func (m importPickerModel) View() string {
	if m.confirmed || m.cancelled {
		return ""
	}
	switch m.page {
	case pagePickItems:
		return m.viewItems()
	case pagePickParams:
		return m.viewParams()
	}
	return ""
}

func (m importPickerModel) viewItems() string {
	var b strings.Builder
	title := lipgloss.NewStyle().Bold(true).Render(
		fmt.Sprintf("Import from %s", m.inputPath))
	b.WriteString(title + "\n")
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render(
		"Step 1 of 2 — pick what to import") + "\n\n")

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
		"↑/↓ move  space toggle  a toggle-all  enter next  q cancel"))
	return b.String()
}

func (m importPickerModel) viewParams() string {
	var b strings.Builder
	title := lipgloss.NewStyle().Bold(true).Render(
		fmt.Sprintf("Import from %s", m.inputPath))
	b.WriteString(title + "\n")
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render(
		"Step 2 of 2 — safety options (defaults are conservative)") + "\n\n")

	hint := lipgloss.NewStyle().Foreground(lipgloss.Color("245"))
	for i, p := range m.params {
		cursor := "  "
		if i == m.cursor {
			cursor = "▸ "
		}
		check := "[ ]"
		if p.on {
			check = "[x]"
		}
		// Mark non-default toggles so the user can spot at a glance
		// when they've moved off the safe baseline.
		marker := ""
		if p.on != p.defaultOn {
			marker = lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Render(" *")
		}
		b.WriteString(fmt.Sprintf("  %s%s %s%s\n", cursor, check, p.label, marker))
		b.WriteString(hint.Render(fmt.Sprintf("        %s", p.desc)) + "\n")
	}

	b.WriteString("\n")
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render(
		"* = changed from default") + "\n")
	b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("245")).Render(
		"↑/↓ move  space toggle  enter import  b back  q cancel"))
	return b.String()
}

type importPickerKeyMap struct {
	up        key.Binding
	down      key.Binding
	toggle    key.Binding
	toggleAll key.Binding
	confirm   key.Binding
	cancel    key.Binding
	back      key.Binding
}

var importPickerKeys = importPickerKeyMap{
	up:        key.NewBinding(key.WithKeys("up", "k")),
	down:      key.NewBinding(key.WithKeys("down", "j")),
	toggle:    key.NewBinding(key.WithKeys(" ", "x")),
	toggleAll: key.NewBinding(key.WithKeys("a")),
	confirm:   key.NewBinding(key.WithKeys("enter")),
	cancel:    key.NewBinding(key.WithKeys("q", "esc", "ctrl+c")),
	back:      key.NewBinding(key.WithKeys("b", "backspace", "left", "h")),
}
