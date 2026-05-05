package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/table"
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
// The visual language matches the export-side picker (tabs + bubbles
// table for items) and the wizard's bordered framing (params page),
// so users moving between operations see consistent UI.
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

	model := newImportPickerModel(cols, sys, inputPath, manifest)
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
	// itemCount is the number of rows the archive has for this
	// collection (0 for system entities — those have their own counts
	// reported separately by the manifest).
	itemCount int
}

// pickerPage is the two-step picker state. Page 1 (items) is the table
// of collections + system entities; page 2 (params) is the safety-
// toggles screen. Splitting them keeps each screen visually tidy and
// lets `b` walk back without losing the items selection.
type pickerPage int

const (
	pagePickItems pickerPage = iota
	pagePickParams
)

// paramItem is a row on the parameters page.
type paramItem struct {
	key       string
	label     string
	desc      string
	on        bool
	defaultOn bool
}

// importTab labels the segments shown above the items table.
type importTab struct {
	label string
	kind  itemKind
}

type importPickerModel struct {
	page      pickerPage
	inputPath string

	// Archive metadata for the header block.
	sourceURL       string
	directusVersion string
	exportedAt      string

	// Items page
	items     []pickerItem
	tabs      []importTab
	activeTab int
	tbl       table.Model
	tblReady  bool

	// Params page
	params      []paramItem
	paramCursor int

	// State
	width     int
	height    int
	confirmed bool
	cancelled bool
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

func newImportPickerModel(cols, sys []string, inputPath string, manifest Manifest) importPickerModel {
	items := make([]pickerItem, 0, len(cols)+len(sys))
	for _, c := range cols {
		items = append(items, pickerItem{
			kind:      itemKindCollection,
			name:      c,
			selected:  true,
			itemCount: manifest.ItemCounts[c],
		})
	}
	for _, s := range sys {
		items = append(items, pickerItem{kind: itemKindSystem, name: s, selected: true})
	}

	tabs := []importTab{{label: "Collections", kind: itemKindCollection}}
	if len(sys) > 0 {
		tabs = append(tabs, importTab{label: "System Entities", kind: itemKindSystem})
	}

	m := importPickerModel{
		inputPath:       inputPath,
		sourceURL:       manifest.SourceURL,
		directusVersion: manifest.DirectusVersion,
		exportedAt:      manifest.ExportedAt,
		items:           items,
		tabs:            tabs,
		params:          defaultParamItems(),
	}
	m.rebuildTable()
	return m
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

// activeKind returns the itemKind backing the current tab. With no system
// entities in the archive, the second tab simply doesn't exist and we
// always render collections.
func (m importPickerModel) activeKind() itemKind {
	if m.activeTab >= 0 && m.activeTab < len(m.tabs) {
		return m.tabs[m.activeTab].kind
	}
	return itemKindCollection
}

func (m *importPickerModel) rebuildTable() {
	w := m.width
	if w <= 0 {
		w = 80
	}
	// Reserve a slim col for the selection dot, give the items count
	// some room, and let the name take the rest. Same spacing as the
	// export picker.
	nameW := max(w-28, 30)

	kind := m.activeKind()
	var cols []table.Column
	if kind == itemKindCollection {
		cols = []table.Column{
			{Title: "Collection", Width: nameW},
			{Title: "Items", Width: 10},
		}
	} else {
		cols = []table.Column{
			{Title: "Entity type", Width: nameW},
			{Title: "Count", Width: 10},
		}
	}

	// Selection markers — same glyphs and colors the export picker uses
	// (see picker.buildRows). Green ● for selected, dim ○ for not.
	selectedMark := lipgloss.NewStyle().Foreground(okColor).Bold(true).Render("●")
	emptyMark := lipgloss.NewStyle().Foreground(dimColor).Render("○")

	rows := make([]table.Row, 0, len(m.items))
	for _, it := range m.items {
		if it.kind != kind {
			continue
		}
		mark := emptyMark
		if it.selected {
			mark = selectedMark
		}
		nameStyled := it.name
		if !it.selected {
			nameStyled = lipgloss.NewStyle().Foreground(dimColor).Render(it.name)
		}
		var count string
		if it.kind == itemKindCollection {
			count = colorizeCount(it.itemCount)
		}
		rows = append(rows, table.Row{
			mark + " " + nameStyled,
			count,
		})
	}

	s := table.DefaultStyles()
	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(borderColor).
		BorderBottom(true).
		Bold(true)
	s.Selected = s.Selected.
		Foreground(lipgloss.Color("229")).
		Background(lipgloss.Color("57")).
		Bold(false)

	height := max(m.height-12, 5)
	if !m.tblReady {
		m.tbl = table.New(
			table.WithColumns(cols),
			table.WithRows(rows),
			table.WithFocused(true),
			table.WithHeight(height),
			table.WithStyles(s),
		)
		m.tblReady = true
		return
	}
	cursor := m.tbl.Cursor()
	m.tbl.SetColumns(cols)
	m.tbl.SetRows(rows)
	m.tbl.SetHeight(height)
	if cursor >= len(rows) {
		cursor = len(rows) - 1
	}
	if cursor < 0 {
		cursor = 0
	}
	m.tbl.SetCursor(cursor)
}

// activeItemIndex maps the current table cursor (which counts only rows
// of the active kind) back to an index into m.items (which holds both
// kinds). Returns -1 when there's no row under the cursor.
func (m importPickerModel) activeItemIndex() int {
	if !m.tblReady {
		return -1
	}
	target := m.tbl.Cursor()
	kind := m.activeKind()
	pos := 0
	for i, it := range m.items {
		if it.kind != kind {
			continue
		}
		if pos == target {
			return i
		}
		pos++
	}
	return -1
}

func (m importPickerModel) Init() tea.Cmd { return nil }

func (m importPickerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.rebuildTable()
		return m, nil
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
	case key.Matches(msg, importPickerKeys.toggle):
		if idx := m.activeItemIndex(); idx >= 0 {
			m.items[idx].selected = !m.items[idx].selected
			m.rebuildTable()
		}
		return m, nil
	case key.Matches(msg, importPickerKeys.toggleAll):
		// Flip the bulk state for the active tab only.
		kind := m.activeKind()
		anyOff := false
		for _, it := range m.items {
			if it.kind == kind && !it.selected {
				anyOff = true
				break
			}
		}
		for i := range m.items {
			if m.items[i].kind == kind {
				m.items[i].selected = anyOff
			}
		}
		m.rebuildTable()
		return m, nil
	case key.Matches(msg, importPickerKeys.tab):
		if len(m.tabs) > 1 {
			m.activeTab = (m.activeTab + 1) % len(m.tabs)
			m.rebuildTable()
		}
		return m, nil
	case key.Matches(msg, importPickerKeys.shiftTab):
		if len(m.tabs) > 1 {
			m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
			m.rebuildTable()
		}
		return m, nil
	case key.Matches(msg, importPickerKeys.confirm):
		// Advance to params; cursor reset so the user lands on the
		// first toggle.
		m.page = pagePickParams
		m.paramCursor = 0
		return m, nil
	}

	// Forward navigation (up/down/pgup/pgdn) to the table itself.
	var cmd tea.Cmd
	m.tbl, cmd = m.tbl.Update(msg)
	return m, cmd
}

func (m importPickerModel) handleParamsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, importPickerKeys.up):
		if m.paramCursor > 0 {
			m.paramCursor--
		}
	case key.Matches(msg, importPickerKeys.down):
		if m.paramCursor < len(m.params)-1 {
			m.paramCursor++
		}
	case key.Matches(msg, importPickerKeys.toggle):
		if len(m.params) > 0 {
			m.params[m.paramCursor].on = !m.params[m.paramCursor].on
		}
	case key.Matches(msg, importPickerKeys.back):
		// Return to items page.
		m.page = pagePickItems
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
	w := m.width
	h := m.height
	if w < 40 {
		w = 40
	}
	if h < 6 {
		h = 6
	}
	switch m.page {
	case pagePickItems:
		return m.viewItems(w, h)
	case pagePickParams:
		return m.viewParams(w, h)
	}
	return ""
}

// viewItems mirrors the export picker's table layout: title bar, header
// metadata block, tabs, table, key bar. Same widths, same colours — the
// goal is "indistinguishable from export at a glance".
func (m importPickerModel) viewItems(w, h int) string {
	title := titleBar.Render(m.titleText())
	meta := m.viewHeaderMeta(w)
	metaLines := strings.Count(meta, "\n") + 1

	var tabs string
	if len(m.tabs) > 1 {
		tabs = m.renderTabs(w)
	}

	keyBar := renderKeyBar(w-2,
		[2]string{"space", "select"},
		[2]string{"a", "all"},
		[2]string{"tab", "next tab"},
		[2]string{"enter", "next"},
		[2]string{"q", "cancel"},
	)

	// Chrome lines: title(1) + meta(N) + blank(1) + tabs(0/2) + keybar(1)
	chrome := 3 + metaLines
	if tabs != "" {
		chrome += 2
	}
	tblH := max(h-chrome, 3)
	m.tbl.SetHeight(tblH)
	tbl := lipgloss.NewStyle().Padding(0, 1).Render(m.tbl.View())

	parts := []string{title, meta, ""}
	if tabs != "" {
		parts = append(parts, tabs, "")
	}
	parts = append(parts, tbl, lipgloss.NewStyle().Padding(0, 1).Render(keyBar))

	return padToHeight(lipgloss.JoinVertical(lipgloss.Left, parts...), h)
}

// viewHeaderMeta is the k9s-style two-column block at the top of the
// items page, mirroring the export picker. Shows where the archive came
// from + selection summary.
func (m importPickerModel) viewHeaderMeta(w int) string {
	row := func(label, value string) string {
		return headerLabelStyle.Render(fmt.Sprintf("%-9s", label)) + " " +
			headerValueStyle.Render(value)
	}

	colCount, colSel := 0, 0
	sysCount, sysSel := 0, 0
	for _, it := range m.items {
		if it.kind == itemKindCollection {
			colCount++
			if it.selected {
				colSel++
			}
		} else {
			sysCount++
			if it.selected {
				sysSel++
			}
		}
	}

	src := m.sourceURL
	if src == "" {
		src = "—"
	}
	dirVer := m.directusVersion
	if dirVer == "" {
		dirVer = "—"
	}
	exported := m.exportedAt
	if exported == "" {
		exported = "—"
	}

	leftRows := []string{
		row("Archive", truncate(filepathBase(m.inputPath), max(w/2-12, 20))),
		row("Source", truncate(src, max(w/2-12, 20))),
		row("Exported", exported),
	}
	rightRows := []string{
		row("Diet", version),
		row("Directus", dirVer),
		row("Selected", fmt.Sprintf("%d/%d cols, %d/%d sys", colSel, colCount, sysSel, sysCount)),
	}

	left := lipgloss.JoinVertical(lipgloss.Left, leftRows...)
	right := lipgloss.JoinVertical(lipgloss.Left, rightRows...)
	leftCol := lipgloss.NewStyle().Width(w / 2).Padding(0, 1).Render(left)
	rightCol := lipgloss.NewStyle().Padding(0, 1).Render(right)
	return lipgloss.JoinHorizontal(lipgloss.Top, leftCol, rightCol)
}

// titleText is the breadcrumb-style title shown at the top of the items
// page, mirroring the export picker's "◆ Diet › Export › Collections (97)".
// The active tab segment with its row count is the rightmost element so
// the user always sees how many things are in the bucket they're looking
// at.
func (m importPickerModel) titleText() string {
	sep := lipgloss.NewStyle().Foreground(dimColor).Render(" › ")
	parts := []string{"◆ Diet", "Import"}
	if m.activeTab >= 0 && m.activeTab < len(m.tabs) {
		t := m.tabs[m.activeTab]
		count := 0
		for _, it := range m.items {
			if it.kind == t.kind {
				count++
			}
		}
		parts = append(parts, fmt.Sprintf("%s (%d)", t.label, count))
	}
	return strings.Join(parts, sep)
}

func (m importPickerModel) renderTabs(maxW int) string {
	var parts []string
	for i, t := range m.tabs {
		count := 0
		for _, it := range m.items {
			if it.kind == t.kind {
				count++
			}
		}
		label := fmt.Sprintf("%s (%d)", t.label, count)
		if i == m.activeTab {
			parts = append(parts, activeTabStyle.Render(label))
		} else {
			parts = append(parts, inactiveTabStyle.Render(label))
		}
	}
	line := strings.Join(parts, " ")
	return lipgloss.NewStyle().Padding(0, 1).MaxWidth(maxW).Render(line)
}

// viewParams renders the safety-toggles page wrapped in the wizard's
// rounded-border frame, centered on screen. Same visual language as
// stepProfile / stepNewProfile so the user doesn't experience a jarring
// style change between picker pages and wizard.
func (m importPickerModel) viewParams(w, h int) string {
	heading := lipgloss.NewStyle().Foreground(dimColor).
		Render("Import · Step 2 of 2 — safety options (defaults are conservative)")

	rows := make([]string, 0, len(m.params)*2)
	for i, p := range m.params {
		isCursor := i == m.paramCursor

		var cursor, box string
		if isCursor {
			cursor = lipgloss.NewStyle().Foreground(borderColor).Bold(true).Render("▸ ")
		} else {
			cursor = "  "
		}
		if p.on {
			box = lipgloss.NewStyle().Foreground(borderColor).Bold(true).Render("[x]")
		} else {
			box = lipgloss.NewStyle().Foreground(dimColor).Render("[ ]")
		}

		labelStyle := lipgloss.NewStyle().Foreground(labelCol)
		if isCursor {
			labelStyle = labelStyle.Foreground(valueCol).Bold(true)
		}
		marker := ""
		if p.on != p.defaultOn {
			marker = lipgloss.NewStyle().Foreground(warnColor).Bold(true).Render(" *")
		}
		descStyle := lipgloss.NewStyle().Foreground(dimColor).Italic(true)

		rows = append(rows,
			cursor+box+" "+labelStyle.Render(p.label)+marker,
			"      "+descStyle.Render(p.desc),
		)
	}

	body := lipgloss.JoinVertical(lipgloss.Left, rows...)
	hints := renderKeyBar(72,
		[2]string{"↑↓", "move"},
		[2]string{"space", "toggle"},
		[2]string{"enter", "import"},
		[2]string{"b", "back"},
		[2]string{"q", "cancel"},
	)
	legend := lipgloss.NewStyle().Foreground(dimColor).Italic(true).
		Render("* = changed from default")
	banner := lipgloss.NewStyle().
		Bold(true).
		Foreground(borderColor).
		Render("◆  D I E T")

	inner := lipgloss.JoinVertical(lipgloss.Center,
		banner,
		"",
		heading,
		"",
		lipgloss.NewStyle().Width(72).Render(body),
		"",
		legend,
		"",
		hints,
	)

	box := frameBorder.Padding(1, 4).Render(inner)
	return lipgloss.Place(w, h, lipgloss.Center, lipgloss.Center, box)
}

type importPickerKeyMap struct {
	up        key.Binding
	down      key.Binding
	toggle    key.Binding
	toggleAll key.Binding
	confirm   key.Binding
	cancel    key.Binding
	back      key.Binding
	tab       key.Binding
	shiftTab  key.Binding
}

var importPickerKeys = importPickerKeyMap{
	up:        key.NewBinding(key.WithKeys("up", "k")),
	down:      key.NewBinding(key.WithKeys("down", "j")),
	toggle:    key.NewBinding(key.WithKeys(" ", "x")),
	toggleAll: key.NewBinding(key.WithKeys("a")),
	confirm:   key.NewBinding(key.WithKeys("enter")),
	cancel:    key.NewBinding(key.WithKeys("q", "esc", "ctrl+c")),
	back:      key.NewBinding(key.WithKeys("b", "backspace", "left", "h")),
	tab:       key.NewBinding(key.WithKeys("tab")),
	shiftTab:  key.NewBinding(key.WithKeys("shift+tab")),
}
