package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Layout styles

var (
	borderColor = lipgloss.Color("39")

	frameBorder = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(borderColor)

	titleBar = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("39")).
			Padding(0, 1)

	statusBar = lipgloss.NewStyle().
			Foreground(lipgloss.Color("240")).
			Padding(0, 1)

	helpBar = lipgloss.NewStyle().
		Foreground(lipgloss.Color("240")).
		Padding(0, 1)

	okColor   = lipgloss.Color("42")
	warnColor = lipgloss.Color("214")
	dimColor  = lipgloss.Color("240")
	boldWhite = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255"))

	okStyle = lipgloss.NewStyle().Foreground(okColor)

	spinChars = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

	activeTabStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("255")).
			Background(lipgloss.Color("57")).
			Padding(0, 1)

	inactiveTabStyle = lipgloss.NewStyle().
				Foreground(dimColor).
				Padding(0, 1)
)

// Tab definition

type tabDef struct {
	label      string // display name
	systemType string // empty for collections tab
	count      int    // item count
}

// Collection data

type collectionRow struct {
	name       string
	isFolder   bool
	expanded   bool
	selected   bool
	depth      int
	group      string
	itemCount  int
	fieldCount int
	isSystem   bool            // true for system entity rows
	systemType string          // "flows", "dashboards", etc.
	systemData json.RawMessage // raw item JSON (system items only)
	tab        int             // tab index
}

// Picker mode

type pickerMode int

const (
	modeExport pickerMode = iota
	modeClean
)

// Messages

type (
	schemaLoadedMsg struct {
		rows []collectionRow
		tabs []tabDef
	}
	infoLoadedMsg struct {
		collection string
		fields     []FieldInfo
	}
	exportProgressMsg struct {
		phase   string
		detail  string
		current int
		total   int
	}
	exportDoneMsg struct {
		output, size              string
		cols, fields, rels, items int
		sysEntities               int
	}
	exportErrorMsg struct{ err error }
	cleanDoneMsg   struct {
		deletedCols   int
		deletedSystem int
		errors        int
	}
	tickMsg struct{}
)

// Picker model

type pickerModel struct {
	mode        pickerMode
	client      *apiClient
	url         string
	profileName string
	format      string
	output      string

	// Data
	allRows   []collectionRow
	tabs      []tabDef
	activeTab int

	// Table
	tbl      table.Model
	tblReady bool

	// Progress bar
	prog progress.Model

	// Info panel
	showInfo     bool
	infoCol      string
	infoViewport viewport.Model

	// Search
	searching  bool
	search     textinput.Model
	filterText string

	// Action channel
	exportCh chan tea.Msg

	// Help modal
	showHelp bool

	// Easter egg
	konami konamiTracker
	easter *easterModel

	// State
	loading        bool
	exporting      bool
	exportPhase    string
	exportDetail   string
	exportCur      int
	exportTotal    int
	done           bool
	doneInfo       exportDoneMsg
	cleanInfo      cleanDoneMsg
	confirming     bool
	cachedSysItems map[string][]json.RawMessage
	cachedColNames []string
	quitting       bool
	errMsg         string
	spinFrame      int

	width  int
	height int
}

func newPicker(client *apiClient, sourceURL, profileName, format, output string, mode pickerMode) pickerModel {
	p := progress.New(
		progress.WithDefaultGradient(),
		progress.WithoutPercentage(),
	)

	ti := textinput.New()
	ti.Placeholder = "search..."
	ti.Prompt = "/ "
	ti.PromptStyle = lipgloss.NewStyle().Foreground(borderColor)
	ti.CharLimit = 50

	return pickerModel{
		mode:        mode,
		profileName: profileName,
		client:      client,
		url:         sourceURL,
		format:      format,
		output:      output,
		loading:     true,
		prog:        p,
		search:      ti,
		width:       80,
		height:      24,
	}
}

func (m pickerModel) Init() tea.Cmd {
	return tea.Batch(m.loadSchema(), m.tick())
}

func (m pickerModel) tick() tea.Cmd {
	return tea.Tick(120*time.Millisecond, func(time.Time) tea.Msg { return tickMsg{} })
}

// Schema loader

func (m pickerModel) loadSchema() tea.Cmd {
	return func() tea.Msg {
		collections, err := fetchCollections(m.client)
		if err != nil {
			return exportErrorMsg{err}
		}

		type node struct {
			info  CollectionInfo
			group string
		}
		var nodes []node
		for _, c := range collections {
			var meta struct {
				Group string `json:"group"`
			}
			_ = json.Unmarshal(c.Meta, &meta)
			nodes = append(nodes, node{info: c, group: meta.Group})
		}

		// Fetch field/item counts.
		fieldCounts := map[string]int{}
		itemCounts := map[string]int{}
		for _, n := range nodes {
			if string(n.info.Schema) == "null" || len(n.info.Schema) == 0 {
				continue
			}
			fields, _ := fetchFields(m.client, n.info.Collection)
			fieldCounts[n.info.Collection] = len(fields)
			itemCounts[n.info.Collection] = countItems(m.client, n.info.Collection)
		}

		// Compute depths (with cycle protection).
		depthOf := map[string]int{}
		visiting := map[string]bool{}
		var depth func(string) int
		depth = func(name string) int {
			if d, ok := depthOf[name]; ok {
				return d
			}
			if visiting[name] {
				return 0 // break circular reference
			}
			visiting[name] = true
			for _, n := range nodes {
				if n.info.Collection == name && n.group != "" {
					d := depth(n.group) + 1
					depthOf[name] = d
					return d
				}
			}
			depthOf[name] = 0
			return 0
		}
		for _, n := range nodes {
			depth(n.info.Collection)
		}

		// Build tree order (DFS).
		children := map[string][]int{}
		var rootIdxs []int
		for i, n := range nodes {
			if n.group != "" {
				children[n.group] = append(children[n.group], i)
			} else {
				rootIdxs = append(rootIdxs, i)
			}
		}

		// Tab 0: Collections.
		var tabs []tabDef
		totalCollections := 0
		for _, n := range nodes {
			isFolder := string(n.info.Schema) == "null" || len(n.info.Schema) == 0
			if !isFolder {
				totalCollections++
			}
		}
		tabs = append(tabs, tabDef{label: "Collections", count: totalCollections})

		var rows []collectionRow
		var walk func([]int)
		walk = func(idxs []int) {
			for _, i := range idxs {
				n := nodes[i]
				isFolder := string(n.info.Schema) == "null" || len(n.info.Schema) == 0

				// Count direct non-folder children for folder display.
				folderChildCount := 0
				if isFolder {
					for _, ci := range children[n.info.Collection] {
						cn := nodes[ci]
						if string(cn.info.Schema) != "null" && len(cn.info.Schema) > 0 {
							folderChildCount++
						}
					}
				}

				ic := itemCounts[n.info.Collection]
				if isFolder {
					ic = folderChildCount
				}

				rows = append(rows, collectionRow{
					name:       n.info.Collection,
					isFolder:   isFolder,
					expanded:   false,
					selected:   !isFolder && m.mode == modeExport,
					depth:      depthOf[n.info.Collection],
					group:      n.group,
					itemCount:  ic,
					fieldCount: fieldCounts[n.info.Collection],
					tab:        0,
				})
				walk(children[n.info.Collection])
			}
		}
		walk(rootIdxs)

		// System entity tabs (1+). Skip empty and operations (bundled with flows).
		tabIdx := 1
		for _, et := range systemEntityTypes {
			items, _ := fetchSystemItems(m.client, et.Endpoint)
			if len(items) == 0 {
				continue
			}
			displayName := strings.ToUpper(et.Name[:1]) + et.Name[1:]
			tabs = append(tabs, tabDef{label: displayName, systemType: et.Name, count: len(items)})

			for _, item := range items {
				label := extractSystemItemLabel(item)
				rows = append(rows, collectionRow{
					name:       label,
					isFolder:   false,
					selected:   m.mode == modeExport,
					isSystem:   true,
					systemType: et.Name,
					systemData: item,
					tab:        tabIdx,
				})
			}
			tabIdx++
		}

		return schemaLoadedMsg{rows: rows, tabs: tabs}
	}
}

// Update

func (m pickerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.updateTableColumns()
		m.prog.Width = m.width - 6

	case tickMsg:
		m.spinFrame++
		return m, m.tick()

	case schemaLoadedMsg:
		m.loading = false
		m.allRows = msg.rows
		m.tabs = msg.tabs
		m.buildTable()
		m.tblReady = true

	case infoLoadedMsg:
		m.showInfo = true
		m.infoCol = msg.collection
		vpW := m.width - 6
		vpH := m.height - 6
		m.infoViewport = viewport.New(vpW, vpH)
		m.infoViewport.SetContent(renderFieldInfo(msg.fields, vpW-2))

	case easterTickMsg:
		if m.easter != nil {
			e := m.easter.update(msg)
			m.easter = &e
			if m.easter.done {
				m.easter = nil
				return m, nil
			}
			return m, m.easter.tickCmd()
		}

	case exportProgressMsg:
		m.exportPhase = msg.phase
		m.exportDetail = msg.detail
		m.exportCur = msg.current
		m.exportTotal = msg.total
		cmds = append(cmds, m.waitForExportMsg())

	case exportDoneMsg:
		m.done = true
		m.exporting = false
		m.doneInfo = msg

	case cleanDoneMsg:
		m.done = true
		m.exporting = false
		m.cleanInfo = msg

	case exportErrorMsg:
		m.loading = false
		m.exporting = false
		m.errMsg = msg.err.Error()
		m.quitting = true
		return m, tea.Quit

	case progress.FrameMsg:
		pm, cmd := m.prog.Update(msg)
		m.prog = pm.(progress.Model)
		cmds = append(cmds, cmd)

	case tea.KeyMsg:
		cmd := m.handleKey(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
		// Pass keys to table, but NOT left/right arrows (used for expand/collapse).
		key := msg.String()
		if m.tblReady && !m.showInfo && !m.showHelp && !m.exporting && !m.done && !m.confirming &&
			key != "right" && key != "left" && key != "l" && key != "h" {
			var tblCmd tea.Cmd
			m.tbl, tblCmd = m.tbl.Update(msg)
			cmds = append(cmds, tblCmd)
		}
		return m, tea.Batch(cmds...)
	}

	if m.tblReady && !m.showInfo && !m.showHelp && !m.exporting && !m.done && !m.confirming {
		var cmd tea.Cmd
		m.tbl, cmd = m.tbl.Update(msg)
		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func (m *pickerModel) handleKey(msg tea.KeyMsg) tea.Cmd {
	if m.done {
		return tea.Quit
	}
	if m.exporting {
		if msg.String() == "q" || msg.String() == "ctrl+c" {
			m.quitting = true
			return tea.Quit
		}
		return nil
	}

	// Easter egg animation — any key dismisses.
	if m.easter != nil {
		m.easter = nil
		return nil
	}

	if m.showHelp {
		m.showHelp = false
		return nil
	}

	if m.confirming {
		switch msg.String() {
		case "y":
			m.confirming = false
			m.exporting = true
			m.exportCh = make(chan tea.Msg, 64)
			go m.cleanWorkerCached(m.cachedColNames, m.cachedSysItems)
			return m.waitForExportMsg()
		case "n", "esc":
			m.confirming = false
		case "q", "ctrl+c":
			m.quitting = true
			return tea.Quit
		}
		return nil
	}

	if m.showInfo {
		switch msg.String() {
		case "esc", "i", "q":
			m.showInfo = false
		default:
			m.infoViewport, _ = m.infoViewport.Update(msg)
		}
		return nil
	}

	if m.searching {
		switch msg.String() {
		case "esc":
			m.searching = false
			m.search.Blur()
			m.filterText = ""
			m.rebuildTable()
		case "enter":
			m.searching = false
			m.search.Blur()
		default:
			m.search, _ = m.search.Update(msg)
			m.filterText = m.search.Value()
			m.rebuildTable()
		}
		return nil
	}

	// Feed konami tracker.
	if m.konami.feed(msg.String()) {
		e := newEasterAnimation(m.width-2, m.height-2)
		m.easter = &e
		return e.tickCmd()
	}

	switch msg.String() {
	case "q", "ctrl+c":
		m.quitting = true
		return tea.Quit

	case "?":
		m.showHelp = true
		return nil

	case "tab":
		if len(m.tabs) == 0 {
			return nil
		}
		m.activeTab = (m.activeTab + 1) % len(m.tabs)
		m.filterText = ""
		m.search.SetValue("")
		m.buildTable()
		m.tblReady = true

	case "shift+tab":
		if len(m.tabs) == 0 {
			return nil
		}
		m.activeTab = (m.activeTab - 1 + len(m.tabs)) % len(m.tabs)
		m.filterText = ""
		m.search.SetValue("")
		m.buildTable()
		m.tblReady = true

	case "esc":
		if m.filterText != "" {
			m.filterText = ""
			m.search.SetValue("")
			m.rebuildTable()
		}
		return nil

	case "/":
		m.searching = true
		m.search.SetValue("")
		m.search.Focus()
		return nil

	case " ":
		if idx := m.selectedIdx(); idx >= 0 {
			row := &m.allRows[idx]
			if row.isFolder {
				row.expanded = !row.expanded
				m.rebuildTable()
			} else {
				row.selected = !row.selected
				m.rebuildTable()
			}
		}

	case "right", "l":
		if idx := m.selectedIdx(); idx >= 0 {
			row := &m.allRows[idx]
			if row.isFolder && !row.expanded {
				row.expanded = true
				m.rebuildTable()
			}
		}

	case "left", "h":
		if idx := m.selectedIdx(); idx >= 0 {
			row := &m.allRows[idx]
			if row.isFolder && row.expanded {
				row.expanded = false
				m.rebuildTable()
			}
		}

	case "a": // toggle all in active tab
		sel, total := m.countSelected()
		val := sel < total
		for i := range m.allRows {
			if m.allRows[i].tab == m.activeTab && !m.allRows[i].isFolder {
				m.allRows[i].selected = val
			}
		}
		m.rebuildTable()

	case "i": // info
		if idx := m.selectedIdx(); idx >= 0 && !m.allRows[idx].isFolder {
			row := m.allRows[idx]
			if row.isSystem && row.systemData != nil {
				// Show system item JSON details.
				m.showInfo = true
				m.infoCol = row.name
				vpW := m.width - 6
				vpH := m.height - 6
				m.infoViewport = viewport.New(vpW, vpH)
				m.infoViewport.SetContent(renderSystemItemInfo(row.systemData, vpW-2))
			} else if !row.isSystem {
				col := row.name
				return func() tea.Msg {
					fields, _ := fetchFields(m.client, col)
					return infoLoadedMsg{collection: col, fields: fields}
				}
			}
		}

	case "e": // export
		if m.mode == modeExport {
			names := m.selectedCollectionNames()
			sysItems := m.selectedSystemItems()
			if len(names) > 0 || len(sysItems) > 0 {
				m.exporting = true
				m.exportCh = make(chan tea.Msg, 64)
				go m.exportWorker(names, sysItems)
				return m.waitForExportMsg()
			}
		}

	case "d": // delete (clean mode)
		if m.mode == modeClean {
			names := m.selectedCollectionNames()
			sysItems := m.selectedSystemItems()
			if len(names) > 0 || len(sysItems) > 0 {
				m.cachedColNames = names
				m.cachedSysItems = sysItems
				m.confirming = true
			}
		}
	}

	return nil
}

// Table management

func (m *pickerModel) updateTableColumns() {
	if !m.tblReady {
		return
	}
	w := m.width - 2
	nameW := max(w-28, 30)
	if m.activeTab == 0 {
		m.tbl.SetColumns([]table.Column{
			{Title: "Collection", Width: nameW},
			{Title: "Items", Width: 10},
			{Title: "Fields", Width: 10},
		})
	} else {
		m.tbl.SetColumns([]table.Column{
			{Title: "Name", Width: nameW},
			{Title: "", Width: 10},
			{Title: "Status", Width: 10},
		})
	}
}

func (m *pickerModel) buildTable() {
	w := m.width - 2
	nameW := max(w-28, 30)

	var cols []table.Column
	if m.activeTab == 0 {
		cols = []table.Column{
			{Title: "Collection", Width: nameW},
			{Title: "Items", Width: 10},
			{Title: "Fields", Width: 10},
		}
	} else {
		cols = []table.Column{
			{Title: "Name", Width: nameW},
			{Title: "", Width: 10},
			{Title: "Status", Width: 10},
		}
	}

	rows := m.buildRows()

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

	m.tbl = table.New(
		table.WithColumns(cols),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithHeight(max(m.height-10, 5)),
		table.WithStyles(s),
	)
}

func (m *pickerModel) rebuildTable() {
	cursor := m.tbl.Cursor()
	m.tbl.SetRows(m.buildRows())
	m.tbl.SetCursor(cursor)
}

func (m pickerModel) buildRows() []table.Row {
	visible := m.visibleRows()
	rows := make([]table.Row, len(visible))
	for i, r := range visible {
		indent := strings.Repeat("  ", r.depth)
		switch {
		case r.isFolder:
			arrow := "▶"
			if r.expanded {
				arrow = "▼"
			}
			countStr := ""
			if r.itemCount > 0 {
				countStr = fmt.Sprintf("%d", r.itemCount)
			}
			rows[i] = table.Row{
				fmt.Sprintf("%s%s %s", indent, arrow, r.name),
				countStr,
				"",
			}
		case r.isSystem:
			check := "○"
			if r.selected {
				check = "●"
			}
			status := extractSystemItemStatus(r.systemData)
			rows[i] = table.Row{
				fmt.Sprintf("%s%s %s", indent, check, r.name),
				"",
				status,
			}
		default:
			check := "○"
			if r.selected {
				check = "●"
			}
			rows[i] = table.Row{
				fmt.Sprintf("%s%s %s", indent, check, r.name),
				fmt.Sprintf("%d", r.itemCount),
				fmt.Sprintf("%d", r.fieldCount),
			}
		}
	}
	return rows
}

func (m pickerModel) visibleRows() []collectionRow {
	// Filter by active tab.
	var tabRows []collectionRow
	for _, row := range m.allRows {
		if row.tab == m.activeTab {
			tabRows = append(tabRows, row)
		}
	}

	// Respect collapsed folders.
	collapsed := map[string]bool{}
	var expanded []collectionRow
	for _, row := range tabRows {
		if row.group != "" && collapsed[row.group] {
			collapsed[row.name] = true
			continue
		}
		expanded = append(expanded, row)
		if row.isFolder && !row.expanded {
			collapsed[row.name] = true
		}
	}

	// Apply search filter.
	if m.filterText == "" {
		return expanded
	}
	lower := strings.ToLower(m.filterText)
	var filtered []collectionRow
	for _, row := range expanded {
		if strings.Contains(strings.ToLower(row.name), lower) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func (m pickerModel) selectedIdx() int {
	visible := m.visibleRows()
	cursor := m.tbl.Cursor()
	if cursor < 0 || cursor >= len(visible) {
		return -1
	}
	target := visible[cursor]
	for i, r := range m.allRows {
		if r.tab == m.activeTab && r.name == target.name && r.group == target.group && r.isFolder == target.isFolder {
			return i
		}
	}
	return -1
}

func (m pickerModel) countSelected() (int, int) {
	sel, total := 0, 0
	for _, r := range m.allRows {
		if r.tab == m.activeTab && !r.isFolder {
			total++
			if r.selected {
				sel++
			}
		}
	}
	return sel, total
}

func (m pickerModel) selectedCollectionNames() []string {
	var names []string
	for _, r := range m.allRows {
		if r.tab == 0 && !r.isFolder && r.selected {
			names = append(names, r.name)
		}
	}
	return names
}

func (m pickerModel) selectedSystemItems() map[string][]json.RawMessage {
	result := make(map[string][]json.RawMessage)
	for _, r := range m.allRows {
		if r.tab > 0 && !r.isFolder && r.selected && r.systemData != nil {
			item := stripSensitiveFields(r.systemType, r.systemData)
			result[r.systemType] = append(result[r.systemType], item)
		}
	}

	// Auto-include operations for selected flows.
	if flows, ok := result["flows"]; ok && len(flows) > 0 {
		ids := make(map[string]bool)
		for _, f := range flows {
			ids[extractID(f)] = true
		}
		ops, _ := fetchSystemItems(m.client, "/operations")
		for _, op := range ops {
			var obj struct {
				Flow string `json:"flow"`
			}
			if json.Unmarshal(op, &obj) == nil && ids[obj.Flow] {
				result["operations"] = append(result["operations"], op)
			}
		}
	}

	// Auto-include panels for selected dashboards.
	if dashes, ok := result["dashboards"]; ok && len(dashes) > 0 {
		ids := make(map[string]bool)
		for _, d := range dashes {
			ids[extractID(d)] = true
		}
		panels, _ := fetchSystemItems(m.client, "/panels")
		for _, p := range panels {
			var obj struct {
				Dashboard string `json:"dashboard"`
			}
			if json.Unmarshal(p, &obj) == nil && ids[obj.Dashboard] {
				result["panels"] = append(result["panels"], p)
			}
		}
	}

	// Auto-include presets for selected collections.
	colNames := m.selectedCollectionNames()
	if len(colNames) > 0 {
		colSet := make(map[string]bool)
		for _, n := range colNames {
			colSet[n] = true
		}
		presets, _ := fetchSystemItems(m.client, "/presets")
		for _, p := range presets {
			var obj struct {
				Collection string `json:"collection"`
			}
			if json.Unmarshal(p, &obj) == nil && colSet[obj.Collection] {
				result["presets"] = append(result["presets"], p)
			}
		}
	}

	return result
}

// View

func (m pickerModel) View() string {
	w := m.width - 2
	h := m.height - 2
	if w < 40 {
		w = 40
	}
	if h < 6 {
		h = 6
	}

	var body string
	switch {
	case m.done && m.mode == modeClean:
		body = m.viewCleanDone(h)
	case m.done:
		body = m.viewDone(h)
	case m.confirming:
		body = m.viewConfirm(h)
	case m.exporting:
		body = m.viewExporting(w, h)
	case m.easter != nil:
		body = m.easter.view(w, h)
	case m.showHelp:
		body = m.viewHelp(h)
	case m.showInfo:
		body = m.viewInfo(w, h)
	case m.loading:
		body = m.viewLoading(h)
	default:
		body = m.viewTable(w, h)
	}

	frame := frameBorder.Width(w)
	return frame.Render(body)
}

func (m pickerModel) titleText() string {
	op := "Export"
	if m.mode == modeClean {
		op = "Clean"
	}
	return fmt.Sprintf("Diet %s (Directus Import Export Tool)", op)
}

func (m pickerModel) headerInfo() string {
	dim := lipgloss.NewStyle().Foreground(dimColor)
	parts := []string{}
	if m.profileName != "" {
		parts = append(parts, lipgloss.NewStyle().Foreground(okColor).Bold(true).Render(m.profileName))
	}
	parts = append(parts, m.url, dim.Render(configPathDisplay()))
	return statusBar.Render(strings.Join(parts, "  ·  "))
}

func (m pickerModel) renderTabs(maxW int) string {
	var parts []string
	for i, t := range m.tabs {
		label := fmt.Sprintf("%s (%d)", t.label, t.count)
		if i == m.activeTab {
			parts = append(parts, activeTabStyle.Render(label))
		} else {
			parts = append(parts, inactiveTabStyle.Render(label))
		}
	}
	line := strings.Join(parts, " ")
	return lipgloss.NewStyle().Padding(0, 1).MaxWidth(maxW).Render(line)
}

func (m pickerModel) viewHelp(h int) string {
	title := titleBar.Render("◆ Keyboard Shortcuts")

	keyStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255")).Width(16)
	descStyle := lipgloss.NewStyle().Foreground(dimColor)

	row := func(key, desc string) string {
		return fmt.Sprintf("  %s %s", keyStyle.Render(key), descStyle.Render(desc))
	}

	sections := []string{
		"",
		lipgloss.NewStyle().Bold(true).Foreground(borderColor).Padding(0, 1).Render("Navigation"),
		row("↑ / ↓", "Move cursor up/down"),
		row("→ / l", "Expand folder"),
		row("← / h", "Collapse folder"),
		row("tab", "Next tab"),
		row("shift+tab", "Previous tab"),
		"",
		lipgloss.NewStyle().Bold(true).Foreground(borderColor).Padding(0, 1).Render("Selection"),
		row("space", "Toggle selection"),
		row("a", "Select / deselect all"),
		"",
		lipgloss.NewStyle().Bold(true).Foreground(borderColor).Padding(0, 1).Render("Actions"),
	}

	if m.mode == modeExport {
		sections = append(sections, row("e", "Export selected"))
	} else {
		sections = append(sections, row("d", "Delete selected"))
	}

	sections = append(sections,
		row("i", "Item details"),
		row("/", "Search / filter"),
		row("esc", "Clear filter / back"),
		row("?", "This help"),
		row("q", "Quit"),
	)

	help := helpBar.Render("Press any key to close")

	parts := []string{title}
	parts = append(parts, sections...)
	parts = append(parts, "", help)

	return padToHeight(lipgloss.JoinVertical(lipgloss.Left, parts...), h)
}

func (m pickerModel) viewLoading(h int) string {
	spin := spinChars[m.spinFrame%len(spinChars)]
	title := titleBar.Render("◆ " + m.titleText())
	source := m.headerInfo()
	loading := fmt.Sprintf(" %s Loading schema...",
		lipgloss.NewStyle().Foreground(warnColor).Render(spin))

	content := lipgloss.JoinVertical(lipgloss.Left, title, source, "", loading)
	return padToHeight(content, h)
}

func (m pickerModel) viewTable(w, h int) string {
	title := titleBar.Render("◆ " + m.titleText())
	source := m.headerInfo()

	tabs := m.renderTabs(w)

	var searchLine string
	hasSearch := false
	if m.searching {
		searchLine = lipgloss.NewStyle().Padding(0, 1).Render(m.search.View())
		hasSearch = true
	} else if m.filterText != "" {
		searchLine = statusBar.Render(fmt.Sprintf("filter: %s  (/ to edit, esc to clear)",
			lipgloss.NewStyle().Foreground(borderColor).Render(m.filterText)))
		hasSearch = true
	}

	var helpText string
	switch m.mode {
	case modeClean:
		helpText = "space:select  →/←:expand  a:all  /:search  tab:next  d:delete  ?:help  q:quit"
	default:
		helpText = "space:select  →/←:expand  a:all  i:info  /:search  tab:next  e:export  ?:help  q:quit"
	}
	sel, total := m.countSelected()
	selText := fmt.Sprintf("%d/%d selected", sel, total)

	barStyle := lipgloss.NewStyle().
		Background(lipgloss.Color("57")).
		Foreground(lipgloss.Color("255")).
		Padding(0, 1)
	selStyle := lipgloss.NewStyle().
		Background(lipgloss.Color("57")).
		Foreground(okColor).
		Bold(true)

	left := barStyle.Render(helpText)
	right := barStyle.Render(selStyle.Render(selText))
	gap := max(w-lipgloss.Width(left)-lipgloss.Width(right), 0)
	fill := lipgloss.NewStyle().Background(lipgloss.Color("57")).Render(strings.Repeat(" ", gap))
	help := left + fill + right

	// chrome: title(1) + source(1) + blank(1) + tabs(1) + blank(1) + help(1) + table_header(1) = 7
	chrome := 7
	if hasSearch {
		chrome++
	}
	tblH := max(h-chrome, 3)
	m.tbl.SetHeight(tblH)
	tbl := lipgloss.NewStyle().Padding(0, 1).Render(m.tbl.View())

	parts := []string{title, source, "", tabs, ""}
	if hasSearch {
		parts = append(parts, searchLine)
	}
	parts = append(parts, tbl, help)

	return padToHeight(lipgloss.JoinVertical(lipgloss.Left, parts...), h)
}

func (m pickerModel) viewInfo(w, h int) string {
	title := titleBar.Render(fmt.Sprintf("◆ %s — Schema", m.infoCol))
	help := helpBar.Render("esc:back  up/down:scroll")

	vpH := max(h-4, 3)
	m.infoViewport.Width = w - 2
	m.infoViewport.Height = vpH

	vp := lipgloss.NewStyle().Padding(0, 1).Render(m.infoViewport.View())

	content := lipgloss.JoinVertical(lipgloss.Left, title, "", vp, "", help)
	return padToHeight(content, h)
}

func (m pickerModel) viewConfirm(h int) string {
	title := titleBar.Render("◆ " + m.titleText())

	colNames := m.cachedColNames
	sysItems := m.cachedSysItems

	warn := lipgloss.NewStyle().Foreground(warnColor).Bold(true)
	dim := lipgloss.NewStyle().Foreground(dimColor)

	var lines []string
	lines = append(lines, warn.Render("  Delete the following?"), "")

	if len(colNames) > 0 {
		lines = append(lines, fmt.Sprintf("    Collections: %d (with all data)", len(colNames)))
		for _, n := range colNames {
			lines = append(lines, dim.Render(fmt.Sprintf("      - %s", n)))
		}
	}
	if len(sysItems) > 0 {
		totalSys := 0
		for _, items := range sysItems {
			totalSys += len(items)
		}
		lines = append(lines, fmt.Sprintf("    System: %d items", totalSys))
		for name, items := range sysItems {
			lines = append(lines, dim.Render(fmt.Sprintf("      - %s: %d", name, len(items))))
		}
	}

	body := strings.Join(lines, "\n")
	help := helpBar.Render("y: confirm  n: cancel")

	content := lipgloss.JoinVertical(lipgloss.Left, title, "", body, "", help)
	return padToHeight(content, h)
}

func (m pickerModel) viewExporting(w, h int) string {
	spin := spinChars[m.spinFrame%len(spinChars)]
	title := titleBar.Render("◆ " + m.titleText())

	phase := m.exportPhase
	if phase == "" {
		phase = "Preparing"
	}
	status := fmt.Sprintf(" %s %s",
		lipgloss.NewStyle().Foreground(warnColor).Render(spin),
		boldWhite.Render(phase))

	detail := ""
	if m.exportDetail != "" {
		detail = lipgloss.NewStyle().Foreground(dimColor).Padding(0, 1).
			Render(m.exportDetail)
	}

	pct := 0.0
	if m.exportTotal > 0 {
		pct = float64(m.exportCur) / float64(m.exportTotal)
		if pct > 1 {
			pct = 1
		}
	}
	barW := max(w-4, 20)
	m.prog.Width = barW
	bar := lipgloss.NewStyle().Padding(0, 1).Render(m.prog.ViewAs(pct))

	counter := ""
	if m.exportTotal > 0 {
		counter = lipgloss.NewStyle().Foreground(dimColor).Padding(0, 1).
			Render(fmt.Sprintf("%d/%d  (%d%%)", m.exportCur, m.exportTotal, int(pct*100)))
	}

	help := helpBar.Render("q: cancel")

	parts := []string{title, "", status}
	if detail != "" {
		parts = append(parts, detail)
	}
	parts = append(parts, "", bar)
	if counter != "" {
		parts = append(parts, counter)
	}
	parts = append(parts, "", help)

	return padToHeight(lipgloss.JoinVertical(lipgloss.Left, parts...), h)
}

func (m pickerModel) viewDone(h int) string {
	d := m.doneInfo
	title := titleBar.Render("◆ " + m.titleText())
	ok := lipgloss.NewStyle().Foreground(okColor)
	dim := lipgloss.NewStyle().Foreground(dimColor)

	var sysLine string
	if d.sysEntities > 0 {
		sysLine = fmt.Sprintf("\n  %s  %d types", boldWhite.Render("System:"), d.sysEntities)
	}

	result := lipgloss.NewStyle().Padding(1, 1).Render(
		fmt.Sprintf("%s %s\n\n  %s  %s\n  %s     %s\n  %s  %d collections · %d fields · %d relations\n  %s    %d items%s",
			ok.Render("✓"), ok.Render("Export complete"),
			boldWhite.Render("Archive:"), d.output,
			boldWhite.Render("Size:"), d.size,
			boldWhite.Render("Schema:"), d.cols, d.fields, d.rels,
			boldWhite.Render("Data:"), d.items, sysLine))

	help := helpBar.Render(dim.Render("Press any key to exit"))

	content := lipgloss.JoinVertical(lipgloss.Left, title, result, "", help)
	return padToHeight(content, h)
}

func (m pickerModel) viewCleanDone(h int) string {
	title := titleBar.Render("◆ " + m.titleText())
	ok := lipgloss.NewStyle().Foreground(okColor)
	dim := lipgloss.NewStyle().Foreground(dimColor)

	d := m.cleanInfo
	var lines []string
	lines = append(lines, fmt.Sprintf("%s %s", ok.Render("✓"), ok.Render("Clean complete")), "")
	if d.deletedCols > 0 {
		lines = append(lines, fmt.Sprintf("  %s  %d deleted", boldWhite.Render("Collections:"), d.deletedCols))
	}
	if d.deletedSystem > 0 {
		lines = append(lines, fmt.Sprintf("  %s        %d deleted", boldWhite.Render("System:"), d.deletedSystem))
	}
	if d.errors > 0 {
		lines = append(lines, fmt.Sprintf("  %s        %d",
			lipgloss.NewStyle().Foreground(warnColor).Bold(true).Render("Errors:"), d.errors))
	}

	result := lipgloss.NewStyle().Padding(1, 1).Render(strings.Join(lines, "\n"))
	help := helpBar.Render(dim.Render("Press any key to exit"))

	content := lipgloss.JoinVertical(lipgloss.Left, title, result, "", help)
	return padToHeight(content, h)
}

// Export runner

func (m pickerModel) waitForExportMsg() tea.Cmd {
	ch := m.exportCh
	return func() tea.Msg {
		msg, ok := <-ch
		if !ok {
			return nil
		}
		return msg
	}
}

func (m pickerModel) exportWorker(names []string, systemItems map[string][]json.RawMessage) {
	ch := m.exportCh
	defer close(ch)

	send := func(phase, detail string, cur, total int) {
		ch <- exportProgressMsg{phase: phase, detail: detail, current: cur, total: total}
	}

	client := m.client
	selectedSet := make(map[string]bool)
	for _, n := range names {
		selectedSet[n] = true
	}

	// Phase 1: Schema
	send("Fetching schema", "collections", 0, 3)

	collections, err := fetchCollections(client)
	if err != nil {
		ch <- exportErrorMsg{err}
		return
	}
	var exportCollections []CollectionInfo
	for _, c := range collections {
		if selectedSet[c.Collection] || string(c.Schema) == "null" || len(c.Schema) == 0 {
			exportCollections = append(exportCollections, c)
		}
	}
	send("Fetching schema", "fields", 1, 3)

	allFields, err := fetchAllFields(client, names)
	if err != nil {
		ch <- exportErrorMsg{err}
		return
	}
	send("Fetching schema", "relations", 2, 3)

	allRelations, err := fetchRelations(client)
	if err != nil {
		ch <- exportErrorMsg{err}
		return
	}
	var exportRelations []RelationInfo
	for _, r := range allRelations {
		if selectedSet[r.Collection] || selectedSet[r.RelatedCollection] {
			exportRelations = append(exportRelations, r)
		}
	}
	send("Fetching schema", "done", 3, 3)

	// Phase 2: Data
	send("Pulling data", "", 0, len(names))

	dataMap := pullAllDataWithProgress(client, names, func(col string, done, total int) {
		send("Pulling data", col, done, total)
	})

	// Phase 3: System data (already have items from picker).
	sysNames := make([]string, 0, len(systemItems))
	for name := range systemItems {
		sysNames = append(sysNames, name)
	}

	// Phase 4: Archive
	send("Packing archive", m.format, 0, 1)

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

	itemCounts := make(map[string]int)
	totalItems := 0
	for col, items := range dataMap {
		itemCounts[col] = len(items)
		totalItems += len(items)
	}

	manifest := Manifest{
		DietVersion:     version,
		DirectusVersion: directusVersion,
		SourceURL:       m.url,
		ExportedAt:      time.Now().UTC().Format(time.RFC3339),
		Format:          m.format,
		Collections:     names,
		ItemCounts:      itemCounts,
		SystemEntities:  sysNames,
	}
	schema := SchemaBundle{
		Collections: exportCollections,
		Fields:      allFields,
		Relations:   exportRelations,
	}

	output := m.output
	if output == "" {
		ext := ".tar.zst"
		if m.format == "zip" {
			ext = ".zip"
		}
		output = fmt.Sprintf("diet-export-%s%s", time.Now().Format("20060102-150405"), ext)
	}

	if err := createArchive(m.format, output, manifest, schema, dataMap, systemItems); err != nil {
		ch <- exportErrorMsg{err}
		return
	}

	ch <- exportDoneMsg{
		output:      output,
		size:        archiveSize(output),
		cols:        len(names),
		fields:      len(allFields),
		rels:        len(exportRelations),
		items:       totalItems,
		sysEntities: len(sysNames),
	}
}

// Clean runner

func (m pickerModel) cleanWorkerCached(colNames []string, sysItems map[string][]json.RawMessage) {
	ch := m.exportCh
	defer close(ch)

	send := func(phase, detail string, cur, total int) {
		ch <- exportProgressMsg{phase: phase, detail: detail, current: cur, total: total}
	}

	client := m.client

	totalSteps := len(colNames)
	for _, items := range sysItems {
		totalSteps += len(items)
	}
	step := 0
	deletedCols := 0
	deletedSystem := 0
	errors := 0

	// Phase 1: Delete system entity items (dependents first).
	if len(sysItems) > 0 {
		for _, name := range systemDeleteOrder {
			items, ok := sysItems[name]
			if !ok || len(items) == 0 {
				continue
			}
			entity, ok := systemEntityByName(name)
			if !ok {
				continue
			}
			for _, item := range items {
				id := extractID(item)
				label := extractSystemItemLabel(item)
				send("Deleting system", fmt.Sprintf("%s: %s", name, label), step, totalSteps)
				if id != "" {
					status, err := client.del(entity.Endpoint + "/" + url.PathEscape(id))
					if err != nil || status >= 400 {
						errors++
					} else {
						deletedSystem++
					}
				} else {
					errors++
				}
				step++
			}
		}
	}

	// Phase 2: Delete collections (reverse FK order).
	if len(colNames) > 0 {
		send("Fetching relations", "", step, totalSteps)
		relations, _ := fetchRelations(client)

		insertOrder := buildInsertOrder(colNames, relations)
		deleteOrder := make([]string, len(insertOrder))
		for i, c := range insertOrder {
			deleteOrder[len(insertOrder)-1-i] = c
		}

		for _, col := range deleteOrder {
			send("Deleting collections", col, step, totalSteps)
			if err := deleteCollection(client, col); err != nil {
				errors++
			} else {
				deletedCols++
			}
			step++
		}

		// Delete orphan folders.
		collections, _ := fetchCollections(client)
		nameSet := make(map[string]bool)
		for _, n := range colNames {
			nameSet[n] = true
		}
		for _, c := range collections {
			isFolder := string(c.Schema) == "null" || len(c.Schema) == 0
			if !isFolder {
				continue
			}
			hasChildren := false
			for _, c2 := range collections {
				var meta struct {
					Group string `json:"group"`
				}
				json.Unmarshal(c2.Meta, &meta)
				if meta.Group == c.Collection && c2.Collection != c.Collection {
					hasChildren = true
					break
				}
			}
			if !hasChildren {
				_ = deleteCollection(client, c.Collection)
			}
		}
	}

	ch <- cleanDoneMsg{
		deletedCols:   deletedCols,
		deletedSystem: deletedSystem,
		errors:        errors,
	}
}

// Field info renderer

func renderFieldInfo(fields []FieldInfo, w int) string {
	nameW := 30
	typeW := 12
	noteMax := max(w-nameW-typeW-4, 10)

	if w < 1 {
		w = 1
	}
	nameCol := lipgloss.NewStyle().Width(nameW).Bold(true).Foreground(lipgloss.Color("255"))
	typeCol := lipgloss.NewStyle().Width(typeW).Foreground(lipgloss.Color("39"))
	noteStyle := lipgloss.NewStyle().Foreground(dimColor)
	sep := lipgloss.NewStyle().Foreground(lipgloss.Color("236")).Render(strings.Repeat("─", w))

	var b strings.Builder
	fmt.Fprintf(&b, " %s %s %s\n", nameCol.Render("Field"), typeCol.Render("Type"), noteStyle.Render("Note"))
	b.WriteString(" " + sep + "\n")

	for _, f := range fields {
		var meta struct {
			Note   string `json:"note"`
			Hidden bool   `json:"hidden"`
		}
		_ = safeUnmarshal(f.Meta, &meta)
		if meta.Hidden {
			continue
		}
		note := ""
		if meta.Note != "" {
			note = noteStyle.Render(truncate(meta.Note, noteMax))
		}
		fmt.Fprintf(&b, " %s %s %s\n", nameCol.Render(f.Field), typeCol.Render(f.Type), note)
	}
	return b.String()
}

func renderSystemItemInfo(data json.RawMessage, w int) string {
	var obj map[string]any
	if json.Unmarshal(data, &obj) != nil {
		return " (invalid JSON)"
	}

	if w < 1 {
		w = 1
	}
	keyStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255")).Width(22)
	valStyle := lipgloss.NewStyle().Foreground(dimColor)
	sep := lipgloss.NewStyle().Foreground(lipgloss.Color("236")).Render(strings.Repeat("─", w))

	var b strings.Builder
	b.WriteString(" " + sep + "\n")

	priority := []string{"id", "name", "first_name", "last_name", "email", "status", "role",
		"description", "key", "type", "trigger", "icon", "color", "admin_access", "app_access"}
	seen := map[string]bool{}

	maxVal := max(w-24, 20)

	render := func(k string, v any) {
		var valStr string
		switch val := v.(type) {
		case nil:
			valStr = "null"
		case []any:
			if len(val) == 0 {
				valStr = "[]"
			} else {
				valStr = fmt.Sprintf("[%d items]", len(val))
			}
		case map[string]any:
			valStr = fmt.Sprintf("{%d fields}", len(val))
		case bool:
			valStr = fmt.Sprintf("%v", val)
		default:
			valStr = fmt.Sprintf("%v", val)
		}
		valStr = truncate(valStr, maxVal)
		fmt.Fprintf(&b, " %s %s\n", keyStyle.Render(k), valStyle.Render(valStr))
	}

	for _, k := range priority {
		if v, ok := obj[k]; ok {
			render(k, v)
			seen[k] = true
		}
	}

	// Remaining fields alphabetically, skip noisy ones.
	skip := map[string]bool{"password": true, "token": true, "last_access": true, "last_page": true}
	var keys []string
	for k := range obj {
		if !seen[k] && !skip[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		render(k, obj[k])
	}

	return b.String()
}

func padToHeight(content string, h int) string {
	lines := strings.Split(content, "\n")
	if len(lines) > h {
		lines = lines[:h]
	}
	for len(lines) < h {
		lines = append(lines, "")
	}
	return strings.Join(lines, "\n")
}

func safeUnmarshal(data []byte, v any) error {
	if len(data) == 0 || string(data) == "null" {
		return nil
	}
	return json.Unmarshal(data, v)
}
