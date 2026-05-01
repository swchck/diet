package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

func newDiffCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff",
		Short: "Compare two Directus instances side by side",
		RunE:  runDiff,
	}
	cmd.Flags().String("target-url", "", "Target Directus URL")
	cmd.Flags().String("target-token", "", "Target Directus static token")
	return cmd
}

// Diff result types

type diffStatus int

const (
	diffMatch   diffStatus = iota // identical on both sides
	diffChanged                   // exists on both, different
	diffAdded                     // only on source (left)
	diffRemoved                   // only on target (right)
)

type collectionDiff struct {
	name        string
	status      diffStatus
	sourceItems int
	targetItems int
	fields      []fieldDiff
}

type fieldDiff struct {
	name       string
	status     diffStatus
	sourceType string
	targetType string
}

type systemDiff struct {
	name        string
	sourceCount int
	targetCount int
}

type diffResult struct {
	sourceURL    string
	targetURL    string
	sourceClient *apiClient
	targetClient *apiClient
	collections  []collectionDiff
	relations    relationsDiff
	system       []systemDiff
}

type relationsDiff struct {
	sourceOnly []string
	targetOnly []string
	common     int
}

// Data fetching

func runDiff(cmd *cobra.Command, args []string) error {
	sourceURL, _ := cmd.Flags().GetString("url")
	sourceToken, _ := cmd.Flags().GetString("token")
	targetURL, _ := cmd.Flags().GetString("target-url")
	targetToken, _ := cmd.Flags().GetString("target-token")
	plain, _ := cmd.Flags().GetBool("plain")

	// If all flags provided, run directly.
	if sourceURL != "" && sourceToken != "" && targetURL != "" && targetToken != "" {
		source := newClient(sourceURL, sourceToken)
		target := newClient(targetURL, targetToken)
		if plain {
			return runSimpleDiff(source, target, sourceURL, targetURL)
		}
		result, err := computeDiff(source, target, sourceURL, targetURL, func(string) {})
		if err != nil {
			return err
		}
		return showDiffResult(result)
	}

	// TUI mode: pick profiles from config.
	cfg := loadConfig()
	m := newDiffPickerModel(cfg)
	p := tea.NewProgram(m, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}

	dm := finalModel.(diffPickerModel)
	if dm.cancelled {
		fmt.Println("Cancelled.")
		return nil
	}
	if dm.errMsg != "" {
		return fmt.Errorf("%s", dm.errMsg)
	}
	if dm.result != nil {
		return showDiffResult(dm.result)
	}
	return nil
}

func showDiffResult(result *diffResult) error {
	m := newDiffModel(result)
	p := tea.NewProgram(m, tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("TUI error: %w", err)
	}
	return nil
}

func runSimpleDiff(source, target *apiClient, sourceURL, targetURL string) error {
	fmt.Println("Comparing instances...")
	fmt.Printf("  Source: %s\n", sourceURL)
	fmt.Printf("  Target: %s\n", targetURL)

	result, err := computeDiff(source, target, sourceURL, targetURL, func(msg string) {
		fmt.Println("  " + msg)
	})
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Println(renderDiffPlainText(result))
	return nil
}

type instanceData struct {
	collections []CollectionInfo
	fields      map[string][]FieldInfo
	relations   []RelationInfo
	itemCounts  map[string]int
	systemCount map[string]int
}

func fetchInstanceData(client *apiClient, log func(string)) (*instanceData, error) {
	data := &instanceData{
		fields:      make(map[string][]FieldInfo),
		itemCounts:  make(map[string]int),
		systemCount: make(map[string]int),
	}

	log("Fetching collections...")
	collections, err := fetchCollections(client)
	if err != nil {
		return nil, fmt.Errorf("fetch collections: %w", err)
	}
	data.collections = collections

	log("Fetching relations...")
	relations, err := fetchRelations(client)
	if err != nil {
		return nil, fmt.Errorf("fetch relations: %w", err)
	}
	data.relations = relations

	// Fetch fields and item counts in parallel.
	var tableNames []string
	for _, c := range collections {
		isFolder := string(c.Schema) == "null" || len(c.Schema) == 0
		if !isFolder {
			tableNames = append(tableNames, c.Collection)
		}
	}

	log(fmt.Sprintf("Fetching fields & counts for %d collections...", len(tableNames)))

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 6)

	for _, col := range tableNames {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			fields, _ := fetchFields(client, col)
			count := countItems(client, col)

			mu.Lock()
			data.fields[col] = fields
			data.itemCounts[col] = count
			mu.Unlock()
		})
	}
	wg.Wait()

	log("Fetching system entities...")
	for _, et := range systemEntityTypes {
		data.systemCount[et.Name] = countSystemItems(client, et.Endpoint)
	}

	return data, nil
}

func computeDiff(source, target *apiClient, sourceURL, targetURL string, log func(string)) (*diffResult, error) {
	var srcData, tgtData *instanceData
	var srcErr, tgtErr error
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		srcData, srcErr = fetchInstanceData(source, func(msg string) { log("[source] " + msg) })
	}()
	go func() {
		defer wg.Done()
		tgtData, tgtErr = fetchInstanceData(target, func(msg string) { log("[target] " + msg) })
	}()
	wg.Wait()

	if srcErr != nil {
		return nil, fmt.Errorf("source: %w", srcErr)
	}
	if tgtErr != nil {
		return nil, fmt.Errorf("target: %w", tgtErr)
	}

	result := &diffResult{
		sourceURL:    sourceURL,
		targetURL:    targetURL,
		sourceClient: source,
		targetClient: target,
	}

	// Build collection sets (tables only, not folders).
	srcCols := make(map[string]bool)
	for _, c := range srcData.collections {
		if string(c.Schema) != "null" && len(c.Schema) > 0 {
			srcCols[c.Collection] = true
		}
	}
	tgtCols := make(map[string]bool)
	for _, c := range tgtData.collections {
		if string(c.Schema) != "null" && len(c.Schema) > 0 {
			tgtCols[c.Collection] = true
		}
	}

	// Merge all collection names sorted.
	allCols := make(map[string]bool)
	for c := range srcCols {
		allCols[c] = true
	}
	for c := range tgtCols {
		allCols[c] = true
	}
	var colNames []string
	for c := range allCols {
		colNames = append(colNames, c)
	}
	sort.Strings(colNames)

	for _, name := range colNames {
		inSrc := srcCols[name]
		inTgt := tgtCols[name]

		cd := collectionDiff{
			name:        name,
			sourceItems: srcData.itemCounts[name],
			targetItems: tgtData.itemCounts[name],
		}

		switch {
		case inSrc && !inTgt:
			cd.status = diffAdded
		case !inSrc && inTgt:
			cd.status = diffRemoved
		default:
			cd.fields = compareFields(srcData.fields[name], tgtData.fields[name])
			hasFieldChanges := false
			for _, f := range cd.fields {
				if f.status != diffMatch {
					hasFieldChanges = true
					break
				}
			}
			if hasFieldChanges || cd.sourceItems != cd.targetItems {
				cd.status = diffChanged
			} else {
				cd.status = diffMatch
			}
		}

		result.collections = append(result.collections, cd)
	}

	// Relations diff.
	srcRels := make(map[string]bool)
	for _, r := range srcData.relations {
		srcRels[r.Collection+"."+r.Field] = true
	}
	tgtRels := make(map[string]bool)
	for _, r := range tgtData.relations {
		tgtRels[r.Collection+"."+r.Field] = true
	}
	for key := range srcRels {
		if !tgtRels[key] {
			result.relations.sourceOnly = append(result.relations.sourceOnly, key)
		} else {
			result.relations.common++
		}
	}
	for key := range tgtRels {
		if !srcRels[key] {
			result.relations.targetOnly = append(result.relations.targetOnly, key)
		}
	}
	sort.Strings(result.relations.sourceOnly)
	sort.Strings(result.relations.targetOnly)

	// System entities diff.
	for _, et := range systemEntityTypes {
		result.system = append(result.system, systemDiff{
			name:        et.Name,
			sourceCount: srcData.systemCount[et.Name],
			targetCount: tgtData.systemCount[et.Name],
		})
	}

	return result, nil
}

func compareFields(srcFields, tgtFields []FieldInfo) []fieldDiff {
	srcMap := make(map[string]FieldInfo)
	for _, f := range srcFields {
		if f.Type != "alias" {
			srcMap[f.Field] = f
		}
	}
	tgtMap := make(map[string]FieldInfo)
	for _, f := range tgtFields {
		if f.Type != "alias" {
			tgtMap[f.Field] = f
		}
	}

	allFields := make(map[string]bool)
	for f := range srcMap {
		allFields[f] = true
	}
	for f := range tgtMap {
		allFields[f] = true
	}

	var names []string
	for f := range allFields {
		names = append(names, f)
	}
	sort.Strings(names)

	var diffs []fieldDiff
	for _, name := range names {
		sf, inSrc := srcMap[name]
		tf, inTgt := tgtMap[name]

		fd := fieldDiff{name: name}
		switch {
		case inSrc && !inTgt:
			fd.status = diffAdded
			fd.sourceType = sf.Type
		case !inSrc && inTgt:
			fd.status = diffRemoved
			fd.targetType = tf.Type
		case sf.Type != tf.Type:
			fd.status = diffChanged
			fd.sourceType = sf.Type
			fd.targetType = tf.Type
		default:
			fd.status = diffMatch
			fd.sourceType = sf.Type
			fd.targetType = tf.Type
		}
		diffs = append(diffs, fd)
	}
	return diffs
}

// Profile picker TUI for diff

const (
	diffPickSource = iota
	diffPickTarget
	diffPickNewProfile
	diffPickLoading
)

const newProfileLabel = "+ New profile"

type diffPickerModel struct {
	cfg       dietConfig
	names     []string // profile names + newProfileLabel at end
	sourceIdx int
	targetIdx int
	step      int
	prevStep  int // step to return to after new profile
	spinFrame int

	// New profile form
	inputs   []textinput.Model
	labels   []string
	focusIdx int

	result    *diffResult
	errMsg    string
	cancelled bool

	width  int
	height int
}

type diffComputedMsg struct {
	result *diffResult
	err    error
}

func newDiffPickerModel(cfg dietConfig) diffPickerModel {
	m := diffPickerModel{
		cfg:   cfg,
		step:  diffPickSource,
		width: 80, height: 24,
	}
	m.rebuildNames()
	return m
}

func (m *diffPickerModel) rebuildNames() {
	m.names = sortedProfileNames(m.cfg)
	m.names = append(m.names, newProfileLabel)
}

func (m *diffPickerModel) initNewProfileForm() {
	m.labels = []string{"Name", "URL", "Token"}
	m.inputs = make([]textinput.Model, 3)
	m.inputs[0] = newInput("my-server", "")
	m.inputs[1] = newInput("https://directus.example.com", "")
	m.inputs[2] = newInput("static-token", "")
	m.inputs[2].EchoMode = textinput.EchoPassword
	m.focusIdx = 0
	m.inputs[0].Focus()
}

func (m diffPickerModel) isNewProfile(idx int) bool {
	return idx == len(m.names)-1
}

func (m diffPickerModel) Init() tea.Cmd {
	return tea.Tick(120*time.Millisecond, func(time.Time) tea.Msg { return tickMsg{} })
}

func (m diffPickerModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case tickMsg:
		m.spinFrame++
		return m, tea.Tick(120*time.Millisecond, func(time.Time) tea.Msg { return tickMsg{} })

	case diffComputedMsg:
		if msg.err != nil {
			m.errMsg = msg.err.Error()
			return m, tea.Quit
		}
		m.result = msg.result
		return m, tea.Quit

	case tea.KeyMsg:
		if m.step == diffPickNewProfile {
			return m.handleNewProfileKey(msg)
		}
		return m.handleKey(msg)
	}

	// Update text inputs when in form mode.
	if m.step == diffPickNewProfile && m.focusIdx < len(m.inputs) {
		var cmd tea.Cmd
		m.inputs[m.focusIdx], cmd = m.inputs[m.focusIdx].Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m diffPickerModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if m.step == diffPickLoading {
		if msg.String() == "q" || msg.String() == "ctrl+c" {
			m.cancelled = true
			return m, tea.Quit
		}
		return m, nil
	}

	switch msg.String() {
	case "q", "ctrl+c":
		m.cancelled = true
		return m, tea.Quit

	case "esc":
		if m.step == diffPickTarget {
			m.step = diffPickSource
			return m, nil
		}
		m.cancelled = true
		return m, tea.Quit

	case "up", "k":
		switch m.step {
		case diffPickSource:
			if m.sourceIdx > 0 {
				m.sourceIdx--
			}
		case diffPickTarget:
			if m.targetIdx > 0 {
				m.targetIdx--
			}
		}

	case "down", "j":
		switch m.step {
		case diffPickSource:
			if m.sourceIdx < len(m.names)-1 {
				m.sourceIdx++
			}
		case diffPickTarget:
			if m.targetIdx < len(m.names)-1 {
				m.targetIdx++
			}
		}

	case "enter":
		switch m.step {
		case diffPickSource:
			if m.isNewProfile(m.sourceIdx) {
				m.prevStep = diffPickSource
				m.step = diffPickNewProfile
				m.initNewProfileForm()
				return m, m.inputs[0].Focus()
			}
			m.step = diffPickTarget
			m.targetIdx = 0
			if m.targetIdx == m.sourceIdx && len(m.names) > 1 {
				m.targetIdx = 1
			}
		case diffPickTarget:
			if m.isNewProfile(m.targetIdx) {
				m.prevStep = diffPickTarget
				m.step = diffPickNewProfile
				m.initNewProfileForm()
				return m, m.inputs[0].Focus()
			}
			if m.targetIdx == m.sourceIdx {
				return m, nil
			}
			m.step = diffPickLoading
			return m, m.startDiff()
		}
	}
	return m, nil
}

func (m diffPickerModel) handleNewProfileKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c":
		m.cancelled = true
		return m, tea.Quit

	case "esc":
		m.step = m.prevStep
		return m, nil

	case "tab", "down":
		if m.focusIdx < len(m.inputs)-1 {
			m.inputs[m.focusIdx].Blur()
			m.focusIdx++
			return m, m.inputs[m.focusIdx].Focus()
		}

	case "shift+tab", "up":
		if m.focusIdx > 0 {
			m.inputs[m.focusIdx].Blur()
			m.focusIdx--
			return m, m.inputs[m.focusIdx].Focus()
		}

	case "enter":
		name := strings.TrimSpace(m.inputs[0].Value())
		url := strings.TrimSpace(m.inputs[1].Value())
		token := strings.TrimSpace(m.inputs[2].Value())

		if name == "" || url == "" || token == "" {
			return m, nil
		}

		// Save new profile.
		m.cfg.Profiles[name] = profile{URL: url, Token: token}
		_ = saveConfig(m.cfg)
		m.rebuildNames()

		// Go back to the step that triggered new profile, with the new profile selected.
		newIdx := 0
		for i, n := range m.names {
			if n == name {
				newIdx = i
				break
			}
		}
		if m.prevStep == diffPickSource {
			m.sourceIdx = newIdx
			m.step = diffPickSource
		} else {
			m.targetIdx = newIdx
			m.step = diffPickTarget
		}
		return m, nil
	}

	// Pass to active input.
	var cmd tea.Cmd
	m.inputs[m.focusIdx], cmd = m.inputs[m.focusIdx].Update(msg)
	return m, cmd
}

func (m diffPickerModel) startDiff() tea.Cmd {
	srcName := m.names[m.sourceIdx]
	tgtName := m.names[m.targetIdx]
	srcProf := m.cfg.Profiles[srcName]
	tgtProf := m.cfg.Profiles[tgtName]

	source := newClientWithOptions(srcProf.URL, srcProf.Token, srcProf.clientOptions())
	target := newClientWithOptions(tgtProf.URL, tgtProf.Token, tgtProf.clientOptions())

	return func() tea.Msg {
		result, err := computeDiff(source, target, srcProf.URL, tgtProf.URL, func(string) {})
		return diffComputedMsg{result: result, err: err}
	}
}

func (m diffPickerModel) View() string {
	if m.width == 0 || m.height == 0 {
		return ""
	}

	var body string
	switch m.step {
	case diffPickSource:
		body = m.viewSelectStep("source", m.sourceIdx, -1)
	case diffPickTarget:
		body = m.viewSelectStep("target", m.targetIdx, m.sourceIdx)
	case diffPickNewProfile:
		body = m.viewNewProfileStep()
	case diffPickLoading:
		body = m.viewLoadingStep()
	}

	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(1, 4).
		Render(body)

	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, box)
}

func (m diffPickerModel) banner() string {
	logo := lipgloss.NewStyle().
		Bold(true).
		Foreground(borderColor).
		Render("◆  D I E T   ⇄   D I F F")
	tag := lipgloss.NewStyle().
		Foreground(dimColor).
		Render("Compare two Directus instances side by side")
	return lipgloss.JoinVertical(lipgloss.Center, logo, tag)
}

func (m diffPickerModel) viewSelectStep(role string, cursor, disabledIdx int) string {
	heading := lipgloss.NewStyle().Foreground(dimColor).
		Render("Choose " + role + " instance")

	rows := make([]string, len(m.names))
	for i, name := range m.names {
		isNew := m.isNewProfile(i)
		desc := ""
		if !isNew {
			if prof, ok := m.cfg.Profiles[name]; ok && prof.URL != "" {
				desc = prof.URL
			}
		}
		if i == disabledIdx {
			rows[i] = lipgloss.NewStyle().Foreground(lipgloss.Color("236")).
				Render(fmt.Sprintf("  %-18s %s  (already source)", name, desc))
			continue
		}
		rows[i] = renderListRow(name, desc, i == cursor, isNew)
	}

	body := lipgloss.JoinVertical(lipgloss.Left, rows...)

	hints := renderKeyHint("↑↓", "select") + "   " +
		renderKeyHint("enter", "confirm") + "   " +
		renderKeyHint("esc", "back") + "   " +
		renderKeyHint("q", "quit")

	return lipgloss.JoinVertical(lipgloss.Center,
		m.banner(),
		"",
		heading,
		"",
		lipgloss.NewStyle().Width(64).Render(body),
		"",
		hints,
	)
}

func (m diffPickerModel) viewNewProfileStep() string {
	heading := lipgloss.NewStyle().Foreground(dimColor).Render("New profile")

	var fields []string
	for i, inp := range m.inputs {
		labelStyle := lipgloss.NewStyle().
			Foreground(labelCol).
			Width(10).
			Align(lipgloss.Right)
		if i == m.focusIdx {
			labelStyle = labelStyle.Foreground(valueCol).Bold(true)
		}
		fields = append(fields, lipgloss.JoinHorizontal(lipgloss.Top,
			labelStyle.Render(m.labels[i]+" "),
			inp.View(),
		))
	}
	body := lipgloss.JoinVertical(lipgloss.Left, fields...)

	hints := renderKeyHint("tab", "next field") + "   " +
		renderKeyHint("enter", "save") + "   " +
		renderKeyHint("esc", "back")

	return lipgloss.JoinVertical(lipgloss.Center,
		m.banner(),
		"",
		heading,
		"",
		lipgloss.NewStyle().Width(56).Render(body),
		"",
		hints,
	)
}

func (m diffPickerModel) viewLoadingStep() string {
	spin := spinChars[m.spinFrame%len(spinChars)]
	srcName := m.names[m.sourceIdx]
	tgtName := m.names[m.targetIdx]

	msg := fmt.Sprintf("%s Comparing %s  ⇄  %s",
		lipgloss.NewStyle().Foreground(warnColor).Render(spin),
		lipgloss.NewStyle().Foreground(borderColor).Bold(true).Render(srcName),
		lipgloss.NewStyle().Foreground(borderColor).Bold(true).Render(tgtName))

	return lipgloss.JoinVertical(lipgloss.Center,
		m.banner(),
		"",
		msg,
		"",
		renderKeyHint("q", "cancel"),
	)
}

// Diff result viewer TUI

type dataDiffMsg struct {
	collection string
	content    string
	err        error
}

type diffModel struct {
	result *diffResult

	// Main view: scrollable diff overview.
	viewport viewport.Model
	ready    bool

	// Collection list for cursor navigation.
	visibleCols []int // indices into result.collections (non-match only)
	cursor      int
	scrollOff   int // scroll offset for the list

	// Data inspection overlay.
	inspecting   bool
	inspectCol   string
	inspectVP    viewport.Model
	inspectReady bool
	inspectLoad  bool

	width  int
	height int
}

func newDiffModel(result *diffResult) diffModel {
	m := diffModel{
		result: result,
		width:  80,
		height: 24,
	}
	// Build list of non-identical collections for navigation.
	for i, c := range result.collections {
		if c.status != diffMatch {
			m.visibleCols = append(m.visibleCols, i)
		}
	}
	return m
}

func (m diffModel) Init() tea.Cmd { return nil }

func (m diffModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		contentW := max(m.width-4, 40)
		vpH := max(m.height-7, 5)
		if !m.ready {
			m.viewport = viewport.New(contentW, vpH)
			m.viewport.SetContent(renderDiffContent(m.result, contentW))
			m.ready = true
		} else {
			m.viewport.Width = contentW
			m.viewport.Height = vpH
			m.viewport.SetContent(renderDiffContent(m.result, contentW))
		}
		if m.inspectReady {
			m.inspectVP.Width = contentW
			m.inspectVP.Height = vpH
		}

	case dataDiffMsg:
		m.inspectLoad = false
		if msg.err != nil {
			m.inspecting = false
			return m, nil
		}
		m.inspectReady = true
		contentW := max(m.width-4, 40)
		vpH := max(m.height-7, 5)
		m.inspectVP = viewport.New(contentW, vpH)
		m.inspectVP.SetContent(msg.content)
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)
	}

	// Delegate scroll to the active viewport.
	if m.inspecting && m.inspectReady {
		var cmd tea.Cmd
		m.inspectVP, cmd = m.inspectVP.Update(msg)
		return m, cmd
	}
	if m.ready && !m.inspecting {
		var cmd tea.Cmd
		m.viewport, cmd = m.viewport.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m diffModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	// Inspect overlay.
	if m.inspecting {
		if key == "esc" || key == "q" {
			m.inspecting = false
			m.inspectReady = false
			m.inspectLoad = false
			return m, nil
		}
		if m.inspectReady {
			var cmd tea.Cmd
			m.inspectVP, cmd = m.inspectVP.Update(msg)
			return m, cmd
		}
		return m, nil
	}

	switch key {
	case "q", "ctrl+c":
		return m, tea.Quit

	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}

	case "down", "j":
		if m.cursor < len(m.visibleCols)-1 {
			m.cursor++
		}

	case "i", "enter":
		if len(m.visibleCols) == 0 {
			return m, nil
		}
		col := m.result.collections[m.visibleCols[m.cursor]]
		m.inspecting = true
		m.inspectLoad = true
		m.inspectReady = false
		m.inspectCol = col.name
		return m, m.fetchDataDiff(col.name)

	case "esc":
		return m, tea.Quit
	}
	return m, nil
}

func (m diffModel) fetchDataDiff(collection string) tea.Cmd {
	src := m.result.sourceClient
	tgt := m.result.targetClient
	w := max(m.width-6, 40)

	return func() tea.Msg {
		var srcItems, tgtItems []json.RawMessage
		var srcErr, tgtErr error
		var wg sync.WaitGroup

		wg.Add(2)
		go func() {
			defer wg.Done()
			srcItems, srcErr = src.pullAllItems(collection)
		}()
		go func() {
			defer wg.Done()
			tgtItems, tgtErr = tgt.pullAllItems(collection)
		}()
		wg.Wait()

		if srcErr != nil {
			return dataDiffMsg{collection: collection, err: srcErr}
		}
		if tgtErr != nil {
			return dataDiffMsg{collection: collection, err: tgtErr}
		}

		content := renderDataDiff(collection, srcItems, tgtItems, w)
		return dataDiffMsg{collection: collection, content: content}
	}
}

func (m diffModel) View() string {
	w := m.width
	h := m.height
	if w < 40 {
		w = 40
	}
	if h < 6 {
		h = 6
	}

	if m.inspecting {
		return m.viewInspect(w, h)
	}
	return m.viewMain(w, h)
}

// renderDiffHeader renders a two-column k9s-style header showing both
// source and target connection details.
func (m diffModel) renderDiffHeader(w int) string {
	srcToken, tgtToken := "—", "—"
	if m.result.sourceClient != nil {
		srcToken = maskToken(m.result.sourceClient.token)
	}
	if m.result.targetClient != nil {
		tgtToken = maskToken(m.result.targetClient.token)
	}

	row := func(label, value string) string {
		return headerLabelStyle.Render(fmt.Sprintf("%-7s", label)) + " " +
			headerValueStyle.Render(value)
	}

	colW := max(w/2-2, 20)
	leftRows := []string{
		row("Source", truncate(m.result.sourceURL, colW-12)),
		row("Token", srcToken),
	}
	rightRows := []string{
		row("Target", truncate(m.result.targetURL, colW-12)),
		row("Token", tgtToken),
	}

	left := lipgloss.JoinVertical(lipgloss.Left, leftRows...)
	right := lipgloss.JoinVertical(lipgloss.Left, rightRows...)
	leftCol := lipgloss.NewStyle().Width(w / 2).Padding(0, 1).Render(left)
	rightCol := lipgloss.NewStyle().Padding(0, 1).Render(right)
	return lipgloss.JoinHorizontal(lipgloss.Top, leftCol, rightCol)
}

// summaryChips formats the diff totals as colored chips.
func (m diffModel) summaryChips() string {
	added, removed, changed, matched := 0, 0, 0, 0
	for _, c := range m.result.collections {
		switch c.status {
		case diffAdded:
			added++
		case diffRemoved:
			removed++
		case diffChanged:
			changed++
		case diffMatch:
			matched++
		}
	}

	chip := func(bg lipgloss.Color, text string) string {
		return lipgloss.NewStyle().
			Background(bg).
			Foreground(lipgloss.Color("16")).
			Bold(true).
			Padding(0, 1).
			Render(text)
	}
	muted := func(text string) string {
		return lipgloss.NewStyle().
			Background(lipgloss.Color("236")).
			Foreground(lipgloss.Color("250")).
			Padding(0, 1).
			Render(text)
	}

	parts := []string{muted(fmt.Sprintf("%d total", added+removed+changed+matched))}
	if changed > 0 {
		parts = append(parts, chip(lipgloss.Color("214"), fmt.Sprintf("~ %d changed", changed)))
	}
	if added > 0 {
		parts = append(parts, chip(lipgloss.Color("42"), fmt.Sprintf("+ %d source-only", added)))
	}
	if removed > 0 {
		parts = append(parts, chip(lipgloss.Color("203"), fmt.Sprintf("− %d target-only", removed)))
	}
	if matched > 0 {
		parts = append(parts, muted(fmt.Sprintf("= %d identical", matched)))
	}
	return lipgloss.NewStyle().Padding(0, 1).Render(strings.Join(parts, " "))
}

func (m diffModel) viewMain(w, h int) string {
	title := titleBar.Render("◆ Diet › Diff")
	header := m.renderDiffHeader(w)
	summary := m.summaryChips()

	keyBar := renderKeyBar(w-2,
		[2]string{"↑↓", "navigate"},
		[2]string{"i", "inspect data"},
		[2]string{"q", "quit"})

	// chrome lines reserved for non-list content:
	//   title(1) + header(2) + blank(1) + summary(1) + blank(1) + blank-before-keybar(1) + keybar(1) = 8
	chrome := 8
	visH := max(h-chrome, 3)

	if m.cursor < m.scrollOff {
		m.scrollOff = m.cursor
	}
	if m.cursor >= m.scrollOff+visH {
		m.scrollOff = m.cursor - visH + 1
	}

	var listLines []string
	for vi := m.scrollOff; vi < len(m.visibleCols) && vi < m.scrollOff+visH; vi++ {
		c := m.result.collections[m.visibleCols[vi]]
		listLines = append(listLines, renderDiffRow(c, vi == m.cursor))
	}
	if len(listLines) == 0 {
		listLines = []string{lipgloss.NewStyle().Foreground(okColor).Render("✓ All collections identical")}
	}

	list := lipgloss.NewStyle().Padding(0, 1).Render(strings.Join(listLines, "\n"))
	keyBarLine := lipgloss.NewStyle().Padding(0, 1).Render(keyBar)

	return padToHeight(lipgloss.JoinVertical(lipgloss.Left,
		title, header, "", summary, "", list, "", keyBarLine), h)
}

func (m diffModel) viewInspect(w, h int) string {
	colTitle := titleBar.Render("◆ Diet › Diff › " + m.inspectCol)
	keyBar := renderKeyBar(w-2,
		[2]string{"↑↓", "scroll"},
		[2]string{"esc", "back"})

	var body string
	if m.inspectLoad {
		spin := spinChars[0]
		body = lipgloss.NewStyle().Padding(1, 1).Render(
			fmt.Sprintf("%s %s",
				lipgloss.NewStyle().Foreground(warnColor).Render(spin),
				lipgloss.NewStyle().Foreground(valueCol).Render("Fetching items from both instances...")))
	} else if m.inspectReady {
		body = lipgloss.NewStyle().Padding(0, 1).Render(m.inspectVP.View())
	}

	keyBarLine := lipgloss.NewStyle().Padding(0, 1).Render(keyBar)
	return padToHeight(lipgloss.JoinVertical(lipgloss.Left,
		colTitle, "", body, "", keyBarLine), h)
}

// diffRowNameW caps the visible width of a collection name in the list so
// that each diff row stays on one terminal line. Names longer than this are
// truncated with an ellipsis — the full name is always available via inspect.
const diffRowNameW = 34

func renderDiffRow(c collectionDiff, isCursor bool) string {
	cursor := "  "
	if isCursor {
		cursor = lipgloss.NewStyle().Foreground(lipgloss.Color("39")).Render("▸ ")
	}

	// Truncate the plain name first; styling is applied after so ANSI
	// escape codes never reach the truncate / Width call (both count
	// printable runes only).
	plainName := truncate(c.name, diffRowNameW)

	var prefix, styledName, detail string

	switch c.status {
	case diffChanged:
		prefix = diffChgStyle.Render("~")
		if isCursor {
			styledName = diffHeaderStyle.Render(plainName)
		} else {
			styledName = plainName
		}
		if c.sourceItems != c.targetItems {
			detail = diffChgStyle.Render(itemCountDiff(c.sourceItems, c.targetItems))
		} else {
			detail = diffMatchStyle.Render(fmt.Sprintf("%d items", c.sourceItems))
		}
		fieldChanges := 0
		for _, f := range c.fields {
			if f.status != diffMatch {
				fieldChanges++
			}
		}
		if fieldChanges > 0 {
			detail += diffChgStyle.Render(fmt.Sprintf("  %d field changes", fieldChanges))
		}
	case diffAdded:
		prefix = diffAddStyle.Render("+")
		styledName = diffAddStyle.Render(plainName)
		detail = diffMatchStyle.Render(fmt.Sprintf("%d items (source only)", c.sourceItems))
	case diffRemoved:
		prefix = diffRemStyle.Render("−")
		styledName = diffRemStyle.Render(plainName)
		detail = diffMatchStyle.Render(fmt.Sprintf("%d items (target only)", c.targetItems))
	}

	nameCol := lipgloss.NewStyle().Width(diffRowNameW).Render(styledName)
	return cursor + prefix + " " + nameCol + " " + detail
}

// Data diff: compare items by content (ignoring id and system fields)

// systemFields are stripped before comparison — they differ per instance.
var systemFields = map[string]bool{
	"id": true, "user_created": true, "user_updated": true,
	"date_created": true, "date_updated": true,
}

func renderDataDiff(collection string, srcItems, tgtItems []json.RawMessage, w int) string {
	srcNorm := normalizeItems(srcItems)
	tgtNorm := normalizeItems(tgtItems)

	// Build hash → items maps.
	srcByHash := make(map[string]normalizedItem, len(srcNorm))
	for _, item := range srcNorm {
		srcByHash[item.hash] = item
	}
	tgtByHash := make(map[string]normalizedItem, len(tgtNorm))
	for _, item := range tgtNorm {
		tgtByHash[item.hash] = item
	}

	var srcOnly, tgtOnly []normalizedItem
	matched := 0

	for hash, item := range srcByHash {
		if _, ok := tgtByHash[hash]; ok {
			matched++
		} else {
			srcOnly = append(srcOnly, item)
		}
	}
	for hash, item := range tgtByHash {
		if _, ok := srcByHash[hash]; !ok {
			tgtOnly = append(tgtOnly, item)
		}
	}
	sort.Slice(srcOnly, func(i, j int) bool { return srcOnly[i].label < srcOnly[j].label })
	sort.Slice(tgtOnly, func(i, j int) bool { return tgtOnly[i].label < tgtOnly[j].label })

	sep := diffSepStyle.Render("  " + strings.Repeat("─", max(w-4, 1)))
	var b strings.Builder

	// Summary.
	total := len(srcNorm) + len(tgtOnly)
	var parts []string
	parts = append(parts, fmt.Sprintf("source: %d  target: %d", len(srcNorm), len(tgtNorm)))
	if matched > 0 {
		parts = append(parts, diffMatchStyle.Render(fmt.Sprintf("%d identical", matched)))
	}
	if len(srcOnly) > 0 {
		parts = append(parts, diffAddStyle.Render(fmt.Sprintf("%d source-only", len(srcOnly))))
	}
	if len(tgtOnly) > 0 {
		parts = append(parts, diffRemStyle.Render(fmt.Sprintf("%d target-only", len(tgtOnly))))
	}
	b.WriteString("  " + strings.Join(parts, diffMatchStyle.Render(" · ")) + "\n")
	b.WriteString(sep + "\n\n")

	if matched == total && len(srcOnly) == 0 && len(tgtOnly) == 0 {
		b.WriteString(diffMatchStyle.Render("  All items identical (ignoring id and system fields)") + "\n")
		return b.String()
	}

	// Source-only items.
	if len(srcOnly) > 0 {
		b.WriteString(diffHeaderStyle.Render("  Source-only items") + "\n\n")
		limit := min(len(srcOnly), 50)
		for _, item := range srcOnly[:limit] {
			b.WriteString(diffAddStyle.Render("  + ") + item.label + "\n")
			renderItemPreview(&b, item.fields, 6, w)
		}
		if len(srcOnly) > limit {
			b.WriteString(diffMatchStyle.Render(fmt.Sprintf("\n  ... and %d more\n", len(srcOnly)-limit)))
		}
		b.WriteString("\n")
	}

	// Target-only items.
	if len(tgtOnly) > 0 {
		b.WriteString(diffHeaderStyle.Render("  Target-only items") + "\n\n")
		limit := min(len(tgtOnly), 50)
		for _, item := range tgtOnly[:limit] {
			b.WriteString(diffRemStyle.Render("  − ") + item.label + "\n")
			renderItemPreview(&b, item.fields, 6, w)
		}
		if len(tgtOnly) > limit {
			b.WriteString(diffMatchStyle.Render(fmt.Sprintf("\n  ... and %d more\n", len(tgtOnly)-limit)))
		}
		b.WriteString("\n")
	}

	if matched > 0 {
		b.WriteString(diffMatchStyle.Render(fmt.Sprintf("  %d identical items", matched)) + "\n")
	}

	return b.String()
}

type normalizedItem struct {
	hash   string
	label  string            // human-readable label for display
	fields map[string]string // field → value (for preview)
}

func normalizeItems(items []json.RawMessage) []normalizedItem {
	result := make([]normalizedItem, 0, len(items))
	for _, raw := range items {
		var obj map[string]any
		if json.Unmarshal(raw, &obj) != nil {
			continue
		}

		// Build normalized map (sorted keys, no system fields).
		norm := make(map[string]string)
		var keys []string
		for k := range obj {
			if !systemFields[k] {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)

		for _, k := range keys {
			norm[k] = fmt.Sprintf("%v", obj[k])
		}

		// Hash.
		h := sha256.New()
		for _, k := range keys {
			h.Write([]byte(k))
			h.Write([]byte{0})
			h.Write([]byte(norm[k]))
			h.Write([]byte{0})
		}
		hash := hex.EncodeToString(h.Sum(nil))[:16]

		// Label: pick the most meaningful field for display.
		label := itemLabel(obj)

		result = append(result, normalizedItem{hash: hash, label: label, fields: norm})
	}
	return result
}

// itemLabel picks a human-readable label from an item.
func itemLabel(obj map[string]any) string {
	// Try common name fields in priority order.
	for _, key := range []string{"name", "title", "label", "slug", "key", "email", "code"} {
		if v, ok := obj[key]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	// Fall back to id if available.
	if id, ok := obj["id"]; ok {
		return fmt.Sprintf("(id: %v)", id)
	}
	return "(unknown)"
}

func renderItemPreview(b *strings.Builder, fields map[string]string, indent, maxW int) {
	pad := strings.Repeat(" ", indent)
	// Show up to 4 key fields as a compact preview.
	preview := []string{"name", "title", "label", "slug", "key", "status", "type", "value", "email"}
	shown := 0
	for _, k := range preview {
		v, ok := fields[k]
		if !ok || v == "" || v == "<nil>" {
			continue
		}
		b.WriteString(diffMatchStyle.Render(fmt.Sprintf("%s%-14s %s", pad, k, truncate(v, maxW-indent-16))) + "\n")
		shown++
		if shown >= 4 {
			break
		}
	}
}

// Rendering

var (
	diffAddStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))  // green
	diffRemStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("196")) // red
	diffChgStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("214")) // yellow
	diffMatchStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("240")) // dim
	diffHeaderStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("255"))
	diffSepStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("236"))
)

func renderDiffContent(r *diffResult, w int) string {
	var b strings.Builder
	sep := diffSepStyle.Render("  " + strings.Repeat("─", max(w-4, 1)))

	// Count statuses.
	added, removed, changed, matched := 0, 0, 0, 0
	for _, c := range r.collections {
		switch c.status {
		case diffAdded:
			added++
		case diffRemoved:
			removed++
		case diffChanged:
			changed++
		case diffMatch:
			matched++
		}
	}

	// Summary bar.
	var parts []string
	parts = append(parts, fmt.Sprintf("%d collections", added+removed+changed+matched))
	if changed > 0 {
		parts = append(parts, diffChgStyle.Render(fmt.Sprintf("%d changed", changed)))
	}
	if added > 0 {
		parts = append(parts, diffAddStyle.Render(fmt.Sprintf("%d source-only", added)))
	}
	if removed > 0 {
		parts = append(parts, diffRemStyle.Render(fmt.Sprintf("%d target-only", removed)))
	}
	if matched > 0 {
		parts = append(parts, diffMatchStyle.Render(fmt.Sprintf("%d identical", matched)))
	}
	b.WriteString("  " + strings.Join(parts, diffMatchStyle.Render(" · ")) + "\n")
	b.WriteString(sep + "\n\n")

	// Changed collections.
	if changed > 0 {
		b.WriteString(diffHeaderStyle.Render("  Changed") + "\n\n")
		for _, c := range r.collections {
			if c.status != diffChanged {
				continue
			}
			// Collection header.
			b.WriteString(diffChgStyle.Render("  ~ ") + diffHeaderStyle.Render(c.name))
			if c.sourceItems != c.targetItems {
				b.WriteString(diffChgStyle.Render(fmt.Sprintf("  %s", itemCountDiff(c.sourceItems, c.targetItems))))
			} else {
				b.WriteString(diffMatchStyle.Render(fmt.Sprintf("  %d items", c.sourceItems)))
			}
			b.WriteString("\n")

			// Field table for this collection.
			hasFieldDiff := false
			for _, f := range c.fields {
				if f.status == diffMatch {
					continue
				}
				hasFieldDiff = true
				switch f.status {
				case diffAdded:
					fmt.Fprintf(&b, "      %s %-24s %s\n",
						diffAddStyle.Render("+"),
						diffAddStyle.Render(f.name),
						diffMatchStyle.Render(f.sourceType))
				case diffRemoved:
					fmt.Fprintf(&b, "      %s %-24s %s\n",
						diffRemStyle.Render("−"),
						diffRemStyle.Render(f.name),
						diffMatchStyle.Render(f.targetType))
				case diffChanged:
					fmt.Fprintf(&b, "      %s %-24s %s %s %s\n",
						diffChgStyle.Render("~"),
						diffChgStyle.Render(f.name),
						diffRemStyle.Render(f.sourceType),
						diffMatchStyle.Render("→"),
						diffAddStyle.Render(f.targetType))
				}
			}
			if !hasFieldDiff && c.sourceItems != c.targetItems {
				b.WriteString(diffMatchStyle.Render("      schema identical, data differs") + "\n")
			}
			b.WriteString("\n")
		}
	}

	// Source-only collections.
	if added > 0 {
		b.WriteString(diffHeaderStyle.Render("  Source only") + "\n\n")
		for _, c := range r.collections {
			if c.status != diffAdded {
				continue
			}
			fmt.Fprintf(&b, "  %s %-30s %s\n",
				diffAddStyle.Render("+"),
				diffAddStyle.Render(c.name),
				diffMatchStyle.Render(fmt.Sprintf("%d items", c.sourceItems)))
		}
		b.WriteString("\n")
	}

	// Target-only collections.
	if removed > 0 {
		b.WriteString(diffHeaderStyle.Render("  Target only") + "\n\n")
		for _, c := range r.collections {
			if c.status != diffRemoved {
				continue
			}
			fmt.Fprintf(&b, "  %s %-30s %s\n",
				diffRemStyle.Render("−"),
				diffRemStyle.Render(c.name),
				diffMatchStyle.Render(fmt.Sprintf("%d items", c.targetItems)))
		}
		b.WriteString("\n")
	}

	// Identical collections (collapsed).
	if matched > 0 {
		b.WriteString(diffMatchStyle.Render(fmt.Sprintf("  %d identical collections", matched)) + "\n\n")
	}

	// Relations.
	b.WriteString(sep + "\n")
	relParts := []string{fmt.Sprintf("%d relations", r.relations.common+len(r.relations.sourceOnly)+len(r.relations.targetOnly))}
	if r.relations.common > 0 {
		relParts = append(relParts, diffMatchStyle.Render(fmt.Sprintf("%d shared", r.relations.common)))
	}
	if len(r.relations.sourceOnly) > 0 {
		relParts = append(relParts, diffAddStyle.Render(fmt.Sprintf("%d source-only", len(r.relations.sourceOnly))))
	}
	if len(r.relations.targetOnly) > 0 {
		relParts = append(relParts, diffRemStyle.Render(fmt.Sprintf("%d target-only", len(r.relations.targetOnly))))
	}
	b.WriteString("  " + strings.Join(relParts, diffMatchStyle.Render(" · ")) + "\n")

	for _, rel := range r.relations.sourceOnly {
		fmt.Fprintf(&b, "    %s %s\n", diffAddStyle.Render("+"), diffAddStyle.Render(rel))
	}
	for _, rel := range r.relations.targetOnly {
		fmt.Fprintf(&b, "    %s %s\n", diffRemStyle.Render("−"), diffRemStyle.Render(rel))
	}

	// System entities.
	b.WriteString("\n" + sep + "\n")
	var sysParts []string
	for _, s := range r.system {
		if s.sourceCount == s.targetCount {
			sysParts = append(sysParts, diffMatchStyle.Render(fmt.Sprintf("%s %d", s.name, s.sourceCount)))
		} else {
			sysParts = append(sysParts, diffChgStyle.Render(fmt.Sprintf("%s %s", s.name, itemCountDiff(s.sourceCount, s.targetCount))))
		}
	}
	b.WriteString("  " + strings.Join(sysParts, diffMatchStyle.Render(" · ")) + "\n")

	return b.String()
}

func itemCountDiff(src, tgt int) string {
	if src == tgt {
		return fmt.Sprintf("%d items", src)
	}
	delta := src - tgt
	sign := "+"
	if delta < 0 {
		sign = ""
	}
	return fmt.Sprintf("%d → %d (%s%d)", src, tgt, sign, delta)
}

func renderDiffPlainText(r *diffResult) string {
	var b strings.Builder

	b.WriteString("Collections:\n")
	for _, c := range r.collections {
		switch c.status {
		case diffAdded:
			fmt.Fprintf(&b, "  + %-30s  %d items (source only)\n", c.name, c.sourceItems)
		case diffRemoved:
			fmt.Fprintf(&b, "  - %-30s  %d items (target only)\n", c.name, c.targetItems)
		case diffChanged:
			fmt.Fprintf(&b, "  ~ %-30s  %s\n", c.name, itemCountDiff(c.sourceItems, c.targetItems))
			for _, f := range c.fields {
				switch f.status {
				case diffAdded:
					fmt.Fprintf(&b, "      + %-20s  %s\n", f.name, f.sourceType)
				case diffRemoved:
					fmt.Fprintf(&b, "      - %-20s  %s\n", f.name, f.targetType)
				case diffChanged:
					fmt.Fprintf(&b, "      ~ %-20s  %s -> %s\n", f.name, f.sourceType, f.targetType)
				}
			}
		case diffMatch:
			fmt.Fprintf(&b, "    %-30s  %d items\n", c.name, c.sourceItems)
		}
	}

	if len(r.relations.sourceOnly) > 0 || len(r.relations.targetOnly) > 0 {
		b.WriteString("\nRelations:\n")
		fmt.Fprintf(&b, "  %d shared\n", r.relations.common)
		for _, rel := range r.relations.sourceOnly {
			fmt.Fprintf(&b, "  + %s\n", rel)
		}
		for _, rel := range r.relations.targetOnly {
			fmt.Fprintf(&b, "  - %s\n", rel)
		}
	}

	b.WriteString("\nSystem Entities:\n")
	for _, s := range r.system {
		if s.sourceCount == s.targetCount {
			fmt.Fprintf(&b, "    %-14s  %d\n", s.name, s.sourceCount)
		} else {
			fmt.Fprintf(&b, "  ~ %-14s  %s\n", s.name, itemCountDiff(s.sourceCount, s.targetCount))
		}
	}

	return b.String()
}
