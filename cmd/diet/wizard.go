package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"gopkg.in/yaml.v3"
)

// Config: ~/.config/diet/config.yml (or $XDG_CONFIG_HOME/diet/config.yml)
// holds named connection profiles. The file is created on first save with
// 0600 permissions because it stores Directus static tokens. Tokens are
// echoed as a password field in the wizard.

// profile is a single Directus server entry. Numeric tuning fields default
// to zero; clientOptions() substitutes the package defaults so the YAML
// stays small (only non-default values are written back).
type profile struct {
	URL         string `yaml:"url"`
	Token       string `yaml:"token"`
	Concurrency int    `yaml:"concurrency,omitempty"` // parallel workers (default 6)
	Timeout     int    `yaml:"timeout,omitempty"`     // HTTP timeout in seconds (default 60)
	BatchSize   int    `yaml:"batch_size,omitempty"`  // items per batch POST (default 100)
	RetryPasses int    `yaml:"retry_passes,omitempty"` // max retry passes (default 5)
	Format      string `yaml:"format,omitempty"`      // archive format: zstd or zip (default zstd)
}

func (p profile) clientOptions() clientOptions {
	return clientOptions{
		Timeout:     p.Timeout,
		Concurrency: p.Concurrency,
		BatchSize:   p.BatchSize,
		RetryPasses: p.RetryPasses,
	}
}

func (p profile) archiveFormat() string {
	if p.Format != "" {
		return p.Format
	}
	return "zstd"
}

// dietConfig is the on-disk YAML root.
type dietConfig struct {
	Profiles map[string]profile `yaml:"profiles"`
}

func configPath() string {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			home = "."
		}
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "diet", "config.yml")
}

func configPathDisplay() string {
	p := configPath()
	if home, err := os.UserHomeDir(); err == nil {
		if rel, ok := strings.CutPrefix(p, home); ok {
			return "~" + rel
		}
	}
	return p
}

func loadConfig() dietConfig {
	cfg := dietConfig{Profiles: map[string]profile{}}
	data, err := os.ReadFile(configPath())
	if err != nil {
		return cfg
	}
	yaml.Unmarshal(data, &cfg)
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]profile{}
	}
	return cfg
}

func saveConfig(cfg dietConfig) error {
	path := configPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func sortedProfileNames(cfg dietConfig) []string {
	var names []string
	for name := range cfg.Profiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Wizard result

type wizardResult struct {
	operation   string // "export", "import", "clean"
	prof        profile
	targetProf  profile // for import: the target server
	inputFile   string  // for import: archive path
	profileName string
	cancelled   bool
}

// Wizard steps

const (
	stepOperation    = iota
	stepProfile      // select server profile
	stepNewProfile   // create new profile (name + url + token + settings)
	stepImportFile   // import: enter archive path
	stepImportTarget // import: select target profile
	stepNewTarget    // import: create new target profile
)

// Wizard model

type wizardModel struct {
	step int

	// Step: operation
	operation  int
	operations []string

	// Profile selection
	cfg          dietConfig
	profileNames []string // sorted, + "+ New" at end
	profileIdx   int

	// New profile / fields
	inputs   []textinput.Model
	labels   []string
	focusIdx int

	// Collected data
	selectedProfile string
	selectedTarget  string
	inputFile       string
	done            bool
	cancelled       bool
	width           int
	height          int
}

func newWizard() wizardModel {
	cfg := loadConfig()
	return wizardModel{
		step:       stepOperation,
		operations: []string{"Export", "Import", "Clean", "Diff"},
		cfg:        cfg,
		width:      80,
		height:     24,
	}
}

func (m *wizardModel) buildProfileList() {
	m.profileNames = sortedProfileNames(m.cfg)
	m.profileNames = append(m.profileNames, "+ New profile")
	m.profileIdx = 0
}

func (m *wizardModel) buildNewProfileInputs() {
	m.labels = []string{"Name", "URL", "Token", "Concurrency", "Timeout (s)", "Batch size", "Format"}
	m.inputs = make([]textinput.Model, 7)
	m.inputs[0] = newInput("my-server", "")
	m.inputs[1] = newInput("https://directus.example.com", "")
	m.inputs[2] = newInput("static-token", "")
	m.inputs[2].EchoMode = textinput.EchoPassword
	m.inputs[3] = newInput("6", "6")
	m.inputs[4] = newInput("60", "60")
	m.inputs[5] = newInput("100", "100")
	m.inputs[6] = newInput("zstd", "zstd")
	m.focusIdx = 0
	m.inputs[0].Focus()
}

func (m *wizardModel) buildFileInput() {
	m.labels = []string{"Archive path"}
	m.inputs = make([]textinput.Model, 1)
	m.inputs[0] = newInput("diet-export-20240115-100000.tar.zst", "")
	m.focusIdx = 0
	m.inputs[0].Focus()
}

func newInput(placeholder, value string) textinput.Model {
	ti := textinput.New()
	ti.Placeholder = placeholder
	ti.CharLimit = 256
	ti.Width = 50
	ti.PromptStyle = lipgloss.NewStyle().Foreground(borderColor)
	ti.Prompt = "  "
	if value != "" {
		ti.SetValue(value)
	}
	return ti
}

func atoi(s string, def int) int {
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil || n <= 0 {
		return def
	}
	return n
}

// Update

func (m wizardModel) Init() tea.Cmd { return nil }

func (m wizardModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height

	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			m.cancelled = true
			return m, tea.Quit
		}
		return m.handleKey(msg)
	}

	// Forward non-key messages to focused input.
	if (m.step == stepNewProfile || m.step == stepImportFile || m.step == stepNewTarget) && m.focusIdx < len(m.inputs) {
		var cmd tea.Cmd
		m.inputs[m.focusIdx], cmd = m.inputs[m.focusIdx].Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m wizardModel) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch m.step {
	case stepOperation:
		return m.handleOperationKey(msg)
	case stepProfile, stepImportTarget:
		return m.handleProfileKey(msg)
	case stepNewProfile, stepNewTarget:
		return m.handleNewProfileKey(msg)
	case stepImportFile:
		return m.handleFileKey(msg)
	}
	return m, nil
}

func (m wizardModel) handleOperationKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k":
		if m.operation > 0 {
			m.operation--
		}
	case "down", "j":
		if m.operation < len(m.operations)-1 {
			m.operation++
		}
	case "enter":
		switch m.operations[m.operation] {
		case "Import":
			m.step = stepImportFile
			m.buildFileInput()
		case "Diff":
			// Short-circuit: runWizard will hand off to the dedicated
			// diffPickerModel which has its own source/target flow.
			m.done = true
			return m, tea.Quit
		default: // Export, Clean
			m.goToProfileSelect(stepProfile)
		}
	case "q", "esc":
		m.cancelled = true
		return m, tea.Quit
	}
	return m, nil
}

func (m *wizardModel) goToProfileSelect(step int) {
	if len(m.cfg.Profiles) > 0 {
		m.step = step
		m.buildProfileList()
	} else {
		if step == stepImportTarget {
			m.step = stepNewTarget
		} else {
			m.step = stepNewProfile
		}
		m.buildNewProfileInputs()
	}
}

func (m wizardModel) handleProfileKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k":
		if m.profileIdx > 0 {
			m.profileIdx--
		}
	case "down", "j":
		if m.profileIdx < len(m.profileNames)-1 {
			m.profileIdx++
		}
	case "enter":
		isNew := m.profileIdx >= len(m.profileNames)-1
		if isNew {
			if m.step == stepImportTarget {
				m.step = stepNewTarget
			} else {
				m.step = stepNewProfile
			}
			m.buildNewProfileInputs()
		} else {
			name := m.profileNames[m.profileIdx]
			if m.step == stepImportTarget {
				m.selectedTarget = name
				m.done = true
				return m, tea.Quit
			}
			m.selectedProfile = name
			m.done = true
			return m, tea.Quit
		}
	case "esc":
		if m.step == stepImportTarget {
			m.step = stepImportFile
			m.buildFileInput()
		} else {
			m.step = stepOperation
		}
	case "q":
		m.cancelled = true
		return m, tea.Quit
	}
	return m, nil
}

func (m wizardModel) handleNewProfileKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		if m.focusIdx < len(m.inputs)-1 {
			m.inputs[m.focusIdx].Blur()
			m.focusIdx++
			return m, m.inputs[m.focusIdx].Focus()
		}
		if m.validateRequired() {
			m.saveNewProfile()
			m.done = true
			return m, tea.Quit
		}
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
	case "esc":
		if len(m.cfg.Profiles) == 0 {
			if m.step == stepNewTarget {
				m.step = stepImportFile
				m.buildFileInput()
			} else {
				m.step = stepOperation
			}
		} else {
			if m.step == stepNewTarget {
				m.goToProfileSelect(stepImportTarget)
			} else {
				m.goToProfileSelect(stepProfile)
			}
		}
		return m, nil
	default:
		var cmd tea.Cmd
		m.inputs[m.focusIdx], cmd = m.inputs[m.focusIdx].Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m wizardModel) handleFileKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "enter":
		if strings.TrimSpace(m.inputs[0].Value()) != "" {
			m.inputFile = strings.TrimSpace(m.inputs[0].Value())
			m.goToProfileSelect(stepImportTarget)
		}
	case "esc":
		m.step = stepOperation
		return m, nil
	default:
		var cmd tea.Cmd
		m.inputs[m.focusIdx], cmd = m.inputs[m.focusIdx].Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m *wizardModel) validateRequired() bool {
	// First 3 fields (name, url, token) are required.
	for i := 0; i < 3 && i < len(m.inputs); i++ {
		if strings.TrimSpace(m.inputs[i].Value()) == "" {
			return false
		}
	}
	return true
}

func (m *wizardModel) saveNewProfile() {
	name := strings.TrimSpace(m.inputs[0].Value())
	p := profile{
		URL:         strings.TrimSpace(m.inputs[1].Value()),
		Token:       strings.TrimSpace(m.inputs[2].Value()),
		Concurrency: atoi(m.inputs[3].Value(), 0),
		Timeout:     atoi(m.inputs[4].Value(), 0),
		BatchSize:   atoi(m.inputs[5].Value(), 0),
		Format:      strings.TrimSpace(m.inputs[6].Value()),
	}
	// Only store non-default values.
	if p.Concurrency == 6 {
		p.Concurrency = 0
	}
	if p.Timeout == 60 {
		p.Timeout = 0
	}
	if p.BatchSize == 100 {
		p.BatchSize = 0
	}
	if p.Format == "zstd" {
		p.Format = ""
	}
	m.cfg.Profiles[name] = p
	if m.step == stepNewTarget {
		m.selectedTarget = name
	} else {
		m.selectedProfile = name
	}
}

func (m wizardModel) result() wizardResult {
	res := wizardResult{
		operation:   strings.ToLower(m.operations[m.operation]),
		profileName: m.selectedProfile,
		inputFile:   m.inputFile,
		cancelled:   m.cancelled,
	}
	if m.selectedProfile != "" {
		res.prof = m.cfg.Profiles[m.selectedProfile]
	}
	if m.selectedTarget != "" {
		res.targetProf = m.cfg.Profiles[m.selectedTarget]
	}
	return res
}

// View

// wizardBanner is the centered title block shown at the top of every step.
func (m wizardModel) wizardBanner() string {
	logo := lipgloss.NewStyle().
		Bold(true).
		Foreground(borderColor).
		Render("◆  D I E T")
	tag := lipgloss.NewStyle().
		Foreground(dimColor).
		Render("Directus Import Export Tool")
	ver := lipgloss.NewStyle().
		Foreground(dimColor).
		Render("v" + version)
	return lipgloss.JoinVertical(lipgloss.Center, logo, tag, ver)
}

// wizardHints renders a row of colored key chips for the current step.
func (m wizardModel) wizardHints() string {
	switch m.step {
	case stepNewProfile, stepNewTarget, stepImportFile:
		return renderKeyHint("tab", "next field") + "   " +
			renderKeyHint("enter", "confirm") + "   " +
			renderKeyHint("esc", "back")
	default:
		return renderKeyHint("↑↓", "select") + "   " +
			renderKeyHint("enter", "confirm") + "   " +
			renderKeyHint("esc", "back") + "   " +
			renderKeyHint("q", "quit")
	}
}

func (m wizardModel) View() string {
	if m.width == 0 || m.height == 0 {
		return ""
	}

	var body string
	switch m.step {
	case stepOperation:
		body = m.viewOperationStep()
	case stepProfile:
		body = m.viewProfileStep("Select source server", m.cfg.Profiles)
	case stepImportTarget:
		body = m.viewProfileStep("Select target server", m.cfg.Profiles)
	case stepNewProfile, stepNewTarget:
		body = m.viewInputStep()
	case stepImportFile:
		body = m.viewInputStep()
	}

	box := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(borderColor).
		Padding(1, 4).
		Render(body)

	return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, box)
}

// stepLabel describes the current step in the breadcrumb-style sub-heading.
func (m wizardModel) stepLabel() string {
	switch m.step {
	case stepOperation:
		return "Choose operation"
	case stepProfile:
		return m.operations[m.operation] + " · choose source"
	case stepImportTarget:
		return fmt.Sprintf("Import · %s · choose target", filepathBase(m.inputFile))
	case stepNewProfile:
		return m.operations[m.operation] + " · new profile"
	case stepNewTarget:
		return "Import · new target profile"
	case stepImportFile:
		return "Import · archive path"
	}
	return ""
}

// operationDescriptions tracks the one-line description shown next to each
// operation in the wizard's table. Keep entries aligned with the order
// in newWizard().operations.
var operationDescriptions = map[string]string{
	"Export": "pack collections + system into an archive",
	"Import": "restore from a diet archive",
	"Clean":  "delete collections and system entities",
	"Diff":   "compare two Directus instances",
}

func (m wizardModel) viewOperationStep() string {
	heading := lipgloss.NewStyle().Foreground(dimColor).Render(m.stepLabel())

	const (
		cursorW = 2
		nameW   = 10
		descW   = 44
	)
	tableW := cursorW + nameW + descW

	// Table header.
	headerStyle := lipgloss.NewStyle().
		Foreground(labelCol).
		Bold(true)
	headerRow := strings.Repeat(" ", cursorW) +
		headerStyle.Width(nameW).Render("OPERATION") +
		headerStyle.Render("DESCRIPTION")
	separator := lipgloss.NewStyle().
		Foreground(lipgloss.Color("236")).
		Render(strings.Repeat(" ", cursorW) + strings.Repeat("─", nameW+descW-2))

	// Body rows.
	rows := []string{headerRow, separator}
	for i, op := range m.operations {
		isCursor := i == m.operation
		isDanger := op == "Clean"

		var cursor string
		switch {
		case isCursor && isDanger:
			cursor = lipgloss.NewStyle().Foreground(dangerCol).Bold(true).Render("▸ ")
		case isCursor:
			cursor = lipgloss.NewStyle().Foreground(borderColor).Bold(true).Render("▸ ")
		default:
			cursor = "  "
		}

		nameStyle := lipgloss.NewStyle().Width(nameW)
		switch {
		case isCursor && isDanger:
			nameStyle = nameStyle.Foreground(dangerCol).Bold(true)
		case isCursor:
			nameStyle = nameStyle.Foreground(valueCol).Bold(true)
		case isDanger:
			nameStyle = nameStyle.Foreground(dangerCol)
		default:
			nameStyle = nameStyle.Foreground(labelCol)
		}

		descStyle := lipgloss.NewStyle().Foreground(dimColor).Italic(true)
		if isCursor {
			descStyle = descStyle.Italic(false)
		}

		rows = append(rows, cursor+nameStyle.Render(op)+descStyle.Render(operationDescriptions[op]))
	}

	body := lipgloss.NewStyle().Width(tableW).Render(
		lipgloss.JoinVertical(lipgloss.Left, rows...))

	return lipgloss.JoinVertical(lipgloss.Center,
		m.wizardBanner(),
		"",
		heading,
		"",
		body,
		"",
		m.wizardHints(),
	)
}

func (m wizardModel) viewProfileStep(_ string, profiles map[string]profile) string {
	heading := lipgloss.NewStyle().Foreground(dimColor).Render(m.stepLabel())

	rows := make([]string, len(m.profileNames))
	for i, name := range m.profileNames {
		desc := ""
		if prof, ok := profiles[name]; ok && prof.URL != "" {
			desc = prof.URL
		}
		isNew := i == len(m.profileNames)-1 // last entry is "+ New profile"
		rows[i] = renderListRow(name, desc, i == m.profileIdx, isNew)
	}

	body := lipgloss.JoinVertical(lipgloss.Left, rows...)

	return lipgloss.JoinVertical(lipgloss.Center,
		m.wizardBanner(),
		"",
		heading,
		"",
		lipgloss.NewStyle().Width(56).Render(body),
		"",
		m.wizardHints(),
	)
}

func (m wizardModel) viewInputStep() string {
	heading := lipgloss.NewStyle().Foreground(dimColor).Render(m.stepLabel())

	var fields []string
	for i, inp := range m.inputs {
		labelStyle := lipgloss.NewStyle().
			Foreground(labelCol).
			Width(14).
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

	return lipgloss.JoinVertical(lipgloss.Center,
		m.wizardBanner(),
		"",
		heading,
		"",
		lipgloss.NewStyle().Width(64).Render(body),
		"",
		m.wizardHints(),
	)
}

// renderListRow renders one selectable list row: cursor arrow, label,
// dimmed description if any. The "danger" flag tints destructive options
// (Clean) red so the operator can't pick it by accident in the dark.
func renderListRow(label, desc string, selected, danger bool) string {
	cursor := "  "
	labelStyle := lipgloss.NewStyle().Foreground(dimColor)
	descStyle := lipgloss.NewStyle().Foreground(dimColor).Italic(true)

	switch {
	case selected && danger:
		cursor = lipgloss.NewStyle().Foreground(dangerCol).Bold(true).Render("▸ ")
		labelStyle = labelStyle.Foreground(dangerCol).Bold(true)
	case selected:
		cursor = lipgloss.NewStyle().Foreground(borderColor).Bold(true).Render("▸ ")
		labelStyle = labelStyle.Foreground(valueCol).Bold(true)
	case danger:
		labelStyle = labelStyle.Foreground(dangerCol)
	}

	left := cursor + labelStyle.Render(fmt.Sprintf("%-18s", label))
	if desc != "" {
		return left + " " + descStyle.Render(desc)
	}
	return left
}

func filepathBase(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

// Run wizard and dispatch

func runWizard() error {
	wiz := newWizard()
	p := tea.NewProgram(wiz, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return err
	}

	m := finalModel.(wizardModel)
	if m.cancelled || !m.done {
		return nil
	}

	res := m.result()
	if err := saveConfig(m.cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not save config: %v\n", err)
	} else {
		fmt.Printf("Config saved to %s\n", configPathDisplay())
	}

	switch res.operation {
	case "export":
		client := newClientWithOptions(res.prof.URL, res.prof.Token, res.prof.clientOptions())
		picker := newPicker(client, res.prof.URL, res.profileName, res.prof.archiveFormat(), "", modeExport)
		ep := tea.NewProgram(picker, tea.WithAltScreen())
		finalPicker, err := ep.Run()
		if err != nil {
			return err
		}
		pm := finalPicker.(pickerModel)
		if pm.quitting && !pm.done {
			fmt.Println("Cancelled.")
		}

	case "import":
		client := newClientWithOptions(res.targetProf.URL, res.targetProf.Token, res.targetProf.clientOptions())
		if err := executeImport(client, res.inputFile, true, true); err != nil {
			return err
		}

	case "clean":
		client := newClientWithOptions(res.prof.URL, res.prof.Token, res.prof.clientOptions())
		picker := newPicker(client, res.prof.URL, res.profileName, "", "", modeClean)
		cp := tea.NewProgram(picker, tea.WithAltScreen())
		finalPicker, err := cp.Run()
		if err != nil {
			return err
		}
		pm := finalPicker.(pickerModel)
		if pm.quitting && !pm.done {
			fmt.Println("Cancelled.")
		}

	case "diff":
		// Hand off to the diff picker, seeded with the wizard's config so
		// any newly-added profiles are immediately available.
		dpm := newDiffPickerModel(m.cfg)
		dp := tea.NewProgram(dpm, tea.WithAltScreen())
		finalDiff, err := dp.Run()
		if err != nil {
			return err
		}
		dm := finalDiff.(diffPickerModel)
		if dm.cancelled || dm.result == nil {
			fmt.Println("Cancelled.")
			return nil
		}
		if dm.errMsg != "" {
			return fmt.Errorf("%s", dm.errMsg)
		}
		return showDiffResult(dm.result)
	}

	return nil
}
