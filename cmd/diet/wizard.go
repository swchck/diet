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
		operations: []string{"Export", "Import", "Clean"},
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
		if m.operation == 1 {
			// Import: first ask for archive file.
			m.step = stepImportFile
			m.buildFileInput()
		} else {
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

func (m wizardModel) View() string {
	w := m.width - 2
	h := m.height - 2
	if w < 40 {
		w = 40
	}
	if h < 6 {
		h = 6
	}

	var body string
	switch m.step {
	case stepOperation:
		body = m.viewList("DIET — Directus Import Export Tool", "Select operation:",
			m.operations, m.operation, nil, h)
	case stepProfile:
		body = m.viewList(m.operations[m.operation], "Select server:",
			m.profileNames, m.profileIdx, m.cfg.Profiles, h)
	case stepImportTarget:
		body = m.viewList(fmt.Sprintf("Import [%s]", m.inputFile), "Select target server:",
			m.profileNames, m.profileIdx, m.cfg.Profiles, h)
	case stepNewProfile, stepNewTarget:
		body = m.viewInputFields(h)
	case stepImportFile:
		body = m.viewInputFields(h)
	}

	frame := frameBorder.Width(w)
	return frame.Render(body)
}

func (m wizardModel) viewList(titleText, subtitle string, items []string, cursor int, profiles map[string]profile, h int) string {
	title := titleBar.Render("◆ " + titleText)
	sub := statusBar.Render(subtitle)

	var rows []string
	for i, item := range items {
		cur := "  "
		style := lipgloss.NewStyle().Foreground(dimColor).Padding(0, 1)
		if i == cursor {
			cur = "▸ "
			style = lipgloss.NewStyle().Foreground(lipgloss.Color("255")).Bold(true).Padding(0, 1)
		}

		label := item
		if profiles != nil {
			if prof, ok := profiles[item]; ok && prof.URL != "" {
				label = fmt.Sprintf("%-16s %s", item,
					lipgloss.NewStyle().Foreground(dimColor).Render(prof.URL))
			}
		}
		rows = append(rows, style.Render(cur+label))
	}

	help := helpBar.Render("↑↓: select  enter: confirm  esc: back  q: quit")

	parts := []string{title, "", sub, ""}
	parts = append(parts, rows...)
	parts = append(parts, "", help)

	return padToHeight(lipgloss.JoinVertical(lipgloss.Left, parts...), h)
}

func (m wizardModel) viewInputFields(h int) string {
	var titleText string
	switch m.step {
	case stepNewProfile:
		titleText = fmt.Sprintf("%s — New profile", m.operations[m.operation])
	case stepNewTarget:
		titleText = "Import — New target profile"
	case stepImportFile:
		titleText = "Import — Archive"
	default:
		titleText = "DIET"
	}
	title := titleBar.Render("◆ " + titleText)

	var fields []string
	for i, inp := range m.inputs {
		label := m.labels[i]
		labelStyle := lipgloss.NewStyle().Foreground(dimColor).Width(16).Align(lipgloss.Right)
		if i == m.focusIdx {
			labelStyle = labelStyle.Foreground(lipgloss.Color("255")).Bold(true)
		}
		fields = append(fields, lipgloss.JoinHorizontal(lipgloss.Top,
			labelStyle.Render(label+": "),
			inp.View(),
		))
	}

	help := helpBar.Render("tab/↑↓: switch  enter: confirm  esc: back")

	parts := []string{title, ""}
	parts = append(parts, fields...)
	parts = append(parts, "", help)

	return padToHeight(lipgloss.JoinVertical(lipgloss.Left, parts...), h)
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
	}

	return nil
}
