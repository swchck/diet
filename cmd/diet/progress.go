package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Import progress tracker (thread-safe, shared with goroutines)

type progressTracker struct {
	mu        sync.Mutex
	phase     string
	startTime time.Time
	logs      []string
	done      bool
	total     int
	current   int
}

func newTracker() *progressTracker {
	return &progressTracker{startTime: time.Now(), phase: "Initializing"}
}

func (t *progressTracker) setPhase(phase string) {
	t.mu.Lock()
	t.phase = phase
	t.mu.Unlock()
}

func (t *progressTracker) log(msg string) {
	t.mu.Lock()
	t.logs = append(t.logs, msg)
	t.mu.Unlock()
}

func (t *progressTracker) setTotal(n int) {
	t.mu.Lock()
	t.total = n
	t.mu.Unlock()
}

func (t *progressTracker) advance() {
	t.mu.Lock()
	t.current++
	t.mu.Unlock()
}

// Bubbletea model for import progress

type progressModel struct {
	tracker  *progressTracker
	prog     progress.Model
	quitting bool
	width    int
	height   int
	spin     int
}

type progressTickMsg time.Time
type progressDoneMsg struct{}

func newProgressModel(tracker *progressTracker) progressModel {
	p := progress.New(
		progress.WithDefaultGradient(),
		progress.WithWidth(60),
	)
	return progressModel{
		tracker: tracker,
		prog:    p,
		width:   80,
		height:  24,
	}
}

func (m progressModel) Init() tea.Cmd {
	return tea.Tick(150*time.Millisecond, func(t time.Time) tea.Msg {
		return progressTickMsg(t)
	})
}

func (m progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.prog.Width = msg.Width - 10

	case tea.KeyMsg:
		m.tracker.mu.Lock()
		done := m.tracker.done
		m.tracker.mu.Unlock()
		if done {
			return m, tea.Quit
		}
		if msg.String() == "q" || msg.String() == "ctrl+c" {
			m.quitting = true
			return m, tea.Quit
		}

	case progressDoneMsg:
		m.tracker.mu.Lock()
		m.tracker.done = true
		m.tracker.mu.Unlock()
		return m, tea.Quit

	case progressTickMsg:
		m.spin++
		m.tracker.mu.Lock()
		done := m.tracker.done
		m.tracker.mu.Unlock()
		if done {
			return m, nil
		}
		return m, tea.Tick(150*time.Millisecond, func(t time.Time) tea.Msg {
			return progressTickMsg(t)
		})

	case progress.FrameMsg:
		pm, cmd := m.prog.Update(msg)
		m.prog = pm.(progress.Model)
		return m, cmd
	}
	return m, nil
}

func (m progressModel) View() string {
	t := m.tracker
	t.mu.Lock()
	phase := t.phase
	logs := make([]string, len(t.logs))
	copy(logs, t.logs)
	isDone := t.done
	elapsed := time.Since(t.startTime).Truncate(time.Second)
	current := t.current
	total := t.total
	t.mu.Unlock()

	w := max(m.width-4, 60)

	var sections []string

	// Title
	sections = append(sections, titleBar.Render("◆ DIET Import"))

	// Status line
	if isDone {
		sections = append(sections,
			lipgloss.NewStyle().Padding(0, 1).Render(
				fmt.Sprintf("%s %s  ·  %s",
					lipgloss.NewStyle().Foreground(okColor).Render("✓"),
					lipgloss.NewStyle().Foreground(okColor).Render("Complete"),
					lipgloss.NewStyle().Foreground(dimColor).Render(elapsed.String()))))
	} else {
		spin := spinChars[m.spin%len(spinChars)]
		sections = append(sections,
			lipgloss.NewStyle().Padding(0, 1).Render(
				fmt.Sprintf("%s %s  ·  %s",
					lipgloss.NewStyle().Foreground(warnColor).Render(spin),
					boldWhite.Render(phase),
					lipgloss.NewStyle().Foreground(dimColor).Render(elapsed.String()))))
	}

	// Progress bar
	if total > 0 {
		pct := float64(current) / float64(total)
		if pct > 1 {
			pct = 1
		}
		sections = append(sections,
			lipgloss.NewStyle().Padding(1, 1).Render(
				fmt.Sprintf("%s  %d/%d", m.prog.ViewAs(pct), current, total)))
	}

	// Log area — fill remaining space
	sections = append(sections, "")

	logH := max(m.height-10-len(sections), 3)
	start := 0
	if len(logs) > logH {
		start = len(logs) - logH
	}
	var logLines []string
	for _, l := range logs[start:] {
		logLines = append(logLines,
			lipgloss.NewStyle().Foreground(dimColor).Padding(0, 1).Render(l))
	}
	// Pad to fixed height
	for len(logLines) < logH {
		logLines = append(logLines, "")
	}
	sections = append(sections, strings.Join(logLines, "\n"))

	// Help
	if isDone {
		sections = append(sections, helpBar.Render("Press any key to exit"))
	} else {
		sections = append(sections, helpBar.Render("q: quit"))
	}

	body := lipgloss.JoinVertical(lipgloss.Left, sections...)
	frame := frameBorder.Width(w).Height(m.height - 2)
	return frame.Render(body)
}
