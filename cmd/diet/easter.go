package main

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/harmonica"
	"github.com/charmbracelet/lipgloss"
)

// Konami code: ↑↑↓↓←→←→BA

var konamiSequence = []string{
	"up", "up", "down", "down", "left", "right", "left", "right", "b", "a",
}

type konamiTracker struct {
	pos int
}

func (k *konamiTracker) feed(key string) bool {
	if key == konamiSequence[k.pos] {
		k.pos++
		if k.pos == len(konamiSequence) {
			k.pos = 0
			return true
		}
	} else {
		k.pos = 0
		if key == konamiSequence[0] {
			k.pos = 1
		}
	}
	return false
}

// Animation model

type easterModel struct {
	spring    harmonica.Spring
	posY      float64
	velY      float64
	targetY   float64
	frame     int
	tick      int
	done      bool
	startTime time.Time
	width     int
	height    int
}

type easterTickMsg time.Time

func newEasterAnimation(w, h int) easterModel {
	return easterModel{
		spring:    harmonica.NewSpring(harmonica.FPS(60), 6.0, 0.4),
		posY:      float64(h),
		targetY:   0,
		width:     w,
		height:    h,
		startTime: time.Now(),
	}
}

func (m easterModel) tickCmd() tea.Cmd {
	return tea.Tick(time.Second/15, func(t time.Time) tea.Msg {
		return easterTickMsg(t)
	})
}

func (m easterModel) update(msg easterTickMsg) easterModel {
	m.tick++

	// Spring for Y offset (bounce in from bottom).
	m.posY, m.velY = m.spring.Update(m.posY, m.velY, m.targetY)

	// Advance animation frame (~7.5 fps for the morph).
	if m.tick%2 == 0 {
		m.frame++
		if m.frame >= len(morphFrames) {
			m.frame = 0
		}
	}

	// Auto-dismiss after full cycle + a bit.
	elapsed := time.Since(m.startTime).Seconds()
	if elapsed > float64(len(morphFrames))/7.5+2.0 {
		m.done = true
	}

	return m
}

func (m easterModel) view(w, h int) string {
	art := morphFrames[m.frame]
	artLines := strings.Split(art, "\n")

	// Color based on animation progress.
	colorIdx := (m.tick / 2) % len(morphColors)
	color := morphColors[colorIdx]
	artStyle := lipgloss.NewStyle().Foreground(color)

	// Reserve space for title below art.
	titleLines := 3
	availH := max(h-titleLines, 1)

	// Vertical centering + spring offset.
	artH := len(artLines)
	baseY := (availH - artH) / 2
	yOff := baseY + int(m.posY)

	// Horizontal centering.
	artW := 0
	for _, l := range artLines {
		if len(l) > artW {
			artW = len(l)
		}
	}
	xOff := max((w-artW)/2, 0)
	pad := strings.Repeat(" ", xOff)

	var output []string
	for y := range availH {
		artLine := y - yOff
		if artLine >= 0 && artLine < artH {
			line := artLines[artLine]
			// Clip to viewport width.
			if xOff+len(line) > w {
				line = line[:max(0, w-xOff)]
			}
			output = append(output, pad+artStyle.Render(line))
		} else {
			output = append(output, "")
		}
	}

	// Title at bottom center.
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(color)
	title := titleStyle.Render("D  I  E  T")
	titlePad := max((w-lipgloss.Width(title))/2, 0)
	output = append(output, "", strings.Repeat(" ", titlePad)+title)

	subStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
	sub := subStyle.Render(fmt.Sprintf("diet v%s", version))
	subPad := max((w-lipgloss.Width(sub))/2, 0)
	output = append(output, strings.Repeat(" ", subPad)+sub)

	return strings.Join(output, "\n")
}

var morphColors = []lipgloss.Color{
	"57", "93", "129", "165",
	"141", "177", "213", "177",
	"141", "165", "129", "93",
}
