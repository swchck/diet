package main

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// Shared UI primitives. The wizard, the import picker's params page, and
// (eventually) the diff picker all build screens out of the same handful
// of pieces — banner, heading, bordered body, key-hint footer. Keeping
// the recipe in one place means a glyph or colour change ripples to all
// callers without copy-paste drift.

// uiBanner is the centred "◆ D I E T" title block used on every framed
// screen. Pulled out of wizard.go so the import picker (and future TUI
// surfaces) don't grow their own subtly-different copies. Includes the
// short tag-line + version hint underneath so users can spot which
// build they're running without leaving the wizard.
func uiBanner() string {
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

// uiCursor returns the row-prefix indicator: a coloured `▸ ` when the
// row is under the cursor, two padding spaces otherwise. Two characters
// either way so adjacent columns stay aligned regardless of selection.
func uiCursor(active bool) string {
	if active {
		return lipgloss.NewStyle().Foreground(borderColor).Bold(true).Render("▸ ")
	}
	return "  "
}

// uiDot returns the boolean-state glyph used on every selectable row:
// green `●` for on / dim `○` for off. Same colours and characters the
// export picker has used since v0 — keep callers in sync.
func uiDot(on bool) string {
	if on {
		return lipgloss.NewStyle().Foreground(okColor).Bold(true).Render("●")
	}
	return lipgloss.NewStyle().Foreground(dimColor).Render("○")
}

// uiFramedScreen wraps the conventional stack — banner, heading body,
// content body, optional footer (key hints, legend, …) — in the wizard's
// rounded border and centres the whole thing in the terminal.
//
// Pass an empty string for any section to skip it and its trailing
// blank line. The composer is intentionally rigid: every framed screen
// in diet has the same structure (banner, heading, body, footer), and
// keeping it that way makes future additions (e.g. theming) one-file
// changes.
func uiFramedScreen(width, height int, heading, body string, footer ...string) string {
	parts := []string{uiBanner(), ""}
	if heading != "" {
		parts = append(parts, heading, "")
	}
	if body != "" {
		parts = append(parts, body)
	}
	for _, f := range footer {
		if f == "" {
			continue
		}
		parts = append(parts, "", f)
	}

	// Trim a trailing empty entry if `body` was empty AND no footer
	// was provided — otherwise the box renders an awkward bottom gap.
	parts = trimTrailingEmpty(parts)

	inner := lipgloss.JoinVertical(lipgloss.Center, parts...)
	box := frameBorder.Padding(1, 4).Render(inner)
	return lipgloss.Place(width, height, lipgloss.Center, lipgloss.Center, box)
}

func trimTrailingEmpty(parts []string) []string {
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// uiToggleRow renders one boolean toggle as two/three lines (label,
// then word-wrapped description) using the shared cursor + dot prefix.
//
// `descWidth` is the wrap column for the description. The description is
// indented to hang under the label text (past the cursor + dot), so a
// long blurb visually belongs to its toggle without bleeding to the
// frame's left edge. `marker` (e.g. " *") is appended to the label
// verbatim — colour styling is the caller's responsibility because
// "non-default" semantics differ across pages.
func uiToggleRow(label, desc, marker string, on, active bool, descWidth int) string {
	const (
		cursorW = 2 // "▸ " or "  "
		dotW    = 2 // "●" or "○" + space
	)
	descPad := cursorW + dotW

	labelStyle := lipgloss.NewStyle().Foreground(labelCol)
	if active {
		labelStyle = labelStyle.Foreground(valueCol).Bold(true)
	}

	descStyle := lipgloss.NewStyle().
		Foreground(dimColor).
		Italic(true).
		Width(descWidth)

	header := uiCursor(active) + uiDot(on) + " " + labelStyle.Render(label) + marker
	wrapped := descStyle.Render(desc)
	descLines := make([]string, 0, strings.Count(wrapped, "\n")+1)
	for _, line := range strings.Split(wrapped, "\n") {
		descLines = append(descLines, strings.Repeat(" ", descPad)+line)
	}

	return lipgloss.JoinVertical(lipgloss.Left,
		append([]string{header}, descLines...)...,
	)
}
