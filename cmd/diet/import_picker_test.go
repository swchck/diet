package main

import (
	"slices"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// keyRune builds a tea.KeyMsg from a single rune. Bubbletea's runtime
// distinguishes runes from named keys — for runes we set Type=Runes and
// the rune itself; for named keys (enter, esc) Type carries the
// identity.
func keyRune(r rune) tea.KeyMsg {
	return tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}}
}

func keyNamed(t tea.KeyType) tea.KeyMsg {
	return tea.KeyMsg{Type: t}
}

// step runs Update on a model and returns the post-update concrete
// type. Lets tests chain key presses without re-asserting the type each
// time.
func step(m importPickerModel, msg tea.Msg) importPickerModel {
	out, _ := m.Update(msg)
	return out.(importPickerModel)
}

// fixtureModel mints a picker for tests. Default size matches a typical
// terminal so the table builds with non-trivial dimensions.
func fixtureModel(cols, sys []string) importPickerModel {
	mf := Manifest{ItemCounts: map[string]int{}}
	m := newImportPickerModel(cols, sys, "test.tar.zst", mf)
	// Simulate WindowSizeMsg so the table picks up dimensions.
	out, _ := m.Update(tea.WindowSizeMsg{Width: 100, Height: 30})
	return out.(importPickerModel)
}

// Tests below verify that the picker behaves as a TUI alternative to the
// --collections / --system-entities CLI flags: items default to
// all-selected, toggling unchecks specific rows, and confirmation
// returns the selection through to the import driver.

func TestImportPicker_DefaultsToAllSelected(t *testing.T) {
	m := fixtureModel([]string{"posts", "tags"}, []string{"flows", "dashboards"})
	if got := m.selected(itemKindCollection); !slices.Equal(got, []string{"posts", "tags"}) {
		t.Errorf("default collections = %v, want all", got)
	}
	if got := m.selected(itemKindSystem); !slices.Equal(got, []string{"flows", "dashboards"}) {
		t.Errorf("default system = %v, want all", got)
	}
}

// TestImportPicker_ToggleSingle — pressing space on the cursor toggles
// the highlighted item, leaving others alone. The table cursor starts at
// row 0 (first collection alphabetically).
func TestImportPicker_ToggleSingle(t *testing.T) {
	m := fixtureModel([]string{"posts", "tags"}, nil)
	// cursor is at posts (alphabetical first); space toggles it off.
	m = step(m, keyRune(' '))
	if got := m.selected(itemKindCollection); !slices.Equal(got, []string{"tags"}) {
		t.Errorf("after toggle posts off: %v, want [tags]", got)
	}
}

// TestImportPicker_ToggleAll — `a` flips the bulk state for the current
// tab. If anything is off, select all; if all on, deselect all.
func TestImportPicker_ToggleAll(t *testing.T) {
	m := fixtureModel([]string{"posts", "tags"}, nil)
	m = step(m, keyRune('a'))
	if got := m.selected(itemKindCollection); len(got) != 0 {
		t.Errorf("after `a` from all-on: %v, want []", got)
	}
	m = step(m, keyRune('a'))
	if got := m.selected(itemKindCollection); !slices.Equal(got, []string{"posts", "tags"}) {
		t.Errorf("after second `a`: %v, want all", got)
	}
}

// TestImportPicker_TabSwitchesKind — tab cycles between Collections and
// System Entities. With only one kind present, tab is a no-op.
func TestImportPicker_TabSwitchesKind(t *testing.T) {
	m := fixtureModel([]string{"posts"}, []string{"flows"})
	if m.activeKind() != itemKindCollection {
		t.Errorf("default tab kind = %v, want collection", m.activeKind())
	}
	m = step(m, keyNamed(tea.KeyTab))
	if m.activeKind() != itemKindSystem {
		t.Errorf("after tab: kind = %v, want system", m.activeKind())
	}
	m = step(m, keyNamed(tea.KeyTab))
	if m.activeKind() != itemKindCollection {
		t.Errorf("after tab x2 (wraparound): kind = %v, want collection", m.activeKind())
	}
}

// TestImportPicker_ToggleAllScopedToActiveTab — `a` only flips items in
// the currently visible tab. System entities stay untouched while we
// bulk-deselect collections.
func TestImportPicker_ToggleAllScopedToActiveTab(t *testing.T) {
	m := fixtureModel([]string{"posts", "tags"}, []string{"flows", "dashboards"})
	m = step(m, keyRune('a')) // deselect all collections
	if len(m.selected(itemKindCollection)) != 0 {
		t.Errorf("collections not bulk-deselected")
	}
	if len(m.selected(itemKindSystem)) != 2 {
		t.Errorf("system entities should be untouched, got %v", m.selected(itemKindSystem))
	}
}

// TestImportPicker_Confirm — `enter` on the items page advances to the
// params page; a second `enter` on the params page confirms and quits.
func TestImportPicker_Confirm(t *testing.T) {
	m := fixtureModel([]string{"a"}, nil)
	m = step(m, keyNamed(tea.KeyEnter))
	if m.confirmed {
		t.Errorf("first enter should not confirm — should advance to params")
	}
	if m.page != pagePickParams {
		t.Errorf("page = %v, want params after first enter", m.page)
	}
	out, cmd := m.Update(keyNamed(tea.KeyEnter))
	mm := out.(importPickerModel)
	if !mm.confirmed {
		t.Errorf("confirmed=false after second enter")
	}
	if cmd == nil {
		t.Errorf("expected tea.Quit cmd after confirm")
	}
}

// TestImportPicker_Cancel — q/esc/ctrl+c set cancelled=true and quit.
func TestImportPicker_Cancel(t *testing.T) {
	for _, k := range []tea.KeyMsg{
		keyRune('q'),
		keyNamed(tea.KeyEsc),
		keyNamed(tea.KeyCtrlC),
	} {
		m := fixtureModel([]string{"a"}, nil)
		out, _ := m.Update(k)
		mm := out.(importPickerModel)
		if !mm.cancelled {
			t.Errorf("key %v: cancelled=false", k)
		}
	}
}

// TestImportPicker_ParamsDefaults — the params page must start with
// safety-first defaults: --no-folders ON; data import ON (skip-data
// OFF); accountability untouched (strip-accountability OFF).
func TestImportPicker_ParamsDefaults(t *testing.T) {
	m := fixtureModel([]string{"a"}, nil)
	opts := m.collectOptions()
	if !opts.NoFolders {
		t.Errorf("NoFolders default = false, want true (safety)")
	}
	if opts.SkipData {
		t.Errorf("SkipData default = true, want false (data flows by default)")
	}
	if opts.StripAccountability {
		t.Errorf("StripAccountability default = true, want false")
	}
}

// TestImportPicker_ToggleParam — space on the cursor flips the highlighted
// param. Verifies that handleParamsKey is wired to the params slice and
// not still toggling items.
func TestImportPicker_ToggleParam(t *testing.T) {
	m := fixtureModel([]string{"a"}, nil)
	m = step(m, keyNamed(tea.KeyEnter)) // advance to params
	if !m.params[0].on {
		t.Fatalf("params[0] (no-folders) default off, want on")
	}
	m = step(m, keyRune(' '))
	if m.params[0].on {
		t.Errorf("after toggle, no-folders still on")
	}
	if m.collectOptions().NoFolders {
		t.Errorf("collectOptions reports NoFolders=true after toggle off")
	}
}

// TestImportPicker_BackFromParamsToItems — `b` returns to items.
func TestImportPicker_BackFromParamsToItems(t *testing.T) {
	m := fixtureModel([]string{"a", "b"}, nil)
	m = step(m, keyNamed(tea.KeyEnter)) // → params
	m = step(m, keyRune('j'))           // params cursor=1
	m = step(m, keyRune('b'))           // back
	if m.page != pagePickItems {
		t.Errorf("page = %v, want items after back", m.page)
	}
}

// TestImportPicker_View_RendersHeaderAndTable — sanity that the items
// view emits header chrome (title bar, archive metadata, key hints) and
// the actual collection name inside the table area.
func TestImportPicker_View_RendersHeaderAndTable(t *testing.T) {
	m := fixtureModel([]string{"posts"}, []string{"flows"})
	v := m.View()
	for _, want := range []string{
		"Diet",
		"Import",
		"Collections",
		"Archive",
		"posts",
		"next tab",
	} {
		if !contains(v, want) {
			t.Errorf("items view missing %q in:\n%s", want, v)
		}
	}
}

// TestImportPicker_View_ParamsPage — the params page renders the
// step-2-of-2 hint, every label and description, and the legend for the
// * marker. The view should also include the bordered banner.
func TestImportPicker_View_ParamsPage(t *testing.T) {
	m := fixtureModel([]string{"a"}, nil)
	m = step(m, keyNamed(tea.KeyEnter)) // advance
	v := m.View()
	for _, want := range []string{
		"Step 2 of 2",
		"Skip folder collections",
		"Skip data import",
		"Strip accountability",
		"changed from default",
		"D I E T", // banner
	} {
		if !contains(v, want) {
			t.Errorf("params view missing %q: %s", want, v)
		}
	}
}

// TestImportPicker_CollectOptions_ReadsToggleState — flipping each
// individual param is reflected in collectOptions output.
func TestImportPicker_CollectOptions_ReadsToggleState(t *testing.T) {
	m := fixtureModel([]string{"a"}, nil)
	m = step(m, keyNamed(tea.KeyEnter)) // params
	for i := range m.params {
		m.paramCursor = i
		before := m.params[i].on
		m = step(m, keyRune(' '))
		after := m.collectOptions()
		switch m.params[i].key {
		case "no-folders":
			if after.NoFolders == before {
				t.Errorf("no-folders not flipped in collectOptions")
			}
		case "skip-data":
			if after.SkipData == before {
				t.Errorf("skip-data not flipped in collectOptions")
			}
		case "strip-accountability":
			if after.StripAccountability == before {
				t.Errorf("strip-accountability not flipped in collectOptions")
			}
		}
	}
}
