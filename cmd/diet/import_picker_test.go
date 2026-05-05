package main

import (
	"slices"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

// keyMsg builds a tea.KeyMsg from a single rune. Bubbletea's runtime
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

// TestImportPicker_DefaultsToAllSelected — every collection and system
// entity from the archive starts with selected=true. That matches the
// "import everything" baseline the user opts out from, rather than
// forcing them to tick every box.
func TestImportPicker_DefaultsToAllSelected(t *testing.T) {
	m := newImportPickerModel(
		[]string{"posts", "tags"},
		[]string{"flows", "dashboards"},
		"x.tar.zst",
	)
	if got := m.selected(itemKindCollection); !slices.Equal(got, []string{"posts", "tags"}) {
		t.Errorf("default collections = %v, want all", got)
	}
	if got := m.selected(itemKindSystem); !slices.Equal(got, []string{"flows", "dashboards"}) {
		t.Errorf("default system = %v, want all", got)
	}
}

// TestImportPicker_ToggleSingle — pressing space on the cursor toggles
// the highlighted item, leaving others alone.
func TestImportPicker_ToggleSingle(t *testing.T) {
	m := newImportPickerModel([]string{"posts", "tags"}, nil, "x")
	// cursor is at index 0 (posts) by default; space toggles it off.
	m = step(m, keyRune(' '))
	if got := m.selected(itemKindCollection); !slices.Equal(got, []string{"tags"}) {
		t.Errorf("after toggle posts off: %v, want [tags]", got)
	}
}

// TestImportPicker_ToggleAll — `a` flips the bulk state. If anything is
// off, select all; if all on, deselect all.
func TestImportPicker_ToggleAll(t *testing.T) {
	m := newImportPickerModel([]string{"posts", "tags"}, nil, "x")
	// Start all-on. `a` should deselect everything.
	m = step(m, keyRune('a'))
	if got := m.selected(itemKindCollection); len(got) != 0 {
		t.Errorf("after `a` from all-on: %v, want []", got)
	}
	// `a` again should re-select everything (because some are off).
	m = step(m, keyRune('a'))
	if got := m.selected(itemKindCollection); !slices.Equal(got, []string{"posts", "tags"}) {
		t.Errorf("after second `a`: %v, want all", got)
	}
}

// TestImportPicker_NavigationDoesNotWrap — at the top, up is a no-op;
// at the bottom, down is a no-op. (Wrap-around is a UX choice; we'd
// rather pin and not surprise the user with cursor jumping.)
func TestImportPicker_NavigationDoesNotWrap(t *testing.T) {
	m := newImportPickerModel([]string{"a", "b"}, nil, "x")
	// At top, up does nothing.
	m = step(m, keyRune('k'))
	if m.cursor != 0 {
		t.Errorf("up at top: cursor = %d, want 0", m.cursor)
	}
	// down twice — second one should pin at len-1.
	m = step(m, keyRune('j'))
	m = step(m, keyRune('j'))
	if m.cursor != 1 {
		t.Errorf("down past bottom: cursor = %d, want 1", m.cursor)
	}
}

// TestImportPicker_Confirm — `enter` on the items page advances to the
// params page; a second `enter` on the params page confirms and quits.
// The two-step flow exists so users can review safety toggles before
// triggering a destructive operation.
func TestImportPicker_Confirm(t *testing.T) {
	m := newImportPickerModel([]string{"a"}, nil, "x")
	// First enter — should advance to params, NOT confirm.
	m = step(m, keyNamed(tea.KeyEnter))
	if m.confirmed {
		t.Errorf("first enter should not confirm — should advance to params")
	}
	if m.page != pagePickParams {
		t.Errorf("page = %v, want params after first enter", m.page)
	}
	if m.cursor != 0 {
		t.Errorf("cursor = %d, want 0 after page transition", m.cursor)
	}
	// Second enter — confirms.
	out, cmd := m.Update(keyNamed(tea.KeyEnter))
	mm := out.(importPickerModel)
	if !mm.confirmed {
		t.Errorf("confirmed=false after second enter")
	}
	if cmd == nil {
		t.Errorf("expected tea.Quit cmd after confirm")
	}
}

// TestImportPicker_ParamsDefaults — the params page must start with
// safety-first defaults. Concretely: --no-folders ON; data import ON
// (skip-data OFF); accountability untouched (strip-accountability OFF).
//
// This is the contract the user explicitly asked for ("включёнными
// дефолтами которые дают безопасность"). Don't loosen without asking.
func TestImportPicker_ParamsDefaults(t *testing.T) {
	m := newImportPickerModel([]string{"a"}, nil, "x")
	opts := m.collectOptions()
	if !opts.NoFolders {
		t.Errorf("NoFolders default = false, want true (safety)")
	}
	if opts.SkipData {
		t.Errorf("SkipData default = true, want false (data flows by default)")
	}
	if opts.StripAccountability {
		t.Errorf("StripAccountability default = true, want false (don't mutate target meta)")
	}
}

// TestImportPicker_ToggleParam — space on the cursor flips the highlighted
// param. Verifies that handleParamsKey is wired to the params slice and not
// still toggling items.
func TestImportPicker_ToggleParam(t *testing.T) {
	m := newImportPickerModel([]string{"a"}, nil, "x")
	m = step(m, keyNamed(tea.KeyEnter)) // advance to params
	if m.params[0].on != true {
		t.Fatalf("params[0] (no-folders) default off, want on")
	}
	m = step(m, keyRune(' ')) // toggle no-folders off
	if m.params[0].on {
		t.Errorf("after toggle, no-folders still on")
	}
	if !m.collectOptions().NoFolders == false {
		// nothing — sanity for the inverse direction:
	}
	if m.collectOptions().NoFolders {
		t.Errorf("collectOptions reports NoFolders=true after toggle off")
	}
}

// TestImportPicker_BackFromParamsToItems — `b` (or backspace, left, h)
// returns from params to items. cursor resets so the user lands at the top.
func TestImportPicker_BackFromParamsToItems(t *testing.T) {
	m := newImportPickerModel([]string{"a", "b"}, nil, "x")
	m = step(m, keyRune('j'))           // cursor=1
	m = step(m, keyNamed(tea.KeyEnter)) // advance to params
	m = step(m, keyRune('j'))           // params cursor=1
	m = step(m, keyRune('b'))           // back
	if m.page != pagePickItems {
		t.Errorf("page = %v, want items after back", m.page)
	}
	if m.cursor != 0 {
		t.Errorf("cursor not reset after back: %d", m.cursor)
	}
}

// TestImportPicker_View_ParamsPage — the params page renders the
// step-2-of-2 hint, every label and description from defaultParamItems,
// and the legend for the * marker.
func TestImportPicker_View_ParamsPage(t *testing.T) {
	m := newImportPickerModel([]string{"a"}, nil, "x")
	m = step(m, keyNamed(tea.KeyEnter)) // advance
	v := m.View()
	for _, want := range []string{
		"Step 2 of 2",
		"Skip folder collections",
		"Skip data import",
		"Strip accountability",
		"changed from default",
	} {
		if !contains(v, want) {
			t.Errorf("params view missing %q: %s", want, v)
		}
	}
}

// TestImportPicker_CollectOptions_ReadsToggleState — flipping each
// individual param is reflected in collectOptions output. Guards against
// future drift between the param key strings and the typed struct fields.
func TestImportPicker_CollectOptions_ReadsToggleState(t *testing.T) {
	m := newImportPickerModel([]string{"a"}, nil, "x")
	m = step(m, keyNamed(tea.KeyEnter)) // params
	// Toggle every row off (or on, depending on default) — verify each
	// transition reflects in collectOptions.
	for i := range m.params {
		m.cursor = i
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

// TestImportPicker_Cancel — q/esc/ctrl+c set cancelled=true and quit.
// runImportPicker returns nil for cancelled imports so the command
// can exit cleanly without doing anything.
func TestImportPicker_Cancel(t *testing.T) {
	for _, k := range []tea.KeyMsg{
		keyRune('q'),
		keyNamed(tea.KeyEsc),
		keyNamed(tea.KeyCtrlC),
	} {
		m := newImportPickerModel([]string{"a"}, nil, "x")
		out, _ := m.Update(k)
		mm := out.(importPickerModel)
		if !mm.cancelled {
			t.Errorf("key %v: cancelled=false", k)
		}
	}
}

// TestImportPicker_View_RendersBothSections — sanity that View() emits
// both section headers and item rows. Doesn't assert on exact layout
// (that's UX flake bait) — just on presence of important strings.
func TestImportPicker_View_RendersBothSections(t *testing.T) {
	m := newImportPickerModel([]string{"posts"}, []string{"flows"}, "test.tar.zst")
	v := m.View()
	for _, want := range []string{"Collections", "System Entities", "posts", "flows", "test.tar.zst"} {
		if !contains(v, want) {
			t.Errorf("View() missing %q in output: %s", want, v)
		}
	}
}
