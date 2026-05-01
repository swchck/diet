package main

import (
	"testing"
)

func TestKonamiTracker_FullSequence(t *testing.T) {
	k := konamiTracker{}
	keys := []string{"up", "up", "down", "down", "left", "right", "left", "right", "b", "a"}
	for i, key := range keys {
		triggered := k.feed(key)
		if i < len(keys)-1 && triggered {
			t.Errorf("triggered early at key %d (%s)", i, key)
		}
		if i == len(keys)-1 && !triggered {
			t.Error("expected trigger on last key")
		}
	}
}

func TestKonamiTracker_WrongKey_Resets(t *testing.T) {
	k := konamiTracker{}
	k.feed("up")
	k.feed("up")
	k.feed("down")
	k.feed("x") // wrong key
	if k.pos != 0 {
		t.Errorf("pos = %d, want 0 after wrong key", k.pos)
	}
}

func TestKonamiTracker_WrongKey_RestartMatch(t *testing.T) {
	k := konamiTracker{}
	k.feed("up")
	k.feed("up")
	k.feed("down")
	k.feed("up") // wrong for sequence, but matches start
	if k.pos != 1 {
		t.Errorf("pos = %d, want 1 (restart match from beginning)", k.pos)
	}
}

func TestKonamiTracker_RepeatedActivation(t *testing.T) {
	k := konamiTracker{}
	keys := []string{"up", "up", "down", "down", "left", "right", "left", "right", "b", "a"}

	// First activation.
	for _, key := range keys {
		k.feed(key)
	}

	// Should reset and work again.
	for i, key := range keys {
		triggered := k.feed(key)
		if i == len(keys)-1 && !triggered {
			t.Error("expected second activation")
		}
	}
}

func TestMorphFrames_NotEmpty(t *testing.T) {
	if len(morphFrames) != 36 {
		t.Errorf("expected 36 frames, got %d", len(morphFrames))
	}
	for i, frame := range morphFrames {
		if frame == "" {
			t.Errorf("frame %d is empty", i)
		}
	}
}

func TestNewEasterAnimation(t *testing.T) {
	m := newEasterAnimation(80, 40)
	if m.width != 80 {
		t.Errorf("width = %d, want 80", m.width)
	}
	if m.height != 40 {
		t.Errorf("height = %d, want 40", m.height)
	}
	if m.done {
		t.Error("new animation should not be done")
	}
	if m.posY != 40 {
		t.Errorf("posY = %f, want 40 (start from bottom)", m.posY)
	}
}

func TestEasterModel_ViewFitsViewport(t *testing.T) {
	m := newEasterAnimation(100, 50)
	// Simulate a few ticks.
	for range 10 {
		m = m.update(easterTickMsg{})
	}
	output := m.view(100, 50)
	lines := splitLines(output)
	if len(lines) > 50 {
		t.Errorf("view output %d lines, exceeds viewport height 50", len(lines))
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
