package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// TestScanLocalArchives_DiscoversArchivesNewestFirst seeds a temp dir
// with a mix of archive types and unrelated files, then asserts that
// scanLocalArchives returns only the archives, sorted by mtime
// descending (newest first — the typical "I just exported, now I'm
// importing" workflow).
func TestScanLocalArchives_DiscoversArchivesNewestFirst(t *testing.T) {
	tmp := t.TempDir()

	// Files in arbitrary creation order; we'll set explicit mtimes
	// below so the order on disk doesn't matter.
	files := []string{
		"old.tar.zst",
		"medium.zip",
		"new.tar.zst",
		"unrelated.txt",
		"notes.md",
	}
	for _, n := range files {
		if err := os.WriteFile(filepath.Join(tmp, n), []byte("x"), 0o600); err != nil {
			t.Fatalf("write %s: %v", n, err)
		}
	}

	// Force a known mtime ordering: new > medium > old.
	now := time.Now()
	for n, off := range map[string]time.Duration{
		"old.tar.zst": -2 * time.Hour,
		"medium.zip":  -1 * time.Hour,
		"new.tar.zst": 0,
	} {
		t := now.Add(off)
		if err := os.Chtimes(filepath.Join(tmp, n), t, t); err != nil {
			panic(err)
		}
	}

	got := scanLocalArchives(tmp)
	want := []string{"new.tar.zst", "medium.zip", "old.tar.zst"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// TestScanLocalArchives_EmptyDir is a no-archives directory; the wizard
// must still work (textinput remains usable), so we expect a clean empty
// result rather than an error.
func TestScanLocalArchives_EmptyDir(t *testing.T) {
	tmp := t.TempDir()
	if got := scanLocalArchives(tmp); len(got) != 0 {
		t.Errorf("got %v, want empty", got)
	}
}

// TestScanLocalArchives_NonexistentDir mirrors a CWD that vanished
// between exec and wizard start (rare but possible — symlink resolved
// to nothing). We must return nil rather than panic.
func TestScanLocalArchives_NonexistentDir(t *testing.T) {
	if got := scanLocalArchives("/this/does/not/exist/diet-tests-XYZ"); got != nil {
		t.Errorf("got %v, want nil for missing dir", got)
	}
}

// TestScanLocalArchives_IgnoresSubdirectoriesAndOtherExtensions covers
// two filters at once: directories named `*.tar.zst` (e.g. a workspace
// folder someone packed and then unpacked next to it) shouldn't be
// listed, and non-archive extensions are ignored.
func TestScanLocalArchives_IgnoresSubdirectoriesAndOtherExtensions(t *testing.T) {
	tmp := t.TempDir()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.Mkdir(filepath.Join(tmp, "fake.tar.zst"), 0o700))
	must(os.WriteFile(filepath.Join(tmp, "valid.tar.zst"), []byte{}, 0o600))
	must(os.WriteFile(filepath.Join(tmp, "valid.zip"), []byte{}, 0o600))
	must(os.WriteFile(filepath.Join(tmp, "other.gz"), []byte{}, 0o600))
	must(os.WriteFile(filepath.Join(tmp, "no_ext"), []byte{}, 0o600))

	got := scanLocalArchives(tmp)
	for _, name := range got {
		if name == "fake.tar.zst" {
			t.Errorf("directory fake.tar.zst should not be listed")
		}
		if name == "other.gz" || name == "no_ext" {
			t.Errorf("non-archive %q listed", name)
		}
	}
	hasValid := slices.Contains(got, "valid.tar.zst") && slices.Contains(got, "valid.zip")
	if !hasValid {
		t.Errorf("expected valid archives in result, got %v", got)
	}
}
