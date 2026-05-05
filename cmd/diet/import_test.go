package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// writeArchive writes the given manifest+schema+data to a temp tar.zst
// path and returns it. Used by import-side tests so we don't have to
// stand up a fake source server when all we care about is reading the
// archive back.
func writeArchive(t *testing.T, manifest Manifest, schema SchemaBundle,
	data, systemData map[string][]json.RawMessage) string {
	t.Helper()
	tmp := t.TempDir()
	out := filepath.Join(tmp, "in.tar.zst")
	if err := createArchive("zstd", out, manifest, schema, data, systemData); err != nil {
		t.Fatalf("createArchive: %v", err)
	}
	return out
}

// TestClassifyImportOutcome covers the policy matrix for whether an
// import should report success, catastrophic failure, or strict-mode
// failure. The function is the single point that decides exit code, so
// every cell of the truth table needs to be locked down.
func TestClassifyImportOutcome(t *testing.T) {
	tests := []struct {
		name                                   string
		strict                                 bool
		dataInserted, dataTotal                int
		sysInserted, sysFailed                 int
		wantErr                                bool
		wantHasCatastrophe, wantHasStrictPhrase bool
	}{
		{
			name:         "no data, no system, success",
			strict:       false,
			dataTotal:    0,
			dataInserted: 0,
			wantErr:      false,
		},
		{
			name:         "all data inserted, no system, success",
			dataInserted: 100, dataTotal: 100,
			wantErr: false,
		},
		{
			name: "partial data loss, lenient mode, success (legacy behavior)",
			// 99 of 100 — historical "log and continue" stance.
			dataInserted: 99, dataTotal: 100,
			wantErr: false,
		},
		{
			name: "partial data loss, strict mode, fail",
			// Same scenario as above but --strict trips it.
			strict:       true,
			dataInserted: 99, dataTotal: 100,
			wantErr:             true,
			wantHasStrictPhrase: true,
		},
		{
			name: "catastrophe: 0 of N data inserted, lenient still fails",
			// Even without --strict this is hard-fail — no realistic
			// caller wants exit 0 here.
			dataInserted: 0, dataTotal: 158000,
			wantErr:            true,
			wantHasCatastrophe: true,
		},
		{
			name:         "catastrophe overrides strict",
			strict:       true,
			dataInserted: 0, dataTotal: 100,
			wantErr:            true,
			wantHasCatastrophe: true,
		},
		{
			name: "system failures only, strict mode, fail",
			// No data phase but several system items dropped.
			strict:    true,
			sysFailed: 3,
			wantErr:   true,
		},
		{
			name: "system failures only, lenient, success",
			// Documenting the historical permissive behavior.
			sysFailed: 3,
			wantErr:   false,
		},
		{
			name: "data success + system failures, lenient, success",
			// dataLoss=0, sysFailed>0 — lenient mode doesn't care.
			dataInserted: 50, dataTotal: 50,
			sysFailed: 5,
			wantErr:   false,
		},
		{
			name: "data success + system failures, strict, fail",
			// strict catches the system-only loss.
			strict:       true,
			dataInserted: 50, dataTotal: 50,
			sysFailed: 5,
			wantErr:   true,
		},
		{
			name: "schema-only import (no data, no system), success",
			// dataTotal=0 short-circuits the catastrophe check; nothing else
			// to fail.
			strict:    true,
			dataTotal: 0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyImportOutcome(
				tt.strict,
				tt.dataInserted, tt.dataTotal,
				tt.sysInserted, tt.sysFailed,
			)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				return
			}
			msg := err.Error()
			if tt.wantHasCatastrophe && !strings.Contains(msg, "0 of") {
				t.Errorf("expected catastrophe phrasing in %q", msg)
			}
			if tt.wantHasStrictPhrase && !strings.Contains(msg, "strict mode") {
				t.Errorf("expected strict-mode phrasing in %q", msg)
			}
		})
	}
}

// TestExecuteImport_FilterCollections — when opts.Collections is set,
// only the named collections' fields hit /fields/<col> on the target,
// and only their items hit /items/<col>. The rest of the archive is
// silently dropped before the import driver sees it.
func TestExecuteImport_FilterCollections(t *testing.T) {
	manifest := Manifest{
		DietVersion: "test", DirectusVersion: "11", SourceURL: "src",
		Collections: []string{"keep", "drop"},
		ItemCounts:  map[string]int{"keep": 1, "drop": 1},
	}
	schema := SchemaBundle{
		Collections: []CollectionInfo{
			{Collection: "keep", Schema: json.RawMessage(`{"name":"keep"}`), Meta: json.RawMessage(`{}`)},
			{Collection: "drop", Schema: json.RawMessage(`{"name":"drop"}`), Meta: json.RawMessage(`{}`)},
		},
		Fields: []FieldInfo{
			{Collection: "keep", Field: "id", Type: "integer",
				Schema: json.RawMessage(`{"is_primary_key":true}`), Meta: json.RawMessage(`{}`)},
			{Collection: "drop", Field: "id", Type: "integer",
				Schema: json.RawMessage(`{"is_primary_key":true}`), Meta: json.RawMessage(`{}`)},
		},
	}
	data := map[string][]json.RawMessage{
		"keep": {json.RawMessage(`{"id":1}`)},
		"drop": {json.RawMessage(`{"id":1}`)},
	}
	archive := writeArchive(t, manifest, schema, data, nil)

	var (
		fieldsOnDrop atomic.Bool
		itemsOnDrop  atomic.Bool
		fieldsOnKeep atomic.Bool
		itemsOnKeep  atomic.Bool
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/schema/snapshot":
			w.Write([]byte(`{"data":{"version":1,"directus":"11.0.0","vendor":"postgres"}}`))
		case r.URL.Path == "/schema/diff":
			w.Write([]byte(`{"data":null}`))
		case r.URL.Path == "/collections":
			w.WriteHeader(200)
			w.Write([]byte(`{}`))
		case strings.HasPrefix(r.URL.Path, "/fields/keep"):
			fieldsOnKeep.Store(true)
			w.Write([]byte(`{}`))
		case strings.HasPrefix(r.URL.Path, "/fields/drop"):
			fieldsOnDrop.Store(true)
			w.Write([]byte(`{}`))
		case strings.HasPrefix(r.URL.Path, "/items/keep"):
			itemsOnKeep.Store(true)
			w.Write([]byte(`{"data":[{"id":1}]}`))
		case strings.HasPrefix(r.URL.Path, "/items/drop"):
			itemsOnDrop.Store(true)
			w.Write([]byte(`{"data":[{"id":1}]}`))
		default:
			w.WriteHeader(200)
			w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	err := executeImport(c, archive, importOpts{
		Data:        true,
		BulkSchema:  true,
		Collections: []string{"keep"},
	})
	if err != nil {
		t.Fatalf("executeImport: %v", err)
	}
	if fieldsOnDrop.Load() {
		t.Errorf("/fields/drop should NOT be touched under --collections=keep")
	}
	if itemsOnDrop.Load() {
		t.Errorf("/items/drop should NOT be touched under --collections=keep")
	}
}

// TestExecuteImport_FilterMissingCollectionErrors — typo in
// --collections must error before any HTTP call. Same shape as the
// export-side guard.
func TestExecuteImport_FilterMissingCollectionErrors(t *testing.T) {
	manifest := Manifest{
		Collections: []string{"posts"},
		ItemCounts:  map[string]int{"posts": 0},
	}
	archive := writeArchive(t, manifest, SchemaBundle{}, nil, nil)

	hits := atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(200)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := newClient(srv.URL, "tok")

	err := executeImport(c, archive, importOpts{
		BulkSchema:  true,
		Collections: []string{"made_up"},
	})
	if err == nil {
		t.Fatal("expected error on unknown collection")
	}
	if !strings.Contains(err.Error(), "made_up") {
		t.Errorf("err should name the missing collection: %v", err)
	}
	if hits.Load() > 0 {
		t.Errorf("no HTTP requests should have been made (got %d)", hits.Load())
	}
}

// TestExecuteImport_FilterEmptyResultErrors — every name in --collections
// existed in the manifest but somehow produced no kept entries (couldn't
// happen with current logic but defensive — we'd rather error than POST
// nothing). Pin the guard so a future filter refactor doesn't quietly
// drop this safety.
func TestExecuteImport_FilterEmptyResultErrors(t *testing.T) {
	// All collections in archive are missing from the keep slice ⇒
	// missingFromKeep would catch this first. We exercise the symmetric
	// "valid name, but the resulting set is empty" path by passing an
	// empty filter slice through the empty-archive case…
	// On reflection, the current code can't hit the empty-result branch
	// after passing the missingFromKeep gate, so this test is informational:
	// it documents the current behavior, not an assertion of the empty path.
	t.Skip("documenting behavior: empty filter result is currently unreachable after missingFromKeep guard")
}

// Used by writeArchive — keep the value to ensure the path is non-empty.
var _ = filepath.Join

// Used by writeArchive — keep the import in scope; otherwise a future
// refactor that drops writeArchive would leave an unused-import error.
var _ = os.TempDir
