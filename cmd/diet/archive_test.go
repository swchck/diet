package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestArchiveRoundTrip_Zstd(t *testing.T) {
	testArchiveRoundTrip(t, "zstd", ".tar.zst")
}

func TestArchiveRoundTrip_Zip(t *testing.T) {
	testArchiveRoundTrip(t, "zip", ".zip")
}

func testArchiveRoundTrip(t *testing.T, format, ext string) {
	t.Helper()

	dir := t.TempDir()
	archivePath := filepath.Join(dir, "test-export"+ext)

	// Create test data.
	manifest := Manifest{
		DietVersion:     "0.1.0",
		DirectusVersion: "11.0.0",
		SourceURL:       "http://localhost:8055",
		ExportedAt:      "2024-01-15T10:00:00Z",
		Format:          format,
		Collections:     []string{"posts", "tags"},
		ItemCounts:      map[string]int{"posts": 2, "tags": 1},
		SystemEntities:  []string{"flows"},
	}

	schema := SchemaBundle{
		Collections: []CollectionInfo{
			{Collection: "posts", Schema: json.RawMessage(`{}`), Meta: json.RawMessage(`{"icon":"article"}`)},
			{Collection: "tags", Schema: json.RawMessage(`{}`), Meta: json.RawMessage(`{"icon":"tag"}`)},
		},
		Fields: []FieldInfo{
			{Collection: "posts", Field: "id", Type: "integer", Schema: json.RawMessage(`{"is_primary_key":true}`), Meta: json.RawMessage(`{"sort":1}`)},
			{Collection: "posts", Field: "title", Type: "string", Schema: json.RawMessage(`{}`), Meta: json.RawMessage(`{"sort":2}`)},
		},
		Relations: []RelationInfo{
			{Collection: "posts", Field: "tag_id", RelatedCollection: "tags", Schema: json.RawMessage(`{}`), Meta: json.RawMessage(`{}`)},
		},
	}

	data := map[string][]json.RawMessage{
		"posts": {json.RawMessage(`{"id":1,"title":"hello"}`), json.RawMessage(`{"id":2,"title":"world"}`)},
		"tags":  {json.RawMessage(`{"id":1,"name":"go"}`)},
	}

	systemData := map[string][]json.RawMessage{
		"flows": {json.RawMessage(`{"id":"flow-1","name":"My Flow","status":"active"}`)},
	}

	// Create archive.
	if err := createArchive(format, archivePath, manifest, schema, data, systemData); err != nil {
		t.Fatalf("createArchive: %v", err)
	}

	// Verify file exists.
	info, err := os.Stat(archivePath)
	if err != nil {
		t.Fatalf("archive not created: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("archive is empty")
	}

	// Extract and verify.
	gotManifest, gotSchema, gotData, gotSystem, err := extractArchive(archivePath)
	if err != nil {
		t.Fatalf("extractArchive: %v", err)
	}

	// Verify manifest.
	if gotManifest.DietVersion != "0.1.0" {
		t.Errorf("manifest.DietVersion = %q, want %q", gotManifest.DietVersion, "0.1.0")
	}
	if gotManifest.SourceURL != "http://localhost:8055" {
		t.Errorf("manifest.SourceURL = %q", gotManifest.SourceURL)
	}
	if len(gotManifest.Collections) != 2 {
		t.Errorf("manifest.Collections = %v, want 2 items", gotManifest.Collections)
	}
	if len(gotManifest.SystemEntities) != 1 || gotManifest.SystemEntities[0] != "flows" {
		t.Errorf("manifest.SystemEntities = %v, want [flows]", gotManifest.SystemEntities)
	}

	// Verify schema.
	if len(gotSchema.Collections) != 2 {
		t.Errorf("schema.Collections = %d, want 2", len(gotSchema.Collections))
	}
	if len(gotSchema.Fields) != 2 {
		t.Errorf("schema.Fields = %d, want 2", len(gotSchema.Fields))
	}
	if len(gotSchema.Relations) != 1 {
		t.Errorf("schema.Relations = %d, want 1", len(gotSchema.Relations))
	}

	// Verify data.
	if len(gotData["posts"]) != 2 {
		t.Errorf("data[posts] = %d items, want 2", len(gotData["posts"]))
	}
	if len(gotData["tags"]) != 1 {
		t.Errorf("data[tags] = %d items, want 1", len(gotData["tags"]))
	}

	// Verify system data.
	if len(gotSystem["flows"]) != 1 {
		t.Errorf("system[flows] = %d items, want 1", len(gotSystem["flows"]))
	}
}

func TestArchiveRoundTrip_EmptyData(t *testing.T) {
	dir := t.TempDir()
	archivePath := filepath.Join(dir, "empty.tar.zst")

	manifest := Manifest{
		DietVersion: "0.1.0",
		Format:      "zstd",
		Collections: []string{},
		ItemCounts:  map[string]int{},
	}
	schema := SchemaBundle{}
	data := map[string][]json.RawMessage{}
	systemData := map[string][]json.RawMessage{}

	if err := createArchive("zstd", archivePath, manifest, schema, data, systemData); err != nil {
		t.Fatalf("createArchive: %v", err)
	}

	gotManifest, _, gotData, gotSystem, err := extractArchive(archivePath)
	if err != nil {
		t.Fatalf("extractArchive: %v", err)
	}

	if gotManifest.DietVersion != "0.1.0" {
		t.Errorf("manifest.DietVersion = %q", gotManifest.DietVersion)
	}
	if len(gotData) != 0 {
		t.Errorf("expected empty data, got %d collections", len(gotData))
	}
	if len(gotSystem) != 0 {
		t.Errorf("expected empty system data, got %d types", len(gotSystem))
	}
}

func TestArchiveSize(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "test.txt")
	os.WriteFile(f, []byte("hello"), 0o644)

	size := archiveSize(f)
	if size != "5 B" {
		t.Errorf("archiveSize = %q, want '5 B'", size)
	}

	size = archiveSize(filepath.Join(dir, "nonexistent"))
	if size != "?" {
		t.Errorf("archiveSize for missing file = %q, want '?'", size)
	}
}
