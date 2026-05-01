package main

import (
	"encoding/json"
	"testing"
)

func TestCompareFields_Added(t *testing.T) {
	src := []FieldInfo{
		{Field: "id", Type: "integer"},
		{Field: "name", Type: "string"},
		{Field: "extra", Type: "text"},
	}
	tgt := []FieldInfo{
		{Field: "id", Type: "integer"},
		{Field: "name", Type: "string"},
	}
	diffs := compareFields(src, tgt)

	found := false
	for _, d := range diffs {
		if d.name == "extra" {
			if d.status != diffAdded {
				t.Errorf("extra: want diffAdded, got %d", d.status)
			}
			found = true
		}
	}
	if !found {
		t.Error("extra field not found in diffs")
	}
}

func TestCompareFields_Removed(t *testing.T) {
	src := []FieldInfo{
		{Field: "id", Type: "integer"},
	}
	tgt := []FieldInfo{
		{Field: "id", Type: "integer"},
		{Field: "removed_field", Type: "string"},
	}
	diffs := compareFields(src, tgt)

	for _, d := range diffs {
		if d.name == "removed_field" {
			if d.status != diffRemoved {
				t.Errorf("removed_field: want diffRemoved, got %d", d.status)
			}
			return
		}
	}
	t.Error("removed_field not found in diffs")
}

func TestCompareFields_TypeChanged(t *testing.T) {
	src := []FieldInfo{
		{Field: "status", Type: "string"},
	}
	tgt := []FieldInfo{
		{Field: "status", Type: "integer"},
	}
	diffs := compareFields(src, tgt)

	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d", len(diffs))
	}
	if diffs[0].status != diffChanged {
		t.Errorf("status: want diffChanged, got %d", diffs[0].status)
	}
	if diffs[0].sourceType != "string" || diffs[0].targetType != "integer" {
		t.Errorf("types: got %s/%s", diffs[0].sourceType, diffs[0].targetType)
	}
}

func TestCompareFields_SkipsAlias(t *testing.T) {
	src := []FieldInfo{
		{Field: "id", Type: "integer"},
		{Field: "related", Type: "alias"},
	}
	tgt := []FieldInfo{
		{Field: "id", Type: "integer"},
	}
	diffs := compareFields(src, tgt)

	for _, d := range diffs {
		if d.name == "related" {
			t.Error("alias field should be skipped")
		}
	}
}

func TestCompareFields_AllMatch(t *testing.T) {
	fields := []FieldInfo{
		{Field: "id", Type: "integer"},
		{Field: "name", Type: "string"},
	}
	diffs := compareFields(fields, fields)

	for _, d := range diffs {
		if d.status != diffMatch {
			t.Errorf("%s: want diffMatch, got %d", d.name, d.status)
		}
	}
}

func TestItemCountDiff(t *testing.T) {
	tests := []struct {
		src, tgt int
		contains string
	}{
		{100, 100, "100 items"},
		{150, 100, "150 → 100 (+50)"},
		{50, 100, "50 → 100 (-50)"},
	}
	for _, tt := range tests {
		got := itemCountDiff(tt.src, tt.tgt)
		if got != tt.contains {
			t.Errorf("itemCountDiff(%d, %d) = %q, want %q", tt.src, tt.tgt, got, tt.contains)
		}
	}
}

func TestRenderDiffPlainText(t *testing.T) {
	result := &diffResult{
		sourceURL: "https://src.example.com",
		targetURL: "https://tgt.example.com",
		collections: []collectionDiff{
			{name: "articles", status: diffAdded, sourceItems: 10},
			{name: "users", status: diffMatch, sourceItems: 5, targetItems: 5},
			{name: "old_table", status: diffRemoved, targetItems: 3},
		},
		system: []systemDiff{
			{name: "flows", sourceCount: 2, targetCount: 2},
		},
	}

	output := renderDiffPlainText(result)
	if output == "" {
		t.Error("expected non-empty output")
	}

	for _, want := range []string{"articles", "users", "old_table", "flows"} {
		if !contains(output, want) {
			t.Errorf("output missing %q", want)
		}
	}
}

func TestRenderDiffContent(t *testing.T) {
	result := &diffResult{
		sourceURL: "https://a.example.com",
		targetURL: "https://b.example.com",
		collections: []collectionDiff{
			{
				name: "posts", status: diffChanged,
				sourceItems: 100, targetItems: 80,
				fields: []fieldDiff{
					{name: "title", status: diffMatch, sourceType: "string", targetType: "string"},
					{name: "body", status: diffChanged, sourceType: "text", targetType: "string"},
				},
			},
		},
		system: []systemDiff{
			{name: "flows", sourceCount: 3, targetCount: 1},
		},
	}

	output := renderDiffContent(result, 80)
	if output == "" {
		t.Error("expected non-empty output")
	}

	for _, want := range []string{"posts", "body", "text", "string", "100", "80"} {
		if !contains(output, want) {
			t.Errorf("output missing %q", want)
		}
	}
}

func TestNormalizeItems(t *testing.T) {
	items := []json.RawMessage{
		json.RawMessage(`{"id":"abc","name":"first","user_created":"x"}`),
		json.RawMessage(`{"id":"def","name":"first","user_created":"y"}`),
		json.RawMessage(`{"id":"ghi","name":"second"}`),
	}
	norm := normalizeItems(items)
	if len(norm) != 3 {
		t.Fatalf("expected 3 items, got %d", len(norm))
	}
	// First two items have same content (ignoring id, user_created) → same hash.
	if norm[0].hash != norm[1].hash {
		t.Error("items with same content should have same hash")
	}
	// Third is different.
	if norm[0].hash == norm[2].hash {
		t.Error("items with different content should have different hashes")
	}
}

func TestItemLabel(t *testing.T) {
	tests := []struct {
		obj  map[string]any
		want string
	}{
		{map[string]any{"id": "1", "name": "hello"}, "hello"},
		{map[string]any{"id": "1", "title": "post"}, "post"},
		{map[string]any{"id": "42"}, "(id: 42)"},
		{map[string]any{}, "(unknown)"},
	}
	for _, tt := range tests {
		got := itemLabel(tt.obj)
		if got != tt.want {
			t.Errorf("itemLabel(%v) = %q, want %q", tt.obj, got, tt.want)
		}
	}
}

func TestRenderDataDiff(t *testing.T) {
	src := []json.RawMessage{
		json.RawMessage(`{"id":"1","name":"alpha","value":10}`),
		json.RawMessage(`{"id":"2","name":"beta","value":20}`),
		json.RawMessage(`{"id":"3","name":"gamma","value":30}`),
	}
	tgt := []json.RawMessage{
		json.RawMessage(`{"id":"100","name":"alpha","value":10}`),
		json.RawMessage(`{"id":"200","name":"beta","value":20}`),
		json.RawMessage(`{"id":"300","name":"delta","value":40}`),
	}

	output := renderDataDiff("test_col", src, tgt, 80)
	if output == "" {
		t.Fatal("expected non-empty output")
	}
	// alpha and beta match by content (different ids ignored).
	// gamma is source-only, delta is target-only.
	for _, want := range []string{"2 identical", "1 source-only", "1 target-only"} {
		if !contains(output, want) {
			t.Errorf("output missing %q", want)
		}
	}
	if !contains(output, "gamma") {
		t.Error("should show gamma as source-only")
	}
	if !contains(output, "delta") {
		t.Error("should show delta as target-only")
	}
}

func TestRenderDiffRow(t *testing.T) {
	c := collectionDiff{
		name: "test_col", status: diffChanged,
		sourceItems: 100, targetItems: 80,
		fields: []fieldDiff{
			{name: "f1", status: diffAdded, sourceType: "string"},
		},
	}
	row := renderDiffRow(c, true)
	if !contains(row, "test_col") {
		t.Error("row should contain collection name")
	}
	if !contains(row, "1 field") {
		t.Error("row should mention field changes")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && searchString(s, sub)
}

func searchString(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestDiffResult_EmptyCollections(t *testing.T) {
	result := &diffResult{
		sourceURL: "https://a.example.com",
		targetURL: "https://b.example.com",
	}

	// Should not panic on empty data.
	output := renderDiffContent(result, 80)
	if output == "" {
		t.Error("expected non-empty output")
	}
	plain := renderDiffPlainText(result)
	_ = json.RawMessage(plain) // just ensure it's a valid string
}
