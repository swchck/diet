package main

import (
	"encoding/json"
	"testing"
)

func TestBuildInsertOrder_LinearChain(t *testing.T) {
	// A → B → C (C depends on B, B depends on A)
	collections := []string{"C", "B", "A"}
	relations := []RelationInfo{
		{Collection: "B", RelatedCollection: "A"},
		{Collection: "C", RelatedCollection: "B"},
	}

	order := buildInsertOrder(collections, relations)

	idx := map[string]int{}
	for i, c := range order {
		idx[c] = i
	}

	if idx["A"] > idx["B"] {
		t.Errorf("A should come before B, got order: %v", order)
	}
	if idx["B"] > idx["C"] {
		t.Errorf("B should come before C, got order: %v", order)
	}
}

func TestBuildInsertOrder_EmptyGraph(t *testing.T) {
	order := buildInsertOrder(nil, nil)
	if len(order) != 0 {
		t.Errorf("expected empty order, got %v", order)
	}
}

func TestBuildInsertOrder_NoRelations(t *testing.T) {
	collections := []string{"X", "Y", "Z"}
	order := buildInsertOrder(collections, nil)

	if len(order) != 3 {
		t.Errorf("expected 3 items, got %d", len(order))
	}

	// All should be present.
	found := map[string]bool{}
	for _, c := range order {
		found[c] = true
	}
	for _, c := range collections {
		if !found[c] {
			t.Errorf("missing %s in order", c)
		}
	}
}

func TestBuildInsertOrder_CircularDeps(t *testing.T) {
	// A → B → A (circular)
	collections := []string{"A", "B"}
	relations := []RelationInfo{
		{Collection: "A", RelatedCollection: "B"},
		{Collection: "B", RelatedCollection: "A"},
	}

	order := buildInsertOrder(collections, relations)

	if len(order) != 2 {
		t.Errorf("expected 2 items despite cycle, got %d: %v", len(order), order)
	}
}

func TestBuildInsertOrder_SelfReference(t *testing.T) {
	collections := []string{"tree"}
	relations := []RelationInfo{
		{Collection: "tree", RelatedCollection: "tree"},
	}

	order := buildInsertOrder(collections, relations)

	if len(order) != 1 || order[0] != "tree" {
		t.Errorf("expected [tree], got %v", order)
	}
}

func TestBuildInsertOrder_Diamond(t *testing.T) {
	// D depends on B and C, B and C depend on A
	collections := []string{"D", "C", "B", "A"}
	relations := []RelationInfo{
		{Collection: "B", RelatedCollection: "A"},
		{Collection: "C", RelatedCollection: "A"},
		{Collection: "D", RelatedCollection: "B"},
		{Collection: "D", RelatedCollection: "C"},
	}

	order := buildInsertOrder(collections, relations)

	idx := map[string]int{}
	for i, c := range order {
		idx[c] = i
	}

	if idx["A"] > idx["B"] || idx["A"] > idx["C"] {
		t.Errorf("A should come before B and C, got order: %v", order)
	}
	if idx["B"] > idx["D"] || idx["C"] > idx["D"] {
		t.Errorf("B and C should come before D, got order: %v", order)
	}
}

func TestBuildAliasFields(t *testing.T) {
	fields := []FieldInfo{
		{Collection: "posts", Field: "tags", Type: "alias"},
		{Collection: "posts", Field: "title", Type: "string"},
		{Collection: "posts", Field: "user_created", Type: "uuid"},
		{Collection: "posts", Field: "user_updated", Type: "uuid"},
		{Collection: "users", Field: "name", Type: "string"},
	}

	result := buildAliasFields(fields)

	if !result["posts"]["tags"] {
		t.Error("posts.tags (alias) should be marked for stripping")
	}
	// user_created/user_updated are stripped in stripDataFields, not here.
	if result["posts"]["user_created"] {
		t.Error("posts.user_created should NOT be in alias fields (stripped elsewhere)")
	}
	if result["posts"]["title"] {
		t.Error("posts.title should NOT be marked for stripping")
	}
	if len(result["users"]) > 0 {
		t.Error("users collection should have no stripped fields")
	}
}

func TestIsPrimaryKey(t *testing.T) {
	pk := FieldInfo{
		Schema: json.RawMessage(`{"is_primary_key": true}`),
	}
	nonPK := FieldInfo{
		Schema: json.RawMessage(`{"is_primary_key": false}`),
	}
	noSchema := FieldInfo{
		Schema: json.RawMessage(`null`),
	}

	if !isPrimaryKey(pk) {
		t.Error("expected isPrimaryKey to return true")
	}
	if isPrimaryKey(nonPK) {
		t.Error("expected isPrimaryKey to return false")
	}
	if isPrimaryKey(noSchema) {
		t.Error("expected isPrimaryKey to return false for null schema")
	}
}

func TestStripMetaID(t *testing.T) {
	meta := json.RawMessage(`{"id":123,"sort":5,"note":"hello"}`)
	result := stripMetaID(meta)

	var obj map[string]json.RawMessage
	json.Unmarshal(result, &obj)

	if _, ok := obj["id"]; ok {
		t.Error("id should be stripped")
	}
	if _, ok := obj["sort"]; !ok {
		t.Error("sort should be preserved")
	}
	if _, ok := obj["note"]; !ok {
		t.Error("note should be preserved")
	}
}
