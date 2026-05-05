package main

import (
	"encoding/json"
	"slices"
	"testing"
)

// fixtureBundle mints a small but realistic archive shape. Two user
// collections (posts, tags) plus a folder, both with their own fields, a
// junction-style relation between them, a relation pointing at
// directus_users (system), a custom system field on directus_users, and
// some data per collection.
func fixtureBundle() (Manifest, SchemaBundle, map[string][]json.RawMessage) {
	manifest := Manifest{
		Collections: []string{"posts", "tags", "comments"},
		ItemCounts:  map[string]int{"posts": 2, "tags": 5, "comments": 7},
	}
	schema := SchemaBundle{
		Collections: []CollectionInfo{
			{Collection: "Content", Schema: json.RawMessage(`null`)},
			{Collection: "posts", Schema: json.RawMessage(`{"name":"posts"}`),
				Meta: json.RawMessage(`{"group":"Content"}`)},
			{Collection: "tags", Schema: json.RawMessage(`{"name":"tags"}`),
				Meta: json.RawMessage(`{}`)},
			{Collection: "comments", Schema: json.RawMessage(`{"name":"comments"}`),
				Meta: json.RawMessage(`{}`)},
		},
		Fields: []FieldInfo{
			{Collection: "posts", Field: "id", Type: "integer"},
			{Collection: "posts", Field: "title", Type: "string"},
			{Collection: "tags", Field: "id", Type: "integer"},
			{Collection: "tags", Field: "name", Type: "string"},
			{Collection: "comments", Field: "id", Type: "integer"},
			{Collection: "comments", Field: "post", Type: "integer"},
			// Custom system field anchored by a relation to a kept
			// collection.
			{Collection: "directus_users", Field: "favorite_post", Type: "integer",
				Schema: json.RawMessage(`{}`),
				Meta:   json.RawMessage(`{}`)},
			// Custom system field anchored by a relation to a DROPPED
			// collection — should be removed when only `posts` is kept.
			{Collection: "directus_users", Field: "favorite_tag", Type: "integer",
				Schema: json.RawMessage(`{}`),
				Meta:   json.RawMessage(`{}`)},
		},
		Relations: []RelationInfo{
			// posts ↔ tags junction (M2M imagined; comments are simpler)
			{Collection: "comments", Field: "post", RelatedCollection: "posts"},
			// posts → directus_users (system FK)
			{Collection: "directus_users", Field: "favorite_post", RelatedCollection: "posts"},
			// tags → directus_users (system FK on a non-kept collection)
			{Collection: "directus_users", Field: "favorite_tag", RelatedCollection: "tags"},
		},
	}
	data := map[string][]json.RawMessage{
		"posts":    {json.RawMessage(`{"id":1}`), json.RawMessage(`{"id":2}`)},
		"tags":     {json.RawMessage(`{"id":1}`)},
		"comments": {json.RawMessage(`{"id":1,"post":1}`)},
	}
	return manifest, schema, data
}

// TestFilterArchiveSubset_NoOpOnEmptyKeep — passing nil/empty keep
// returns inputs unchanged with a zero report. Lets callers cheap-skip
// the filter without conditional branching at every call site.
func TestFilterArchiveSubset_NoOpOnEmptyKeep(t *testing.T) {
	manifest, schema, data := fixtureBundle()
	origCollections := slices.Clone(manifest.Collections)
	origFields := len(schema.Fields)

	m, s, d, report := filterArchiveSubset(manifest, schema, data, nil)

	if !slices.Equal(m.Collections, origCollections) {
		t.Errorf("manifest.Collections changed: %v", m.Collections)
	}
	if len(s.Fields) != origFields {
		t.Errorf("fields count changed: %d → %d", origFields, len(s.Fields))
	}
	if len(d) != 3 {
		t.Errorf("data map size changed: %d", len(d))
	}
	if report.droppedRelations != 0 || report.droppedSystemFields != 0 ||
		len(report.missingFromKeep) != 0 {
		t.Errorf("expected zero report on empty keep, got %+v", report)
	}
}

// TestFilterArchiveSubset_SingleCollection — keep just `posts`. Tags and
// comments are dropped from manifest and data; their fields are dropped;
// the comments→posts relation is dropped (comments not kept); the
// directus_users.favorite_tag field is dropped because its anchor
// relation went away. directus_users.favorite_post stays — the
// favorite_post relation survives because posts is kept.
func TestFilterArchiveSubset_SingleCollection(t *testing.T) {
	manifest, schema, data := fixtureBundle()
	m, s, d, report := filterArchiveSubset(manifest, schema, data, []string{"posts"})

	if !slices.Equal(m.Collections, []string{"posts"}) {
		t.Errorf("manifest.Collections = %v, want [posts]", m.Collections)
	}
	if m.ItemCounts["posts"] != 2 {
		t.Errorf("ItemCounts[posts] = %d, want 2", m.ItemCounts["posts"])
	}
	if _, ok := m.ItemCounts["tags"]; ok {
		t.Errorf("ItemCounts[tags] should be removed")
	}

	// Fields: posts.id, posts.title, directus_users.favorite_post.
	wantFieldKeys := map[string]bool{
		"posts.id":                       true,
		"posts.title":                    true,
		"directus_users.favorite_post":   true,
	}
	gotFieldKeys := map[string]bool{}
	for _, f := range s.Fields {
		gotFieldKeys[f.Collection+"."+f.Field] = true
	}
	for k := range wantFieldKeys {
		if !gotFieldKeys[k] {
			t.Errorf("missing field %s", k)
		}
	}
	for k := range gotFieldKeys {
		if !wantFieldKeys[k] {
			t.Errorf("unexpected field present: %s", k)
		}
	}

	// Relations: only directus_users.favorite_post → posts survives.
	if len(s.Relations) != 1 {
		t.Errorf("relations count = %d, want 1: %+v", len(s.Relations), s.Relations)
	}
	if len(s.Relations) > 0 {
		r := s.Relations[0]
		if r.Collection != "directus_users" || r.Field != "favorite_post" {
			t.Errorf("surviving relation = %+v, want directus_users.favorite_post", r)
		}
	}

	// Data: only posts.
	if _, ok := d["posts"]; !ok {
		t.Errorf("data[posts] missing")
	}
	if _, ok := d["tags"]; ok {
		t.Errorf("data[tags] should be dropped")
	}

	// Report: 2 dropped relations (comments→posts and directus_users→tags),
	// 1 dropped system field (favorite_tag).
	if report.droppedRelations != 2 {
		t.Errorf("droppedRelations = %d, want 2", report.droppedRelations)
	}
	if report.droppedSystemFields != 1 {
		t.Errorf("droppedSystemFields = %d, want 1", report.droppedSystemFields)
	}
	if len(report.missingFromKeep) != 0 {
		t.Errorf("missingFromKeep = %v, want []", report.missingFromKeep)
	}
}

// TestFilterArchiveSubset_KeepReferencedPair — when the user keeps both
// sides of an inter-user relation (posts + comments), the relation
// survives.
func TestFilterArchiveSubset_KeepReferencedPair(t *testing.T) {
	manifest, schema, data := fixtureBundle()
	_, s, _, report := filterArchiveSubset(manifest, schema, data, []string{"posts", "comments"})

	hasCommentsPostRelation := false
	for _, r := range s.Relations {
		if r.Collection == "comments" && r.Field == "post" {
			hasCommentsPostRelation = true
		}
	}
	if !hasCommentsPostRelation {
		t.Errorf("comments→posts relation should survive when both kept")
	}
	// Tags-related relation should still be dropped (tags not kept).
	for _, r := range s.Relations {
		if r.Collection == "directus_users" && r.Field == "favorite_tag" {
			t.Errorf("favorite_tag relation should be dropped (tags not kept)")
		}
	}
	if report.droppedRelations < 1 {
		t.Errorf("expected at least 1 dropped relation (favorite_tag), got %d", report.droppedRelations)
	}
}

// TestFilterArchiveSubset_FoldersAlwaysKept — folders are kept regardless
// of the keep set. Tables under them survive only if named explicitly.
func TestFilterArchiveSubset_FoldersAlwaysKept(t *testing.T) {
	manifest, schema, data := fixtureBundle()
	_, s, _, _ := filterArchiveSubset(manifest, schema, data, []string{"posts"})

	hasFolder := false
	for _, c := range s.Collections {
		if c.Collection == "Content" {
			hasFolder = true
		}
	}
	if !hasFolder {
		t.Errorf("Content folder should be kept (folders are always retained)")
	}
}

// TestFilterArchiveSubset_MissingFromArchive — a typo / stale archive:
// caller asks for a collection that isn't present. Surface in the report
// so the caller can warn rather than silently producing an empty result.
func TestFilterArchiveSubset_MissingFromArchive(t *testing.T) {
	manifest, schema, data := fixtureBundle()
	_, _, _, report := filterArchiveSubset(manifest, schema, data, []string{"posts", "made_up"})

	if !slices.Contains(report.missingFromKeep, "made_up") {
		t.Errorf("expected made_up in missingFromKeep, got %v", report.missingFromKeep)
	}
	if slices.Contains(report.missingFromKeep, "posts") {
		t.Errorf("posts should NOT be in missingFromKeep (it exists)")
	}
}

// TestFilterArchiveSubset_AllNamesMissing — every name was a typo.
// Result is empty manifest + empty schema; report flags all of them
// missing. Caller is expected to bail before sending nothing to target.
func TestFilterArchiveSubset_AllNamesMissing(t *testing.T) {
	manifest, schema, data := fixtureBundle()
	m, s, d, report := filterArchiveSubset(manifest, schema, data,
		[]string{"typo_one", "typo_two"})

	if len(m.Collections) != 0 {
		t.Errorf("expected empty Collections, got %v", m.Collections)
	}
	if len(s.Fields) != 0 {
		t.Errorf("expected zero fields, got %d", len(s.Fields))
	}
	if len(d) != 0 {
		t.Errorf("expected empty data, got keys %v", mapKeys(d))
	}
	if len(report.missingFromKeep) != 2 {
		t.Errorf("missingFromKeep = %v, want both typos", report.missingFromKeep)
	}
}

// TestFilterSystemSubset — entity-type filter for flows/dashboards/etc.
// is independent of the collection filter.
func TestFilterSystemSubset(t *testing.T) {
	manifest := Manifest{SystemEntities: []string{"flows", "dashboards", "roles"}}
	systemData := map[string][]json.RawMessage{
		"flows":      {json.RawMessage(`{"id":"f1"}`)},
		"dashboards": {json.RawMessage(`{"id":"d1"}`)},
		"roles":      {json.RawMessage(`{"id":"r1"}`)},
	}

	m, sd := filterSystemSubset(manifest, systemData, []string{"flows"})

	if len(sd) != 1 {
		t.Errorf("systemData size = %d, want 1", len(sd))
	}
	if _, ok := sd["flows"]; !ok {
		t.Errorf("flows missing")
	}
	if _, ok := sd["dashboards"]; ok {
		t.Errorf("dashboards should be dropped")
	}
	if !slices.Equal(m.SystemEntities, []string{"flows"}) {
		t.Errorf("manifest.SystemEntities = %v, want [flows]", m.SystemEntities)
	}
}

// TestFilterSystemSubset_NoOp — empty keep returns inputs unchanged.
func TestFilterSystemSubset_NoOp(t *testing.T) {
	manifest := Manifest{SystemEntities: []string{"flows"}}
	systemData := map[string][]json.RawMessage{"flows": {json.RawMessage(`{}`)}}
	m, sd := filterSystemSubset(manifest, systemData, nil)
	if len(sd) != 1 || len(m.SystemEntities) != 1 {
		t.Errorf("no-op should preserve everything")
	}
}

// TestStripFolderCollections covers the --no-folders trim. Folders are
// detected via Schema being null/empty; real tables have a non-empty
// Schema and must survive.
func TestStripFolderCollections_DropsFoldersKeepsTables(t *testing.T) {
	cols := []CollectionInfo{
		{Collection: "ACL", Schema: json.RawMessage(`null`)},
		{Collection: "posts", Schema: json.RawMessage(`{"name":"posts"}`)},
		{Collection: "Empty", Schema: json.RawMessage(``)}, // also folder-like
		{Collection: "tags", Schema: json.RawMessage(`{"name":"tags"}`)},
	}
	out, dropped := stripFolderCollections(cols)
	if dropped != 2 {
		t.Errorf("dropped = %d, want 2 (ACL + Empty)", dropped)
	}
	if len(out) != 2 {
		t.Fatalf("kept = %d, want 2", len(out))
	}
	got := []string{out[0].Collection, out[1].Collection}
	if !slices.Contains(got, "posts") || !slices.Contains(got, "tags") {
		t.Errorf("expected posts+tags kept, got %v", got)
	}
}

// TestStripFolderCollections_NoFoldersIsPassThrough — when the slice has
// no folders, we shouldn't allocate. Validates the fast path.
func TestStripFolderCollections_NoFoldersIsPassThrough(t *testing.T) {
	cols := []CollectionInfo{
		{Collection: "posts", Schema: json.RawMessage(`{"name":"posts"}`)},
		{Collection: "tags", Schema: json.RawMessage(`{"name":"tags"}`)},
	}
	out, dropped := stripFolderCollections(cols)
	if dropped != 0 {
		t.Errorf("dropped = %d on no-folders input, want 0", dropped)
	}
	// Same backing slice — verifies pass-through, not a copy.
	if &cols[0] != &out[0] {
		t.Errorf("expected pass-through (same slice), got new alloc")
	}
}

// TestStripFolderCollections_AllFolders — pathological all-folder input
// returns an empty slice without panicking.
func TestStripFolderCollections_AllFolders(t *testing.T) {
	cols := []CollectionInfo{
		{Collection: "ACL", Schema: json.RawMessage(`null`)},
		{Collection: "Clans", Schema: json.RawMessage(`null`)},
	}
	out, dropped := stripFolderCollections(cols)
	if dropped != 2 {
		t.Errorf("dropped = %d, want 2", dropped)
	}
	if len(out) != 0 {
		t.Errorf("kept = %d, want 0", len(out))
	}
}

func mapKeys(m map[string][]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
