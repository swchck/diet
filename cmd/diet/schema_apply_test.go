package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBuildSnapshot_OmitsSchemaForFolders(t *testing.T) {
	bundle := SchemaBundle{
		Collections: []CollectionInfo{
			{Collection: "ACL", Schema: json.RawMessage(`null`), Meta: json.RawMessage(`{"icon":"lock"}`)},
			{Collection: "posts", Schema: json.RawMessage(`{"name":"posts"}`), Meta: json.RawMessage(`{"icon":"article"}`)},
			{Collection: "tags", Schema: json.RawMessage(``), Meta: json.RawMessage(`{}`)},
		},
	}
	snap := buildSnapshot(snapshotMeta{Version: 1, Directus: "11.17.0", Vendor: "postgres"}, bundle)
	colls := snap["collections"].([]map[string]any)

	if len(colls) != 3 {
		t.Fatalf("got %d collections, want 3", len(colls))
	}
	if _, has := colls[0]["schema"]; has {
		t.Errorf("folder ACL should NOT have schema key, got %v", colls[0])
	}
	if _, has := colls[2]["schema"]; has {
		t.Errorf("empty-schema collection tags should NOT have schema key, got %v", colls[2])
	}
	if s, has := colls[1]["schema"]; !has {
		t.Errorf("table posts should have schema key")
	} else {
		sm := s.(map[string]any)
		if sm["name"] != "posts" {
			t.Errorf("schema.name = %v, want posts", sm["name"])
		}
	}
}

// TestSanitizeFieldForSnapshot_StripsMetaID — the source instance's
// directus_fields surrogate id is instance-scoped; sending it into the
// target's snapshot risks colliding with the target's own auto-increment.
// The per-field path strips it via stripMetaID; parity required here.
func TestSanitizeFieldForSnapshot_StripsMetaID(t *testing.T) {
	f := FieldInfo{
		Collection: "posts",
		Field:      "title",
		Type:       "string",
		Schema:     json.RawMessage(`{"default_value":"hi"}`),
		Meta:       json.RawMessage(`{"id":12345,"sort":3,"interface":"input"}`),
	}
	raw := sanitizeFieldForSnapshot(f)
	var out FieldInfo
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var meta map[string]json.RawMessage
	json.Unmarshal(out.Meta, &meta)
	if _, has := meta["id"]; has {
		t.Errorf("meta.id should be stripped, got %s", out.Meta)
	}
	// Other meta keys preserved.
	if _, has := meta["sort"]; !has {
		t.Errorf("sort should be preserved, got %s", out.Meta)
	}
	if _, has := meta["interface"]; !has {
		t.Errorf("interface should be preserved")
	}
}

func TestSanitizeFieldForSnapshot_StripsNextvalDefault(t *testing.T) {
	schema, _ := json.Marshal(map[string]any{
		"default_value":      `nextval('x_id_seq'::regclass)`,
		"is_primary_key":     true,
		"has_auto_increment": true,
	})
	f := FieldInfo{
		Collection: "x", Field: "id", Type: "integer",
		Schema: schema,
	}
	out := sanitizeFieldForSnapshot(f)
	var fixed FieldInfo
	if err := json.Unmarshal(out, &fixed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	var schemaMap map[string]any
	json.Unmarshal(fixed.Schema, &schemaMap)
	if schemaMap["default_value"] != nil {
		t.Errorf("default_value = %v, want nil after sanitize", schemaMap["default_value"])
	}
	if schemaMap["is_primary_key"] != true {
		t.Errorf("is_primary_key = %v, want true (other fields preserved)", schemaMap["is_primary_key"])
	}
}

func TestSanitizeFieldForSnapshot_PreservesArbitraryEnumCast(t *testing.T) {
	// 'pending'::status_enum is a Postgres enum cast — not a sequence default,
	// so sanitize must keep it untouched.
	schema, _ := json.Marshal(map[string]any{
		"default_value": "'pending'::status_enum",
		"name":          "x",
	})
	f := FieldInfo{Field: "x", Schema: schema}
	out := sanitizeFieldForSnapshot(f)
	var fixed FieldInfo
	json.Unmarshal(out, &fixed)
	var sm map[string]any
	json.Unmarshal(fixed.Schema, &sm)
	if sm["default_value"] == nil {
		t.Error("non-regclass cast default should NOT be stripped")
	}
}

func TestSanitizeFieldForSnapshot_PassesThroughClean(t *testing.T) {
	f := FieldInfo{
		Field:  "name",
		Schema: json.RawMessage(`{"default_value":"hello","name":"name"}`),
	}
	out := sanitizeFieldForSnapshot(f)
	var fixed FieldInfo
	json.Unmarshal(out, &fixed)
	var sm map[string]any
	json.Unmarshal(fixed.Schema, &sm)
	if sm["default_value"] != "hello" {
		t.Errorf("default_value = %v, want hello", sm["default_value"])
	}
}

func TestSanitizeFieldForSnapshot_NilSchema(t *testing.T) {
	f := FieldInfo{Field: "alias", Type: "alias", Schema: json.RawMessage(`null`)}
	out := sanitizeFieldForSnapshot(f)
	if !strings.Contains(string(out), `"alias"`) {
		t.Errorf("expected alias field to round-trip, got %s", string(out))
	}
}

func TestClassifySchemaError_PayloadTooLarge(t *testing.T) {
	body := []byte(`{"errors":[{"message":"Invalid payload. request entity too large."}]}`)
	err := classifySchemaError("schema/diff", 400, body)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "MAX_PAYLOAD_SIZE") {
		t.Errorf("err = %v, want hint about MAX_PAYLOAD_SIZE", err)
	}
}

func TestClassifySchemaError_GenericError(t *testing.T) {
	err := classifySchemaError("schema/apply", 500, []byte(`{"errors":[{"message":"boom"}]}`))
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "MAX_PAYLOAD_SIZE") {
		t.Errorf("generic error wrongly classified as payload size: %v", err)
	}
}

// TestSchemaApplyBulk_HappyPath wires a fake Directus that:
//   - serves /schema/snapshot with version+directus+vendor
//   - accepts /schema/diff?force=true and returns a non-null diff
//   - accepts /schema/apply with 204
func TestSchemaApplyBulk_HappyPath(t *testing.T) {
	var (
		gotSnapshot atomic.Bool
		gotDiff     atomic.Bool
		gotApply    atomic.Bool
		applyBody   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/schema/snapshot":
			gotSnapshot.Store(true)
			w.Write([]byte(`{"data":{"version":1,"directus":"11.17.0","vendor":"postgres","collections":[],"fields":[],"relations":[]}}`))
		case r.Method == "POST" && r.URL.Path == "/schema/diff":
			gotDiff.Store(true)
			if r.URL.RawQuery != "force=true" {
				t.Errorf("expected force=true, got query=%q", r.URL.RawQuery)
			}
			w.Write([]byte(`{"data":{"hash":"abc","diff":{"collections":[{"collection":"x","diff":[{"kind":"N"}]}]}}}`))
		case r.Method == "POST" && r.URL.Path == "/schema/apply":
			gotApply.Store(true)
			buf := make([]byte, 1024)
			n, _ := r.Body.Read(buf)
			applyBody = string(buf[:n])
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
		}
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	bundle := SchemaBundle{
		Collections: []CollectionInfo{{Collection: "x", Schema: json.RawMessage(`{"name":"x"}`), Meta: json.RawMessage(`{}`)}},
		Fields:      []FieldInfo{{Collection: "x", Field: "id", Type: "integer", Schema: json.RawMessage(`{}`), Meta: json.RawMessage(`{}`)}},
		Relations:   nil,
	}

	var logs []string
	if err := schemaApplyBulk(c, bundle, func(s string) { logs = append(logs, s) }); err != nil {
		t.Fatalf("schemaApplyBulk: %v", err)
	}
	if !gotSnapshot.Load() || !gotDiff.Load() || !gotApply.Load() {
		t.Errorf("missing call: snapshot=%v diff=%v apply=%v", gotSnapshot.Load(), gotDiff.Load(), gotApply.Load())
	}
	// Apply body should be the inner `data` object from /schema/diff response,
	// not the whole envelope.
	if !strings.Contains(applyBody, `"hash":"abc"`) || strings.Contains(applyBody, `"data"`) {
		t.Errorf("apply body unexpected: %s", applyBody)
	}
}

func TestSchemaApplyBulk_PayloadTooLargeReturnsActionableError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schema/snapshot":
			w.Write([]byte(`{"data":{"version":1,"directus":"11.17.0","vendor":"postgres"}}`))
		case "/schema/diff":
			w.WriteHeader(400)
			w.Write([]byte(`{"errors":[{"message":"Invalid payload. request entity too large.","extensions":{"code":"INVALID_PAYLOAD"}}]}`))
		}
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	err := schemaApplyBulk(c, SchemaBundle{}, func(string) {})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "MAX_PAYLOAD_SIZE") {
		t.Errorf("expected actionable hint, got %v", err)
	}
}

func TestSchemaApplyBulk_EmptyDiffIsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schema/snapshot":
			w.Write([]byte(`{"data":{"version":1,"directus":"11.17.0","vendor":"postgres"}}`))
		case "/schema/diff":
			w.WriteHeader(204) // Directus returns 204 when source matches target
		case "/schema/apply":
			t.Error("apply should not be called when diff is empty")
		}
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	var logs []string
	err := schemaApplyBulk(c, SchemaBundle{}, func(s string) { logs = append(logs, s) })
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if !anyContains(logs, "up to date") {
		t.Errorf("expected log mentioning up-to-date, got %v", logs)
	}
}

func anyContains(logs []string, needle string) bool {
	for _, l := range logs {
		if strings.Contains(l, needle) {
			return true
		}
	}
	return false
}

// Sanity check that a real-world-ish snapshot makes it through the build
// pipeline without panics or losing fields.
func TestStripAccountability_OverwritesAllValues(t *testing.T) {
	schema := SchemaBundle{
		Collections: []CollectionInfo{
			{Collection: "a", Meta: json.RawMessage(`{"icon":"x","accountability":"all"}`)},
			{Collection: "b", Meta: json.RawMessage(`{"icon":"y","accountability":"activity"}`)},
			{Collection: "c", Meta: json.RawMessage(`{"icon":"z"}`)}, // no accountability key
			{Collection: "d", Meta: json.RawMessage(`{"icon":"w","accountability":null}`)},
		},
	}
	n := stripAccountability(&schema)
	if n != 4 {
		t.Errorf("touched %d, want 4 (every collection)", n)
	}
	for _, c := range schema.Collections {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(c.Meta, &m); err != nil {
			t.Fatalf("%s: %v", c.Collection, err)
		}
		acc, ok := m["accountability"]
		if !ok {
			t.Errorf("%s: accountability key missing", c.Collection)
			continue
		}
		if string(acc) != "null" {
			t.Errorf("%s: accountability = %s, want null", c.Collection, acc)
		}
		// Other keys preserved.
		if _, ok := m["icon"]; !ok {
			t.Errorf("%s: icon dropped", c.Collection)
		}
	}
}

func TestStripAccountability_HandlesGarbageMeta(t *testing.T) {
	schema := SchemaBundle{
		Collections: []CollectionInfo{
			{Collection: "broken", Meta: json.RawMessage(`not json at all`)},
			{Collection: "empty", Meta: json.RawMessage(``)},
		},
	}
	n := stripAccountability(&schema)
	if n != 2 {
		t.Errorf("touched %d, want 2", n)
	}
	for _, c := range schema.Collections {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(c.Meta, &m); err != nil {
			t.Fatalf("%s: result not valid JSON: %v", c.Collection, err)
		}
		if string(m["accountability"]) != "null" {
			t.Errorf("%s: accountability = %s, want null", c.Collection, m["accountability"])
		}
	}
}

// TestBuildSnapshot_StripsMetaIDFromCollectionsAndRelations — same logic as
// the per-field stripMetaID path, but applied to the bulk-snapshot collections
// and relations slices. Without this, the target's directus_collections /
// directus_relations rows can land with foreign surrogate IDs.
func TestBuildSnapshot_StripsMetaIDFromCollectionsAndRelations(t *testing.T) {
	bundle := SchemaBundle{
		Collections: []CollectionInfo{
			{Collection: "posts", Schema: json.RawMessage(`{"name":"posts"}`),
				Meta: json.RawMessage(`{"id":7,"icon":"article","note":"hi"}`)},
		},
		Fields: []FieldInfo{
			{Collection: "posts", Field: "id", Type: "integer",
				Schema: json.RawMessage(`{"is_primary_key":true}`),
				Meta:   json.RawMessage(`{"id":99,"sort":1}`)},
		},
		Relations: []RelationInfo{
			{Collection: "posts", Field: "author", RelatedCollection: "users",
				Schema: json.RawMessage(`{}`),
				Meta:   json.RawMessage(`{"id":42,"one_field":"author_posts"}`)},
		},
	}
	snap := buildSnapshot(snapshotMeta{Version: 1, Directus: "11.17.0", Vendor: "postgres"}, bundle)

	// Collections meta.id stripped, other keys preserved.
	c := snap["collections"].([]map[string]any)[0]
	cMeta := c["meta"].(json.RawMessage)
	var cm map[string]json.RawMessage
	json.Unmarshal(cMeta, &cm)
	if _, has := cm["id"]; has {
		t.Errorf("collection meta.id leaked: %s", cMeta)
	}
	if _, has := cm["icon"]; !has {
		t.Errorf("collection meta.icon dropped: %s", cMeta)
	}

	// Fields meta.id stripped, sort preserved.
	var f FieldInfo
	json.Unmarshal(snap["fields"].([]json.RawMessage)[0], &f)
	var fm map[string]json.RawMessage
	json.Unmarshal(f.Meta, &fm)
	if _, has := fm["id"]; has {
		t.Errorf("field meta.id leaked: %s", f.Meta)
	}
	if _, has := fm["sort"]; !has {
		t.Errorf("field meta.sort dropped")
	}

	// Relations meta.id stripped.
	var r RelationInfo
	json.Unmarshal(snap["relations"].([]json.RawMessage)[0], &r)
	var rm map[string]json.RawMessage
	json.Unmarshal(r.Meta, &rm)
	if _, has := rm["id"]; has {
		t.Errorf("relation meta.id leaked: %s", r.Meta)
	}
	if _, has := rm["one_field"]; !has {
		t.Errorf("relation meta.one_field dropped")
	}
	if r.Collection != "posts" || r.Field != "author" || r.RelatedCollection != "users" {
		t.Errorf("relation core fields lost: %+v", r)
	}
}

// TestBuildSnapshot_EmitsCustomFieldsOnSystemCollections — when the bundle
// carries a custom field on directus_users (e.g. an M2M alias), the snapshot
// must include the field WITHOUT inventing a directus_users collection
// entry. Directus already owns the system collection and will reject any
// attempt to recreate it.
func TestBuildSnapshot_EmitsCustomFieldsOnSystemCollections(t *testing.T) {
	bundle := SchemaBundle{
		Collections: []CollectionInfo{
			// Only user collections — fetchCollections strips directus_*.
			{Collection: "features_whitelist", Schema: json.RawMessage(`{"name":"features_whitelist"}`), Meta: json.RawMessage(`{}`)},
		},
		Fields: []FieldInfo{
			{Collection: "features_whitelist", Field: "id", Type: "integer",
				Schema: json.RawMessage(`{"is_primary_key":true}`), Meta: json.RawMessage(`{}`)},
			{Collection: "directus_users", Field: "game_accounts", Type: "alias",
				Schema: json.RawMessage(`null`),
				Meta:   json.RawMessage(`{"interface":"list-m2m","special":["m2m"]}`)},
		},
		Relations: []RelationInfo{
			{Collection: "directus_users", Field: "game_accounts", RelatedCollection: "features_whitelist",
				Schema: json.RawMessage(`null`), Meta: json.RawMessage(`{}`)},
		},
	}
	snap := buildSnapshot(snapshotMeta{Version: 1, Directus: "11.17.0", Vendor: "postgres"}, bundle)

	// Snapshot must NOT contain a directus_users collection entry — Directus
	// owns that table and the diff endpoint would reject creation.
	for _, c := range snap["collections"].([]map[string]any) {
		if c["collection"] == "directus_users" {
			t.Errorf("snapshot must not include system collection directus_users")
		}
	}

	// But the custom field on directus_users must be present.
	var seen bool
	for _, raw := range snap["fields"].([]json.RawMessage) {
		var f FieldInfo
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("field unmarshal: %v", err)
		}
		if f.Collection == "directus_users" && f.Field == "game_accounts" {
			seen = true
			if f.Type != "alias" {
				t.Errorf("type = %s, want alias", f.Type)
			}
		}
	}
	if !seen {
		t.Errorf("custom field directus_users.game_accounts missing from snapshot")
	}

	// Relation must be preserved as-is.
	if len(snap["relations"].([]json.RawMessage)) != 1 {
		t.Errorf("expected 1 relation, got %d", len(snap["relations"].([]json.RawMessage)))
	}
}

func TestBuildSnapshot_RoundTripSize(t *testing.T) {
	colls := make([]CollectionInfo, 100)
	for i := range colls {
		colls[i] = CollectionInfo{
			Collection: fmt.Sprintf("c%d", i),
			Schema:     json.RawMessage(`{"name":"c"}`),
			Meta:       json.RawMessage(`{}`),
		}
	}
	fields := make([]FieldInfo, 500)
	for i := range fields {
		fields[i] = FieldInfo{
			Collection: fmt.Sprintf("c%d", i%100),
			Field:      fmt.Sprintf("f%d", i),
			Type:       "string",
			Schema:     json.RawMessage(`{"name":"f"}`),
			Meta:       json.RawMessage(`{}`),
		}
	}
	snap := buildSnapshot(snapshotMeta{Version: 1, Directus: "11.17.0", Vendor: "postgres"},
		SchemaBundle{Collections: colls, Fields: fields})
	if len(snap["collections"].([]map[string]any)) != 100 {
		t.Errorf("collections count mismatch")
	}
	if len(snap["fields"].([]json.RawMessage)) != 500 {
		t.Errorf("fields count mismatch")
	}
}
