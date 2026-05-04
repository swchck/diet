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
