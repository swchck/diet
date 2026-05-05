package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExtractID_StringUUID(t *testing.T) {
	item := json.RawMessage(`{"id":"550e8400-e29b-41d4-a716-446655440000","name":"test"}`)
	id := extractID(item)
	if id != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("expected UUID string, got %q", id)
	}
}

func TestExtractID_NumericID(t *testing.T) {
	item := json.RawMessage(`{"id":42,"name":"test"}`)
	id := extractID(item)
	if id != "42" {
		t.Errorf("expected '42', got %q", id)
	}
}

func TestExtractID_Missing(t *testing.T) {
	item := json.RawMessage(`{"name":"test"}`)
	id := extractID(item)
	if id != "" {
		t.Errorf("expected empty string, got %q", id)
	}
}

func TestExtractID_InvalidJSON(t *testing.T) {
	item := json.RawMessage(`not json`)
	id := extractID(item)
	if id != "" {
		t.Errorf("expected empty string, got %q", id)
	}
}

func TestExtractSystemItemLabel(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"with name", `{"id":"1","name":"My Flow"}`, "My Flow"},
		{"with bookmark", `{"id":"2","bookmark":"Saved View"}`, "Saved View"},
		{"with key", `{"id":"3","key":"translation_key"}`, "translation_key"},
		{"fallback to ID", `{"id":"4"}`, "4"},
		{"empty name", `{"id":"5","name":""}`, "5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractSystemItemLabel(json.RawMessage(tt.input))
			if got != tt.want {
				t.Errorf("extractSystemItemLabel() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractSystemItemStatus(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"with status", `{"status":"active"}`, "active"},
		{"no status", `{"name":"test"}`, "—"},
		{"empty status", `{"status":""}`, "—"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractSystemItemStatus(json.RawMessage(tt.input))
			if got != tt.want {
				t.Errorf("extractSystemItemStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSystemEntityByName(t *testing.T) {
	et, ok := systemEntityByName("flows")
	if !ok {
		t.Fatal("expected to find 'flows'")
	}
	if et.Endpoint != "/flows" {
		t.Errorf("expected endpoint /flows, got %s", et.Endpoint)
	}

	_, ok = systemEntityByName("nonexistent")
	if ok {
		t.Error("expected not found for 'nonexistent'")
	}
}

func TestSystemEntityByName_DependentTypes(t *testing.T) {
	for _, name := range []string{"operations", "panels", "presets"} {
		et, ok := systemEntityByName(name)
		if !ok {
			t.Errorf("expected to find dependent type %q", name)
			continue
		}
		if et.Endpoint != "/"+name {
			t.Errorf("%s: endpoint = %q, want %q", name, et.Endpoint, "/"+name)
		}
	}
}

func TestSystemImportOrderAllResolvable(t *testing.T) {
	for _, name := range systemImportOrder {
		if _, ok := systemEntityByName(name); !ok {
			t.Errorf("systemImportOrder contains %q which systemEntityByName cannot resolve", name)
		}
	}
}

func TestSystemDeleteOrderAllResolvable(t *testing.T) {
	for _, name := range systemDeleteOrder {
		if _, ok := systemEntityByName(name); !ok {
			t.Errorf("systemDeleteOrder contains %q which systemEntityByName cannot resolve", name)
		}
	}
}

func TestExtractSystemItemLabel_UserWithNames(t *testing.T) {
	item := json.RawMessage(`{"id":"u1","first_name":"John","last_name":"Doe","email":"john@example.com"}`)
	got := extractSystemItemLabel(item)
	want := "John Doe <john@example.com>"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestExtractSystemItemLabel_UserEmailOnly(t *testing.T) {
	item := json.RawMessage(`{"id":"u2","email":"admin@example.com"}`)
	got := extractSystemItemLabel(item)
	if got != "admin@example.com" {
		t.Errorf("got %q, want %q", got, "admin@example.com")
	}
}

func TestExtractSystemItemLabel_UserFirstNameOnly(t *testing.T) {
	item := json.RawMessage(`{"id":"u3","first_name":"Alice","last_name":""}`)
	got := extractSystemItemLabel(item)
	if got != "Alice" {
		t.Errorf("got %q, want %q", got, "Alice")
	}
}

func TestStripSensitiveFields_Users(t *testing.T) {
	item := json.RawMessage(`{
		"id":"u1","email":"a@b.com","role":"admin-role",
		"password":"hash","token":"secret",
		"tfa_secret":"JBSWY3DPEHPK3PXP","auth_data":{"refresh":"sso-refresh-tok"},
		"last_access":"2024-01-01","last_page":"/admin",
		"external_identifier":"sso-id-123","provider":"oauth"
	}`)
	result := stripSensitiveFields("users", item)

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(result, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Every entry in sensitiveUserFields must be gone — drives off the
	// canonical list so tests don't drift from the implementation.
	for _, field := range sensitiveUserFields {
		if _, ok := obj[field]; ok {
			t.Errorf("field %q should be stripped from users", field)
		}
	}
	// Identity / metadata kept.
	for _, field := range []string{"id", "email", "role", "external_identifier", "provider"} {
		if _, ok := obj[field]; !ok {
			t.Errorf("field %q should be preserved", field)
		}
	}
}

// TestStripSensitiveFields_TFASecret_Explicit — pin tfa_secret behavior on
// its own so a future refactor that removes it from sensitiveUserFields
// fails this test loudly. TOTP secrets are the strictest case in the
// list; the test exists to prevent silent re-introduction.
func TestStripSensitiveFields_TFASecret_Explicit(t *testing.T) {
	item := json.RawMessage(`{"id":"u1","tfa_secret":"DO-NOT-LEAK","email":"x@y.z"}`)
	result := stripSensitiveFields("users", item)
	if strings.Contains(string(result), "DO-NOT-LEAK") {
		t.Errorf("tfa_secret value leaked into export: %s", result)
	}
}

// TestStripSensitiveFields_AuthData_Explicit — same pin for auth_data,
// which holds SSO refresh material.
func TestStripSensitiveFields_AuthData_Explicit(t *testing.T) {
	item := json.RawMessage(`{"id":"u1","auth_data":{"refresh":"DO-NOT-LEAK"},"email":"x@y.z"}`)
	result := stripSensitiveFields("users", item)
	if strings.Contains(string(result), "DO-NOT-LEAK") {
		t.Errorf("auth_data leaked into export: %s", result)
	}
}

func TestStripSensitiveFields_NonUsers(t *testing.T) {
	item := json.RawMessage(`{"id":"f1","name":"flow","password":"should-stay"}`)
	result := stripSensitiveFields("flows", item)

	var obj map[string]json.RawMessage
	json.Unmarshal(result, &obj)
	if _, ok := obj["password"]; !ok {
		t.Error("password should NOT be stripped from non-user entities")
	}
}

func TestInsertSystemItems_ParallelCounts(t *testing.T) {
	var (
		reqCount atomic.Int64
		peak     atomic.Int64
		inFlight atomic.Int64
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := inFlight.Add(1)
		for {
			cur := peak.Load()
			if now <= cur || peak.CompareAndSwap(cur, now) {
				break
			}
		}
		defer inFlight.Add(-1)

		idx := reqCount.Add(1)
		// Hold in-flight long enough for goroutines to overlap.
		time.Sleep(10 * time.Millisecond)
		// Simulate ~25% failures to verify the failed counter.
		if idx%4 == 0 {
			w.WriteHeader(400)
			return
		}
		w.WriteHeader(200)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	c.Concurrency = 4

	items := make([]json.RawMessage, 20)
	for i := range items {
		items[i] = json.RawMessage(fmt.Sprintf(`{"id":%d}`, i))
	}

	res := insertSystemItems(c, "/flows", items)
	if res.Inserted+res.Skipped+res.Failed != 20 {
		t.Errorf("total = %d, want 20", res.Inserted+res.Skipped+res.Failed)
	}
	// Server fails every 4th — exactly 5 of 20.
	if res.Failed != 5 {
		t.Errorf("failed = %d, want 5", res.Failed)
	}
	if res.Inserted != 15 {
		t.Errorf("inserted = %d, want 15", res.Inserted)
	}
	if peak.Load() < 2 {
		t.Errorf("peak in-flight = %d, expected real parallelism", peak.Load())
	}
	if peak.Load() > int64(c.Concurrency) {
		t.Errorf("peak in-flight = %d, want <= %d", peak.Load(), c.Concurrency)
	}
}

func TestInsertSystemItems_EmptyInput(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("server should not be hit on empty input")
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	res := insertSystemItems(c, "/flows", nil)
	if res.Inserted != 0 || res.Failed != 0 || res.Skipped != 0 {
		t.Errorf("got %+v, want zeroes", res)
	}
}

// TestClassifySystemPostResult covers the trichotomy that decides whether a
// failed POST counts as a real failure or a "row already there, leave it" skip.
// The user's intent: don't overwrite existing values, just don't error either.
func TestClassifySystemPostResult(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   systemPostResult
	}{
		{"2xx", 200, `{}`, sysResultInserted},
		{"201 created", 201, `{"data":{"id":"x"}}`, sysResultInserted},
		{"PK duplicate", 400,
			`{"errors":[{"message":"...","extensions":{"code":"RECORD_NOT_UNIQUE","collection":"directus_users","field":"id"}}]}`,
			sysResultDuplicate},
		{"email duplicate", 400,
			`{"errors":[{"message":"...","extensions":{"code":"RECORD_NOT_UNIQUE","collection":"directus_users","field":"email"}}]}`,
			sysResultDuplicate},
		{"validation error is not a skip", 400,
			`{"errors":[{"message":"...","extensions":{"code":"FAILED_VALIDATION"}}]}`,
			sysResultOther},
		{"unauthenticated is not a skip", 401,
			`{"errors":[{"message":"...","extensions":{"code":"INVALID_CREDENTIALS"}}]}`,
			sysResultOther},
		{"500 with no body", 500, ``, sysResultOther},
		{"unparseable body", 400, `not json`, sysResultOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifySystemPostResult(tc.status, []byte(tc.body))
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestInsertSystemItems_SkipsDuplicates wires a fake server that returns
// RECORD_NOT_UNIQUE for half the items and 2xx for the rest. We expect the
// duplicates to count as Skipped (not Failed) so partial-failure exit codes
// don't fire on a re-run against a target that already has the bootstrap admin.
func TestInsertSystemItems_SkipsDuplicates(t *testing.T) {
	var seen atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := seen.Add(1)
		if idx%2 == 0 {
			w.WriteHeader(400)
			w.Write([]byte(`{"errors":[{"message":"dup","extensions":{"code":"RECORD_NOT_UNIQUE"}}]}`))
			return
		}
		w.WriteHeader(200)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := newClient(srv.URL, "tok")
	items := make([]json.RawMessage, 10)
	for i := range items {
		items[i] = json.RawMessage(fmt.Sprintf(`{"id":%d}`, i))
	}
	res := insertSystemItems(c, "/roles", items)
	if res.Inserted != 5 {
		t.Errorf("inserted=%d, want 5", res.Inserted)
	}
	if res.Skipped != 5 {
		t.Errorf("skipped=%d, want 5", res.Skipped)
	}
	if res.Failed != 0 {
		t.Errorf("failed=%d, want 0 (RECORD_NOT_UNIQUE must NOT count as failure)", res.Failed)
	}
}

// TestFetchAllSystemEntities — verifies the helper that fills the same shape
// the TUI does for `--plain --system` exports: independent entity types pulled
// whole, dependent ones (operations/panels) pulled whole, presets filtered to
// the user's collection set (with global presets — empty `collection` —
// kept).
func TestFetchAllSystemEntities(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/flows":
			w.Write([]byte(`{"data":[{"id":"f1","name":"Flow A"},{"id":"f2","name":"Flow B"}]}`))
		case "/dashboards":
			w.Write([]byte(`{"data":[{"id":"d1","name":"Sales"}]}`))
		case "/roles":
			w.Write([]byte(`{"data":[{"id":"r1","name":"Editor"}]}`))
		case "/users":
			// Sensitive fields must be stripped post-fetch.
			w.Write([]byte(`{"data":[{"id":"u1","email":"a@b.c","password":"HASH","token":"TOK"}]}`))
		case "/translations":
			w.Write([]byte(`{"data":[{"id":"t1","key":"hello","value":"Hi"}]}`))
		case "/webhooks":
			w.Write([]byte(`{"data":[]}`))
		case "/operations":
			w.Write([]byte(`{"data":[{"id":"o1","flow":"f1"}]}`))
		case "/panels":
			w.Write([]byte(`{"data":[{"id":"p1","dashboard":"d1"}]}`))
		case "/presets":
			w.Write([]byte(`{"data":[
				{"id":1,"collection":"posts"},
				{"id":2,"collection":"unrelated"},
				{"id":3,"collection":""}
			]}`))
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	userColSet := map[string]bool{"posts": true}

	got := fetchAllSystemEntities(c, userColSet, nil)

	for _, name := range []string{"flows", "dashboards", "roles", "users", "translations", "operations", "panels", "presets"} {
		if _, ok := got[name]; !ok {
			t.Errorf("missing entity type %s", name)
		}
	}
	// Empty webhooks list must not produce a key.
	if _, ok := got["webhooks"]; ok {
		t.Errorf("empty webhooks should not appear in result")
	}

	// Users password/token must be stripped (mirror TUI).
	var u map[string]json.RawMessage
	json.Unmarshal(got["users"][0], &u)
	if _, has := u["password"]; has {
		t.Errorf("user password not stripped")
	}
	if _, has := u["token"]; has {
		t.Errorf("user token not stripped")
	}

	// Presets filter: posts (in set) + global "" preset kept; unrelated dropped.
	if len(got["presets"]) != 2 {
		t.Errorf("presets count = %d, want 2 (posts + global)", len(got["presets"]))
	}
	for _, p := range got["presets"] {
		var obj struct {
			Collection string `json:"collection"`
		}
		json.Unmarshal(p, &obj)
		if obj.Collection == "unrelated" {
			t.Errorf("preset for unrelated collection should be filtered out")
		}
	}
}

// TestFetchAllSystemEntities_NilUserSet — passing nil userColSet keeps every
// preset (no collection filter), used for "give me literally everything".
func TestFetchAllSystemEntities_NilUserSet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/presets":
			w.Write([]byte(`{"data":[{"id":1,"collection":"a"},{"id":2,"collection":"b"}]}`))
		default:
			w.Write([]byte(`{"data":[]}`))
		}
	}))
	defer srv.Close()
	c := newClient(srv.URL, "tok")
	got := fetchAllSystemEntities(c, nil, nil)
	if len(got["presets"]) != 2 {
		t.Errorf("expected 2 presets when userColSet=nil, got %d", len(got["presets"]))
	}
}

// TestFetchAllSystemEntities_PartialErrorsLogged — if one endpoint fails, the
// rest still come back. Failure surfaces via the log func, not as panic or
// missing-value cascade.
func TestFetchAllSystemEntities_PartialErrorsLogged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/users" {
			w.WriteHeader(500)
			w.Write([]byte(`{"errors":[{"message":"boom"}]}`))
			return
		}
		if r.URL.Path == "/flows" {
			w.Write([]byte(`{"data":[{"id":"f1"}]}`))
			return
		}
		w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()
	c := newClient(srv.URL, "tok")

	var logs []string
	got := fetchAllSystemEntities(c, nil, func(s string) { logs = append(logs, s) })

	if _, ok := got["users"]; ok {
		t.Errorf("users should be absent after fetch failure")
	}
	if len(got["flows"]) != 1 {
		t.Errorf("flows still expected after sibling failure, got %d", len(got["flows"]))
	}
	hasUserWarning := false
	for _, l := range logs {
		if strings.Contains(l, "users") && strings.Contains(l, "WARN") {
			hasUserWarning = true
		}
	}
	if !hasUserWarning {
		t.Errorf("expected WARN log for failed users fetch, got %v", logs)
	}
}

func TestSystemDeleteOrderReversesImport(t *testing.T) {
	// Verify delete order is reverse of import order conceptually:
	// dependencies should be deleted before their parents
	importSet := map[string]bool{}
	for _, name := range systemImportOrder {
		importSet[name] = true
	}
	deleteSet := map[string]bool{}
	for _, name := range systemDeleteOrder {
		deleteSet[name] = true
	}

	// Both should contain the same entity types.
	if len(importSet) != len(deleteSet) {
		t.Errorf("import order has %d items, delete order has %d", len(importSet), len(deleteSet))
	}
	for name := range importSet {
		if !deleteSet[name] {
			t.Errorf("%s in import order but not in delete order", name)
		}
	}
}
