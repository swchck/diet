package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// TestCreateFields_Parallel verifies that:
//   - PK fields are skipped (not POSTed),
//   - non-PK fields are all POSTed exactly once,
//   - the reorder PATCH pass runs only for fields with explicit meta.sort,
//   - the call respects client.Concurrency.
func TestCreateFields_Parallel(t *testing.T) {
	var (
		postCount atomic.Int64
		patchCount atomic.Int64
		inFlight   atomic.Int64
		peak       atomic.Int64
		mu         sync.Mutex
		postedFields = map[string]bool{}
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

		switch r.Method {
		case "POST":
			postCount.Add(1)
			// /fields/<col>
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			if len(parts) != 2 || parts[0] != "fields" {
				t.Errorf("unexpected POST path: %s", r.URL.Path)
			}
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			fname, _ := body["field"].(string)
			mu.Lock()
			postedFields[parts[1]+"."+fname] = true
			mu.Unlock()
			w.WriteHeader(200)
			w.Write([]byte(`{}`))
		case "PATCH":
			patchCount.Add(1)
			w.WriteHeader(200)
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	c.Concurrency = 4

	fields := []FieldInfo{
		// PK on posts — must be skipped.
		{Collection: "posts", Field: "id", Type: "integer",
			Schema: json.RawMessage(`{"is_primary_key":true}`),
			Meta:   json.RawMessage(`{"sort":1}`)},
		{Collection: "posts", Field: "title", Type: "string",
			Schema: json.RawMessage(`{}`),
			Meta:   json.RawMessage(`{"sort":2}`)},
		{Collection: "posts", Field: "tags", Type: "alias",
			Meta: json.RawMessage(`{"sort":3}`)},
		// Field without explicit sort — should NOT be PATCHed.
		{Collection: "users", Field: "email", Type: "string",
			Schema: json.RawMessage(`{}`),
			Meta:   json.RawMessage(`{}`)},
		{Collection: "users", Field: "name", Type: "string",
			Schema: json.RawMessage(`{}`),
			Meta:   json.RawMessage(`{"sort":1}`)},
	}

	if err := createFields(c, fields, func(string) {}); err != nil {
		t.Fatalf("createFields: %v", err)
	}

	if postCount.Load() != 4 {
		t.Errorf("POST count = %d, want 4 (PK skipped)", postCount.Load())
	}
	if !postedFields["posts.title"] || !postedFields["posts.tags"] ||
		!postedFields["users.email"] || !postedFields["users.name"] {
		t.Errorf("missing expected fields: %v", postedFields)
	}
	if postedFields["posts.id"] {
		t.Error("PK field posts.id should be skipped")
	}
	if patchCount.Load() != 3 {
		t.Errorf("PATCH count = %d, want 3 (only fields with explicit sort)", patchCount.Load())
	}
	if peak.Load() < 2 {
		t.Errorf("peak in-flight = %d, expected real parallelism", peak.Load())
	}
	if peak.Load() > int64(c.Concurrency) {
		t.Errorf("peak in-flight = %d, want <= %d", peak.Load(), c.Concurrency)
	}
}

func TestCreateFields_TransportErrorReturns(t *testing.T) {
	// Server immediately closes connection — every POST returns a transport
	// error. createFields should surface the first one.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("hijack not supported")
		}
		conn, _, _ := hj.Hijack()
		conn.Close()
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	c.Concurrency = 2

	fields := []FieldInfo{
		{Collection: "x", Field: "a", Type: "string", Schema: json.RawMessage(`{}`), Meta: json.RawMessage(`{}`)},
		{Collection: "x", Field: "b", Type: "string", Schema: json.RawMessage(`{}`), Meta: json.RawMessage(`{}`)},
	}
	err := createFields(c, fields, func(string) {})
	if err == nil {
		t.Fatal("expected error on transport failure")
	}
	if !strings.Contains(err.Error(), "create field") {
		t.Errorf("err = %v, want it to mention 'create field'", err)
	}
}

func TestCreateRelations_Parallel(t *testing.T) {
	var (
		postCount atomic.Int64
		peak      atomic.Int64
		inFlight  atomic.Int64
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

		if r.Method != "POST" || r.URL.Path != "/relations" {
			t.Errorf("unexpected req: %s %s", r.Method, r.URL.Path)
		}
		postCount.Add(1)
		// Hold in-flight long enough for goroutines to overlap.
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(200)
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c := newClient(srv.URL, "tok")
	c.Concurrency = 3

	relations := make([]RelationInfo, 12)
	for i := range relations {
		relations[i] = RelationInfo{
			Collection:        "posts",
			Field:             fmt.Sprintf("rel_%d", i),
			RelatedCollection: "users",
			Schema:            json.RawMessage(`{}`),
			Meta:              json.RawMessage(`{}`),
		}
	}

	if err := createRelations(c, relations, func(string) {}); err != nil {
		t.Fatalf("createRelations: %v", err)
	}
	if postCount.Load() != 12 {
		t.Errorf("POST count = %d, want 12", postCount.Load())
	}
	if peak.Load() < 2 {
		t.Errorf("peak in-flight = %d, expected real parallelism", peak.Load())
	}
	if peak.Load() > int64(c.Concurrency) {
		t.Errorf("peak in-flight = %d, want <= %d", peak.Load(), c.Concurrency)
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
