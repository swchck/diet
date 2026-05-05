package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeDirectus stands up an httptest server that serves the minimum surface
// runSimpleExport needs: collections, fields, relations, items, server info,
// and (optionally) every system endpoint we touch under --system.
//
// The handler is keyed off URL path prefix so adding new endpoints is cheap
// — tests only need to override what they care about.
func fakeDirectus(t *testing.T, withSystem bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/collections":
			w.Write([]byte(`{"data":[
				{"collection":"posts","schema":{"name":"posts"},"meta":{"icon":"article"}},
				{"collection":"directus_users","schema":{"name":"directus_users"},"meta":{"system":true}}
			]}`))
		case strings.HasPrefix(r.URL.Path, "/fields/posts"):
			w.Write([]byte(`{"data":[
				{"collection":"posts","field":"id","type":"integer","schema":{"is_primary_key":true},"meta":{"id":1,"sort":1}},
				{"collection":"posts","field":"title","type":"string","schema":{},"meta":{"id":2,"sort":2}}
			]}`))
		case r.URL.Path == "/relations":
			w.Write([]byte(`{"data":[]}`))
		case r.URL.Path == "/server/info":
			w.Write([]byte(`{"data":{"version":"11.17.0"}}`))
		case strings.HasPrefix(r.URL.Path, "/items/posts"):
			// aggregate count vs paginated items — both go to /items/posts.
			if strings.Contains(r.URL.RawQuery, "aggregate") {
				w.Write([]byte(`{"data":[{"count":1}]}`))
				return
			}
			w.Write([]byte(`{"data":[{"id":1,"title":"hello"}]}`))
		// System endpoints (only hit when withSystem == true).
		case r.URL.Path == "/flows":
			if !withSystem {
				t.Errorf("/flows should not be hit without --system")
			}
			w.Write([]byte(`{"data":[{"id":"f1","name":"Flow"}]}`))
		case r.URL.Path == "/dashboards", r.URL.Path == "/roles", r.URL.Path == "/users",
			r.URL.Path == "/translations", r.URL.Path == "/webhooks",
			r.URL.Path == "/operations", r.URL.Path == "/panels", r.URL.Path == "/presets":
			if !withSystem {
				t.Errorf("%s should not be hit without --system", r.URL.Path)
			}
			w.Write([]byte(`{"data":[]}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
		}
	}))
}

// TestRunSimpleExport_DefaultExcludesSystem — historical default behavior
// of `--all` / `--plain` without --system: archive contains user data only,
// and no /flows /dashboards etc. requests are made.
func TestRunSimpleExport_DefaultExcludesSystem(t *testing.T) {
	srv := fakeDirectus(t, false)
	defer srv.Close()

	tmp := t.TempDir()
	out := filepath.Join(tmp, "test.tar.zst")

	c := newClient(srv.URL, "tok")
	if err := runSimpleExport(c, srv.URL, out, "zstd", true /*all*/, false /*system*/); err != nil {
		t.Fatalf("runSimpleExport: %v", err)
	}

	manifest, _, _, sysData, err := extractArchive(out)
	if err != nil {
		t.Fatalf("extractArchive: %v", err)
	}
	if len(sysData) != 0 {
		t.Errorf("system data should be empty without --system, got %d types", len(sysData))
	}
	if len(manifest.SystemEntities) != 0 {
		t.Errorf("manifest.SystemEntities should be empty, got %v", manifest.SystemEntities)
	}
	if len(manifest.Collections) != 1 || manifest.Collections[0] != "posts" {
		t.Errorf("manifest.Collections = %v, want [posts]", manifest.Collections)
	}
}

// TestRunSimpleExport_WithSystem — flips the flag, archive must now carry the
// system entity types and the manifest reflects the new section.
func TestRunSimpleExport_WithSystem(t *testing.T) {
	srv := fakeDirectus(t, true)
	defer srv.Close()

	tmp := t.TempDir()
	out := filepath.Join(tmp, "test.tar.zst")

	c := newClient(srv.URL, "tok")
	if err := runSimpleExport(c, srv.URL, out, "zstd", true, true); err != nil {
		t.Fatalf("runSimpleExport: %v", err)
	}

	manifest, _, _, sysData, err := extractArchive(out)
	if err != nil {
		t.Fatalf("extractArchive: %v", err)
	}
	if _, ok := sysData["flows"]; !ok {
		t.Errorf("flows missing from archive system data")
	}
	if len(sysData["flows"]) != 1 {
		t.Errorf("expected 1 flow, got %d", len(sysData["flows"]))
	}
	// Empty endpoints (dashboards/roles/users/translations/webhooks/ops/panels/presets)
	// produced no items — the helper omits them from the result map.
	if _, ok := sysData["dashboards"]; ok {
		t.Errorf("empty dashboards key should not be in sysData")
	}
	// Manifest should list the entity types we actually pulled.
	if len(manifest.SystemEntities) == 0 {
		t.Errorf("manifest.SystemEntities empty despite system data present")
	}
	hasFlows := false
	for _, n := range manifest.SystemEntities {
		if n == "flows" {
			hasFlows = true
		}
	}
	if !hasFlows {
		t.Errorf("manifest.SystemEntities = %v, missing 'flows'", manifest.SystemEntities)
	}
}

// fakeDirectusFiltered serves the same surface as fakeDirectus plus a
// `tags` collection alongside `posts`. Used to verify --collections
// filtering: only the collections explicitly named should be fetched.
func fakeDirectusFiltered(t *testing.T, expectFetched map[string]bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/collections":
			w.Write([]byte(`{"data":[
				{"collection":"posts","schema":{"name":"posts"},"meta":{}},
				{"collection":"tags","schema":{"name":"tags"},"meta":{}},
				{"collection":"unrelated","schema":{"name":"unrelated"},"meta":{}}
			]}`))
		case strings.HasPrefix(r.URL.Path, "/fields/"):
			col := strings.TrimPrefix(r.URL.Path, "/fields/")
			if !expectFetched[col] {
				t.Errorf("/fields/%s should not be fetched under filter", col)
			}
			w.Write([]byte(fmt.Sprintf(`{"data":[
				{"collection":"%s","field":"id","type":"integer","schema":{"is_primary_key":true},"meta":{}}
			]}`, col)))
		case r.URL.Path == "/relations":
			w.Write([]byte(`{"data":[]}`))
		case r.URL.Path == "/server/info":
			w.Write([]byte(`{"data":{"version":"11.17.0"}}`))
		case strings.HasPrefix(r.URL.Path, "/items/"):
			col := strings.TrimPrefix(r.URL.Path, "/items/")
			// /items/<col>?aggregate or paginated
			if strings.Contains(r.URL.RawQuery, "aggregate") {
				w.Write([]byte(`{"data":[{"count":0}]}`))
				return
			}
			if !expectFetched[col] {
				t.Errorf("/items/%s should not be fetched under filter", col)
			}
			w.Write([]byte(`{"data":[]}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
		}
	}))
}

// TestRunFilteredExport_OnlyTouchesNamedCollections — `--collections=posts`
// should hit /fields/posts and /items/posts but never /fields/tags or
// /items/tags. The httptest handler trips the test if any unexpected
// fetch lands.
func TestRunFilteredExport_OnlyTouchesNamedCollections(t *testing.T) {
	expect := map[string]bool{"posts": true}
	srv := fakeDirectusFiltered(t, expect)
	defer srv.Close()

	tmp := t.TempDir()
	out := filepath.Join(tmp, "filtered.tar.zst")

	c := newClient(srv.URL, "tok")
	if err := runFilteredExport(c, srv.URL, out, "zstd", false, []string{"posts"}); err != nil {
		t.Fatalf("runFilteredExport: %v", err)
	}

	manifest, _, _, _, err := extractArchive(out)
	if err != nil {
		t.Fatalf("extractArchive: %v", err)
	}
	if len(manifest.Collections) != 1 || manifest.Collections[0] != "posts" {
		t.Errorf("manifest.Collections = %v, want [posts]", manifest.Collections)
	}
}

// TestRunFilteredExport_ErrorsOnMissingName — typo / stale name. Must
// fail loudly before producing an archive — silent empty exports are
// the worst outcome.
func TestRunFilteredExport_ErrorsOnMissingName(t *testing.T) {
	srv := fakeDirectusFiltered(t, map[string]bool{}) // never expect any fetch
	defer srv.Close()

	tmp := t.TempDir()
	out := filepath.Join(tmp, "should-not-exist.tar.zst")

	c := newClient(srv.URL, "tok")
	err := runFilteredExport(c, srv.URL, out, "zstd", false, []string{"made_up_name"})
	if err == nil {
		t.Fatal("expected error for missing collection")
	}
	if !strings.Contains(err.Error(), "made_up_name") {
		t.Errorf("err should mention the missing name: %v", err)
	}
	// Verify no archive was written.
	if _, statErr := os.Stat(out); statErr == nil {
		t.Errorf("archive was created despite error: %s", out)
	}
}

// TestRunSimpleExport_ManifestRoundTrip — sanity that the on-disk archive can
// be read back and contains exactly the data we wrote.
func TestRunSimpleExport_ManifestRoundTrip(t *testing.T) {
	srv := fakeDirectus(t, false)
	defer srv.Close()

	tmp := t.TempDir()
	out := filepath.Join(tmp, "rt.tar.zst")

	c := newClient(srv.URL, "tok")
	if err := runSimpleExport(c, srv.URL, out, "zstd", true, false); err != nil {
		t.Fatalf("runSimpleExport: %v", err)
	}

	st, err := os.Stat(out)
	if err != nil {
		t.Fatalf("archive not created: %v", err)
	}
	if st.Size() == 0 {
		t.Fatalf("archive is empty")
	}

	manifest, schema, data, _, err := extractArchive(out)
	if err != nil {
		t.Fatalf("extractArchive: %v", err)
	}
	if manifest.DirectusVersion != "11.17.0" {
		t.Errorf("DirectusVersion = %q, want 11.17.0", manifest.DirectusVersion)
	}
	if manifest.ItemCounts["posts"] != 1 {
		t.Errorf("ItemCounts[posts] = %d, want 1", manifest.ItemCounts["posts"])
	}
	if len(schema.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(schema.Fields))
	}
	if len(data["posts"]) != 1 {
		t.Errorf("expected 1 post, got %d", len(data["posts"]))
	}
	var item map[string]json.RawMessage
	json.Unmarshal(data["posts"][0], &item)
	if string(item["title"]) != `"hello"` {
		t.Errorf("post title = %s, want \"hello\"", item["title"])
	}
}
