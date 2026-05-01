package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNewClientDefaults(t *testing.T) {
	c := newClient("http://localhost:8055", "test-token")
	if c.Concurrency != 6 {
		t.Errorf("Concurrency = %d, want 6", c.Concurrency)
	}
	if c.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", c.BatchSize)
	}
	if c.RetryPasses != 5 {
		t.Errorf("RetryPasses = %d, want 5", c.RetryPasses)
	}
	if c.http == nil {
		t.Error("http client is nil")
	}
}

func TestNewClientWithOptions(t *testing.T) {
	c := newClientWithOptions("http://localhost", "tok", clientOptions{
		Timeout:     30,
		Concurrency: 12,
		BatchSize:   50,
		RetryPasses: 3,
	})
	if c.Concurrency != 12 {
		t.Errorf("Concurrency = %d, want 12", c.Concurrency)
	}
	if c.BatchSize != 50 {
		t.Errorf("BatchSize = %d, want 50", c.BatchSize)
	}
	if c.RetryPasses != 3 {
		t.Errorf("RetryPasses = %d, want 3", c.RetryPasses)
	}
}

func TestNewClientWithOptions_ZeroFallsToDefaults(t *testing.T) {
	c := newClientWithOptions("http://localhost", "tok", clientOptions{})
	if c.Concurrency != 6 {
		t.Errorf("Concurrency = %d, want default 6", c.Concurrency)
	}
	if c.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want default 100", c.BatchSize)
	}
}

func TestClientGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing auth header")
		}
		if r.URL.Path != "/items/test" {
			t.Errorf("path = %s, want /items/test", r.URL.Path)
		}
		w.Write([]byte(`{"data":[{"id":1}]}`))
	}))
	defer srv.Close()

	c := newClient(srv.URL, "test-token")
	body, err := c.get("/items/test")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(body) != `{"data":[{"id":1}]}` {
		t.Errorf("body = %q", string(body))
	}
}

func TestClientGet_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		w.Write([]byte(`{"errors":[{"message":"forbidden"}]}`))
	}))
	defer srv.Close()

	c := newClient(srv.URL, "bad-token")
	_, err := c.get("/items/test")
	if err == nil {
		t.Fatal("expected error for 403 response")
	}
}

func TestClientPost(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("missing content-type")
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"data":{"id":1}}`))
	}))
	defer srv.Close()

	c := newClient(srv.URL, "test-token")
	body, status, err := c.post("/items/test", []byte(`{"name":"hello"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
	if string(body) != `{"data":{"id":1}}` {
		t.Errorf("body = %q", string(body))
	}
}

func TestClientPatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PATCH" {
			t.Errorf("method = %s, want PATCH", r.Method)
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	c := newClient(srv.URL, "test-token")
	err := c.patch("/collections/test", map[string]any{"meta": map[string]any{"sort": 1}})
	if err != nil {
		t.Errorf("patch: %v", err)
	}
}

func TestClientPatch_Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Write([]byte(`not found`))
	}))
	defer srv.Close()

	c := newClient(srv.URL, "test-token")
	err := c.patch("/collections/nonexistent", map[string]any{})
	if err == nil {
		t.Error("expected error for 404")
	}
}

func TestClientDel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			t.Errorf("method = %s, want DELETE", r.Method)
		}
		w.WriteHeader(204)
	}))
	defer srv.Close()

	c := newClient(srv.URL, "test-token")
	status, err := c.del("/collections/test")
	if err != nil {
		t.Fatalf("del: %v", err)
	}
	if status != 204 {
		t.Errorf("status = %d, want 204", status)
	}
}

func TestPullAllItems(t *testing.T) {
	page := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		if page == 1 {
			// Return full page (limit=500).
			items := make([]json.RawMessage, 500)
			for i := range items {
				items[i] = json.RawMessage(fmt.Sprintf(`{"id":%d}`, i))
			}
			data, _ := json.Marshal(map[string]any{"data": items})
			w.Write(data)
		} else {
			// Return partial page (end of data).
			w.Write([]byte(`{"data":[{"id":500},{"id":501}]}`))
		}
	}))
	defer srv.Close()

	c := newClient(srv.URL, "test-token")
	items, err := c.pullAllItems("test")
	if err != nil {
		t.Fatalf("pullAllItems: %v", err)
	}
	if len(items) != 502 {
		t.Errorf("got %d items, want 502", len(items))
	}
}

func TestPullAllItems_Empty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":[]}`))
	}))
	defer srv.Close()

	c := newClient(srv.URL, "test-token")
	items, err := c.pullAllItems("empty")
	if err != nil {
		t.Fatalf("pullAllItems: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("got %d items, want 0", len(items))
	}
}

func TestPullAllItems_UnmarshalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`not json at all`))
	}))
	defer srv.Close()

	c := newClient(srv.URL, "test-token")
	_, err := c.pullAllItems("bad")
	if err == nil {
		t.Error("expected error for invalid JSON response")
	}
}
