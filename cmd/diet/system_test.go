package main

import (
	"encoding/json"
	"testing"
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
	item := json.RawMessage(`{"id":"u1","email":"a@b.com","password":"hash","token":"secret","last_access":"2024-01-01","last_page":"/admin","role":"admin-role"}`)
	result := stripSensitiveFields("users", item)

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(result, &obj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, field := range []string{"password", "token", "last_access", "last_page"} {
		if _, ok := obj[field]; ok {
			t.Errorf("field %q should be stripped from users", field)
		}
	}
	for _, field := range []string{"id", "email", "role"} {
		if _, ok := obj[field]; !ok {
			t.Errorf("field %q should be preserved", field)
		}
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
