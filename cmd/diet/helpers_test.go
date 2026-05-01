package main

import (
	"encoding/json"
	"testing"
)

func TestTruncate(t *testing.T) {
	tests := []struct {
		input string
		n     int
		want  string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "he..."},
		{"", 5, ""},
		{"abc", 3, "abc"},
		{"abcd", 3, "abc"},   // n<=3: no ellipsis (would exceed n)
		{"abcdefgh", 6, "abc..."},
		{"привет мир", 6, "при..."}, // 6 runes total: "при" + "..."
	}
	for _, tt := range tests {
		got := truncate(tt.input, tt.n)
		if got != tt.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.n, got, tt.want)
		}
	}
}

func TestStripDataFields(t *testing.T) {
	raw := json.RawMessage(`{"id":1,"name":"test","user_created":"abc","user_updated":"def","alias_field":"x"}`)
	aliases := map[string]bool{"alias_field": true}

	result := stripDataFields(raw, aliases)

	var obj map[string]json.RawMessage
	if err := json.Unmarshal(result, &obj); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	if _, ok := obj["user_created"]; ok {
		t.Error("user_created should be stripped")
	}
	if _, ok := obj["user_updated"]; ok {
		t.Error("user_updated should be stripped")
	}
	if _, ok := obj["alias_field"]; ok {
		t.Error("alias_field should be stripped")
	}
	if _, ok := obj["id"]; !ok {
		t.Error("id should be preserved")
	}
	if _, ok := obj["name"]; !ok {
		t.Error("name should be preserved")
	}
}

func TestFixDateTimeFields(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		field  string
		isNull bool
	}{
		{"valid date", `{"ts":"2024-01-15T10:00:00"}`, "ts", false},
		{"zero date", `{"ts":"0000-00-00T00:00:00"}`, "ts", true},
		{"zero prefix 0-", `{"ts":"0-01-01T00:00:00"}`, "ts", true},
		{"zero prefix 1-", `{"ts":"1-01-01T00:00:00"}`, "ts", true},
		{"epoch prefix", `{"ts":"0001-01-01T00:00:00"}`, "ts", true},
		{"not a date", `{"name":"hello"}`, "name", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fixDateTimeFields(json.RawMessage(tt.input))
			var obj map[string]any
			json.Unmarshal(result, &obj)
			val := obj[tt.field]
			if tt.isNull && val != nil {
				t.Errorf("expected null for field %s, got %v", tt.field, val)
			}
			if !tt.isNull && val == nil {
				t.Errorf("expected non-null for field %s", tt.field)
			}
		})
	}
}

func TestParseAggregateCount(t *testing.T) {
	tests := []struct {
		name string
		body string
		want int
	}{
		{"v10+ flat int", `{"data":[{"count":150}]}`, 150},
		{"v10+ flat string", `{"data":[{"count":"42"}]}`, 42},
		{"older nested", `{"data":[{"count":{"*":"99"}}]}`, 99},
		{"older nested int", `{"data":[{"count":{"*":77}}]}`, 77},
		{"empty data", `{"data":[]}`, 0},
		{"no count field", `{"data":[{"total":5}]}`, 0},
		{"invalid json", `not json`, 0},
		{"null body", ``, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseAggregateCount([]byte(tt.body))
			if got != tt.want {
				t.Errorf("parseAggregateCount(%s) = %d, want %d", tt.name, got, tt.want)
			}
		})
	}
}

func TestIsAlreadyExists(t *testing.T) {
	tests := []struct {
		msg  string
		want bool
	}{
		{`{"errors":[{"message":"Field \"id\" has to be unique"}]}`, true},
		{`already exists`, true},
		{`unique constraint`, true},
		{`permission denied`, false},
		{`server error`, false},
		{``, false},
	}
	for _, tt := range tests {
		got := isAlreadyExists(tt.msg)
		if got != tt.want {
			t.Errorf("isAlreadyExists(%q) = %v, want %v", truncate(tt.msg, 40), got, tt.want)
		}
	}
}

