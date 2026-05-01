package main

import (
	"encoding/json"
	"testing"
)

func TestPadToHeight_Shorter(t *testing.T) {
	result := padToHeight("line1\nline2", 5)
	lines := countNewlines(result) + 1
	if lines != 5 {
		t.Errorf("got %d lines, want 5", lines)
	}
}

func TestPadToHeight_Taller(t *testing.T) {
	result := padToHeight("a\nb\nc\nd\ne\nf", 3)
	lines := countNewlines(result) + 1
	if lines != 3 {
		t.Errorf("got %d lines, want 3 (truncated)", lines)
	}
}

func TestPadToHeight_Exact(t *testing.T) {
	result := padToHeight("a\nb\nc", 3)
	lines := countNewlines(result) + 1
	if lines != 3 {
		t.Errorf("got %d lines, want 3", lines)
	}
}

func TestSafeUnmarshal_NullInput(t *testing.T) {
	var out struct{ X int }
	err := safeUnmarshal([]byte("null"), &out)
	if err != nil {
		t.Errorf("expected no error for null, got %v", err)
	}
}

func TestSafeUnmarshal_EmptyInput(t *testing.T) {
	var out struct{ X int }
	err := safeUnmarshal(nil, &out)
	if err != nil {
		t.Errorf("expected no error for nil, got %v", err)
	}
}

func TestSafeUnmarshal_ValidJSON(t *testing.T) {
	var out struct{ X int `json:"x"` }
	err := safeUnmarshal([]byte(`{"x":42}`), &out)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.X != 42 {
		t.Errorf("X = %d, want 42", out.X)
	}
}

func TestSafeUnmarshal_InvalidJSON(t *testing.T) {
	var out struct{ X int }
	err := safeUnmarshal([]byte(`not json`), &out)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestFieldSort(t *testing.T) {
	tests := []struct {
		name   string
		meta   string
		want   int
		wantOK bool
	}{
		{"with sort", `{"sort":5}`, 5, true},
		{"sort zero", `{"sort":0}`, 0, true},
		{"null sort", `{"sort":null}`, 0, false},
		{"no sort", `{}`, 0, false},
		{"null meta", `null`, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := FieldInfo{Meta: json.RawMessage(tt.meta)}
			got, ok := fieldSort(f)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("fieldSort() = (%d, %v), want (%d, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestSumCounts(t *testing.T) {
	counts := map[string]int{"a": 10, "b": 20, "c": 5}
	got := sumCounts(counts)
	if got != 35 {
		t.Errorf("sumCounts = %d, want 35", got)
	}
}

func TestSumCounts_Empty(t *testing.T) {
	got := sumCounts(map[string]int{})
	if got != 0 {
		t.Errorf("sumCounts(empty) = %d, want 0", got)
	}
}

func countNewlines(s string) int {
	n := 0
	for _, c := range s {
		if c == '\n' {
			n++
		}
	}
	return n
}
