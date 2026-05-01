package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// truncate clamps a string to at most n visible runes, with an ellipsis
// substituted for the trailing characters when needed. Returned width is
// always ≤ n, so callers can pair this with lipgloss.Width(n) without
// accidental line wrapping.
func truncate(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	if n <= 3 {
		return string(runes[:n])
	}
	return string(runes[:n-3]) + "..."
}

// stripDataFields removes virtual alias fields and system user FKs from an item payload.
func stripDataFields(raw json.RawMessage, aliasFields map[string]bool) json.RawMessage {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	delete(obj, "user_created")
	delete(obj, "user_updated")
	for f := range aliasFields {
		delete(obj, f)
	}
	out, _ := json.Marshal(obj)
	return out
}

// fixDateTimeFields replaces sentinel/zero datetime values with null.
// Some Directus instances backed by older MySQL emit "0000-00-00T00:00:00"
// or year-zero strings that PostgreSQL and stricter MySQL configs reject on
// import. Coercing to null is safe — Directus treats both the same.
func fixDateTimeFields(raw json.RawMessage) json.RawMessage {
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return raw
	}
	changed := false
	for k, v := range obj {
		s, ok := v.(string)
		if !ok || len(s) < 5 || !strings.Contains(s, "T") {
			continue
		}
		if strings.HasPrefix(s, "0-") || strings.HasPrefix(s, "1-") ||
			strings.HasPrefix(s, "0001-") || s == "0000-00-00T00:00:00" {
			obj[k] = nil
			changed = true
		}
	}
	if !changed {
		return raw
	}
	out, _ := json.Marshal(obj)
	return out
}

// parseAggregateCount extracts an integer count from a Directus aggregate response.
// Handles multiple formats across Directus versions:
//   - v10+: {"data":[{"count":"150"}]}  or {"data":[{"count":150}]}
//   - older: {"data":[{"count":{"*":"150"}}]}
func parseAggregateCount(body []byte) int {
	var resp struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &resp) != nil || len(resp.Data) == 0 {
		return 0
	}
	raw, ok := resp.Data[0]["count"]
	if !ok {
		return 0
	}
	var n int
	if json.Unmarshal(raw, &n) == nil {
		return n
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		fmt.Sscanf(s, "%d", &n)
		return n
	}
	var nested map[string]json.RawMessage
	if json.Unmarshal(raw, &nested) == nil {
		if star, ok := nested["*"]; ok {
			if json.Unmarshal(star, &n) == nil {
				return n
			}
			if json.Unmarshal(star, &s) == nil {
				fmt.Sscanf(s, "%d", &n)
				return n
			}
		}
	}
	return 0
}

// isAlreadyExists matches "row already exists" responses across DB backends:
//   - "unique"           — Postgres / MySQL UNIQUE violation
//   - "already exists"   — Directus's own duplicate-key wrapper
//   - "has to be unique" — Directus validation layer (different code path)
//
// Treating any of these as success lets retry passes resume after a partial
// batch insert without double-counting.
func isAlreadyExists(errMsg string) bool {
	return strings.Contains(errMsg, "unique") || strings.Contains(errMsg, "already exists") ||
		strings.Contains(errMsg, "has to be unique")
}

