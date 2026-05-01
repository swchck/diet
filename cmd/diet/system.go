package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// System entity types (flows, dashboards, etc.)

// SystemEntityType describes a Directus system resource.
type SystemEntityType struct {
	Name     string
	Endpoint string
}

// Independent system entity types shown as tabs.
var systemEntityTypes = []SystemEntityType{
	{Name: "flows", Endpoint: "/flows"},
	{Name: "dashboards", Endpoint: "/dashboards"},
	{Name: "roles", Endpoint: "/roles"},
	{Name: "users", Endpoint: "/users"},
	{Name: "translations", Endpoint: "/translations"},
	{Name: "webhooks", Endpoint: "/webhooks"},
}

// Dependent entities — auto-included with their parents, not shown as tabs:
//   operations → with flows (by flow field)
//   panels     → with dashboards (by dashboard field)
//   presets    → with selected collections (by collection field)

// systemImportOrder lists entities in dependency order — referenced rows
// must exist before referrers. Edges:
//
//	users.role        → roles
//	operations.flow   → flows
//	panels.dashboard  → dashboards
//	presets.collection → user collection (created earlier)
//	translations / webhooks: no inbound FKs from other system entities.
var systemImportOrder = []string{
	"roles", "users", "dashboards", "flows",
	"operations", "panels",
	"presets", "translations", "webhooks",
}

// systemDeleteOrder is the reverse: dependents first so we never try to
// delete a row that another row still references.
var systemDeleteOrder = []string{
	"webhooks", "translations", "presets",
	"panels", "operations",
	"flows", "dashboards", "users", "roles",
}

// Dependent entity endpoints (not shown as tabs, auto-included).
var dependentEndpoints = map[string]string{
	"operations": "/operations",
	"panels":     "/panels",
	"presets":    "/presets",
}

func systemEntityByName(name string) (SystemEntityType, bool) {
	for _, t := range systemEntityTypes {
		if t.Name == name {
			return t, true
		}
	}
	if ep, ok := dependentEndpoints[name]; ok {
		return SystemEntityType{Name: name, Endpoint: ep}, true
	}
	return SystemEntityType{}, false
}

// Fetch

func fetchSystemItems(client *apiClient, endpoint string) ([]json.RawMessage, error) {
	return client.pullPaginated(endpoint)
}

func countSystemItems(client *apiClient, endpoint string) int {
	body, err := client.get(endpoint + "?aggregate[count]=*")
	if err != nil {
		return 0
	}
	return parseAggregateCount(body)
}

// Insert (import)

func insertSystemItems(client *apiClient, endpoint string, items []json.RawMessage) (int, int) {
	inserted, failed := 0, 0
	for _, item := range items {
		_, status, _ := client.post(endpoint, item)
		if status >= 200 && status < 300 {
			inserted++
		} else {
			failed++
		}
	}
	return inserted, failed
}

// Delete (clean)

func extractID(item json.RawMessage) string {
	var obj struct {
		ID json.RawMessage `json:"id"`
	}
	if json.Unmarshal(item, &obj) != nil || obj.ID == nil {
		return ""
	}
	var s string
	if json.Unmarshal(obj.ID, &s) == nil {
		return s
	}
	var n json.Number
	if json.Unmarshal(obj.ID, &n) == nil {
		return n.String()
	}
	return ""
}

func deleteSystemItems(client *apiClient, endpoint string, items []json.RawMessage) (int, int) {
	deleted, failed := 0, 0
	for _, item := range items {
		id := extractID(item)
		if id == "" {
			failed++
			continue
		}
		path := fmt.Sprintf("%s/%s", endpoint, url.PathEscape(id))
		status, err := client.del(path)
		if err != nil || status >= 400 {
			failed++
		} else {
			deleted++
		}
	}
	return deleted, failed
}

// Item display helpers

func extractSystemItemLabel(item json.RawMessage) string {
	var obj map[string]any
	if json.Unmarshal(item, &obj) != nil {
		return "?"
	}
	// Try first_name + last_name (users).
	first, _ := obj["first_name"].(string)
	last, _ := obj["last_name"].(string)
	if first != "" || last != "" {
		full := strings.TrimSpace(first + " " + last)
		if email, ok := obj["email"].(string); ok && email != "" {
			return full + " <" + email + ">"
		}
		return full
	}
	// Try email (users without names).
	if email, ok := obj["email"].(string); ok && email != "" {
		return email
	}
	// Try common name fields.
	for _, field := range []string{"name", "bookmark", "key"} {
		if v, ok := obj[field]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return extractID(item)
}

func extractSystemItemStatus(item json.RawMessage) string {
	var obj map[string]any
	if json.Unmarshal(item, &obj) != nil {
		return "—"
	}
	if s, ok := obj["status"].(string); ok && s != "" {
		return s
	}
	return "—"
}

// stripSensitiveFields removes passwords, tokens, and other secrets from user items.
func stripSensitiveFields(entityType string, item json.RawMessage) json.RawMessage {
	if entityType != "users" {
		return item
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(item, &obj) != nil {
		return item
	}
	for _, field := range []string{"password", "token", "last_access", "last_page"} {
		delete(obj, field)
	}
	out, _ := json.Marshal(obj)
	return out
}

func deleteCollection(client *apiClient, collection string) error {
	status, err := client.del("/collections/" + url.PathEscape(collection))
	if err != nil {
		return err
	}
	if status >= 400 {
		return fmt.Errorf("HTTP %d", status)
	}
	return nil
}
