package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
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

// fetchAllSystemEntities pulls every system entity into the same map shape
// the TUI builds (`name -> []rawItem`). Independent types are taken whole;
// dependent ones are filtered to keep the archive consistent:
//
//   - operations are kept for every fetched flow (we include all flows here,
//     so we include all operations);
//   - panels are kept for every fetched dashboard;
//   - presets are kept only for the user collections we're exporting —
//     other presets reference collections this archive doesn't carry.
//
// userColSet is the same selection set used to filter relations & data;
// pass nil to skip the presets-by-collection filter (i.e. include all
// presets regardless of which collection they target).
func fetchAllSystemEntities(client *apiClient, userColSet map[string]bool, log func(string)) map[string][]json.RawMessage {
	result := make(map[string][]json.RawMessage)
	for _, et := range systemEntityTypes {
		items, err := fetchSystemItems(client, et.Endpoint)
		if err != nil {
			if log != nil {
				log(fmt.Sprintf("WARN: fetch %s: %v", et.Name, err))
			}
			continue
		}
		// Strip sensitive fields where applicable (mirror TUI behavior).
		for i := range items {
			items[i] = stripSensitiveFields(et.Name, items[i])
		}
		if len(items) > 0 {
			result[et.Name] = items
		}
	}

	// Dependent entities. We include all of them since we included all
	// flows / dashboards above; presets get the userColSet filter.
	if ops, err := fetchSystemItems(client, "/operations"); err == nil && len(ops) > 0 {
		result["operations"] = ops
	}
	if panels, err := fetchSystemItems(client, "/panels"); err == nil && len(panels) > 0 {
		result["panels"] = panels
	}
	if presets, err := fetchSystemItems(client, "/presets"); err == nil && len(presets) > 0 {
		if userColSet == nil {
			result["presets"] = presets
		} else {
			var kept []json.RawMessage
			for _, p := range presets {
				var obj struct {
					Collection string `json:"collection"`
				}
				if json.Unmarshal(p, &obj) != nil {
					continue
				}
				// Empty collection = global preset, keep it.
				if obj.Collection == "" || userColSet[obj.Collection] {
					kept = append(kept, p)
				}
			}
			if len(kept) > 0 {
				result["presets"] = kept
			}
		}
	}

	return result
}

// Insert (import)

// systemInsertOutcome separates the three categories we care about for
// system-entity POSTs: items that landed, items that already exist on the
// target (treated as "leave the existing value alone" — see classify-
// SystemPostError), and items that failed for any other reason.
//
// Skipped is reported separately from failed so partial-failure exit codes
// (--strict) and progress messages don't conflate "user already exists"
// with "Directus rejected the payload".
type systemInsertOutcome struct {
	Inserted int
	Skipped  int
	Failed   int
}

// insertSystemItems posts each item to `endpoint`, classifying the response
// as inserted, skipped (already present — unique constraint hit on id /
// email / etc.), or failed. We do not attempt PATCH-on-conflict: a target
// that already has a record with the same id/email is assumed to be the
// user's intent ("don't overwrite what's already there").
func insertSystemItems(client *apiClient, endpoint string, items []json.RawMessage) systemInsertOutcome {
	var inserted, skipped, failed atomic.Int64
	_ = runParallel(client, items, func(item json.RawMessage) error {
		body, status, _ := client.post(endpoint, item)
		switch classifySystemPostResult(status, body) {
		case sysResultInserted:
			inserted.Add(1)
		case sysResultDuplicate:
			skipped.Add(1)
		default:
			failed.Add(1)
		}
		return nil
	})
	return systemInsertOutcome{
		Inserted: int(inserted.Load()),
		Skipped:  int(skipped.Load()),
		Failed:   int(failed.Load()),
	}
}

// systemPostResult is the trichotomy we want for system-entity POSTs.
// Pulled out so the classification policy is unit-testable independently
// of the network plumbing.
type systemPostResult int

const (
	sysResultInserted systemPostResult = iota
	sysResultDuplicate
	sysResultOther // generic failure
)

// classifySystemPostResult maps an HTTP status + response body from a
// system-entity POST to one of three outcomes.
//
// "Duplicate" is the case the user explicitly asked to swallow silently:
// the target already has a record with this id / email / unique key, and
// we should leave it alone rather than error. Directus signals this with
// `extensions.code == "RECORD_NOT_UNIQUE"` (for both PK collisions and
// secondary unique constraints like users.email).
//
// We deliberately don't trust HTTP status alone — Directus uses 400 for
// many distinct error codes, and false-positive "skip" on a real
// validation error would silently lose data.
func classifySystemPostResult(status int, body []byte) systemPostResult {
	if status >= 200 && status < 300 {
		return sysResultInserted
	}
	if containsErrorCode(body, "RECORD_NOT_UNIQUE") {
		return sysResultDuplicate
	}
	return sysResultOther
}

// containsErrorCode looks for a Directus error envelope with the given
// extensions.code. Directus errors look like:
//
//	{"errors":[{"message":"...","extensions":{"code":"RECORD_NOT_UNIQUE",...}}]}
//
// We parse minimally — enough to catch the code without risking a panic
// on shape drift across Directus versions.
func containsErrorCode(body []byte, code string) bool {
	var env struct {
		Errors []struct {
			Extensions struct {
				Code string `json:"code"`
			} `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return false
	}
	for _, e := range env.Errors {
		if e.Extensions.Code == code {
			return true
		}
	}
	return false
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

// sensitiveUserFields lists every directus_users column we strip on
// export. Each entry is documented so future readers know whether to
// keep / remove / extend the list.
//
//   - password           bcrypt hash; arguably reusable across instances
//                        but treating it as a secret is the safer default.
//   - token              static API token tied to the user; full bearer
//                        access if leaked.
//   - tfa_secret         TOTP shared secret; reproduces the user's 2FA
//                        codes verbatim if leaked. Strict secret.
//   - auth_data          SSO/OAuth refresh material for external IdPs.
//                        Strict secret.
//   - last_access        timestamp telemetry — not a secret, but
//                        instance-scoped and not meaningful on the
//                        target. Stripping keeps imports deterministic.
//   - last_page          last admin-UI route the user opened —
//                        instance-scoped and not meaningful.
//
// `email`, `external_identifier`, `provider`, `role` are kept: they're
// identifiers/metadata the target needs to know who the user is.
var sensitiveUserFields = []string{
	"password", "token", "tfa_secret", "auth_data",
	"last_access", "last_page",
}

// stripSensitiveFields removes passwords, tokens, and other secrets from
// user items at export time. See sensitiveUserFields for the full list +
// rationale per field.
func stripSensitiveFields(entityType string, item json.RawMessage) json.RawMessage {
	if entityType != "users" {
		return item
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(item, &obj) != nil {
		return item
	}
	for _, field := range sensitiveUserFields {
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
