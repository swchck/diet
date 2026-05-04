package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
)

// Types

// CollectionInfo mirrors a row from /collections. Schema is null for
// virtual folders (groups), populated for real tables.
type CollectionInfo struct {
	Collection string          `json:"collection"`
	Schema     json.RawMessage `json:"schema"` // null for folders
	Meta       json.RawMessage `json:"meta"`
}

// FieldInfo mirrors a row from /fields/:collection. Type "alias" indicates
// a virtual relation field (O2M, M2M presentation) with no underlying column.
type FieldInfo struct {
	Collection string          `json:"collection"`
	Field      string          `json:"field"`
	Type       string          `json:"type"`
	Schema     json.RawMessage `json:"schema"`
	Meta       json.RawMessage `json:"meta"`
}

// RelationInfo mirrors a row from /relations.
type RelationInfo struct {
	Collection        string          `json:"collection"`
	Field             string          `json:"field"`
	RelatedCollection string          `json:"related_collection"`
	Schema            json.RawMessage `json:"schema"`
	Meta              json.RawMessage `json:"meta"`
}

// Export: fetch from Directus

func fetchCollections(client *apiClient) ([]CollectionInfo, error) {
	var resp struct {
		Data []CollectionInfo `json:"data"`
	}
	if err := client.getJSON("/collections", &resp); err != nil {
		return nil, err
	}
	// Filter out system collections.
	var result []CollectionInfo
	for _, c := range resp.Data {
		if !strings.HasPrefix(c.Collection, "directus_") {
			result = append(result, c)
		}
	}
	return result, nil
}

func fetchFields(client *apiClient, collection string) ([]FieldInfo, error) {
	var resp struct {
		Data []FieldInfo `json:"data"`
	}
	if err := client.getJSON("/fields/"+url.PathEscape(collection), &resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func fetchAllFields(client *apiClient, collections []string) ([]FieldInfo, error) {
	var all []FieldInfo
	for _, col := range collections {
		fields, err := fetchFields(client, col)
		if err != nil {
			return nil, fmt.Errorf("fields for %s: %w", col, err)
		}
		all = append(all, fields...)
	}
	return all, nil
}

func fetchRelations(client *apiClient) ([]RelationInfo, error) {
	var resp struct {
		Data []RelationInfo `json:"data"`
	}
	if err := client.getJSON("/relations", &resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// countItems returns item count for a collection.
func countItems(client *apiClient, collection string) int {
	path := fmt.Sprintf("/items/%s?aggregate[count]=*", url.PathEscape(collection))
	body, err := client.get(path)
	if err != nil {
		return 0
	}
	return parseAggregateCount(body)
}

// Import: create schema in target Directus

func createCollections(client *apiClient, collections []CollectionInfo, fields []FieldInfo, log func(string)) error {
	// Build PK field lookup: collection -> PK FieldInfo
	pkFields := map[string]FieldInfo{}
	for _, f := range fields {
		if isPrimaryKey(f) {
			pkFields[f.Collection] = f
		}
	}

	// Separate folders (schema=null) from tables.
	var folders, tables []CollectionInfo
	for _, c := range collections {
		if string(c.Schema) == "null" || len(c.Schema) == 0 {
			folders = append(folders, c)
		} else {
			tables = append(tables, c)
		}
	}

	// Strip group references and reapply after all collections exist.
	// Directus rejects a meta.group pointing to a collection it hasn't seen
	// yet, so we defer the reference and PATCH it back at the end.
	groupRefs := map[string]string{}
	stripGroup := func(c *CollectionInfo) {
		var meta map[string]json.RawMessage
		if json.Unmarshal(c.Meta, &meta) != nil {
			return
		}
		if g, ok := meta["group"]; ok {
			var group string
			if json.Unmarshal(g, &group) == nil && group != "" {
				groupRefs[c.Collection] = group
			}
			delete(meta, "group")
			c.Meta, _ = json.Marshal(meta)
		}
	}

	for i := range folders {
		stripGroup(&folders[i])
	}
	for i := range tables {
		stripGroup(&tables[i])
	}

	// Create folders first, then tables.
	for _, c := range append(folders, tables...) {
		payload := map[string]any{
			"collection": c.Collection,
			"meta":       json.RawMessage(c.Meta),
		}

		if string(c.Schema) == "null" || len(c.Schema) == 0 {
			payload["schema"] = nil
		} else {
			payload["schema"] = json.RawMessage(`{}`)

			// Include PK field with correct type.
			if pk, ok := pkFields[c.Collection]; ok {
				pkMeta := stripMetaID(pk.Meta)
				pkPayload := map[string]any{
					"field": pk.Field,
					"type":  pk.Type,
					"meta":  json.RawMessage(pkMeta),
				}
				if len(pk.Schema) > 0 && string(pk.Schema) != "null" {
					pkPayload["schema"] = json.RawMessage(pk.Schema)
				}
				payload["fields"] = []any{pkPayload}
			}
		}

		body, status, err := client.postJSON("/collections", payload)
		if err != nil {
			log(fmt.Sprintf("WARN: create collection %s: %v", c.Collection, err))
		} else if status >= 400 {
			msg := string(body)
			if !isAlreadyExists(msg) {
				log(fmt.Sprintf("WARN: create collection %s: HTTP %d: %s", c.Collection, status, truncate(msg, 200)))
			}
		}
		log(fmt.Sprintf("Collection: %s", c.Collection))
	}

	// Restore group references.
	for col, group := range groupRefs {
		if err := client.patch("/collections/"+col, map[string]any{
			"meta": map[string]any{"group": group},
		}); err != nil {
			log(fmt.Sprintf("WARN: set group for %s: %v", col, err))
		}
	}

	return nil
}

func createFields(client *apiClient, fields []FieldInfo, log func(string)) error {
	// Sort fields by collection, then by meta.sort. Order is informational
	// only once we go parallel — Directus assigns its own sort to each field
	// at creation time based on insertion order, so we PATCH it back below.
	sort.Slice(fields, func(i, j int) bool {
		if fields[i].Collection != fields[j].Collection {
			return fields[i].Collection < fields[j].Collection
		}
		si, oki := fieldSort(fields[i])
		sj, okj := fieldSort(fields[j])
		if !oki {
			si = 999
		}
		if !okj {
			sj = 999
		}
		return si < sj
	})

	// Skip PK fields — created with the collection.
	work := make([]FieldInfo, 0, len(fields))
	for _, f := range fields {
		if !isPrimaryKey(f) {
			work = append(work, f)
		}
	}

	var created, skipped atomic.Int64
	err := runParallel(client, work, func(f FieldInfo) error {
		// Build payload: strip meta.id (instance-specific).
		meta := stripMetaID(f.Meta)

		payload := map[string]any{
			"field": f.Field,
			"type":  f.Type,
			"meta":  json.RawMessage(meta),
		}
		// Alias fields have no schema.
		if f.Type != "alias" && len(f.Schema) > 0 && string(f.Schema) != "null" {
			payload["schema"] = json.RawMessage(f.Schema)
		}

		_, status, err := client.postJSON("/fields/"+url.PathEscape(f.Collection), payload)
		if err != nil {
			return fmt.Errorf("create field %s.%s: %w", f.Collection, f.Field, err)
		}
		if status >= 200 && status < 300 {
			created.Add(1)
		} else {
			skipped.Add(1)
		}
		return nil
	})
	if err != nil {
		return err
	}

	log(fmt.Sprintf("Fields: %d created, %d skipped", created.Load(), skipped.Load()))

	// Reorder: PATCH meta.sort for fields that have an explicit sort value.
	// Mandatory after parallel creation — Directus renumbers meta.sort based
	// on the order POSTs land on the server, which is non-deterministic
	// across goroutines.
	type reorderTask struct {
		f       FieldInfo
		sortVal int
	}
	reorderWork := make([]reorderTask, 0, len(work))
	for _, f := range work {
		if sortVal, ok := fieldSort(f); ok {
			reorderWork = append(reorderWork, reorderTask{f: f, sortVal: sortVal})
		}
	}

	var reordered atomic.Int64
	_ = runParallel(client, reorderWork, func(t reorderTask) error {
		if err := client.patch(
			fmt.Sprintf("/fields/%s/%s", url.PathEscape(t.f.Collection), url.PathEscape(t.f.Field)),
			map[string]any{"meta": map[string]any{"sort": t.sortVal}},
		); err == nil {
			reordered.Add(1)
		}
		return nil
	})
	if r := reordered.Load(); r > 0 {
		log(fmt.Sprintf("Fields: %d reordered", r))
	}

	return nil
}

func createRelations(client *apiClient, relations []RelationInfo, log func(string)) error {
	var created, skipped atomic.Int64
	err := runParallel(client, relations, func(r RelationInfo) error {
		meta := stripMetaID(r.Meta)
		payload := map[string]any{
			"collection":         r.Collection,
			"field":              r.Field,
			"related_collection": r.RelatedCollection,
			"meta":               json.RawMessage(meta),
		}
		if len(r.Schema) > 0 && string(r.Schema) != "null" {
			payload["schema"] = json.RawMessage(r.Schema)
		}

		_, status, err := client.postJSON("/relations", payload)
		if err != nil {
			return fmt.Errorf("create relation %s.%s: %w", r.Collection, r.Field, err)
		}
		if status >= 200 && status < 300 {
			created.Add(1)
		} else {
			skipped.Add(1)
		}
		return nil
	})
	if err != nil {
		return err
	}
	log(fmt.Sprintf("Relations: %d created, %d skipped", created.Load(), skipped.Load()))
	return nil
}

// Helpers

func fieldSort(f FieldInfo) (int, bool) {
	var meta struct {
		Sort *int `json:"sort"`
	}
	json.Unmarshal(f.Meta, &meta)
	if meta.Sort != nil {
		return *meta.Sort, true
	}
	return 0, false
}

func isPrimaryKey(f FieldInfo) bool {
	var schema struct {
		IsPrimaryKey bool `json:"is_primary_key"`
	}
	json.Unmarshal(f.Schema, &schema)
	return schema.IsPrimaryKey
}

// stripMetaID removes the meta.id surrogate key from a Directus metadata
// blob. It's an instance-scoped autoincrement that is meaningless on the
// target — keeping it would either collide with an existing row or anchor
// the imported entity to a slot that doesn't belong to it.
func stripMetaID(meta json.RawMessage) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(meta, &m) != nil {
		return meta
	}
	delete(m, "id")
	out, _ := json.Marshal(m)
	return out
}

// buildAliasFields identifies virtual (alias) fields to strip from data payloads.
// user_created/user_updated are stripped separately in stripDataFields.
func buildAliasFields(fields []FieldInfo) map[string]map[string]bool {
	result := make(map[string]map[string]bool)
	for _, f := range fields {
		if f.Type == "alias" {
			if result[f.Collection] == nil {
				result[f.Collection] = make(map[string]bool)
			}
			result[f.Collection][f.Field] = true
		}
	}
	return result
}

// buildInsertOrder returns collections in FK-safe insert order: a collection
// appears after every collection it references. Uses Kahn's topological sort.
// Collections caught in a cycle are appended at the end and rely on the
// multi-pass retry loop in applyData to eventually settle.
func buildInsertOrder(collections []string, relations []RelationInfo) []string {
	colSet := make(map[string]bool)
	for _, c := range collections {
		colSet[c] = true
	}

	deps := make(map[string]map[string]bool)
	for _, c := range collections {
		deps[c] = make(map[string]bool)
	}
	for _, r := range relations {
		from := r.Collection
		to := r.RelatedCollection
		if from != "" && to != "" && colSet[from] && colSet[to] && from != to {
			deps[from][to] = true
		}
	}

	// Kahn's topological sort.
	inDegree := make(map[string]int)
	for c := range colSet {
		inDegree[c] = len(deps[c])
	}
	reverse := make(map[string][]string)
	for c, d := range deps {
		for dep := range d {
			reverse[dep] = append(reverse[dep], c)
		}
	}

	var queue []string
	for c := range colSet {
		if inDegree[c] == 0 {
			queue = append(queue, c)
		}
	}

	var order []string
	for len(queue) > 0 {
		c := queue[0]
		queue = queue[1:]
		order = append(order, c)
		for _, dep := range reverse[c] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	// Add remaining (circular deps).
	for c := range colSet {
		found := slices.Contains(order, c)
		if !found {
			order = append(order, c)
		}
	}

	return order
}
