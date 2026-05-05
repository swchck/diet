package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

// schemaApplyBulk imports collections + fields + relations through Directus's
// /schema/diff + /schema/apply endpoints in a single transactional shot.
//
// Why: per-field POSTs cost one round-trip per field plus a follow-up PATCH
// pass to fix meta.sort. On a 858-field archive that's ~2000 sequential
// requests. /schema/apply replaces all of it with one diff + one apply call,
// runs ~30-40× faster, and lets Directus order DDL itself so fields that
// would have been "skipped" because their relation was not yet in place
// also land cleanly.
//
// Caveats handled here:
//   - Folder collections (schema=null) must omit the schema key entirely;
//     /schema/diff rejects schema:null with "must be of type object".
//   - Auto-increment columns ship with default_value="nextval(...)::regclass"
//     in Directus's GET /fields output. /schema/apply tries to use that as
//     a literal default and Postgres rejects it as an invalid integer.
//     Strip those before sending; Directus regenerates the sequence.
//   - Source/target version & vendor must agree. We read them from the
//     target's own /schema/snapshot rather than the archive — the archive
//     might have been produced on Directus 11.6 and applied on 11.17.
//   - Default Directus MAX_PAYLOAD_SIZE is 100kb; real-world snapshots
//     blow past that. On 413 we surface a clear error so the caller can
//     fall back to the per-field path.
func schemaApplyBulk(client *apiClient, schema SchemaBundle, log func(string)) error {
	// 1. Read target's snapshot envelope for version/directus/vendor.
	targetMeta, err := fetchSnapshotMeta(client)
	if err != nil {
		return fmt.Errorf("read target snapshot meta: %w", err)
	}

	// 2. Build snapshot payload from archive structures.
	snapshot := buildSnapshot(targetMeta, schema)

	// 3. POST to /schema/diff?force=true — Directus computes the DDL plan.
	body, status, err := client.postJSON("/schema/diff?force=true", snapshot)
	if err != nil {
		return fmt.Errorf("schema/diff: %w", err)
	}
	if status == 204 {
		// 204 = "no diff, schema already matches". Treat as success.
		log("Schema: target already up to date (no diff)")
		return nil
	}
	if status >= 400 {
		return classifySchemaError("schema/diff", status, body)
	}

	// 4. Extract `data` envelope and strip destructive ops before applying.
	//
	// Why: /schema/diff returns the full delta required to make the target
	// equal to the snapshot. For a partial archive (subset of source schema)
	// that delta includes DROP COLLECTION / DROP FIELD / DROP RELATION
	// entries for everything in target that's missing from the snapshot —
	// applying it verbatim wipes data the user never asked us to touch.
	// Import is intentionally additive: we only ever create or update
	// objects mentioned in the archive, never remove anything else.
	var diffEnv struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &diffEnv); err != nil {
		return fmt.Errorf("schema/diff: parse response: %w", err)
	}
	if len(diffEnv.Data) == 0 || string(diffEnv.Data) == "null" {
		log("Schema: target already up to date (empty diff)")
		return nil
	}

	additive, dropped, err := stripDestructiveDiff(diffEnv.Data)
	if err != nil {
		return fmt.Errorf("schema/diff: filter destructive ops: %w", err)
	}
	if dropped.total() > 0 {
		log(fmt.Sprintf("Schema: filtered %d destructive ops from diff (%d collections, %d fields, %d relations would have been dropped)",
			dropped.total(), dropped.collections, dropped.fields, dropped.relations))
	}
	if additive == nil {
		log("Schema: nothing to apply after filtering destructive ops (target already has everything in archive)")
		return nil
	}

	respBody, status, err := client.post("/schema/apply", additive)
	if err != nil {
		return fmt.Errorf("schema/apply: %w", err)
	}
	if status >= 400 {
		return classifySchemaError("schema/apply", status, respBody)
	}

	log(fmt.Sprintf("Schema: applied %d collections, %d fields, %d relations in one shot",
		len(schema.Collections), len(schema.Fields), len(schema.Relations)))
	return nil
}

// destructiveCounts reports what we filtered out of a /schema/diff response.
// All three numbers count entries (collection rows, field rows, relation
// rows) where every inner diff op was destructive — partial filtering of an
// inner array still counts the entry as kept.
type destructiveCounts struct {
	collections int
	fields      int
	relations   int
}

func (d destructiveCounts) total() int { return d.collections + d.fields + d.relations }

// stripDestructiveDiff parses the `data` payload of /schema/diff (shape:
// {hash, diff: {collections, fields, relations}}) and removes every inner
// op with kind == "D". It also drops top-level entries whose inner diff
// array becomes empty after filtering.
//
// Returns the rewritten payload as json.RawMessage suitable for POSTing to
// /schema/apply, or nil if no additive ops remain. The hash is preserved
// verbatim — Directus uses it as a concurrency check against current
// target state, which we never mutated.
//
// We thread the JSON through map[string]json.RawMessage rather than typed
// structs so any future Directus deepdiff fields (lhs/rhs/path/index/item,
// or new top-level diff sections) ride through unchanged. We only inspect
// `kind` on the leaf ops — that's the contract we need.
func stripDestructiveDiff(raw json.RawMessage) (json.RawMessage, destructiveCounts, error) {
	var counts destructiveCounts
	var env map[string]json.RawMessage
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, counts, err
	}
	diffRaw, ok := env["diff"]
	if !ok {
		return raw, counts, nil // unrecognized shape, pass through
	}
	var diffMap map[string]json.RawMessage
	if err := json.Unmarshal(diffRaw, &diffMap); err != nil {
		return nil, counts, err
	}

	type section struct {
		key string
		ptr *int
	}
	for _, sec := range []section{
		{"collections", &counts.collections},
		{"fields", &counts.fields},
		{"relations", &counts.relations},
	} {
		raw, ok := diffMap[sec.key]
		if !ok {
			continue
		}
		var entries []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &entries); err != nil {
			return nil, counts, fmt.Errorf("parse %s: %w", sec.key, err)
		}
		filtered, dropped := filterDiffEntries(entries)
		*sec.ptr = dropped
		newRaw, err := json.Marshal(filtered)
		if err != nil {
			return nil, counts, err
		}
		diffMap[sec.key] = newRaw
	}

	// If every known section is empty after filtering, signal "nothing to
	// apply" by returning nil. We deliberately consider only the three
	// known sections — an unrecognized section we forwarded as-is should
	// also count as "something to apply".
	if isEmptyDiffSection(diffMap, "collections") &&
		isEmptyDiffSection(diffMap, "fields") &&
		isEmptyDiffSection(diffMap, "relations") &&
		!hasUnknownSections(diffMap) {
		return nil, counts, nil
	}

	newDiff, err := json.Marshal(diffMap)
	if err != nil {
		return nil, counts, err
	}
	env["diff"] = newDiff
	out, err := json.Marshal(env)
	if err != nil {
		return nil, counts, err
	}
	return out, counts, nil
}

func isEmptyDiffSection(m map[string]json.RawMessage, key string) bool {
	raw, ok := m[key]
	if !ok {
		return true
	}
	// Match `[]` (after trimming whitespace) or `null`.
	s := strings.TrimSpace(string(raw))
	return s == "[]" || s == "null"
}

func hasUnknownSections(m map[string]json.RawMessage) bool {
	for k := range m {
		switch k {
		case "collections", "fields", "relations":
			continue
		default:
			return true
		}
	}
	return false
}

// filterDiffEntries strips inner ops with kind=="D" from each entry and
// drops entries whose inner `diff` array is empty afterwards. Returns the
// surviving entries plus a count of fully-dropped entries (i.e. entries
// where every inner op was destructive).
func filterDiffEntries(entries []map[string]json.RawMessage) ([]map[string]json.RawMessage, int) {
	dropped := 0
	out := entries[:0]
	for _, ent := range entries {
		innerRaw, ok := ent["diff"]
		if !ok {
			out = append(out, ent)
			continue
		}
		var inner []json.RawMessage
		if err := json.Unmarshal(innerRaw, &inner); err != nil {
			// Unrecognized shape — keep as-is rather than risk dropping
			// a non-destructive op we didn't understand.
			out = append(out, ent)
			continue
		}
		kept := inner[:0]
		for _, op := range inner {
			var head struct {
				Kind string `json:"kind"`
			}
			if err := json.Unmarshal(op, &head); err == nil && head.Kind == "D" {
				continue
			}
			kept = append(kept, op)
		}
		if len(kept) == 0 {
			dropped++
			continue
		}
		newInner, err := json.Marshal(kept)
		if err != nil {
			// Marshal of []RawMessage shouldn't fail; if it somehow does
			// keep the original entry to err on the side of preservation.
			out = append(out, ent)
			continue
		}
		ent["diff"] = newInner
		out = append(out, ent)
	}
	return out, dropped
}

// snapshotMeta describes the target's schema-snapshot envelope. Directus
// requires the source's `version`/`directus`/`vendor` to match the target's
// — sending the target's own values is the safe default.
type snapshotMeta struct {
	Version  int    `json:"version"`
	Directus string `json:"directus"`
	Vendor   string `json:"vendor"`
}

func fetchSnapshotMeta(client *apiClient) (snapshotMeta, error) {
	body, err := client.get("/schema/snapshot")
	if err != nil {
		return snapshotMeta{}, err
	}
	var env struct {
		Data snapshotMeta `json:"data"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return snapshotMeta{}, fmt.Errorf("parse snapshot meta: %w", err)
	}
	if env.Data.Version == 0 || env.Data.Directus == "" || env.Data.Vendor == "" {
		return snapshotMeta{}, fmt.Errorf("snapshot meta incomplete: %+v", env.Data)
	}
	return env.Data, nil
}

// buildSnapshot reshapes archive structures into the on-the-wire snapshot
// shape Directus expects. Collections lose their schema key for folders;
// fields with sequence-managed defaults have the default stripped; every
// `meta.id` is dropped so we don't ship the source instance's surrogate
// keys into the target's directus_collections / directus_fields /
// directus_relations tables (the per-field path already does this — see
// stripMetaID — and parity matters because /schema/diff and the per-field
// POST path are supposed to converge on the same target state).
//
// On `meta.group` (forward-reference to a parent folder collection): the
// per-field path strips it before POST then PATCHes it back, because
// Directus rejects a group reference to a collection it hasn't seen yet.
// We deliberately do NOT replicate that here — /schema/apply is atomic
// (Directus sees the full topology in one shot) and empirically resolves
// group references regardless of declaration order across real-world
// archives. If a future Directus release tightens this, the symptom is a
// 4xx from /schema/apply with a clear error message; we'd add the
// strip+patch dance then. Defensive stripping today would force an extra
// round-trip per import for no measurable correctness gain.
func buildSnapshot(meta snapshotMeta, schema SchemaBundle) map[string]any {
	collections := make([]map[string]any, 0, len(schema.Collections))
	for _, c := range schema.Collections {
		entry := map[string]any{
			"collection": c.Collection,
			"meta":       stripMetaID(c.Meta),
		}
		// Folders ship with schema=null in the archive. The snapshot format
		// represents folders by simply omitting the schema key.
		if !isNullOrEmpty(c.Schema) {
			entry["schema"] = map[string]any{"name": c.Collection}
		}
		collections = append(collections, entry)
	}

	fields := make([]json.RawMessage, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		fields = append(fields, sanitizeFieldForSnapshot(f))
	}

	relations := make([]json.RawMessage, 0, len(schema.Relations))
	for _, r := range schema.Relations {
		// Stripping meta.id from RelationInfo: marshal a copy with cleaned
		// meta. We can't mutate the original because schema is shared and
		// the per-field path may still want to read it.
		clean := r
		clean.Meta = stripMetaID(r.Meta)
		raw, _ := json.Marshal(clean)
		relations = append(relations, raw)
	}

	return map[string]any{
		"version":     meta.Version,
		"directus":    meta.Directus,
		"vendor":      meta.Vendor,
		"collections": collections,
		"fields":      fields,
		"relations":   relations,
	}
}

// sanitizeFieldForSnapshot strips DB-specific defaults that /schema/apply
// can't round-trip and the source instance's surrogate meta.id (which would
// otherwise ride along into the target's directus_fields auto-increment
// space). Right now schema-side: nextval(...) / ::regclass expressions on
// auto-increment columns — Directus regenerates the sequence at apply time.
func sanitizeFieldForSnapshot(f FieldInfo) json.RawMessage {
	clean := f
	clean.Meta = stripMetaID(f.Meta)

	if isNullOrEmpty(f.Schema) {
		raw, _ := json.Marshal(clean)
		return raw
	}
	var schemaMap map[string]any
	if err := json.Unmarshal(f.Schema, &schemaMap); err != nil {
		raw, _ := json.Marshal(clean)
		return raw
	}
	if defv, ok := schemaMap["default_value"].(string); ok {
		if strings.HasPrefix(defv, "nextval(") || strings.Contains(defv, "::regclass") {
			schemaMap["default_value"] = nil
			clean.Schema, _ = json.Marshal(schemaMap)
		}
	}
	out, _ := json.Marshal(clean)
	return out
}

func isNullOrEmpty(raw json.RawMessage) bool {
	return len(raw) == 0 || string(raw) == "null"
}

// stripAccountability rewrites meta.accountability="null" on every
// collection in schema. Directus uses that field to decide whether each
// write goes to directus_activity (audit log) and directus_revisions
// (full row snapshots). Setting it to null skips both, which on large
// data imports is the difference between a 90s and a 30s run — the audit
// path roughly doubles per-row CPU on the Directus side.
//
// Returns the number of collections touched. Reversible via Directus UI
// after import: collection settings → "Activity & Revisions Tracking".
func stripAccountability(schema *SchemaBundle) int {
	n := 0
	for i := range schema.Collections {
		c := &schema.Collections[i]
		var meta map[string]json.RawMessage
		if err := json.Unmarshal(c.Meta, &meta); err != nil {
			meta = map[string]json.RawMessage{}
		}
		meta["accountability"] = json.RawMessage("null")
		c.Meta, _ = json.Marshal(meta)
		n++
	}
	return n
}

// classifySchemaError turns a Directus error response into a typed Go error,
// promoting "payload too large" and version-mismatch into actionable
// messages so the caller can decide whether to fall back.
func classifySchemaError(op string, status int, body []byte) error {
	msg := truncate(string(body), 400)
	switch {
	case status == 413 || strings.Contains(msg, "request entity too large"):
		return fmt.Errorf("%s: HTTP %d: payload too large — bump MAX_PAYLOAD_SIZE on Directus (e.g. 10mb): %s", op, status, msg)
	case strings.Contains(msg, "version") && strings.Contains(msg, "match"):
		return fmt.Errorf("%s: HTTP %d: snapshot version mismatch (re-export against the target version): %s", op, status, msg)
	default:
		return fmt.Errorf("%s: HTTP %d: %s", op, status, msg)
	}
}
