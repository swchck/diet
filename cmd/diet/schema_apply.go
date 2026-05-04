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

	// 4. Extract `data` envelope and POST to /schema/apply.
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

	respBody, status, err := client.post("/schema/apply", diffEnv.Data)
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
// fields with sequence-managed defaults have the default stripped.
func buildSnapshot(meta snapshotMeta, schema SchemaBundle) map[string]any {
	collections := make([]map[string]any, 0, len(schema.Collections))
	for _, c := range schema.Collections {
		entry := map[string]any{
			"collection": c.Collection,
			"meta":       json.RawMessage(c.Meta),
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
		raw, _ := json.Marshal(r)
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
// can't round-trip. Right now: nextval(...) / ::regclass expressions on
// auto-increment columns. Directus regenerates the sequence at apply time.
func sanitizeFieldForSnapshot(f FieldInfo) json.RawMessage {
	raw, _ := json.Marshal(f)
	if isNullOrEmpty(f.Schema) {
		return raw
	}
	var schemaMap map[string]any
	if err := json.Unmarshal(f.Schema, &schemaMap); err != nil {
		return raw
	}
	if defv, ok := schemaMap["default_value"].(string); ok {
		if strings.HasPrefix(defv, "nextval(") || strings.Contains(defv, "::regclass") {
			schemaMap["default_value"] = nil
			fixed := f
			fixed.Schema, _ = json.Marshal(schemaMap)
			out, _ := json.Marshal(fixed)
			return out
		}
	}
	return raw
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
