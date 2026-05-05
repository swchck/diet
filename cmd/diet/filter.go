package main

import (
	"encoding/json"
	"strings"
)

// filterReport summarises what got dropped during filterArchiveSubset so
// callers can warn the user. None of the fields are an "error" per se —
// dropping a relation that crossed the keep boundary is the whole point —
// but they're surfaced so a user running `--collections=foo` doesn't
// silently end up with a half-broken schema and no idea what happened.
type filterReport struct {
	// missingFromKeep lists names the caller asked to keep that aren't
	// actually present in the archive. Almost always a typo or a stale
	// archive — worth surfacing loudly.
	missingFromKeep []string
	// droppedRelations is the count of relations dropped because at
	// least one side referenced a collection that wasn't kept (and
	// wasn't a system collection). The data on the kept side still
	// imports; just won't have its FK back to the dropped side.
	droppedRelations int
	// droppedSystemFields is the count of custom-system-collection
	// fields (e.g. directus_users.game_accounts) that were in the
	// archive but no surviving relation references them anymore. They'd
	// orphan-create on target with no purpose, so we drop them.
	droppedSystemFields int
}

// filterArchiveSubset returns trimmed copies of the archive structures
// scoped to keep — a list of user-collection names to retain. Pass nil
// or empty `keep` to skip filtering entirely (returned values are the
// inputs unchanged, report is zero).
//
// What's kept:
//   - manifest.Collections / ItemCounts: only entries in keep
//   - schema.Collections: kept tables + ALL folders (they're cheap and
//     dropping them risks breaking meta.group transitively — see comment
//     below)
//   - schema.Fields: kept-collection fields + system-collection custom
//     fields whose anchor relation survived
//   - schema.Relations: only when at least one side is a kept user
//     collection AND no side is a non-kept user collection. System (
//     directus_*) sides are always allowed because they exist on target.
//   - data: only kept collections
//
// SystemData (flows/dashboards/...) is independent of collection filter
// and passes through unchanged. Use filterSystemSubset for that.
func filterArchiveSubset(
	manifest Manifest,
	schema SchemaBundle,
	data map[string][]json.RawMessage,
	keep []string,
) (Manifest, SchemaBundle, map[string][]json.RawMessage, filterReport) {
	if len(keep) == 0 {
		return manifest, schema, data, filterReport{}
	}

	keepSet := make(map[string]bool, len(keep))
	for _, k := range keep {
		keepSet[k] = true
	}

	var report filterReport

	// Detect names the caller asked for but the archive doesn't carry.
	inArchive := make(map[string]bool, len(manifest.Collections))
	for _, c := range manifest.Collections {
		inArchive[c] = true
	}
	for _, k := range keep {
		if !inArchive[k] {
			report.missingFromKeep = append(report.missingFromKeep, k)
		}
	}

	// Manifest.Collections + ItemCounts.
	var newCollections []string
	newCounts := make(map[string]int, len(keepSet))
	for _, c := range manifest.Collections {
		if keepSet[c] {
			newCollections = append(newCollections, c)
			if v, ok := manifest.ItemCounts[c]; ok {
				newCounts[c] = v
			}
		}
	}
	manifest.Collections = newCollections
	manifest.ItemCounts = newCounts

	// Schema.Collections: keep tables in keepSet plus EVERY folder.
	// Folders carry meta.group references and pruning them transitively
	// is fragile (meta.group can chain folder-of-folder-of-table).
	// They're tiny — keeping all of them is cheap correctness.
	var newSchemaCols []CollectionInfo
	for _, c := range schema.Collections {
		isFolder := isNullOrEmpty(c.Schema)
		if isFolder || keepSet[c.Collection] {
			newSchemaCols = append(newSchemaCols, c)
		}
	}

	// Relations: keep ones that touch a kept user collection on at
	// least one side AND don't reference a dropped user collection on
	// the other side. directus_* on either side is always fine because
	// the target already has those collections.
	isUserDropped := func(name string) bool {
		if name == "" {
			return false
		}
		if strings.HasPrefix(name, "directus_") {
			return false
		}
		return !keepSet[name]
	}
	// survivingSystemFields keys are "<system_collection>.<field>" of
	// the system side of a surviving relation. We only mark a system
	// field as anchored when the relation places the field NAME on the
	// system side — i.e. r.Collection is system and r.Field belongs to
	// it. Relations where the system side is r.RelatedCollection don't
	// name a specific field on the system collection (the FK lives on
	// the user side), so they don't anchor anything.
	survivingSystemFields := make(map[string]bool)
	var newRelations []RelationInfo
	for _, r := range schema.Relations {
		if isUserDropped(r.Collection) || isUserDropped(r.RelatedCollection) {
			report.droppedRelations++
			continue
		}
		// Skip relations that are entirely outside the keep set
		// (system→system or system→nothing). Those stayed in the
		// archive only because the export-side filter is permissive;
		// at import time, with a narrowed scope, they're noise.
		if !keepSet[r.Collection] && !keepSet[r.RelatedCollection] {
			report.droppedRelations++
			continue
		}
		newRelations = append(newRelations, r)
		if strings.HasPrefix(r.Collection, "directus_") && r.Field != "" {
			survivingSystemFields[r.Collection+"."+r.Field] = true
		}
	}

	// Fields: kept-collection fields + system custom fields anchored
	// by a surviving relation that explicitly names the field.
	var newFields []FieldInfo
	for _, f := range schema.Fields {
		switch {
		case keepSet[f.Collection]:
			newFields = append(newFields, f)
		case strings.HasPrefix(f.Collection, "directus_"):
			if survivingSystemFields[f.Collection+"."+f.Field] {
				newFields = append(newFields, f)
			} else {
				report.droppedSystemFields++
			}
		}
	}

	// Data: kept collections only. Dropped collections' files are
	// silently skipped — we already removed them from manifest, so
	// the import pipeline won't ask for them.
	newData := make(map[string][]json.RawMessage, len(keepSet))
	for col, items := range data {
		if keepSet[col] {
			newData[col] = items
		}
	}

	schema.Collections = newSchemaCols
	schema.Fields = newFields
	schema.Relations = newRelations

	return manifest, schema, newData, report
}

// filterSystemSubset trims systemData and manifest.SystemEntities to the
// chosen entity types (flows / dashboards / roles / ...). Pass nil/empty
// `keep` to skip filtering. `keep` should be a slice of entity-type
// names ("flows", "dashboards", ...) — matches systemEntityTypes /
// systemImportOrder vocabulary.
func filterSystemSubset(
	manifest Manifest,
	systemData map[string][]json.RawMessage,
	keep []string,
) (Manifest, map[string][]json.RawMessage) {
	if len(keep) == 0 {
		return manifest, systemData
	}
	keepSet := make(map[string]bool, len(keep))
	for _, k := range keep {
		keepSet[k] = true
	}

	newData := make(map[string][]json.RawMessage, len(keep))
	for k, v := range systemData {
		if keepSet[k] {
			newData[k] = v
		}
	}

	var newEntities []string
	for _, name := range manifest.SystemEntities {
		if keepSet[name] {
			newEntities = append(newEntities, name)
		}
	}
	manifest.SystemEntities = newEntities

	return manifest, newData
}

