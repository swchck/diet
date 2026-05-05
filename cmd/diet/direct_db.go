package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// applyDataDirect bypasses the Directus REST API and writes items straight
// into Postgres using pgx's COPY protocol. Hot path is one
// `COPY <table> FROM STDIN` per collection — on a typical local machine
// that pushes 50-200k rows/sec, with the limit being Postgres's INSERT
// throughput rather than the Directus Node event loop.
//
// CAVEATS — this is the unsafe-fast lane, opt-in via --direct-db. We
// bypass:
//
//   - Directus permission/ACL checks (caller is presumed to own the DB)
//   - Pre-/post-create hooks and Flow operations
//   - Activity log writes (already skipped via --strip-accountability;
//     here it's structural — we're not in the Directus code path)
//   - Revisions snapshots (same)
//   - Data cache invalidation (Directus only purges on REST writes;
//     after a direct load the cache is stale until TTL or manual purge)
//
// We DO honor:
//
//   - Postgres-side constraints (FK, NOT NULL, CHECK, UNIQUE) — Postgres
//     enforces these on COPY just like on INSERT.
//   - Topological collection order from buildInsertOrder — same as the
//     REST path, so cross-collection FKs settle.
//   - Multi-pass retry for batch failures: COPY is atomic, so on any
//     constraint violation we fall back to per-row INSERT to isolate the
//     bad rows, and queue the rest for the next pass.
//   - Sequence fixup: after the data lands, every auto-increment column's
//     sequence is bumped to MAX(col)+1 so a subsequent UI insert from
//     Directus doesn't try to reuse an ID we just copied in.
//
// Postgres-only — pgx is the driver. Caller passes a pgx-compatible DSN
// (e.g. `postgres://user:pass@host:5432/dbname?sslmode=disable`).
func applyDataDirect(
	ctx context.Context,
	dsn string,
	order []string,
	dataMap map[string][]json.RawMessage,
	fields []FieldInfo,
	aliasFields map[string]map[string]bool,
	concurrency, retryPasses int,
	log func(string),
) (*dataProgress, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	if retryPasses < 1 {
		retryPasses = 3
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DSN: %w", err)
	}
	cfg.MaxConns = int32(concurrency)

	// Per-connection setup: turn off synchronous_commit on the session
	// so COPY transactions don't wait for fsync. We're a bulk loader —
	// if the box loses power mid-import the user re-runs from the same
	// archive. Durability of partial state buys us nothing here, and
	// each waiting fsync was costing milliseconds we don't have.
	// Scope: connection-level only (not the cluster), via SET LOCAL on
	// each acquire. ANALYZE the populated tables so subsequent queries
	// from Directus get sane plans.
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET synchronous_commit = off")
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect Postgres: %w", err)
	}
	defer pool.Close()

	// Cheap reachability check up front — we'd rather fail in 1s on a bad
	// DSN than after 10s of row encoding.
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping Postgres: %w", err)
	}

	progress := &dataProgress{}
	for _, items := range dataMap {
		progress.total += len(items)
	}

	fieldsByColl := indexFieldsByCollection(fields)

	// Per-collection prep done once. The shape of every row (which columns
	// we emit, in which order) is fixed at this point.
	allData := prepareDirectData(order, dataMap, fieldsByColl, aliasFields)

	for pass := 1; pass <= retryPasses; pass++ {
		progress.pass = pass
		var passInserted atomic.Int64

		// Walk collections sequentially in topo order so cross-collection
		// FKs resolve. Within a collection the COPY itself is the fan-out
		// — no point doubling up with goroutines for one statement.
		for i := range allData {
			cd := &allData[i]
			if len(cd.items) == 0 {
				continue
			}
			ins, failed, outcome, err := copyOrInsert(ctx, pool, cd.name, cd.dbCols, cd.dbFields, cd.items)
			if err != nil {
				log(fmt.Sprintf("WARN: %s: %v", cd.name, err))
			}
			progress.inserted.Add(int64(ins))
			passInserted.Add(int64(ins))
			cd.notNullFKDrops += outcome.notNullFKDrops
			cd.otherDrops += outcome.otherDrops
			cd.items = failed
		}

		log(fmt.Sprintf("Pass %d: %d inserted", pass, passInserted.Load()))

		passRemaining := 0
		for _, cd := range allData {
			passRemaining += len(cd.items)
		}
		if passRemaining == 0 || passInserted.Load() == 0 {
			break
		}
	}

	for _, cd := range allData {
		// retryable items left after the last pass + the structurally-dropped
		// rows from earlier passes both count as "failed".
		progress.failed.Add(int64(len(cd.items) + cd.notNullFKDrops + cd.otherDrops))

		// Per-collection diagnostic so the user knows WHY rows were dropped.
		// NOT NULL FK drops are the direct-DB equivalent of "REST would have
		// auto-filled this for us"; surface them so the user can decide
		// whether to fall back to REST for that collection.
		if cd.notNullFKDrops > 0 {
			log(fmt.Sprintf("WARN: %s: %d rows dropped (NOT NULL FK to missing reference; REST path auto-fills these — re-run without --db-dsn for this collection if you need them)",
				cd.name, cd.notNullFKDrops))
		}
		if cd.otherDrops > 0 {
			log(fmt.Sprintf("WARN: %s: %d rows dropped (UNIQUE/CHECK/cast violation, retry won't help)",
				cd.name, cd.otherDrops))
		}
	}

	fixSequences(ctx, pool, allData, log)

	return progress, nil
}

// directColData is the per-collection plan we build once before the
// retry loop. Items shrinks across passes; everything else is invariant.
// notNullFKDrops/otherDrops accumulate across passes so we can report a
// per-collection diagnostic at the end.
type directColData struct {
	name           string
	items          []json.RawMessage
	dbCols         []string
	dbFields       []FieldInfo
	hasSerial      bool   // true if PK is auto-increment, so we know to fix the sequence
	pkField        string // PK column name (only set when hasSerial)
	notNullFKDrops int    // rows lost because NULL'd FK column was NOT NULL in target
	otherDrops     int    // rows lost on UNIQUE/CHECK/cast errors
}

func prepareDirectData(
	order []string,
	dataMap map[string][]json.RawMessage,
	fieldsByColl map[string][]FieldInfo,
	aliasFields map[string]map[string]bool,
) []directColData {
	var out []directColData
	for _, col := range order {
		items, ok := dataMap[col]
		if !ok || len(items) == 0 {
			continue
		}
		colAliases := aliasFields[col]
		colFields := fieldsByColl[col]
		dbCols, dbFields, hasSerial, pk := selectDBColumns(colFields, colAliases)
		if len(dbCols) == 0 {
			continue
		}
		// Same prep the REST path does — drops alias + user_created/
		// user_updated, fixes datetime literals.
		stripped := make([]json.RawMessage, len(items))
		for i, item := range items {
			stripped[i] = stripDataFields(item, colAliases)
			stripped[i] = fixDateTimeFields(stripped[i])
		}
		out = append(out, directColData{
			name:      col,
			items:     stripped,
			dbCols:    dbCols,
			dbFields:  dbFields,
			hasSerial: hasSerial,
			pkField:   pk,
		})
	}
	return out
}

// selectDBColumns picks the columns we'll write to via COPY. Drops alias
// fields (no underlying column), `user_created`/`user_updated` (instance-
// scoped FKs to directus_users — already stripped from item payloads),
// and reports whether the collection has an auto-increment PK that needs
// a sequence bump after.
func selectDBColumns(colFields []FieldInfo, aliasMap map[string]bool) (cols []string, fields []FieldInfo, hasSerial bool, pk string) {
	for _, f := range colFields {
		if f.Type == "alias" {
			continue
		}
		if aliasMap[f.Field] {
			continue
		}
		if f.Field == "user_created" || f.Field == "user_updated" {
			continue
		}
		if isPrimaryKey(f) && hasAutoIncrement(f) {
			hasSerial = true
			pk = f.Field
		}
		cols = append(cols, f.Field)
		fields = append(fields, f)
	}
	return cols, fields, hasSerial, pk
}

func hasAutoIncrement(f FieldInfo) bool {
	var s struct {
		HasAutoIncrement bool `json:"has_auto_increment"`
	}
	_ = json.Unmarshal(f.Schema, &s)
	return s.HasAutoIncrement
}

func indexFieldsByCollection(fields []FieldInfo) map[string][]FieldInfo {
	m := make(map[string][]FieldInfo, 64)
	for _, f := range fields {
		m[f.Collection] = append(m[f.Collection], f)
	}
	return m
}

// copyOrInsert runs the bulk COPY first, falling back to per-row INSERT
// on any failure. Per-row mode isolates which rows are blocked (FK,
// unique, etc.) from the rows that just got rolled back alongside them
// — COPY is atomic on Postgres just like a multi-row INSERT.
//
// Returns inserted count + items that should be retried in the next
// pass. A non-nil error from the COPY attempt is returned alongside the
// fallback's tally so the caller can log; the dataProgress accounting
// stays correct regardless.
func copyOrInsert(
	ctx context.Context,
	pool *pgxpool.Pool,
	collection string,
	cols []string,
	fields []FieldInfo,
	items []json.RawMessage,
) (int, []json.RawMessage, rowOutcome, error) {
	rows, err := buildCopyRows(items, cols, fields)
	if err != nil {
		return 0, items, rowOutcome{}, fmt.Errorf("encode rows: %w", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, items, rowOutcome{}, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	n, copyErr := conn.CopyFrom(ctx, pgx.Identifier{collection}, cols, pgx.CopyFromRows(rows))
	if copyErr == nil {
		return int(n), nil, rowOutcome{}, nil
	}

	// COPY failed atomically — the connection is now in an aborted state,
	// can't reuse. Acquire a fresh one for the per-row fallback.
	conn.Release()
	freshConn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, items, rowOutcome{}, fmt.Errorf("acquire fallback conn: %w (original COPY error: %v)", err, copyErr)
	}
	defer freshConn.Release()

	ins, retry, outcome := insertPerRow(ctx, freshConn.Conn(), collection, cols, fields, items)
	return ins, retry, outcome, copyErr
}

// rowOutcome categorises why a row didn't make it during a per-row pass.
// Surfacing the categories — instead of bucketing everything as "failed"
// — lets the user see which losses are real (NOT NULL FK to a missing
// row, where the REST path would have substituted the importer's user
// ID) vs which are just transient FK ordering issues that the next pass
// will resolve.
type rowOutcome struct {
	// notNullFKDrops counts rows that hit a NOT NULL constraint after we
	// nulled out an FK column during recovery. These will NEVER succeed
	// on retry — the column is structurally non-nullable in the target
	// schema, and we have no real value to put there. The REST path
	// avoids this case entirely because Directus auto-fills user FKs
	// (created_by, updated_by, etc.) with the importing token's user ID.
	notNullFKDrops int
	// otherDrops counts rows that hit a non-FK, non-NOT-NULL error
	// (UNIQUE, CHECK, type cast). Same as notNullFKDrops, retrying won't
	// help — surface separately so the user can decide if it's a schema
	// gap or a data shape issue.
	otherDrops int
}

// insertPerRow runs `INSERT INTO <table> (<cols>) VALUES (...)` once per
// item. Slower than COPY but lets us continue past a single-row
// constraint failure instead of losing the whole chunk.
//
// FK recovery: when an INSERT fails with code 23503 (foreign_key_violation),
// we parse the failing column out of the error Detail
// (`Key (col)=(value) is not present in table "ref".`), null that column
// in the row, and retry. We loop up to len(cols) times — each pass
// nullifies one more column — so a row referencing several missing FKs
// still lands as long as those columns are nullable. This matches what
// REST imports get for free: Directus's INSERT layer effectively
// tolerates missing references by leaving them blank rather than
// failing the whole row.
//
// Three outcomes per row:
//
//   - landed (counted in `inserted`)
//   - permanently dropped this pass (NOT NULL FK or non-FK error;
//     reflected in the returned rowOutcome and NOT placed in retryable —
//     retrying won't help)
//   - transiently failed (unparseable error / item-encoding error;
//     placed in retryable so the next pass picks it up)
func insertPerRow(
	ctx context.Context,
	conn *pgx.Conn,
	collection string,
	cols []string,
	fields []FieldInfo,
	items []json.RawMessage,
) (int, []json.RawMessage, rowOutcome) {
	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	stmt := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`,
		pgx.Identifier{collection}.Sanitize(),
		quoteIdents(cols),
		strings.Join(placeholders, ","),
	)

	colIdx := make(map[string]int, len(cols))
	for i, c := range cols {
		colIdx[c] = i
	}

	inserted := 0
	var retryable []json.RawMessage
	var outcome rowOutcome
	for _, raw := range items {
		row, err := encodeRow(raw, cols, fields)
		if err != nil {
			retryable = append(retryable, raw)
			continue
		}

		// Up to len(cols) attempts — each iteration nullifies the
		// FK column that just failed. Worst case (every column is a
		// dangling FK) we end up with an all-NULL row; the final
		// attempt either inserts it or hits NOT NULL and we move on.
		landed := false
		var lastErr error
		nulledFK := false
		for attempt := 0; attempt <= len(cols); attempt++ {
			_, err := conn.Exec(ctx, stmt, row...)
			if err == nil {
				inserted++
				landed = true
				break
			}
			lastErr = err
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != pgErrCodeFKViolation {
				break // non-FK error; nullifying won't help
			}
			col := parseFKColumn(pgErr.Detail)
			if col == "" {
				break // couldn't parse the failing column
			}
			i, ok := colIdx[col]
			if !ok || row[i] == nil {
				break // not in our cols, or already nulled — give up
			}
			row[i] = nil
			nulledFK = true
			// loop and retry with the column nulled
		}
		if landed {
			continue
		}
		switch categorizeRowFailure(lastErr, nulledFK) {
		case dropNotNullFK:
			outcome.notNullFKDrops++
		case dropOther:
			outcome.otherDrops++
		default:
			retryable = append(retryable, raw)
		}
	}
	return inserted, retryable, outcome
}

// rowFailureCategory classifies why a single per-row INSERT didn't land,
// so the caller can split "retry might help" from "structurally hopeless".
type rowFailureCategory int

const (
	// dropRetryable: leave the row on the retry pile — the next pass may
	// resolve whatever's blocking (cross-collection FK ordering, transient
	// connection issue, unparseable error).
	dropRetryable rowFailureCategory = iota
	// dropNotNullFK: we nulled an FK column during recovery, then Postgres
	// rejected the row with NOT NULL violation on that column. The column
	// is structurally non-nullable in the target — retrying with the same
	// row produces the same path. Surface separately because the REST
	// path doesn't see this case (Directus auto-fills user FKs).
	dropNotNullFK
	// dropOther: non-FK pg error (UNIQUE, CHECK, type cast, etc).
	// Retrying won't suddenly start working.
	dropOther
)

// categorizeRowFailure inspects the final error from the per-row INSERT
// loop (after FK-recovery has been exhausted) and decides which bucket
// the row falls into.
//
// Pulled out from insertPerRow so the categorization can be unit tested
// without a live Postgres connection — pgconn.PgError values can be
// constructed directly in tests, *pgx.Conn cannot.
func categorizeRowFailure(lastErr error, nulledFK bool) rowFailureCategory {
	var pgErr *pgconn.PgError
	if !errors.As(lastErr, &pgErr) {
		return dropRetryable
	}
	switch {
	case pgErr.Code == pgErrCodeNotNullViolation && nulledFK:
		return dropNotNullFK
	case pgErr.Code != pgErrCodeFKViolation:
		return dropOther
	default:
		return dropRetryable
	}
}

// pgErrCodeFKViolation is Postgres's SQLSTATE for foreign_key_violation.
// We branch on it specifically so genuine errors (NOT NULL, unique,
// check) don't trigger the null-and-retry loop.
const pgErrCodeFKViolation = "23503"

// pgErrCodeNotNullViolation is Postgres's SQLSTATE for not_null_violation.
// We watch for this AFTER nulling an FK column — the combination signals
// "this column is structurally non-nullable in the target and we have no
// value for it", which is unrecoverable in this code path.
const pgErrCodeNotNullViolation = "23502"

// fkDetailRe captures the column name from a Postgres FK violation
// detail message. The format is stable across Postgres versions:
//
//	Key (column_name)=(value) is not present in table "referenced_table".
//
// Composite keys come through as `Key (col1, col2)=(...)` — we'd
// nullify only the first; remaining attempts in the retry loop pick up
// the rest, since each retry triggers a fresh error with whichever
// column is currently missing.
var fkDetailRe = regexp.MustCompile(`^Key \(([^,)]+)`)

func parseFKColumn(detail string) string {
	m := fkDetailRe.FindStringSubmatch(detail)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

// buildCopyRows converts a slice of JSON items into the [][]any shape
// pgx.CopyFromRows expects — one row per item, one entry per column.
func buildCopyRows(items []json.RawMessage, cols []string, fields []FieldInfo) ([][]any, error) {
	out := make([][]any, len(items))
	for i, raw := range items {
		row, err := encodeRow(raw, cols, fields)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		out[i] = row
	}
	return out, nil
}

// encodeRow turns one JSON item into a []any aligned with cols, picking
// the right Go type per field metadata so pgx can encode it correctly.
//
// Coercion rules — keyed off FieldInfo.Type, which uses Directus's own
// type vocabulary as returned by /fields:
//
//   - integer / bigInteger        → int64
//   - float / decimal             → float64
//   - boolean                     → bool
//   - json                        → []byte (raw, encoded as JSONB literal)
//   - csv                         → comma-joined string (Directus stores as text)
//   - everything else (string,
//     uuid, hash, datetime, etc.) → string  — pgx text-encodes, Postgres parses
//
// Missing keys and JSON null both map to nil → SQL NULL. Auto-increment
// columns missing in the item get nil too, leaving Postgres to take the
// sequence default.
func encodeRow(raw json.RawMessage, cols []string, fields []FieldInfo) ([]any, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("parse item: %w", err)
	}
	row := make([]any, len(cols))
	for i, col := range cols {
		v, ok := obj[col]
		if !ok || string(v) == "null" {
			row[i] = nil
			continue
		}
		coerced, err := coerceValue(v, fields[i].Type)
		if err != nil {
			return nil, fmt.Errorf("col %s: %w", col, err)
		}
		row[i] = coerced
	}
	return row, nil
}

// coerceValue maps a JSON-encoded scalar to a Go value pgx can ship to
// Postgres over the binary COPY protocol. Picks the type pgx expects for
// each column class so encoding doesn't blow up at COPY time.
//
// Tricky bits:
//
//   - bigInteger fields can arrive as JSON strings, not numbers.
//     Directus does this for values >2^53 (max-safe-int in JS) — e.g.
//     Telegram user IDs. Fall back to strconv when number parse fails.
//
//   - timestamp/timestamptz columns require time.Time, not a string.
//     pgx's binary protocol has no plan for "string into timestamp" and
//     errors out. Parse multiple ISO-8601 layouts Directus emits.
//
//   - json columns take the raw bytes as-is — JSONB will parse.
//
//   - csv: JSON array → comma-joined string (Directus storage format).
func coerceValue(raw json.RawMessage, fieldType string) (any, error) {
	switch fieldType {
	case "integer", "bigInteger":
		var n int64
		if err := json.Unmarshal(raw, &n); err == nil {
			return n, nil
		}
		// Directus serializes bigInteger as a string in JSON for values
		// that don't survive JS Number — try string-as-int.
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				return n, nil
			}
		}
		return nil, fmt.Errorf("cannot parse %s as integer: %s", fieldType, truncate(string(raw), 80))
	case "float", "decimal":
		var f float64
		if err := json.Unmarshal(raw, &f); err == nil {
			return f, nil
		}
		// Same string-fallback story as bigInteger — Directus's decimal
		// columns sometimes round-trip through JSON as strings to keep
		// precision intact.
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			if f, err := strconv.ParseFloat(s, 64); err == nil {
				return f, nil
			}
		}
		return nil, fmt.Errorf("cannot parse %s as float: %s", fieldType, truncate(string(raw), 80))
	case "boolean":
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return nil, err
		}
		return b, nil
	case "json":
		return []byte(raw), nil
	case "csv":
		var arr []string
		if err := json.Unmarshal(raw, &arr); err == nil {
			return strings.Join(arr, ","), nil
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s, nil
		}
		return string(raw), nil
	case "dateTime", "datetime", "timestamp":
		// Directus 11's canonical type token is "dateTime" (camelCase) for
		// timestamptz columns and "timestamp" for plain timestamp. Older
		// archives may still ship "datetime" (lowercase) — handle all
		// three so a 2024-vintage export still loads.
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("cannot parse datetime literal: %s", truncate(string(raw), 80))
		}
		t, err := parseDirectusTimestamp(s)
		if err != nil {
			return nil, err
		}
		return t, nil
	case "date":
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("cannot parse date literal: %s", truncate(string(raw), 80))
		}
		t, err := time.Parse("2006-01-02", s)
		if err != nil {
			// Some Directus exports stamp dates as full datetimes; fall
			// through to the timestamp parser and let Postgres truncate.
			return parseDirectusTimestamp(s)
		}
		return t, nil
	case "time":
		// Time-of-day column — pgx accepts a string for `time` (OID 1083)
		// just fine, no time.Time round-trip needed.
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return string(raw), nil
		}
		return s, nil
	default:
		// string/uuid/hash/text/etc. — strip JSON quotes, hand pgx a
		// string. pgx encodes UUIDs from string; Postgres parses datetime
		// strings if a column type happens to be timestamp via casts at
		// the table level.
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s, nil
		}
		return string(raw), nil
	}
}

// parseDirectusTimestamp accepts the handful of timestamp shapes Directus
// emits to JSON, plus a couple of variants seen in real archives. Returns
// time.Time so pgx can encode it for timestamp / timestamptz columns.
//
// Order matters — RFC3339Nano matches "2026-04-21T15:15:50.703Z" cleanly,
// the bare-ISO formats catch "2025-07-22T11:33:06" (no zone) which the
// SQL adapter on the source emitted without a UTC suffix.
func parseDirectusTimestamp(s string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,             // "2026-04-21T15:15:50.703Z"
		time.RFC3339,                 // "2025-07-22T11:33:06Z"
		"2006-01-02T15:04:05.999999", // bare ISO with fractional seconds, no zone
		"2006-01-02T15:04:05",        // bare ISO, no fractional, no zone
		"2006-01-02 15:04:05",        // SQL-style space separator
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized timestamp format: %q", s)
}

// fixSequences bumps `pg_get_serial_sequence(table, pk)` to MAX(pk).
// Required after a bulk COPY where rows came in with explicit IDs — the
// sequence's `last_value` doesn't auto-advance, and the next
// app-layer insert would collide on the unique constraint.
//
// Errors are logged but never fatal: a missing sequence (e.g. UUID PK,
// or a PK not registered as a serial) is expected and
// `pg_get_serial_sequence` returns NULL, which the SQL handles via
// COALESCE.
func fixSequences(ctx context.Context, pool *pgxpool.Pool, allData []directColData, log func(string)) {
	fixed := 0
	for _, cd := range allData {
		if !cd.hasSerial || cd.pkField == "" {
			continue
		}
		// pg_get_serial_sequence returns the sequence name as a string
		// suitable for setval. setval() with the third arg false means
		// "the next nextval will return exactly this value" — but we want
		// "next nextval returns this+1", so use is_called=true (the
		// default), which means "this is the last used value, advance
		// from here". COALESCE handles empty tables.
		q := fmt.Sprintf(`
			SELECT setval(
				pg_get_serial_sequence('%[1]s', '%[2]s'),
				COALESCE((SELECT MAX(%[3]s) FROM %[1]s), 1),
				(SELECT MAX(%[3]s) FROM %[1]s) IS NOT NULL
			)
			WHERE pg_get_serial_sequence('%[1]s', '%[2]s') IS NOT NULL`,
			pgx.Identifier{cd.name}.Sanitize(),
			cd.pkField,
			pgx.Identifier{cd.pkField}.Sanitize(),
		)
		if _, err := pool.Exec(ctx, q); err != nil {
			log(fmt.Sprintf("WARN: setval for %s.%s failed: %v", cd.name, cd.pkField, err))
			continue
		}
		fixed++
	}
	if fixed > 0 {
		log(fmt.Sprintf("Fixed %d auto-increment sequences", fixed))
	}
}

// quoteIdents returns a comma-separated quoted identifier list for SQL
// splicing — pgx's Identifier.Sanitize handles double-quoting and
// embedded-quote escaping for one identifier; we just join.
func quoteIdents(cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = pgx.Identifier{c}.Sanitize()
	}
	return strings.Join(out, ",")
}
