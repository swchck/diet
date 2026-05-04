package main

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestCoerceValue_Integer(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`42`), "integer")
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(42) {
		t.Errorf("got %v (%T), want int64(42)", got, got)
	}
}

func TestCoerceValue_BigInteger(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`9007199254740993`), "bigInteger")
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(9007199254740993) {
		t.Errorf("got %v, want 9007199254740993", got)
	}
}

func TestCoerceValue_Float(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`3.14`), "float")
	if err != nil {
		t.Fatal(err)
	}
	if got != 3.14 {
		t.Errorf("got %v, want 3.14", got)
	}
}

func TestCoerceValue_Boolean(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`true`), "boolean")
	if err != nil {
		t.Fatal(err)
	}
	if got != true {
		t.Errorf("got %v, want true", got)
	}
}

func TestCoerceValue_JSON_PassesRawBytes(t *testing.T) {
	raw := json.RawMessage(`{"nested":{"a":1}}`)
	got, err := coerceValue(raw, "json")
	if err != nil {
		t.Fatal(err)
	}
	bytes, ok := got.([]byte)
	if !ok {
		t.Fatalf("got %T, want []byte", got)
	}
	if string(bytes) != string(raw) {
		t.Errorf("got %q, want %q", bytes, raw)
	}
}

func TestCoerceValue_CSV_FromArray(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`["a","b","c"]`), "csv")
	if err != nil {
		t.Fatal(err)
	}
	if got != "a,b,c" {
		t.Errorf("got %q, want %q", got, "a,b,c")
	}
}

func TestCoerceValue_CSV_FromString(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`"already,joined"`), "csv")
	if err != nil {
		t.Fatal(err)
	}
	if got != "already,joined" {
		t.Errorf("got %q", got)
	}
}

func TestCoerceValue_Datetime_RFC3339Nano(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`"2026-05-04T12:34:56.789Z"`), "datetime")
	if err != nil {
		t.Fatal(err)
	}
	tm, ok := got.(time.Time)
	if !ok {
		t.Fatalf("got %T, want time.Time (pgx binary protocol needs it)", got)
	}
	if tm.Year() != 2026 || tm.Month() != 5 || tm.Day() != 4 {
		t.Errorf("date wrong: %v", tm)
	}
}

func TestCoerceValue_Datetime_BareISONoZone(t *testing.T) {
	// Some sources emit timestamps without a Z or offset. Real archives
	// from Directus/Postgres do this for `timestamp` (no tz) columns.
	got, err := coerceValue(json.RawMessage(`"2025-07-22T11:33:06"`), "datetime")
	if err != nil {
		t.Fatal(err)
	}
	tm, ok := got.(time.Time)
	if !ok {
		t.Fatalf("got %T, want time.Time", got)
	}
	if tm.Hour() != 11 || tm.Minute() != 33 || tm.Second() != 6 {
		t.Errorf("time wrong: %v", tm)
	}
}

func TestCoerceValue_Date_ParsesShortFormat(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`"2025-12-25"`), "date")
	if err != nil {
		t.Fatal(err)
	}
	tm, ok := got.(time.Time)
	if !ok {
		t.Fatalf("got %T, want time.Time", got)
	}
	if tm.Year() != 2025 || tm.Day() != 25 {
		t.Errorf("date wrong: %v", tm)
	}
}

func TestCoerceValue_Time_PassesAsString(t *testing.T) {
	// pgx's binary plan for the `time` OID accepts strings — no
	// time.Time round-trip needed and Go's time.Time would lose the
	// "no date" semantics anyway.
	got, err := coerceValue(json.RawMessage(`"15:04:05"`), "time")
	if err != nil {
		t.Fatal(err)
	}
	if got != "15:04:05" {
		t.Errorf("got %v, want raw string for time-of-day", got)
	}
}

func TestCoerceValue_Datetime_BadFormatErrors(t *testing.T) {
	_, err := coerceValue(json.RawMessage(`"definitely not a date"`), "datetime")
	if err == nil {
		t.Error("expected error on unrecognized timestamp")
	}
}

func TestCoerceValue_BigInteger_FromJSONString(t *testing.T) {
	// Directus emits big ints as strings (Telegram tg_id, etc.) when
	// they exceed JS's max-safe-int (2^53). Must round-trip via strconv.
	got, err := coerceValue(json.RawMessage(`"7654321098765432100"`), "bigInteger")
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(7654321098765432100) {
		t.Errorf("got %v (%T), want int64(7654321098765432100)", got, got)
	}
}

func TestCoerceValue_Integer_NumericPathStillWorks(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`12345`), "integer")
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(12345) {
		t.Errorf("got %v", got)
	}
}

func TestCoerceValue_Float_FromJSONString(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`"3.14159"`), "float")
	if err != nil {
		t.Fatal(err)
	}
	if got != 3.14159 {
		t.Errorf("got %v", got)
	}
}

func TestCoerceValue_Integer_BadStringErrors(t *testing.T) {
	_, err := coerceValue(json.RawMessage(`"not a number"`), "integer")
	if err == nil {
		t.Error("expected error on unparseable string")
	}
}

func TestCoerceValue_String_StripsQuotes(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`"hello"`), "string")
	if err != nil {
		t.Fatal(err)
	}
	if got != "hello" {
		t.Errorf("got %q, want hello (no JSON quotes)", got)
	}
}

func TestCoerceValue_UUID_StripsQuotes(t *testing.T) {
	got, err := coerceValue(json.RawMessage(`"550e8400-e29b-41d4-a716-446655440000"`), "uuid")
	if err != nil {
		t.Fatal(err)
	}
	if got != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("got %q", got)
	}
}

func TestEncodeRow_AlignedWithCols(t *testing.T) {
	cols := []string{"id", "name", "active", "settings"}
	fields := []FieldInfo{
		{Field: "id", Type: "integer"},
		{Field: "name", Type: "string"},
		{Field: "active", Type: "boolean"},
		{Field: "settings", Type: "json"},
	}
	raw := json.RawMessage(`{"id":7,"name":"hello","active":true,"settings":{"k":"v"}}`)
	row, err := encodeRow(raw, cols, fields)
	if err != nil {
		t.Fatal(err)
	}
	if len(row) != 4 {
		t.Fatalf("got %d cols, want 4", len(row))
	}
	if row[0] != int64(7) {
		t.Errorf("col 0: %v (%T)", row[0], row[0])
	}
	if row[1] != "hello" {
		t.Errorf("col 1: %v", row[1])
	}
	if row[2] != true {
		t.Errorf("col 2: %v", row[2])
	}
	bytes, ok := row[3].([]byte)
	if !ok || string(bytes) != `{"k":"v"}` {
		t.Errorf("col 3 (json): %v (%T)", row[3], row[3])
	}
}

func TestEncodeRow_MissingKeyBecomesNil(t *testing.T) {
	cols := []string{"id", "optional"}
	fields := []FieldInfo{
		{Field: "id", Type: "integer"},
		{Field: "optional", Type: "string"},
	}
	raw := json.RawMessage(`{"id":1}`) // optional missing
	row, err := encodeRow(raw, cols, fields)
	if err != nil {
		t.Fatal(err)
	}
	if row[1] != nil {
		t.Errorf("missing key should be nil, got %v", row[1])
	}
}

func TestEncodeRow_ExplicitNull(t *testing.T) {
	cols := []string{"id", "value"}
	fields := []FieldInfo{
		{Field: "id", Type: "integer"},
		{Field: "value", Type: "string"},
	}
	raw := json.RawMessage(`{"id":1,"value":null}`)
	row, err := encodeRow(raw, cols, fields)
	if err != nil {
		t.Fatal(err)
	}
	if row[1] != nil {
		t.Errorf("JSON null should be nil, got %v", row[1])
	}
}

func TestEncodeRow_MalformedItem(t *testing.T) {
	_, err := encodeRow(json.RawMessage(`not json`),
		[]string{"x"}, []FieldInfo{{Field: "x", Type: "string"}})
	if err == nil {
		t.Error("expected error on malformed JSON")
	}
}

func TestSelectDBColumns_SkipsAliasAndAuditFields(t *testing.T) {
	colFields := []FieldInfo{
		{Collection: "posts", Field: "id", Type: "integer",
			Schema: json.RawMessage(`{"is_primary_key":true,"has_auto_increment":true}`)},
		{Collection: "posts", Field: "title", Type: "string",
			Schema: json.RawMessage(`{}`)},
		{Collection: "posts", Field: "tags", Type: "alias"}, // O2M virtual
		{Collection: "posts", Field: "user_created", Type: "uuid"},
		{Collection: "posts", Field: "user_updated", Type: "uuid"},
		{Collection: "posts", Field: "explicit_alias", Type: "string"}, // alias-marked from buildAliasFields
	}
	aliasMap := map[string]bool{"explicit_alias": true}

	cols, fields, hasSerial, pk := selectDBColumns(colFields, aliasMap)

	wantCols := []string{"id", "title"}
	if !reflect.DeepEqual(cols, wantCols) {
		t.Errorf("cols = %v, want %v", cols, wantCols)
	}
	if len(fields) != 2 {
		t.Errorf("got %d fields, want 2", len(fields))
	}
	if !hasSerial {
		t.Error("hasSerial = false, want true (id has has_auto_increment)")
	}
	if pk != "id" {
		t.Errorf("pk = %q, want id", pk)
	}
}

func TestSelectDBColumns_NonAutoIncrementPK(t *testing.T) {
	colFields := []FieldInfo{
		{Field: "key", Type: "string",
			Schema: json.RawMessage(`{"is_primary_key":true}`)},
		{Field: "value", Type: "string", Schema: json.RawMessage(`{}`)},
	}
	cols, _, hasSerial, _ := selectDBColumns(colFields, nil)
	if len(cols) != 2 {
		t.Errorf("got %d cols, want 2", len(cols))
	}
	if hasSerial {
		t.Error("hasSerial = true, want false (no has_auto_increment)")
	}
}

func TestHasAutoIncrement(t *testing.T) {
	yes := FieldInfo{Schema: json.RawMessage(`{"has_auto_increment":true}`)}
	no := FieldInfo{Schema: json.RawMessage(`{"has_auto_increment":false}`)}
	missing := FieldInfo{Schema: json.RawMessage(`{}`)}
	null := FieldInfo{Schema: json.RawMessage(`null`)}

	if !hasAutoIncrement(yes) {
		t.Error("yes case failed")
	}
	if hasAutoIncrement(no) {
		t.Error("no case failed")
	}
	if hasAutoIncrement(missing) {
		t.Error("missing case failed")
	}
	if hasAutoIncrement(null) {
		t.Error("null schema case failed")
	}
}

func TestPrepareDirectData_RespectsTopologicalOrder(t *testing.T) {
	order := []string{"users", "posts", "comments"}
	dataMap := map[string][]json.RawMessage{
		"comments": {json.RawMessage(`{"id":1}`)},
		"posts":    {json.RawMessage(`{"id":1}`)},
		"users":    {json.RawMessage(`{"id":1}`)},
	}
	fieldsByColl := map[string][]FieldInfo{
		"users":    {{Field: "id", Type: "integer", Schema: json.RawMessage(`{"is_primary_key":true}`)}},
		"posts":    {{Field: "id", Type: "integer", Schema: json.RawMessage(`{"is_primary_key":true}`)}},
		"comments": {{Field: "id", Type: "integer", Schema: json.RawMessage(`{"is_primary_key":true}`)}},
	}
	out := prepareDirectData(order, dataMap, fieldsByColl, nil)
	if len(out) != 3 {
		t.Fatalf("got %d, want 3", len(out))
	}
	gotOrder := []string{out[0].name, out[1].name, out[2].name}
	if !reflect.DeepEqual(gotOrder, order) {
		t.Errorf("order = %v, want %v (must match topo input)", gotOrder, order)
	}
}

func TestPrepareDirectData_SkipsEmptyAndAliasOnly(t *testing.T) {
	order := []string{"empty", "alias_only", "real"}
	dataMap := map[string][]json.RawMessage{
		"empty":      {}, // no items
		"alias_only": {json.RawMessage(`{"id":1}`)},
		"real":       {json.RawMessage(`{"id":1}`)},
	}
	fieldsByColl := map[string][]FieldInfo{
		"alias_only": {{Field: "x", Type: "alias"}}, // only alias → no DB cols
		"real":       {{Field: "id", Type: "integer", Schema: json.RawMessage(`{}`)}},
	}
	out := prepareDirectData(order, dataMap, fieldsByColl, nil)
	if len(out) != 1 || out[0].name != "real" {
		t.Errorf("got %d entries: %+v, want only 'real'", len(out), out)
	}
}

func TestQuoteIdents(t *testing.T) {
	got := quoteIdents([]string{"id", "title", "weird name"})
	// pgx.Identifier.Sanitize wraps each in double quotes, escaping internal quotes
	want := `"id","title","weird name"`
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestParseFKColumn(t *testing.T) {
	tests := []struct {
		name   string
		detail string
		want   string
	}{
		{
			name:   "single column",
			detail: `Key (banned_by)=(550e8400-...) is not present in table "directus_users".`,
			want:   "banned_by",
		},
		{
			name:   "composite key — picks first column",
			detail: `Key (col_a, col_b)=(1, 2) is not present in table "other".`,
			want:   "col_a",
		},
		{
			name:   "snake_case identifier",
			detail: `Key (clicker_passive_profit_id)=(123) is not present in table "clicker_passive_profit".`,
			want:   "clicker_passive_profit_id",
		},
		{
			name:   "garbage input — empty",
			detail: `something completely different`,
			want:   "",
		},
		{
			name:   "empty string",
			detail: ``,
			want:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseFKColumn(tt.detail)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestIndexFieldsByCollection(t *testing.T) {
	fields := []FieldInfo{
		{Collection: "a", Field: "x"},
		{Collection: "a", Field: "y"},
		{Collection: "b", Field: "z"},
	}
	idx := indexFieldsByCollection(fields)
	if len(idx["a"]) != 2 {
		t.Errorf("a: got %d, want 2", len(idx["a"]))
	}
	if len(idx["b"]) != 1 {
		t.Errorf("b: got %d, want 1", len(idx["b"]))
	}
	if len(idx["nonexistent"]) != 0 {
		t.Errorf("missing: should be empty")
	}
}
