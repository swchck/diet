package main

import (
	"strings"
	"testing"
)

// TestClassifyImportOutcome covers the policy matrix for whether an
// import should report success, catastrophic failure, or strict-mode
// failure. The function is the single point that decides exit code, so
// every cell of the truth table needs to be locked down.
func TestClassifyImportOutcome(t *testing.T) {
	tests := []struct {
		name                                   string
		strict                                 bool
		dataInserted, dataTotal                int
		sysInserted, sysFailed                 int
		wantErr                                bool
		wantHasCatastrophe, wantHasStrictPhrase bool
	}{
		{
			name:         "no data, no system, success",
			strict:       false,
			dataTotal:    0,
			dataInserted: 0,
			wantErr:      false,
		},
		{
			name:         "all data inserted, no system, success",
			dataInserted: 100, dataTotal: 100,
			wantErr: false,
		},
		{
			name: "partial data loss, lenient mode, success (legacy behavior)",
			// 99 of 100 — historical "log and continue" stance.
			dataInserted: 99, dataTotal: 100,
			wantErr: false,
		},
		{
			name: "partial data loss, strict mode, fail",
			// Same scenario as above but --strict trips it.
			strict:       true,
			dataInserted: 99, dataTotal: 100,
			wantErr:             true,
			wantHasStrictPhrase: true,
		},
		{
			name: "catastrophe: 0 of N data inserted, lenient still fails",
			// Even without --strict this is hard-fail — no realistic
			// caller wants exit 0 here.
			dataInserted: 0, dataTotal: 158000,
			wantErr:            true,
			wantHasCatastrophe: true,
		},
		{
			name:         "catastrophe overrides strict",
			strict:       true,
			dataInserted: 0, dataTotal: 100,
			wantErr:            true,
			wantHasCatastrophe: true,
		},
		{
			name: "system failures only, strict mode, fail",
			// No data phase but several system items dropped.
			strict:    true,
			sysFailed: 3,
			wantErr:   true,
		},
		{
			name: "system failures only, lenient, success",
			// Documenting the historical permissive behavior.
			sysFailed: 3,
			wantErr:   false,
		},
		{
			name: "data success + system failures, lenient, success",
			// dataLoss=0, sysFailed>0 — lenient mode doesn't care.
			dataInserted: 50, dataTotal: 50,
			sysFailed: 5,
			wantErr:   false,
		},
		{
			name: "data success + system failures, strict, fail",
			// strict catches the system-only loss.
			strict:       true,
			dataInserted: 50, dataTotal: 50,
			sysFailed: 5,
			wantErr:   true,
		},
		{
			name: "schema-only import (no data, no system), success",
			// dataTotal=0 short-circuits the catastrophe check; nothing else
			// to fail.
			strict:    true,
			dataTotal: 0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyImportOutcome(
				tt.strict,
				tt.dataInserted, tt.dataTotal,
				tt.sysInserted, tt.sysFailed,
			)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				return
			}
			msg := err.Error()
			if tt.wantHasCatastrophe && !strings.Contains(msg, "0 of") {
				t.Errorf("expected catastrophe phrasing in %q", msg)
			}
			if tt.wantHasStrictPhrase && !strings.Contains(msg, "strict mode") {
				t.Errorf("expected strict-mode phrasing in %q", msg)
			}
		})
	}
}
