package main

import (
	"bytes"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestHostOf(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"https://directus.example.com", "directus.example.com"},
		{"https://directus.example.com:8055", "directus.example.com"},
		{"https://directus.example.com:8055/admin", "directus.example.com"},
		{"http://localhost:8055", "localhost"},
		{"directus.example.com", "directus.example.com"},      // no scheme
		{"directus.example.com:8055/x", "directus.example.com"}, // bare host with port + path
		{"HTTPS://EXAMPLE.COM", "example.com"},                // case-insensitive
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := hostOf(tc.in); got != tc.want {
				t.Errorf("hostOf(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestConfirmUnsafe_AcceptsExactHost — typing the host of the URL on
// stdin returns true. The match is case-insensitive on the hostOf side.
func TestConfirmUnsafe_AcceptsExactHost(t *testing.T) {
	var out bytes.Buffer
	in := strings.NewReader("directus.example.com\n")
	got := confirmUnsafe(&out, in, "import", "https://directus.example.com:8055")
	if !got {
		t.Errorf("confirmUnsafe = false, want true (correct host typed)")
	}
	if !strings.Contains(out.String(), "UNSAFE") {
		t.Errorf("prompt missing UNSAFE warning: %s", out.String())
	}
	if !strings.Contains(out.String(), "directus.example.com:8055") {
		t.Errorf("prompt missing target URL: %s", out.String())
	}
}

func TestConfirmUnsafe_RejectsEmpty(t *testing.T) {
	var out bytes.Buffer
	in := strings.NewReader("\n")
	if confirmUnsafe(&out, in, "import", "https://x.example.com") {
		t.Error("empty input should NOT confirm")
	}
}

func TestConfirmUnsafe_RejectsYes(t *testing.T) {
	// `yes` is the y/N muscle-memory we explicitly want to defeat.
	var out bytes.Buffer
	in := strings.NewReader("yes\n")
	if confirmUnsafe(&out, in, "import", "https://x.example.com") {
		t.Error("\"yes\" should NOT confirm — must type host")
	}
}

func TestConfirmUnsafe_RejectsWrongHost(t *testing.T) {
	var out bytes.Buffer
	in := strings.NewReader("staging.example.com\n")
	if confirmUnsafe(&out, in, "import", "https://prod.example.com") {
		t.Error("wrong host should NOT confirm")
	}
}

// TestConfirmUnsafe_HandlesEOF — pipe `yes | diet ...` style automation
// closes stdin before we read. We must abort, not block.
func TestConfirmUnsafe_HandlesEOF(t *testing.T) {
	var out bytes.Buffer
	in := strings.NewReader("") // immediate EOF
	if confirmUnsafe(&out, in, "import", "https://x.example.com") {
		t.Error("EOF should NOT confirm")
	}
}

// TestConfirmUnsafe_FailsClosedOnUnparseableURL — when we can't extract a
// host (empty URL, malformed), bail out without prompting. Allowing the
// op to proceed silently against a profile flagged unsafe is exactly what
// the flag is supposed to prevent.
func TestConfirmUnsafe_FailsClosedOnUnparseableURL(t *testing.T) {
	var out bytes.Buffer
	in := strings.NewReader("anything\n")
	if confirmUnsafe(&out, in, "import", "") {
		t.Error("empty URL must fail closed (no host to type)")
	}
	if !strings.Contains(out.String(), "could not parse host") {
		t.Errorf("expected user-visible explanation, got: %s", out.String())
	}
}

// TestProfile_UnsafeYAMLRoundTrip — config writers/readers must serialize
// the unsafe flag stably. `omitempty` keeps it out of profiles where it
// wasn't set so the YAML stays diff-friendly.
func TestProfile_UnsafeYAMLRoundTrip(t *testing.T) {
	src := profile{URL: "https://prod", Token: "tok", Unsafe: true}
	out, err := yaml.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(out), "unsafe: true") {
		t.Errorf("missing unsafe in YAML: %s", out)
	}
	var got profile
	if err := yaml.Unmarshal(out, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.Unsafe {
		t.Errorf("Unsafe lost on round-trip: %+v", got)
	}

	// Default false — must NOT appear in YAML when omitempty does its job.
	defaultOut, _ := yaml.Marshal(profile{URL: "x", Token: "t"})
	if strings.Contains(string(defaultOut), "unsafe") {
		t.Errorf("unsafe leaked into default profile YAML: %s", defaultOut)
	}
}
