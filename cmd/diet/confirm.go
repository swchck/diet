package main

import (
	"bufio"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// confirmUnsafe prompts the user to type the host of `targetURL` to confirm
// a destructive operation. Returns true on match, false on anything else
// (including stdin EOF, mismatch, "no", empty line, or any error).
//
// Why typed-host instead of y/N: the unsafe flag exists because the user
// already knows this profile is dangerous (prod, shared sandbox, etc.).
// A `yes`-prompt is too easy to muscle-memory through; typing the host
// forces a half-second of "wait, am I sure I'm pointed at the right
// instance?". It also makes `yes | diet ...` fail closed on profiles
// flagged unsafe.
//
// `out` is where the prompt is rendered, `in` is where the response is
// read from. Production callers pass os.Stdout / os.Stdin. Tests pass
// buffers so prompt rendering and matching can be exercised without
// touching the real terminal.
func confirmUnsafe(out io.Writer, in io.Reader, operation, targetURL string) bool {
	host := hostOf(targetURL)
	if host == "" {
		// Couldn't parse a host out of the URL — fail closed. We won't
		// prompt because there's nothing meaningful to type, and we
		// don't want to silently let the operation continue against an
		// unsafe profile we couldn't validate.
		fmt.Fprintf(out, "%s could not parse host from %q — aborting unsafe %s.\n",
			warnIcon(), targetURL, operation)
		return false
	}

	warn := lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Bold(true)
	fmt.Fprintln(out)
	fmt.Fprintln(out, warn.Render(fmt.Sprintf("⚠  This profile is marked UNSAFE.")))
	fmt.Fprintf(out, "About to run %s against:\n", strings.ToUpper(operation))
	fmt.Fprintf(out, "    %s\n\n", targetURL)
	fmt.Fprintf(out, "Type the host (%s) to confirm, or anything else to abort:\n> ", host)

	reader := bufio.NewReader(in)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		fmt.Fprintf(out, "%s read failed: %v — aborting.\n", warnIcon(), err)
		return false
	}
	got := strings.TrimSpace(line)
	if got != host {
		fmt.Fprintln(out, "Host did not match — aborting.")
		return false
	}
	return true
}

// hostOf returns the bare host portion of a Directus URL ("example.com"
// from "https://example.com:8055/admin"). Returns empty string on parse
// failure, in which case the caller should fail closed.
func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	h := u.Hostname()
	if h == "" {
		// Bare host without scheme — url.Parse won't populate Hostname.
		// Strip any path / port and use the leading token.
		h = strings.TrimSpace(rawURL)
		if i := strings.IndexAny(h, "/:"); i >= 0 {
			h = h[:i]
		}
	}
	return strings.ToLower(h)
}

func warnIcon() string {
	return lipgloss.NewStyle().Foreground(lipgloss.Color("214")).Render("⚠")
}

// confirmUnsafeFromTTY is the convenience wrapper used by the wizard
// dispatch. It reads from the real terminal — tests should call
// confirmUnsafe directly with buffered streams.
func confirmUnsafeFromTTY(operation, targetURL string) bool {
	return confirmUnsafe(os.Stdout, os.Stdin, operation, targetURL)
}
