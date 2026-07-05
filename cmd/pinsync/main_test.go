package main

import (
	"errors"
	"flag"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/rolesanywhere"
)

func TestParseArgs(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		wantErr string // substring; empty means success
	}{
		{"no subcommand", nil, "missing subcommand"},
		{"unknown subcommand", []string{"sync"}, "unknown subcommand"},
		{"missing bucket", []string{"push", "root"}, "-bucket is required"},
		{"missing positional", []string{"push", "-bucket", "b"}, "root directory"},
		{"pull missing positional", []string{"pull", "-bucket", "b"}, "destination directory"},
		{"extra positional", []string{"pull", "-bucket", "b", "d1", "d2"}, "exactly one"},
		{"valid push", []string{"push", "-bucket", "b", "-prefix", "p", "root"}, ""},
		{"valid pull", []string{"pull", "-bucket", "b", "-endpoint-url", "http://localhost:9000", "dest"}, ""},
		{"ra flag on push rejected", []string{"push", "-bucket", "b", "-ra-trust-anchor-arn", "arn:x", "root"}, "not defined"},
		{"ra bad cert-field", []string{"pull", "-bucket", "b", "-ra-trust-anchor-arn", "a", "-ra-profile-arn", "p", "-ra-role-arn", "r", "-ra-cert-pattern", "x", "-ra-cert-field", "org", "dest"}, "invalid certificate field"},
		{"ra bad cert-store", []string{"pull", "-bucket", "b", "-ra-trust-anchor-arn", "a", "-ra-profile-arn", "p", "-ra-role-arn", "r", "-ra-cert-pattern", "x", "-ra-cert-store", "global", "dest"}, "invalid certificate store"},
		{"ra bad regex", []string{"pull", "-bucket", "b", "-ra-trust-anchor-arn", "a", "-ra-profile-arn", "p", "-ra-role-arn", "r", "-ra-cert-pattern", "[", "dest"}, "invalid -ra-cert-pattern"},
		{"ra cert-field alone triggers mode", []string{"pull", "-bucket", "b", "-ra-cert-field", "subject", "dest"}, "-ra-trust-anchor-arn"},
		{"bad log-level", []string{"pull", "-bucket", "b", "-log-level", "bogus", "dest"}, `invalid -log-level "bogus"`},
		{"bad log-format", []string{"pull", "-bucket", "b", "-log-format", "yaml", "dest"}, `invalid -log-format "yaml"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := parseArgs(tc.args, io.Discard)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("parseArgs(%v): %v", tc.args, err)
				}
				if c.dir == "" || c.bucket == "" {
					t.Errorf("parseArgs(%v) = %+v, missing dir/bucket", tc.args, c)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("parseArgs(%v) = %v, want error containing %q", tc.args, err, tc.wantErr)
			}
		})
	}
}

func TestParseArgsHelp(t *testing.T) {
	_, err := parseArgs([]string{"push", "-h"}, io.Discard)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseArgs(-h) = %v, want flag.ErrHelp", err)
	}
}

// TestParseArgsRAMissingNamesAll verifies that a partial -ra-* set reports
// every missing required flag in one error, not just the first — an operator
// fixes one flag at a time otherwise.
func TestParseArgsRAMissingNamesAll(t *testing.T) {
	_, err := parseArgs([]string{"pull", "-bucket", "b", "-ra-trust-anchor-arn", "arn:ta", "dest"}, io.Discard)
	if err == nil {
		t.Fatal("parseArgs with only -ra-trust-anchor-arn = nil, want missing-flags error")
	}
	for _, want := range []string{"-ra-profile-arn", "-ra-role-arn", "-ra-cert-pattern"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name missing flag %q", err, want)
		}
	}
}

// TestParseArgsRAValid checks that a full valid RA set parses, activates RA
// mode, compiles the pattern, and applies the field/store defaults.
func TestParseArgsRAValid(t *testing.T) {
	c, err := parseArgs([]string{
		"pull", "-bucket", "b",
		"-ra-trust-anchor-arn", "arn:aws:rolesanywhere:us-east-1:1:trust-anchor/t",
		"-ra-profile-arn", "arn:aws:rolesanywhere:us-east-1:1:profile/p",
		"-ra-role-arn", "arn:aws:iam::1:role/r",
		"-ra-cert-pattern", "dev.ce",
		"dest",
	}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !c.raMode {
		t.Error("raMode = false, want true")
	}
	if c.raRegex == nil || !c.raRegex.MatchString("device") {
		t.Errorf("raRegex = %v, want compiled pattern matching \"device\"", c.raRegex)
	}
	if c.raField != rolesanywhere.FieldSubject {
		t.Errorf("raField = %v, want default FieldSubject", c.raField)
	}
	if c.raStore != rolesanywhere.StoreUser {
		t.Errorf("raStore = %v, want default StoreUser", c.raStore)
	}
}

func TestParseArgsDefaults(t *testing.T) {
	c, err := parseArgs([]string{"pull", "-bucket", "b", "dest"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.parallel != 16 {
		t.Errorf("parallel = %d, want default 16", c.parallel)
	}
	if c.prefix != "" {
		t.Errorf("unexpected non-zero defaults: %+v", c)
	}
	if c.logLevel != "info" || c.logFormat != "text" {
		t.Errorf("log defaults = %q/%q, want info/text", c.logLevel, c.logFormat)
	}
	if c.logLevelVal != slog.LevelInfo {
		t.Errorf("logLevelVal = %v, want LevelInfo", c.logLevelVal)
	}
}
