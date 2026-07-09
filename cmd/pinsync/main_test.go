package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/push"
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
		{"push with full", []string{"push", "-bucket", "b", "-full", "root"}, ""},
		{"full flag on pull rejected", []string{"pull", "-bucket", "b", "-full", "dest"}, "not defined"},
		{"push with adopt", []string{"push", "-bucket", "b", "-adopt", "root"}, ""},
		{"adopt with full rejected", []string{"push", "-bucket", "b", "-adopt", "-full", "root"}, "mutually exclusive"},
		{"adopt flag on pull rejected", []string{"pull", "-bucket", "b", "-adopt", "dest"}, "not defined"},
		{"dry-run on push", []string{"push", "-bucket", "b", "-dry-run", "root"}, ""},
		{"dry-run on pull", []string{"pull", "-bucket", "b", "-dry-run", "dest"}, ""},
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

// TestParseArgsFull confirms -full parses on push and lands on the cli struct.
func TestParseArgsFull(t *testing.T) {
	c, err := parseArgs([]string{"push", "-bucket", "b", "-full", "root"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !c.full {
		t.Error("full = false, want true")
	}
}

// TestParseArgsDryRunAdopt confirms -dry-run and -adopt parse on push and land
// on the cli struct.
func TestParseArgsDryRunAdopt(t *testing.T) {
	c, err := parseArgs([]string{"push", "-bucket", "b", "-dry-run", "-adopt", "root"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if !c.dryRun || !c.adopt {
		t.Errorf("dryRun=%v adopt=%v, want both true", c.dryRun, c.adopt)
	}
}

// TestDryRunReport checks the multi-line preview: sorted upload/orphan lines
// followed by a count summary, the "up to date" fallback when nothing is
// pending, and the adopt preview.
func TestDryRunReport(t *testing.T) {
	for _, tc := range []struct {
		name string
		plan push.Plan
		want string
	}{
		{
			"changes",
			push.Plan{Upload: []string{"a.txt", "d.txt"}, Orphan: []string{"b.txt"}, Unchanged: 1, Total: 3},
			"would upload a.txt\nwould upload d.txt\nwould orphan b.txt\n" +
				"dry-run: 2 would upload, 1 unchanged, 1 orphaned of 3 files at s3://b/p",
		},
		{
			"up to date",
			push.Plan{Unchanged: 3, Total: 3},
			"up to date: 3 files unchanged at s3://b/p",
		},
		{
			"adopt",
			push.Plan{Total: 3, ManifestOnly: true},
			"would adopt: publish manifest describing 3 files to s3://b/p (no content uploaded)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := dryRunReport(tc.plan, "b", "p"); got != tc.want {
				t.Errorf("dryRunReport =\n%q\nwant\n%q", got, tc.want)
			}
		})
	}
}

// TestAdoptSummary checks the one-line adopt result.
func TestAdoptSummary(t *testing.T) {
	got := adoptSummary(push.Stats{Total: 5}, "b", "p")
	want := "adopted: published manifest for 5 files to s3://b/p (no content uploaded)"
	if got != want {
		t.Errorf("adoptSummary = %q, want %q", got, want)
	}
}

// TestPushSummary checks the differential summary renders the changed count and
// falls back to an "up to date" line when nothing was uploaded.
func TestPushSummary(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stats push.Stats
		want  string
	}{
		{"partial", push.Stats{Uploaded: 2, Skipped: 3, Total: 5}, "pushed 2 of 5 files (3 unchanged) to s3://b/p"},
		{"noop", push.Stats{Uploaded: 0, Skipped: 5, Total: 5}, "up to date: 5 files unchanged at s3://b/p"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := pushSummary(tc.stats, "b", "p"); got != tc.want {
				t.Errorf("pushSummary = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunVersion(t *testing.T) {
	var out strings.Builder
	if err := run(context.Background(), []string{"version"}, &out, io.Discard); err != nil {
		t.Fatalf("run(version): %v", err)
	}
	if got := strings.TrimSpace(out.String()); got != version {
		t.Errorf("run(version) output = %q, want %q", got, version)
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
