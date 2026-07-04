package main

import (
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
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

func TestParseArgsDefaults(t *testing.T) {
	c, err := parseArgs([]string{"pull", "-bucket", "b", "dest"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.parallel != 16 {
		t.Errorf("parallel = %d, want default 16", c.parallel)
	}
	if c.prefix != "" || c.verbose {
		t.Errorf("unexpected non-zero defaults: %+v", c)
	}
}
