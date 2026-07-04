package manifest_test

import (
	"strings"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

func manifestWithPaths(paths ...string) *manifest.Manifest {
	m := &manifest.Manifest{Version: 1}
	for _, p := range paths {
		m.Files = append(m.Files, manifest.File{Path: p, SHA256: hashA, Size: 1, Mode: "0644"})
	}
	return m
}

func TestValidateRejectsPaths(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		// M3: path shape.
		{"empty", ""},
		{"absolute", "/etc/passwd"},
		{"parent segment", "../evil"},
		{"inner parent segment", "a/../b"},
		{"dot segment", "./a"},
		{"inner dot segment", "a/./b"},
		{"empty segment", "a//b"},
		{"trailing slash", "a/"},
		{"backslash", `a\b`},
		// M5: Windows compatibility.
		{"reserved bare", "CON"},
		{"reserved lowercase", "nul"},
		{"reserved with extension", "nul.txt"},
		{"reserved double extension", "dir/COM1.tar.gz"},
		{"reserved directory component", "aux/file.txt"},
		{"trailing dot", "foo."},
		{"trailing dot component", "foo./bar"},
		{"trailing space", "file.txt "},
		{"trailing space component", "dir /f"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := manifestWithPaths(tc.path).Validate(); err == nil {
				t.Errorf("Validate accepted path %q", tc.path)
			}
		})
	}
}

func TestValidateAcceptsPaths(t *testing.T) {
	ok := []string{
		"a.txt",
		"a/b/c.txt",
		".hidden",
		"dir/.hidden.conf",
		"file with spaces.txt",
		"unicode-ñ-文件.txt",
		"COM0", // only COM1–COM9 are reserved
		"COM10.txt",
		"console.log", // reserved-name prefix but not the bare name
		"nullable/aux2",
		"manifest.json.bak",
		"sub/manifest.json", // only the top-level name is reserved (checked in Build)
	}
	if err := manifestWithPaths(ok...).Validate(); err != nil {
		t.Errorf("Validate rejected valid paths: %v", err)
	}
}

func TestValidateCaseCollision(t *testing.T) {
	err := manifestWithPaths("Foo.txt", "foo.TXT").Validate()
	if err == nil || !strings.Contains(err.Error(), "collide") {
		t.Fatalf("Validate = %v, want case-collision error", err)
	}
}

func TestValidateDuplicatePath(t *testing.T) {
	err := manifestWithPaths("a.txt", "a.txt").Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("Validate = %v, want duplicate-path error", err)
	}
}

func TestValidateAggregatesViolations(t *testing.T) {
	err := manifestWithPaths("../one", "CON", "fine.txt").Validate()
	if err == nil {
		t.Fatal("Validate = nil, want aggregated errors")
	}
	for _, needle := range []string{"../one", "CON"} {
		if !strings.Contains(err.Error(), needle) {
			t.Errorf("aggregated error %q missing violation for %q", err, needle)
		}
	}
}
