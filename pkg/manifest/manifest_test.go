package manifest_test

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

var (
	hashA = strings.Repeat("a", 64)
	hashB = strings.Repeat("b", 64)
)

// golden is the exact serialization of goldenManifest: version first, files
// sorted by path, two-space indent, trailing newline.
func golden() string {
	return `{
  "version": 1,
  "files": [
    {
      "path": "a/c.txt",
      "sha256": "` + hashA + `",
      "size": 1,
      "mode": "0600"
    },
    {
      "path": "b.txt",
      "sha256": "` + hashB + `",
      "size": 2,
      "mode": "0644"
    }
  ]
}
`
}

func goldenManifest() *manifest.Manifest {
	return &manifest.Manifest{
		Version: 1,
		Files: []manifest.File{
			// Deliberately unsorted: Encode must sort by path.
			{Path: "b.txt", SHA256: hashB, Size: 2, Mode: "0644"},
			{Path: "a/c.txt", SHA256: hashA, Size: 1, Mode: "0600"},
		},
	}
}

func TestEncodeGolden(t *testing.T) {
	var buf bytes.Buffer
	if err := goldenManifest().Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if got := buf.String(); got != golden() {
		t.Errorf("Encode mismatch:\ngot:\n%s\nwant:\n%s", got, golden())
	}
}

func TestDecodeRoundTrip(t *testing.T) {
	m, err := manifest.Decode(strings.NewReader(golden()))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	want := goldenManifest()
	var buf bytes.Buffer
	if err := want.Encode(&buf); err != nil { // sorts want.Files
		t.Fatalf("Encode: %v", err)
	}
	if !reflect.DeepEqual(m, want) {
		t.Errorf("round-trip mismatch:\ngot:  %+v\nwant: %+v", m, want)
	}
}

func TestDecodeRejectsUnknownVersion(t *testing.T) {
	_, err := manifest.Decode(strings.NewReader(`{"version": 2, "files": []}`))
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("Decode(version 2) = %v, want unsupported-version error", err)
	}
}

func TestDecodeRejectsInvalidPaths(t *testing.T) {
	in := `{"version": 1, "files": [{"path": "../evil", "sha256": "` + hashA + `", "size": 1, "mode": "0644"}]}`
	if _, err := manifest.Decode(strings.NewReader(in)); err == nil {
		t.Fatal("Decode accepted a manifest with a ../ path")
	}
}

func TestFileMode(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want uint32
	}{
		{"0600", 0o600},
		{"0644", 0o644},
		{"0755", 0o755},
	} {
		f := manifest.File{Path: "x", Mode: tc.mode}
		got, err := f.FileMode()
		if err != nil {
			t.Errorf("FileMode(%q): %v", tc.mode, err)
			continue
		}
		if uint32(got) != tc.want {
			t.Errorf("FileMode(%q) = %#o, want %#o", tc.mode, got, tc.want)
		}
	}
}

func TestFileModeRejects(t *testing.T) {
	for _, mode := range []string{"", "abc", "0999", "1755", "04000"} {
		f := manifest.File{Path: "x", Mode: mode}
		if _, err := f.FileMode(); err == nil {
			t.Errorf("FileMode(%q) succeeded, want error", mode)
		}
	}
}
