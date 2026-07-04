package manifest_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// writeFile creates rel (slash-separated) under root with the given content
// and exact permission bits (chmod defeats the umask).
func writeFile(t *testing.T, root, rel, content string, perm fs.FileMode) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), perm); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, perm); err != nil {
		t.Fatal(err)
	}
}

func sha(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func TestBuild(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "b.txt", "hello", 0o644)
	writeFile(t, root, "sub/a.txt", "world!", 0o600)

	m, err := manifest.Build(root)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if m.Version != 1 {
		t.Errorf("Version = %d, want 1", m.Version)
	}
	want := []manifest.File{
		{Path: "b.txt", SHA256: sha("hello"), Size: 5, Mode: "0644"},
		{Path: "sub/a.txt", SHA256: sha("world!"), Size: 6, Mode: "0600"},
	}
	if len(m.Files) != len(want) {
		t.Fatalf("Files = %+v, want %+v", m.Files, want)
	}
	for i, w := range want {
		if m.Files[i] != w {
			t.Errorf("Files[%d] = %+v, want %+v", i, m.Files[i], w)
		}
	}
}

func TestBuildDeterministic(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "z.txt", "zzz", 0o644)
	writeFile(t, root, "a/b/c.txt", "ccc", 0o600)
	writeFile(t, root, "a/d.txt", "ddd", 0o755)

	encode := func() []byte {
		t.Helper()
		m, err := manifest.Build(root)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		var buf bytes.Buffer
		if err := m.Encode(&buf); err != nil {
			t.Fatalf("Encode: %v", err)
		}
		return buf.Bytes()
	}
	first, second := encode(), encode()
	if !bytes.Equal(first, second) {
		t.Errorf("two builds of the same tree differ:\n%s\n---\n%s", first, second)
	}
}

func TestBuildModeFormatting(t *testing.T) {
	for _, tc := range []struct {
		perm fs.FileMode
		want string
	}{
		{0o600, "0600"},
		{0o644, "0644"},
		{0o755, "0755"},
	} {
		root := t.TempDir()
		writeFile(t, root, "f", "x", tc.perm)
		m, err := manifest.Build(root)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		if m.Files[0].Mode != tc.want {
			t.Errorf("mode %#o recorded as %q, want %q", tc.perm, m.Files[0].Mode, tc.want)
		}
	}
}

func TestBuildRejectsTopLevelManifestName(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "manifest.json", "{}", 0o644)
	if _, err := manifest.Build(root); err == nil || !strings.Contains(err.Error(), "manifest.json") {
		t.Fatalf("Build = %v, want reserved-name error", err)
	}
}

func TestBuildAllowsNestedManifestName(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "sub/manifest.json", "{}", 0o644)
	m, err := manifest.Build(root)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(m.Files) != 1 || m.Files[0].Path != "sub/manifest.json" {
		t.Errorf("Files = %+v, want the nested manifest.json recorded", m.Files)
	}
}

func TestBuildEmptyTree(t *testing.T) {
	m, err := manifest.Build(t.TempDir())
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(m.Files) != 0 {
		t.Errorf("Files = %+v, want none", m.Files)
	}
	var buf bytes.Buffer
	if err := m.Encode(&buf); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if _, err := manifest.Decode(&buf); err != nil {
		t.Errorf("Decode of empty manifest: %v", err)
	}
}
