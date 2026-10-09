package manifest_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// verifyFixture writes a small tree and returns it with its built manifest.
func verifyFixture(t *testing.T) (string, *manifest.Manifest) {
	t.Helper()
	root := t.TempDir()
	writeFile(t, root, "a.txt", "alpha", 0o644)
	writeFile(t, root, "sub/b.sh", "bravo", 0o755)
	m, err := manifest.Build(root)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return root, m
}

func verifyErr(t *testing.T, root string, m *manifest.Manifest, needle string) {
	t.Helper()
	err := manifest.VerifyTree(context.Background(), root, m)
	if err == nil || !strings.Contains(err.Error(), needle) {
		t.Fatalf("VerifyTree = %v, want error containing %q", err, needle)
	}
}

func TestVerifyTreeMatch(t *testing.T) {
	root, m := verifyFixture(t)
	if err := manifest.VerifyTree(context.Background(), root, m); err != nil {
		t.Fatalf("VerifyTree: %v", err)
	}
}

func TestVerifyTreeEmpty(t *testing.T) {
	if err := manifest.VerifyTree(context.Background(), t.TempDir(), &manifest.Manifest{Version: 1}); err != nil {
		t.Fatalf("VerifyTree(empty dir, empty manifest): %v", err)
	}
}

func TestVerifyTreeHashMismatch(t *testing.T) {
	root, m := verifyFixture(t)
	writeFile(t, root, "a.txt", "ALPHA", 0o644) // same size: only the hash catches it
	verifyErr(t, root, m, "hash")
}

func TestVerifyTreeSizeMismatch(t *testing.T) {
	root, m := verifyFixture(t)
	writeFile(t, root, "a.txt", "alpha!", 0o644)
	verifyErr(t, root, m, "size")
}

func TestVerifyTreeModeMismatch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("modes are not meaningful on Windows")
	}
	root, m := verifyFixture(t)
	if err := os.Chmod(filepath.Join(root, "sub", "b.sh"), 0o644); err != nil {
		t.Fatal(err)
	}
	verifyErr(t, root, m, "mode")
}

func TestVerifyTreeMissingEntry(t *testing.T) {
	root, m := verifyFixture(t)
	if err := os.Remove(filepath.Join(root, "sub", "b.sh")); err != nil {
		t.Fatal(err)
	}
	verifyErr(t, root, m, "sub/b.sh")
}

func TestVerifyTreeExtraFile(t *testing.T) {
	root, m := verifyFixture(t)
	writeFile(t, root, "sub/extra.txt", "junk", 0o644)
	verifyErr(t, root, m, "sub/extra.txt")
}

func TestVerifyTreeRejectsSymlink(t *testing.T) {
	root, m := verifyFixture(t)
	// Replace a listed file with a symlink to identical bytes: following it
	// would pass, so only refusing to follow catches it.
	target := filepath.Join(t.TempDir(), "a.txt")
	writeFile(t, filepath.Dir(target), "a.txt", "alpha", 0o644)
	p := filepath.Join(root, "a.txt")
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, p); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	verifyErr(t, root, m, "symlink")
}

func TestVerifyTreeRoot(t *testing.T) {
	_, m := verifyFixture(t)
	verifyErr(t, filepath.Join(t.TempDir(), "missing"), m, "verify")

	file := filepath.Join(t.TempDir(), "file")
	writeFile(t, filepath.Dir(file), "file", "x", 0o644)
	verifyErr(t, file, &manifest.Manifest{Version: 1}, "not a directory")
}

func TestVerifyTreeRejectsInvalidManifest(t *testing.T) {
	root, m := verifyFixture(t)
	m.Files[0].SHA256 = "deadbeef"
	verifyErr(t, root, m, "sha256")

	verifyErr(t, root, &manifest.Manifest{Version: 2}, "version")
}
