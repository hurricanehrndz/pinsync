//go:build unix

package manifest_test

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

func TestBuildRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "a.txt", "x", 0o644)
	if err := os.Symlink("a.txt", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	_, err := manifest.Build(root)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Build = %v, want symlink rejection naming the type", err)
	}
	if err != nil && !strings.Contains(err.Error(), "link") {
		t.Fatalf("Build error %q does not name the offending path", err)
	}
}

func TestBuildRejectsFifo(t *testing.T) {
	root := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(root, "pipe"), 0o644); err != nil {
		t.Skipf("mkfifo unavailable: %v", err)
	}
	_, err := manifest.Build(root)
	if err == nil || !strings.Contains(err.Error(), "fifo") {
		t.Fatalf("Build = %v, want fifo rejection naming the type", err)
	}
}
