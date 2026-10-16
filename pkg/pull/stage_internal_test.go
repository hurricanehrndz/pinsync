package pull

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// These cover the two staging guards that PullWithManifest's refusal to
// retry over a stuck staging tree (TestPullStuckStagingStopsRetry) would
// otherwise hide: a leftover staging name can be a hardlink to a live file.

func entry(rel, content string) manifest.File {
	sum := sha256.Sum256([]byte(content))
	return manifest.File{Path: rel, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(content)), Mode: "0644"}
}

func TestStageRefusesExistingStagingTree(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "dest")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dest, "a.txt"), []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := newSyncer(dest, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(s.tmp, 0o755); err != nil {
		t.Fatal(err)
	}
	live, err := hashTree(dest, "")
	if err != nil {
		t.Fatal(err)
	}
	m := &manifest.Manifest{Version: 1, Files: []manifest.File{entry("a.txt", "alpha")}}
	if _, err := s.stage(context.Background(), m, live, nil); err == nil {
		t.Error("stage reused an existing staging tree")
	}
}

func TestWriteVerifiedNeverOpensExistingFile(t *testing.T) {
	dir := t.TempDir()
	live := filepath.Join(dir, "live.txt")
	if err := os.WriteFile(live, []byte("alpha"), 0o644); err != nil {
		t.Fatal(err)
	}
	staged := filepath.Join(dir, "staged.txt")
	if err := os.Link(live, staged); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
	if err := writeVerified(strings.NewReader("ALPHA"), entry("a.txt", "ALPHA"), staged, 0o644); err == nil {
		t.Error("writeVerified wrote over an existing file")
	}
	if got, _ := os.ReadFile(live); string(got) != "alpha" {
		t.Errorf("live file = %q, want it untouched", got)
	}
}
