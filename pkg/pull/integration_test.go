//go:build integration

package pull_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/hurricanehrndz/pinsync/internal/s3test"
	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/push"
)

// TestPullMinIO exercises the full loop against a real S3 implementation:
// push → pull → corrupt/delete/add-extraneous → pull → exact convergence.
func TestPullMinIO(t *testing.T) {
	client := s3test.StartMinIO(t)
	bucket := s3test.CreateBucket(t, client, "pinsync-pull")
	ctx := context.Background()

	files := map[string]string{
		"a.txt":     "alpha",
		"sub/b.txt": "bravo",
		"empty.txt": "",
	}
	root := t.TempDir()
	for rel, content := range files {
		writeFile(t, root, rel, content, 0o644)
	}
	if _, err := push.Push(ctx, client, bucket, prefix, root, push.Options{}); err != nil {
		t.Fatalf("Push: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "dest")
	stats, err := pull.Pull(ctx, client, bucket, prefix, dest, pull.Options{})
	if err != nil {
		t.Fatalf("fresh Pull: %v", err)
	}
	if stats.Downloaded != len(files) {
		t.Errorf("fresh stats = %+v, want Downloaded=%d", stats, len(files))
	}
	assertTree(t, dest, files)

	// Corrupt in place (same size), delete a file, add extraneous junk.
	if err := os.WriteFile(filepath.Join(dest, "a.txt"), []byte("XXXXX"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dest, "sub", "b.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dest, "extraneous.txt", "junk", 0o644)

	stats, err = pull.Pull(ctx, client, bucket, prefix, dest, pull.Options{})
	if err != nil {
		t.Fatalf("repair Pull: %v", err)
	}
	if stats.Downloaded != 2 || stats.Linked != 1 {
		t.Errorf("repair stats = %+v, want Downloaded=2 Linked=1", stats)
	}
	assertTree(t, dest, files)
	assertNoLeftovers(t, dest)
}
