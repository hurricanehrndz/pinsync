package push_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/hurricanehrndz/pinsync/internal/s3test"
	"github.com/hurricanehrndz/pinsync/pkg/manifest"
	"github.com/hurricanehrndz/pinsync/pkg/push"
)

func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func fixtureTree(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	writeFile(t, root, "a.txt", "alpha")
	writeFile(t, root, "b.txt", "bravo")
	writeFile(t, root, "sub/c.txt", "charlie")
	return root
}

func TestPushUploadsManifestLast(t *testing.T) {
	fake := s3test.NewFake()
	stats, err := push.Push(context.Background(), fake, "bkt", "cfg/prod", fixtureTree(t), push.Options{})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if stats.Uploaded != 3 {
		t.Errorf("Uploaded = %d, want 3", stats.Uploaded)
	}
	puts := fake.Puts()
	if len(puts) != 4 {
		t.Fatalf("puts = %v, want 3 content files + manifest", puts)
	}
	if last := puts[len(puts)-1]; last != "cfg/prod/manifest.json" {
		t.Errorf("last put = %s, want cfg/prod/manifest.json", last)
	}
	for _, key := range puts[:3] {
		if strings.HasSuffix(key, "/manifest.json") {
			t.Errorf("manifest uploaded before all content: %v", puts)
		}
	}
	body, ok := fake.Object("cfg/prod/manifest.json")
	if !ok {
		t.Fatal("manifest object missing")
	}
	m, err := manifest.Decode(strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("uploaded manifest does not decode: %v", err)
	}
	if len(m.Files) != 3 {
		t.Errorf("manifest lists %d files, want 3", len(m.Files))
	}
}

func TestPushAbortLeavesManifestAbsent(t *testing.T) {
	fake := s3test.NewFake()
	fake.PutErr = func(key string, _ int) error {
		if strings.HasSuffix(key, "b.txt") {
			return errors.New("injected upload failure")
		}
		return nil
	}
	_, err := push.Push(context.Background(), fake, "bkt", "p", fixtureTree(t), push.Options{})
	if err == nil {
		t.Fatal("Push succeeded despite a failed upload")
	}
	if _, ok := fake.Object("p/manifest.json"); ok {
		t.Error("manifest was uploaded even though a content upload failed")
	}
}

func TestPushSetsChecksumAlgorithm(t *testing.T) {
	fake := s3test.NewFake()
	if _, err := push.Push(context.Background(), fake, "bkt", "p", fixtureTree(t), push.Options{}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	for _, key := range fake.Puts() {
		if algo := fake.PutAlgorithm(key); algo != types.ChecksumAlgorithmSha256 {
			t.Errorf("put %s used checksum algorithm %q, want SHA256", key, algo)
		}
	}
}

func TestPushRespectsWorkerLimit(t *testing.T) {
	root := t.TempDir()
	for i := range 8 {
		writeFile(t, root, fmt.Sprintf("f%d.txt", i), strings.Repeat("x", i+1))
	}
	fake := s3test.NewFake()
	fake.PutDelay = 5 * time.Millisecond
	if _, err := push.Push(context.Background(), fake, "bkt", "p", root, push.Options{Parallel: 2}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	// The manifest put happens after the group drains, so it cannot raise this.
	if got := fake.MaxInFlight(); got > 2 {
		t.Errorf("observed %d concurrent puts, want at most 2", got)
	}
}

func TestPushPrefixHandling(t *testing.T) {
	for _, tc := range []struct {
		prefix string
		want   string
	}{
		{"cfg/prod", "cfg/prod/a.txt"},
		{"cfg/prod/", "cfg/prod/a.txt"}, // trailing slash normalized
		{"", "a.txt"},                   // no prefix
	} {
		fake := s3test.NewFake()
		root := t.TempDir()
		writeFile(t, root, "a.txt", "alpha")
		if _, err := push.Push(context.Background(), fake, "bkt", tc.prefix, root, push.Options{}); err != nil {
			t.Fatalf("Push(prefix=%q): %v", tc.prefix, err)
		}
		if _, ok := fake.Object(tc.want); !ok {
			t.Errorf("Push(prefix=%q): object %s missing; puts=%v", tc.prefix, tc.want, fake.Puts())
		}
	}
}

func TestPushRejectsInvalidTreeBeforeUploading(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "manifest.json", "{}") // reserved top-level name
	fake := s3test.NewFake()
	_, err := push.Push(context.Background(), fake, "bkt", "p", root, push.Options{})
	if err == nil {
		t.Fatal("Push accepted a tree with a top-level manifest.json")
	}
	if puts := fake.Puts(); len(puts) != 0 {
		t.Errorf("Push attempted uploads before validation failed: %v", puts)
	}
}
