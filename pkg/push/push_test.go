package push_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
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

// pushOK runs Push into fake and fails the test on error, returning the stats.
func pushOK(t *testing.T, fake *s3test.Fake, root string, opts push.Options) push.Stats {
	t.Helper()
	stats, err := push.Push(context.Background(), fake, "bkt", "cfg/prod", root, opts)
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	return stats
}

// TestPushUploadsOnlyChangedFiles verifies the differential path: after an
// initial push, editing one file re-uploads exactly that file plus the
// manifest last — untouched files are not re-sent.
func TestPushUploadsOnlyChangedFiles(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	before := len(fake.Puts())

	writeFile(t, root, "a.txt", "alpha-edited")
	stats := pushOK(t, fake, root, push.Options{})

	if stats.Uploaded != 1 || stats.Skipped != 2 || stats.Total != 3 {
		t.Errorf("stats = %+v, want {Uploaded:1 Skipped:2 Total:3}", stats)
	}
	got := fake.Puts()[before:]
	want := []string{"cfg/prod/a.txt", "cfg/prod/manifest.json"}
	if !slices.Equal(got, want) {
		t.Errorf("second-push puts = %v, want %v", got, want)
	}
}

// TestPushUnchangedTreeIsNoop verifies that re-pushing an identical tree
// uploads nothing at all — not even the manifest — and reports everything
// skipped.
func TestPushUnchangedTreeIsNoop(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	before := len(fake.Puts())

	stats := pushOK(t, fake, root, push.Options{})

	if got := fake.Puts()[before:]; len(got) != 0 {
		t.Errorf("no-op push uploaded %v, want nothing", got)
	}
	if stats.Uploaded != 0 || stats.Skipped != 3 || stats.Total != 3 {
		t.Errorf("stats = %+v, want {Uploaded:0 Skipped:3 Total:3}", stats)
	}
}

// TestPushMetadataChangeRepublishesManifest verifies that changes leaving all
// content bytes untouched — a mode-only edit or a deletion — upload zero
// content but still re-publish the manifest, since the atomic-snapshot pointer
// itself changed.
func TestPushMetadataChangeRepublishesManifest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, root string)
	}{
		{"chmod", func(t *testing.T, root string) {
			if err := os.Chmod(filepath.Join(root, "a.txt"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"delete", func(t *testing.T, root string) {
			if err := os.Remove(filepath.Join(root, "a.txt")); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := fixtureTree(t)
			fake := s3test.NewFake()
			pushOK(t, fake, root, push.Options{})
			before := len(fake.Puts())

			tc.mutate(t, root)
			stats := pushOK(t, fake, root, push.Options{})

			got := fake.Puts()[before:]
			want := []string{"cfg/prod/manifest.json"}
			if !slices.Equal(got, want) {
				t.Errorf("puts = %v, want only the manifest %v", got, want)
			}
			if stats.Uploaded != 0 {
				t.Errorf("Uploaded = %d, want 0 content uploads", stats.Uploaded)
			}
		})
	}
}

// TestPushFullReuploadsUnchangedTree verifies that -full bypasses the diff:
// every content file plus the manifest is re-sent even when nothing changed.
func TestPushFullReuploadsUnchangedTree(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	before := len(fake.Puts())
	beforeGets := len(fake.Gets())

	stats := pushOK(t, fake, root, push.Options{Full: true})

	if got := fake.Puts()[before:]; len(got) != 4 {
		t.Errorf("full re-push uploaded %v, want 3 content files + manifest", got)
	}
	if stats.Uploaded != 3 || stats.Skipped != 0 || stats.Total != 3 {
		t.Errorf("stats = %+v, want {Uploaded:3 Skipped:0 Total:3}", stats)
	}
	// -full must never fetch the remote manifest to diff against.
	if gets := fake.Gets()[beforeGets:]; len(gets) != 0 {
		t.Errorf("full push fetched %v, want no GetObject", gets)
	}
}

// TestPushFirstPushUploadsAll verifies that a NoSuchKey on the remote manifest
// (no prior snapshot) is treated as a first push: everything is uploaded.
func TestPushFirstPushUploadsAll(t *testing.T) {
	fake := s3test.NewFake()
	stats := pushOK(t, fake, fixtureTree(t), push.Options{})
	if stats.Uploaded != 3 || stats.Total != 3 {
		t.Errorf("stats = %+v, want all 3 uploaded", stats)
	}
	if len(fake.Puts()) != 4 {
		t.Errorf("puts = %v, want 3 content files + manifest", fake.Puts())
	}
}

// TestPushRemoteManifestFetchErrorIsFatal verifies that a remote-manifest
// fetch failure that is not NoSuchKey aborts the push (rather than silently
// re-uploading everything) and the error points at -full.
func TestPushRemoteManifestFetchErrorIsFatal(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	// Seed a corrupt manifest at the default key so the diff fetch decodes into
	// an error — a valid "other error" distinct from NoSuchKey.
	fake.Store("cfg/prod/manifest.json", []byte("{not json"))

	_, err := push.Push(context.Background(), fake, "bkt", "cfg/prod", root, push.Options{})
	if err == nil {
		t.Fatal("Push succeeded despite an unreadable remote manifest")
	}
	if !strings.Contains(err.Error(), "-full") {
		t.Errorf("error %q does not mention -full", err)
	}
	// The abort happens before any upload.
	if puts := fake.Puts(); len(puts) != 0 {
		t.Errorf("Push uploaded despite the fetch error: %v", puts)
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

func TestPushManifestKeyCustomLocation(t *testing.T) {
	fake := s3test.NewFake()
	_, err := push.Push(context.Background(), fake, "bkt", "cfg/prod", fixtureTree(t),
		push.Options{ManifestKey: "manifests/site-a.json"})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if _, ok := fake.Object("manifests/site-a.json"); !ok {
		t.Errorf("manifest not written to the custom key; puts=%v", fake.Puts())
	}
	// The default location must stay empty: a custom key relocates the snapshot
	// root, it does not additionally publish at the default path.
	if _, ok := fake.Object("cfg/prod/manifest.json"); ok {
		t.Error("manifest also written to the default key")
	}
}

func TestPushManifestKeyCollisionUploadsNothing(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "site.json", "payload")
	fake := s3test.NewFake()
	// The custom manifest key names the same object as a content file. Allowing
	// this would let the manifest clobber a payload (or vice versa), destroying
	// the atomic-snapshot invariant, so Push must reject it up front.
	_, err := push.Push(context.Background(), fake, "bkt", "cfg/prod", root,
		push.Options{ManifestKey: "cfg/prod/site.json"})
	if err == nil {
		t.Fatal("Push accepted a manifest key colliding with a content file")
	}
	// The guard runs before any upload, so a rejected push leaves the bucket
	// exactly as it was — no half-written snapshot.
	if puts := fake.Puts(); len(puts) != 0 {
		t.Errorf("Push uploaded despite the collision: %v", puts)
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
