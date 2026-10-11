package push_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

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
	// Only the files it skips need verifying; the changed file is not HEADed.
	heads := fake.Heads()
	slices.Sort(heads)
	if want := []string{"cfg/prod/b.txt", "cfg/prod/sub/c.txt"}; !slices.Equal(heads, want) {
		t.Errorf("heads = %v, want %v", heads, want)
	}
}

// TestPushUnchangedTreeIsNoop verifies that re-pushing an identical tree
// uploads nothing at all — not even the manifest — and reports everything
// skipped, but only after verifying every object it skips.
func TestPushUnchangedTreeIsNoop(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	fake.ResetCalls()

	stats := pushOK(t, fake, root, push.Options{})

	if got := fake.Puts(); len(got) != 0 {
		t.Errorf("no-op push uploaded %v, want nothing", got)
	}
	if heads := fake.Heads(); len(heads) != 3 {
		t.Errorf("heads = %v, want all 3 skipped objects verified", heads)
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
	// -full must never fetch the remote manifest to diff against, nor verify.
	if gets := fake.Gets()[beforeGets:]; len(gets) != 0 {
		t.Errorf("full push fetched %v, want no GetObject", gets)
	}
	if heads := fake.Heads(); len(heads) != 0 {
		t.Errorf("full push HEADed %v, want no HeadObject", heads)
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

// TestPushSetsChecksumAlgorithm verifies every put, content and manifest,
// carries a precomputed SHA256 that matches the bytes S3 stored, so S3 can
// reject bytes that differ from what the manifest describes.
func TestPushSetsChecksumAlgorithm(t *testing.T) {
	fake := s3test.NewFake()
	if _, err := push.Push(context.Background(), fake, "bkt", "p", fixtureTree(t), push.Options{}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	for _, key := range fake.Puts() {
		if algo := fake.PutAlgorithm(key); algo != types.ChecksumAlgorithmSha256 {
			t.Errorf("put %s used checksum algorithm %q, want SHA256", key, algo)
		}
		head, err := fake.HeadObject(context.Background(), &s3.HeadObjectInput{Key: aws.String(key)})
		if err != nil {
			t.Fatal(err)
		}
		if got, want := fake.PutChecksum(key), aws.ToString(head.ChecksumSHA256); got == "" || got != want {
			t.Errorf("put %s supplied ChecksumSHA256 %q, want the stored body's %q", key, got, want)
		}
	}
}

// TestPushRejectsFileChangedAfterBuild verifies a file rewritten between the
// manifest build and its upload is not published under a manifest that
// describes other bytes: the put fails, the object and the remote manifest are
// left as they were, and the manifest is not re-put.
func TestPushRejectsFileChangedAfterBuild(t *testing.T) {
	for _, tc := range []struct{ name, late string }{
		{"same size", "ALPHA-EDITED"},    // caught by the checksum
		{"different size", "alpha-edit"}, // caught by the length
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := fixtureTree(t)
			fake := s3test.NewFake()
			pushOK(t, fake, root, push.Options{})
			oldManifest, _ := fake.Object("cfg/prod/manifest.json")
			writeFile(t, root, "a.txt", "alpha-edited")
			fake.ResetCalls()
			fake.BeforePut = func(key string) {
				if key == "cfg/prod/a.txt" {
					writeFile(t, root, "a.txt", tc.late)
				}
			}

			_, err := push.Push(context.Background(), fake, "bkt", "cfg/prod", root, push.Options{})
			if err == nil {
				t.Fatal("Push succeeded although a.txt changed after the manifest was built")
			}
			if got, want := fake.Puts(), []string{"cfg/prod/a.txt"}; !slices.Equal(got, want) {
				t.Errorf("puts = %v, want only the rejected %v (no manifest)", got, want)
			}
			if body, _ := fake.Object("cfg/prod/a.txt"); string(body) != "alpha" {
				t.Errorf("object = %q, want the original alpha left in place", body)
			}
			if body, _ := fake.Object("cfg/prod/manifest.json"); !bytes.Equal(body, oldManifest) {
				t.Error("remote manifest changed despite the failed push")
			}
		})
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

// TestDryRunPreviewsChanges verifies that on a changed tree DryRun classifies
// every path — a new file and an edited file as uploads, a deleted file as an
// orphan, an untouched file as unchanged — sorted, while writing nothing and
// fetching only the one remote manifest.
func TestDryRunPreviewsChanges(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	fake.ResetCalls()

	writeFile(t, root, "a.txt", "alpha-edited") // changed
	if err := os.Remove(filepath.Join(root, "b.txt")); err != nil {
		t.Fatal(err) // deleted -> orphan
	}
	writeFile(t, root, "d.txt", "delta") // new

	plan, err := push.DryRun(context.Background(), fake, "bkt", "cfg/prod", root, push.Options{})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if want := []string{"a.txt", "d.txt"}; !slices.Equal(plan.Upload, want) {
		t.Errorf("Upload = %v, want %v", plan.Upload, want)
	}
	if want := []string{"b.txt"}; !slices.Equal(plan.Orphan, want) {
		t.Errorf("Orphan = %v, want %v", plan.Orphan, want)
	}
	if plan.Unchanged != 1 || plan.Total != 3 {
		t.Errorf("plan = %+v, want Unchanged 1 Total 3", plan)
	}
	// A dry-run previews; it must never upload.
	if puts := fake.Puts(); len(puts) != 0 {
		t.Errorf("DryRun uploaded %v, want nothing", puts)
	}
	// It diffs against exactly one manifest, no more.
	if gets := fake.Gets(); len(gets) != 1 {
		t.Errorf("DryRun made %d GetObject calls, want 1 (the manifest)", len(gets))
	}
}

// TestDryRunUnchangedTree verifies that a byte-identical tree previews as fully
// up to date: nothing to upload or orphan, everything unchanged.
func TestDryRunUnchangedTree(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	fake.ResetCalls()

	plan, err := push.DryRun(context.Background(), fake, "bkt", "cfg/prod", root, push.Options{})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if len(plan.Upload) != 0 || len(plan.Orphan) != 0 {
		t.Errorf("plan = %+v, want no upload/orphan", plan)
	}
	if plan.Unchanged != plan.Total || plan.Total != 3 {
		t.Errorf("plan = %+v, want Unchanged==Total==3", plan)
	}
	if puts := fake.Puts(); len(puts) != 0 {
		t.Errorf("DryRun uploaded %v, want nothing", puts)
	}
	if heads := fake.Heads(); len(heads) != 3 {
		t.Errorf("heads = %v, want all 3 unchanged objects verified", heads)
	}
}

// TestDryRunFullReportsAllUploads verifies that Full mirrors Push: every local
// path is reported as an upload, no orphans are computed, and the remote
// manifest is never fetched.
func TestDryRunFullReportsAllUploads(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	fake.ResetCalls()

	plan, err := push.DryRun(context.Background(), fake, "bkt", "cfg/prod", root, push.Options{Full: true})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if want := []string{"a.txt", "b.txt", "sub/c.txt"}; !slices.Equal(plan.Upload, want) {
		t.Errorf("Upload = %v, want all %v", plan.Upload, want)
	}
	if len(plan.Orphan) != 0 || plan.Unchanged != 0 || plan.Total != 3 {
		t.Errorf("plan = %+v, want Orphan empty Unchanged 0 Total 3", plan)
	}
	if gets := fake.Gets(); len(gets) != 0 {
		t.Errorf("full DryRun fetched %v, want no GetObject", gets)
	}
	if heads := fake.Heads(); len(heads) != 0 {
		t.Errorf("full DryRun HEADed %v, want no HeadObject", heads)
	}
	if puts := fake.Puts(); len(puts) != 0 {
		t.Errorf("DryRun uploaded %v, want nothing", puts)
	}
}

// TestDryRunAdoptIsManifestOnly verifies that an adopt preview needs no remote
// state: it reports ManifestOnly with the file count and makes no S3 calls.
func TestDryRunAdoptIsManifestOnly(t *testing.T) {
	fake := s3test.NewFake()
	plan, err := push.DryRun(context.Background(), fake, "bkt", "cfg/prod", fixtureTree(t), push.Options{Adopt: true})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if !plan.ManifestOnly || plan.Total != 3 {
		t.Errorf("plan = %+v, want ManifestOnly true Total 3", plan)
	}
	if len(plan.Upload) != 0 || len(plan.Orphan) != 0 {
		t.Errorf("plan = %+v, want no upload/orphan", plan)
	}
	if len(fake.Puts()) != 0 || len(fake.Gets()) != 0 || len(fake.Heads()) != 0 {
		t.Errorf("adopt DryRun made S3 calls: puts=%v gets=%v heads=%v", fake.Puts(), fake.Gets(), fake.Heads())
	}
}

// TestPushAdoptPublishesManifestOnly verifies that adopt uploads exactly the
// manifest — no content, no diff GetObject — and reports every file skipped.
func TestPushAdoptPublishesManifestOnly(t *testing.T) {
	fake := s3test.NewFake()
	stats := pushOK(t, fake, fixtureTree(t), push.Options{Adopt: true})

	if stats.Uploaded != 0 || stats.Skipped != 3 || stats.Total != 3 {
		t.Errorf("stats = %+v, want {Uploaded:0 Skipped:3 Total:3}", stats)
	}
	if puts := fake.Puts(); !slices.Equal(puts, []string{"cfg/prod/manifest.json"}) {
		t.Errorf("puts = %v, want only the manifest", puts)
	}
	if gets := fake.Gets(); len(gets) != 0 {
		t.Errorf("adopt fetched %v, want no GetObject (it does not diff)", gets)
	}
	if heads := fake.Heads(); len(heads) != 0 {
		t.Errorf("adopt HEADed %v, want no HeadObject (it does not verify)", heads)
	}
	body, ok := fake.Object("cfg/prod/manifest.json")
	if !ok {
		t.Fatal("manifest not published")
	}
	m, err := manifest.Decode(strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("published manifest does not decode: %v", err)
	}
	if len(m.Files) != 3 {
		t.Errorf("manifest lists %d files, want 3", len(m.Files))
	}
}

// TestPushAdoptCollisionUploadsNothing verifies that the collision guard still
// applies in adopt mode: a manifest key naming a content file is rejected up
// front, so nothing lands.
func TestPushAdoptCollisionUploadsNothing(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "site.json", "payload")
	fake := s3test.NewFake()
	_, err := push.Push(context.Background(), fake, "bkt", "cfg/prod", root,
		push.Options{Adopt: true, ManifestKey: "cfg/prod/site.json"})
	if err == nil {
		t.Fatal("adopt accepted a manifest key colliding with a content file")
	}
	if puts := fake.Puts(); len(puts) != 0 {
		t.Errorf("adopt uploaded despite the collision: %v", puts)
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

// TestPushRepairsAfterFailedManifestPut replays pinsync's own partial failure:
// a push whose content put lands but whose manifest put fails leaves new bytes
// under the old manifest, which pull readers reject. Pushing the original tree
// again yields a byte-identical manifest, so a manifest-only diff would skip
// everything; the push must instead notice the object no longer matches and
// re-upload it, without rewriting the manifest readers already hold.
func TestPushRepairsAfterFailedManifestPut(t *testing.T) {
	root := t.TempDir()
	fake := s3test.NewFake()
	writeFile(t, root, "f", "AAAA")
	pushOK(t, fake, root, push.Options{})

	writeFile(t, root, "f", "BBBB")
	fake.PutErr = func(key string, _ int) error {
		if key == "cfg/prod/manifest.json" {
			return errors.New("injected manifest failure")
		}
		return nil
	}
	if _, err := push.Push(context.Background(), fake, "bkt", "cfg/prod", root, push.Options{}); err == nil {
		t.Fatal("Push succeeded despite the failed manifest put")
	}
	fake.PutErr = nil
	if body, _ := fake.Object("cfg/prod/f"); string(body) != "BBBB" {
		t.Fatalf("arrange: object = %q, want the orphaned BBBB under the old manifest", body)
	}

	writeFile(t, root, "f", "AAAA")
	fake.ResetCalls()
	var logs bytes.Buffer
	stats := pushOK(t, fake, root, push.Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})

	if want := (push.Stats{Uploaded: 1, Repaired: 1, Total: 1}); stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
	if body, _ := fake.Object("cfg/prod/f"); string(body) != "AAAA" {
		t.Errorf("object = %q after repair, want AAAA", body)
	}
	if got, want := fake.Puts(), []string{"cfg/prod/f"}; !slices.Equal(got, want) {
		t.Errorf("puts = %v, want %v (identical manifest is not re-put)", got, want)
	}
	if !strings.Contains(logs.String(), `"reason":"checksum mismatch"`) {
		t.Errorf("logs do not report the checksum mismatch:\n%s", logs.String())
	}
}

// TestPushRepairsUnverifiedObject covers every way an object the identical
// remote manifest lists as unchanged can fail verification. Each is re-uploaded
// once, the manifest is not re-put, and the following push verifies it and
// does nothing.
func TestPushRepairsUnverifiedObject(t *testing.T) {
	putSHA := func(fake *s3test.Fake, body string) {
		if _, err := fake.PutObject(context.Background(), &s3.PutObjectInput{
			Key: aws.String("cfg/prod/a.txt"), Body: strings.NewReader(body),
			ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name   string
		reason string
		mutate func(fake *s3test.Fake)
	}{
		{"replaced different bytes", "no checksum", func(f *s3test.Fake) { f.Store("cfg/prod/a.txt", []byte("ALPHA")) }},
		// Same bytes, but nothing records that: without a stored checksum there is
		// nothing to trust, so it is re-uploaded with one.
		{"replaced same bytes", "no checksum", func(f *s3test.Fake) { f.Store("cfg/prod/a.txt", []byte("alpha")) }},
		{"deleted", "missing", func(f *s3test.Fake) { f.Delete("cfg/prod/a.txt") }},
		{"checksummed different bytes", "checksum mismatch", func(f *s3test.Fake) { putSHA(f, "ALPHA") }},
		{"checksummed different length", "size mismatch", func(f *s3test.Fake) { putSHA(f, "alpha-longer") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := fixtureTree(t)
			fake := s3test.NewFake()
			pushOK(t, fake, root, push.Options{})
			tc.mutate(fake)
			fake.ResetCalls()

			var logs bytes.Buffer
			stats := pushOK(t, fake, root, push.Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})

			if want := (push.Stats{Uploaded: 1, Repaired: 1, Skipped: 2, Total: 3}); stats != want {
				t.Errorf("stats = %+v, want %+v", stats, want)
			}
			if got, want := fake.Puts(), []string{"cfg/prod/a.txt"}; !slices.Equal(got, want) {
				t.Errorf("puts = %v, want %v (identical manifest is not re-put)", got, want)
			}
			if body, _ := fake.Object("cfg/prod/a.txt"); string(body) != "alpha" {
				t.Errorf("object = %q after repair, want alpha", body)
			}
			if out := logs.String(); !strings.Contains(out, `"level":"WARN"`) ||
				!strings.Contains(out, `"path":"a.txt"`) || !strings.Contains(out, fmt.Sprintf(`"reason":%q`, tc.reason)) {
				t.Errorf("logs lack a WARN for a.txt with reason %q:\n%s", tc.reason, out)
			}

			fake.ResetCalls()
			again := pushOK(t, fake, root, push.Options{})
			if puts := fake.Puts(); len(puts) != 0 {
				t.Errorf("push after repair uploaded %v, want nothing", puts)
			}
			if want := (push.Stats{Skipped: 3, Total: 3}); again != want {
				t.Errorf("push after repair stats = %+v, want %+v", again, want)
			}
		})
	}
}

// TestPushRepairWithChangedManifest verifies that a repair alongside a real
// change uploads both and then republishes the manifest last.
func TestPushRepairWithChangedManifest(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	fake.Delete("cfg/prod/b.txt")
	writeFile(t, root, "a.txt", "alpha-edited")
	fake.ResetCalls()

	stats := pushOK(t, fake, root, push.Options{})

	if want := (push.Stats{Uploaded: 2, Repaired: 1, Skipped: 1, Total: 3}); stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
	puts := fake.Puts()
	if len(puts) != 3 || puts[2] != "cfg/prod/manifest.json" {
		t.Fatalf("puts = %v, want a.txt and b.txt then the manifest", puts)
	}
	if content := puts[:2]; !slices.Contains(content, "cfg/prod/a.txt") || !slices.Contains(content, "cfg/prod/b.txt") {
		t.Errorf("content puts = %v, want a.txt and b.txt", content)
	}
}

// TestPushHeadsUseChecksumModeWithinWorkerLimit verifies the verifying HEADs
// ask for the stored checksum (without ChecksumMode S3 omits it) and share the
// Options.Parallel bound.
func TestPushHeadsUseChecksumModeWithinWorkerLimit(t *testing.T) {
	root := t.TempDir()
	for i := range 8 {
		writeFile(t, root, fmt.Sprintf("f%d.txt", i), strings.Repeat("x", i+1))
	}
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	fake.ResetCalls()
	fake.HeadDelay = 5 * time.Millisecond

	pushOK(t, fake, root, push.Options{Parallel: 2})

	heads := fake.Heads()
	if len(heads) != 8 {
		t.Fatalf("heads = %v, want 8", heads)
	}
	for _, key := range heads {
		if mode := fake.HeadMode(key); mode != types.ChecksumModeEnabled {
			t.Errorf("head %s used checksum mode %q, want ENABLED", key, mode)
		}
	}
	if got := fake.MaxInFlight(); got != 2 {
		t.Errorf("observed %d concurrent heads, want exactly the limit of 2", got)
	}
}

// headError builds the error the SDK returns for a failed HeadObject: an
// operation error wrapping an HTTP response error and its API error code.
func headError(status int, code string) error {
	return &smithy.OperationError{ServiceID: "S3", OperationName: "HeadObject", Err: &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      &smithy.GenericAPIError{Code: code, Message: http.StatusText(status)},
		},
	}}
}

// TestPushHeadErrorIsFatal verifies that a HEAD failure other than NotFound or
// 403 aborts the push before any upload, rather than guessing, and points at
// -full.
func TestPushHeadErrorIsFatal(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"plain error", errors.New("injected failure")},
		{"500", headError(http.StatusInternalServerError, "InternalError")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := fixtureTree(t)
			fake := s3test.NewFake()
			pushOK(t, fake, root, push.Options{})
			writeFile(t, root, "a.txt", "alpha-edited") // something that would upload
			fake.ResetCalls()
			fake.HeadErr = func(key string) error {
				if key == "cfg/prod/b.txt" {
					return tc.err
				}
				return nil
			}

			_, err := push.Push(context.Background(), fake, "bkt", "cfg/prod", root, push.Options{})
			if err == nil {
				t.Fatal("Push succeeded despite a failed HeadObject")
			}
			if msg := err.Error(); !strings.Contains(msg, "cfg/prod/b.txt") || !strings.Contains(msg, "-full") {
				t.Errorf("error %q does not name the key and -full", err)
			}
			if puts := fake.Puts(); len(puts) != 0 {
				t.Errorf("Push uploaded despite the HEAD error: %v", puts)
			}
		})
	}
}

// TestPushRepairsForbiddenHead verifies a 403 HEAD is a repair, not a fatal
// error: S3 returns 403 for a missing key when the caller lacks s3:ListBucket,
// so treating it as fatal would block repairing exactly the deleted object.
// The original error is still logged so a real permission problem shows.
func TestPushRepairsForbiddenHead(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"403 response", headError(http.StatusForbidden, "Forbidden")},
		{"AccessDenied code", &smithy.GenericAPIError{Code: "AccessDenied", Message: "Access Denied"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := fixtureTree(t)
			fake := s3test.NewFake()
			pushOK(t, fake, root, push.Options{})
			fake.Delete("cfg/prod/b.txt")
			fake.ResetCalls()
			fake.HeadErr = func(key string) error {
				if key == "cfg/prod/b.txt" {
					return tc.err
				}
				return nil
			}

			var logs bytes.Buffer
			stats := pushOK(t, fake, root, push.Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})

			if want := (push.Stats{Uploaded: 1, Repaired: 1, Skipped: 2, Total: 3}); stats != want {
				t.Errorf("stats = %+v, want %+v", stats, want)
			}
			if got, want := fake.Puts(), []string{"cfg/prod/b.txt"}; !slices.Equal(got, want) {
				t.Errorf("puts = %v, want %v", got, want)
			}
			if body, _ := fake.Object("cfg/prod/b.txt"); string(body) != "bravo" {
				t.Errorf("object = %q after repair, want bravo", body)
			}
			out := logs.String()
			if !strings.Contains(out, "HEAD returned 403") {
				t.Errorf("logs lack the 403 reason:\n%s", out)
			}
			if wantErr, _ := json.Marshal(tc.err.Error()); !strings.Contains(out, `"error":`+string(wantErr)) {
				t.Errorf("logs lack the original error %q:\n%s", tc.err, out)
			}
		})
	}
}

// TestPushRepairsCompositeChecksum verifies an object whose stored checksum is
// a multipart composite ("<sum>-<parts>"), which can never equal a full-object
// SHA256, is re-uploaded as a single part with an explicit reason rather than
// reported as a generic mismatch.
func TestPushRepairsCompositeChecksum(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	fake.ResetCalls()
	fake.HeadChecksum = func(key, sum string) string {
		if key == "cfg/prod/b.txt" {
			return sum + "-2"
		}
		return sum
	}

	var logs bytes.Buffer
	stats := pushOK(t, fake, root, push.Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})

	if want := (push.Stats{Uploaded: 1, Repaired: 1, Skipped: 2, Total: 3}); stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
	if got, want := fake.Puts(), []string{"cfg/prod/b.txt"}; !slices.Equal(got, want) {
		t.Errorf("puts = %v, want %v", got, want)
	}
	if want := `"reason":"composite checksum from a multipart upload; re-uploading as a single part"`; !strings.Contains(logs.String(), want) {
		t.Errorf("logs lack the composite reason:\n%s", logs.String())
	}
}

// TestDryRunReportsRepair verifies a repair shows up in the preview: in Upload
// (Push would send it) and in Repair, and not counted as unchanged.
func TestDryRunReportsRepair(t *testing.T) {
	root := fixtureTree(t)
	fake := s3test.NewFake()
	pushOK(t, fake, root, push.Options{})
	fake.Store("cfg/prod/a.txt", []byte("tampered"))
	writeFile(t, root, "b.txt", "bravo-edited")
	fake.ResetCalls()

	plan, err := push.DryRun(context.Background(), fake, "bkt", "cfg/prod", root, push.Options{})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if want := []string{"a.txt", "b.txt"}; !slices.Equal(plan.Upload, want) {
		t.Errorf("Upload = %v, want %v", plan.Upload, want)
	}
	if want := []string{"a.txt"}; !slices.Equal(plan.Repair, want) {
		t.Errorf("Repair = %v, want %v", plan.Repair, want)
	}
	if plan.Unchanged != 1 || plan.Total != 3 {
		t.Errorf("plan = %+v, want Unchanged 1 Total 3", plan)
	}
	if puts := fake.Puts(); len(puts) != 0 {
		t.Errorf("DryRun uploaded %v, want nothing", puts)
	}
}
