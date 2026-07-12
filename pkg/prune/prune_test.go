package prune_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/internal/s3test"
	"github.com/hurricanehrndz/pinsync/pkg/manifest"
	"github.com/hurricanehrndz/pinsync/pkg/prune"
)

// manifestBody encodes a schema-v1 manifest naming the given relative paths.
func manifestBody(t *testing.T, paths ...string) []byte {
	t.Helper()
	m := manifest.Manifest{Version: manifest.Version}
	for _, p := range paths {
		m.Files = append(m.Files, manifest.File{Path: p, SHA256: "deadbeef", Size: 1, Mode: "0644"})
	}
	var buf bytes.Buffer
	if err := m.Encode(&buf); err != nil {
		t.Fatalf("encode manifest: %v", err)
	}
	return buf.Bytes()
}

// countingClient wraps the fake to count DeleteObjects requests (not keys).
type countingClient struct {
	*s3test.Fake
	deleteCalls int
}

func (c *countingClient) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	c.deleteCalls++
	return c.Fake.DeleteObjects(ctx, in, opts...)
}

// TestPruneKeepsReferencedAndManifest verifies the keep-set: the manifest
// object and every content path it names survive, while an unreferenced object
// is deleted.
func TestPruneKeepsReferencedAndManifest(t *testing.T) {
	fake := s3test.NewFake()
	fake.Store("cfg/prod/manifest.json", manifestBody(t, "a.txt", "b.txt"))
	fake.Store("cfg/prod/a.txt", []byte("alpha"))
	fake.Store("cfg/prod/b.txt", []byte("bravo"))
	fake.Store("cfg/prod/orphan.txt", []byte("stale"))

	stats, err := prune.Prune(context.Background(), fake, "bkt", "cfg/prod", prune.Options{})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if want := []string{"cfg/prod/orphan.txt"}; !slices.Equal(fake.Deletes(), want) {
		t.Errorf("deletes = %v, want %v", fake.Deletes(), want)
	}
	if stats.Deleted != 1 || stats.Referenced != 3 || stats.Protected != 0 || stats.Listed != 4 {
		t.Errorf("stats = %+v, want {Deleted:1 Referenced:3 Protected:0 Listed:4}", stats)
	}
	if _, ok := fake.Object("cfg/prod/manifest.json"); !ok {
		t.Error("manifest was deleted")
	}
	if _, ok := fake.Object("cfg/prod/a.txt"); !ok {
		t.Error("referenced content a.txt was deleted")
	}
}

// TestPruneMissingManifestIsFatal verifies that with no manifest to diff
// against, prune refuses to run and deletes nothing.
func TestPruneMissingManifestIsFatal(t *testing.T) {
	fake := s3test.NewFake()
	fake.Store("cfg/prod/orphan.txt", []byte("stale"))

	_, err := prune.Prune(context.Background(), fake, "bkt", "cfg/prod", prune.Options{})
	if err == nil {
		t.Fatal("Prune succeeded with no manifest")
	}
	if !strings.Contains(err.Error(), "no manifest") {
		t.Errorf("error %q does not mention the missing manifest", err)
	}
	if d := fake.Deletes(); len(d) != 0 {
		t.Errorf("deleted %v despite missing manifest, want nothing", d)
	}
}

// TestPruneCorruptManifestIsFatal verifies that an undecodable manifest body is
// fatal — never treated as an empty reference set — and deletes nothing.
func TestPruneCorruptManifestIsFatal(t *testing.T) {
	fake := s3test.NewFake()
	fake.Store("cfg/prod/manifest.json", []byte("{not json"))
	fake.Store("cfg/prod/orphan.txt", []byte("stale"))

	_, err := prune.Prune(context.Background(), fake, "bkt", "cfg/prod", prune.Options{})
	if err == nil {
		t.Fatal("Prune succeeded with a corrupt manifest")
	}
	if d := fake.Deletes(); len(d) != 0 {
		t.Errorf("deleted %v despite corrupt manifest, want nothing", d)
	}
}

// TestPruneMinAgeProtectsYoungOrphans verifies the grace window: an
// unreferenced object younger than MinAge is protected while an older one is
// deleted.
func TestPruneMinAgeProtectsYoungOrphans(t *testing.T) {
	restore := prune.SetNow(time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC))
	t.Cleanup(restore)
	nowT := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)

	fake := s3test.NewFake()
	fake.Store("cfg/prod/manifest.json", manifestBody(t))
	fake.StoreAt("cfg/prod/young.txt", []byte("recent"), nowT.Add(-30*time.Minute))
	fake.StoreAt("cfg/prod/old.txt", []byte("stale"), nowT.Add(-2*time.Hour))

	stats, err := prune.Prune(context.Background(), fake, "bkt", "cfg/prod", prune.Options{MinAge: time.Hour})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if want := []string{"cfg/prod/old.txt"}; !slices.Equal(fake.Deletes(), want) {
		t.Errorf("deletes = %v, want %v", fake.Deletes(), want)
	}
	if stats.Protected != 1 || stats.Deleted != 1 {
		t.Errorf("stats = %+v, want Protected 1 Deleted 1", stats)
	}
}

// TestPruneCustomManifestKey verifies that a custom ManifestKey under the prefix
// is used as the reference set and is itself excluded from deletion.
func TestPruneCustomManifestKey(t *testing.T) {
	fake := s3test.NewFake()
	fake.Store("cfg/prod/custom-manifest.json", manifestBody(t, "a.txt"))
	fake.Store("cfg/prod/a.txt", []byte("alpha"))
	fake.Store("cfg/prod/orphan.txt", []byte("stale"))

	opts := prune.Options{ManifestKey: "cfg/prod/custom-manifest.json"}
	_, err := prune.Prune(context.Background(), fake, "bkt", "cfg/prod", opts)
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if want := []string{"cfg/prod/orphan.txt"}; !slices.Equal(fake.Deletes(), want) {
		t.Errorf("deletes = %v, want %v", fake.Deletes(), want)
	}
	if _, ok := fake.Object("cfg/prod/custom-manifest.json"); !ok {
		t.Error("custom manifest was deleted")
	}
}

// TestPrunePrefixBoundary verifies that a sibling prefix sharing a string prefix
// (cfg/production vs cfg/prod) is neither listed nor deleted.
func TestPrunePrefixBoundary(t *testing.T) {
	fake := s3test.NewFake()
	fake.Store("cfg/prod/manifest.json", manifestBody(t))
	fake.Store("cfg/prod/orphan.txt", []byte("stale"))
	fake.Store("cfg/production/keep.txt", []byte("sibling"))

	stats, err := prune.Prune(context.Background(), fake, "bkt", "cfg/prod", prune.Options{})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if want := []string{"cfg/prod/orphan.txt"}; !slices.Equal(fake.Deletes(), want) {
		t.Errorf("deletes = %v, want %v", fake.Deletes(), want)
	}
	if stats.Listed != 2 {
		t.Errorf("Listed = %d, want 2 (sibling prefix excluded)", stats.Listed)
	}
	if _, ok := fake.Object("cfg/production/keep.txt"); !ok {
		t.Error("sibling-prefix object was deleted")
	}
}

// TestDryRunDeletesNothing verifies that DryRun plans deletions but issues no
// DeleteObjects request.
func TestDryRunDeletesNothing(t *testing.T) {
	fake := s3test.NewFake()
	fake.Store("cfg/prod/manifest.json", manifestBody(t, "a.txt"))
	fake.Store("cfg/prod/a.txt", []byte("alpha"))
	fake.Store("cfg/prod/orphan.txt", []byte("stale"))
	client := &countingClient{Fake: fake}

	plan, err := prune.DryRun(context.Background(), client, "bkt", "cfg/prod", prune.Options{})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if want := []string{"cfg/prod/orphan.txt"}; !slices.Equal(plan.Delete, want) {
		t.Errorf("plan.Delete = %v, want %v", plan.Delete, want)
	}
	if client.deleteCalls != 0 {
		t.Errorf("DryRun made %d DeleteObjects calls, want 0", client.deleteCalls)
	}
	if d := fake.Deletes(); len(d) != 0 {
		t.Errorf("DryRun deleted %v, want nothing", d)
	}
}

// TestPruneBatchesLargeDeletes verifies that more than 1000 candidates split
// into multiple DeleteObjects requests.
func TestPruneBatchesLargeDeletes(t *testing.T) {
	fake := s3test.NewFake()
	fake.Store("cfg/prod/manifest.json", manifestBody(t))
	for i := range 1001 {
		fake.Store(fmt.Sprintf("cfg/prod/orphan-%04d.txt", i), []byte("stale"))
	}
	client := &countingClient{Fake: fake}

	stats, err := prune.Prune(context.Background(), client, "bkt", "cfg/prod", prune.Options{})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if stats.Deleted != 1001 {
		t.Errorf("Deleted = %d, want 1001", stats.Deleted)
	}
	if client.deleteCalls != 2 {
		t.Errorf("DeleteObjects calls = %d, want 2 (1000 + 1)", client.deleteCalls)
	}
}

// TestPrunePerKeyDeleteErrorSurfaced verifies that a per-key delete failure is
// reported in the returned error while the other keys are still deleted.
func TestPrunePerKeyDeleteErrorSurfaced(t *testing.T) {
	fake := s3test.NewFake()
	fake.Store("cfg/prod/manifest.json", manifestBody(t))
	fake.Store("cfg/prod/orphan-a.txt", []byte("stale"))
	fake.Store("cfg/prod/orphan-b.txt", []byte("stale"))
	fake.DeleteErr = func(key string) error {
		if strings.HasSuffix(key, "orphan-a.txt") {
			return fmt.Errorf("access denied")
		}
		return nil
	}

	stats, err := prune.Prune(context.Background(), fake, "bkt", "cfg/prod", prune.Options{})
	if err == nil {
		t.Fatal("Prune succeeded despite a per-key delete failure")
	}
	if !strings.Contains(err.Error(), "orphan-a.txt") {
		t.Errorf("error %q does not name the failed key", err)
	}
	if stats.Deleted != 1 {
		t.Errorf("Deleted = %d, want 1 (the surviving key)", stats.Deleted)
	}
	if want := []string{"cfg/prod/orphan-b.txt"}; !slices.Equal(fake.Deletes(), want) {
		t.Errorf("deletes = %v, want %v", fake.Deletes(), want)
	}
}
