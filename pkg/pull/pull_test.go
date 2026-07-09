package pull_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/hurricanehrndz/pinsync/internal/s3test"
	"github.com/hurricanehrndz/pinsync/pkg/manifest"
	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/push"
)

const prefix = "cfg/prod"

var manifestKey = prefix + "/manifest.json"

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

// pushFixture publishes files (all 0644) through the real push path into the
// fake, so pull tests consume exactly what push produces. Push is POSIX-only
// (P1), so on Windows the fake is seeded with the same bytes push would have
// produced; push↔pull interop stays covered by the POSIX runs. The recorded
// call history is reset afterward so this arrange step — including push's own
// remote-manifest diff fetch — is invisible to tests that observe the pull.
func pushFixture(t *testing.T, fake *s3test.Fake, files map[string]string) {
	t.Helper()
	defer fake.ResetCalls()
	if runtime.GOOS == "windows" {
		for rel, content := range files {
			fake.Store(prefix+"/"+rel, []byte(content))
		}
		fake.Store(manifestKey, encodeManifest(t, files))
		return
	}
	root := t.TempDir()
	for rel, content := range files {
		writeFile(t, root, rel, content, 0o644)
	}
	if _, err := push.Push(context.Background(), fake, "bkt", prefix, root, push.Options{}); err != nil {
		t.Fatalf("push fixture: %v", err)
	}
}

// encodeManifest renders the manifest push would produce for files (all 0644).
func encodeManifest(t *testing.T, files map[string]string) []byte {
	t.Helper()
	m := &manifest.Manifest{Version: 1}
	for rel, content := range files {
		sum := sha256.Sum256([]byte(content))
		m.Files = append(m.Files, manifest.File{
			Path:   rel,
			SHA256: hex.EncodeToString(sum[:]),
			Size:   int64(len(content)),
			Mode:   "0644",
		})
	}
	var buf bytes.Buffer
	if err := m.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func doPull(t *testing.T, fake *s3test.Fake, dest string) (pull.Stats, error) {
	t.Helper()
	return pull.Pull(context.Background(), fake, "bkt", prefix, dest, pull.Options{})
}

func mustPull(t *testing.T, fake *s3test.Fake, dest string) pull.Stats {
	t.Helper()
	stats, err := doPull(t, fake, dest)
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	return stats
}

// readTree returns every entry under root: files map to their content,
// directories to the marker "<dir>".
func readTree(t *testing.T, root string) map[string]string {
	t.Helper()
	got := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			got[filepath.ToSlash(rel)] = "<dir>"
			return nil
		}
		body, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		got[filepath.ToSlash(rel)] = string(body)
		return nil
	})
	if err != nil {
		t.Fatalf("reading tree %s: %v", root, err)
	}
	return got
}

// assertTree asserts dest holds exactly files (plus their parent dirs).
func assertTree(t *testing.T, dest string, files map[string]string) {
	t.Helper()
	want := map[string]string{}
	for rel, content := range files {
		want[rel] = content
		for dir := filepath.ToSlash(filepath.Dir(filepath.FromSlash(rel))); dir != "."; dir = filepath.ToSlash(filepath.Dir(filepath.FromSlash(dir))) {
			want[dir] = "<dir>"
		}
	}
	got := readTree(t, dest)
	for rel, content := range want {
		if got[rel] != content {
			t.Errorf("dest[%s] = %q, want %q", rel, got[rel], content)
		}
	}
	for rel := range got {
		if _, ok := want[rel]; !ok {
			t.Errorf("dest has extraneous entry %s", rel)
		}
	}
}

func assertNoLeftovers(t *testing.T, dest string) {
	t.Helper()
	for _, p := range []string{dest + ".pinsync-tmp", dest + ".pinsync-old"} {
		if _, err := os.Lstat(p); err == nil {
			t.Errorf("leftover directory %s", p)
		}
	}
}

func TestPullFresh(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo", "empty.txt": ""}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest") // does not exist yet

	stats := mustPull(t, fake, dest)
	assertTree(t, dest, files)
	assertNoLeftovers(t, dest)
	if stats.Downloaded != 3 || stats.Total != 3 {
		t.Errorf("stats = %+v, want 3 downloaded of 3", stats)
	}
	if runtime.GOOS != "windows" {
		if mode := statMode(t, filepath.Join(dest, "a.txt")); mode != 0o644 {
			t.Errorf("a.txt mode = %#o, want 0644", mode)
		}
	}
	if fake.GetMode(manifestKey) != types.ChecksumModeEnabled {
		t.Error("manifest fetched without ChecksumMode=ENABLED")
	}
	if fake.GetMode(prefix+"/a.txt") != types.ChecksumModeEnabled {
		t.Error("content fetched without ChecksumMode=ENABLED")
	}
}

func statMode(t *testing.T, p string) fs.FileMode {
	t.Helper()
	info, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return info.Mode().Perm()
}

func TestPullNoOpHardlinks(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	before, err := os.Stat(filepath.Join(dest, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	stats := mustPull(t, fake, dest)
	if stats.Downloaded != 0 || stats.Linked != 2 {
		t.Errorf("stats = %+v, want Linked=2 Downloaded=0", stats)
	}
	after, err := os.Stat(filepath.Join(dest, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Error("no-op pull did not hardlink: inode changed")
	}
	assertTree(t, dest, files)
}

func TestPullRepairsCorruptedInPlace(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	// Same size, same mtime — the exact blind spot of size+mtime sync.
	target := filepath.Join(dest, "a.txt")
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("XXXXX"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(target, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}

	stats := mustPull(t, fake, dest)
	if stats.Downloaded != 1 || stats.Linked != 1 {
		t.Errorf("stats = %+v, want Downloaded=1 Linked=1", stats)
	}
	assertTree(t, dest, files)
}

func TestPullRepairsTruncated(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	if err := os.Truncate(filepath.Join(dest, "a.txt"), 2); err != nil {
		t.Fatal(err)
	}
	stats := mustPull(t, fake, dest)
	if stats.Downloaded != 1 {
		t.Errorf("stats = %+v, want Downloaded=1", stats)
	}
	assertTree(t, dest, files)
}

func TestPullModeOnlyChangeCopies(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("modes are not meaningful on Windows; the content-only decision is the specified behavior (L3)")
	}
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, files) // manifest records 0644
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	live := filepath.Join(dest, "a.txt")
	if err := os.Chmod(live, 0o600); err != nil {
		t.Fatal(err)
	}
	// Extra hardlink to the live inode: if staging ever chmods it, this shows.
	control := dest + ".control"
	if err := os.Link(live, control); err != nil {
		t.Fatal(err)
	}

	stats := mustPull(t, fake, dest)
	if stats.Copied != 1 || stats.Downloaded != 0 || stats.Linked != 0 {
		t.Errorf("stats = %+v, want Copied=1 only", stats)
	}
	if mode := statMode(t, live); mode != 0o644 {
		t.Errorf("a.txt mode = %#o, want 0644 restored", mode)
	}
	if mode := statMode(t, control); mode != 0o600 {
		t.Errorf("live inode was chmodded during staging: control mode = %#o, want 0600", mode)
	}
}

func TestPullRemovesExtraneous(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	writeFile(t, dest, "extra.txt", "junk", 0o644)
	writeFile(t, dest, "sub/extra2.txt", "junk", 0o644)
	if err := os.MkdirAll(filepath.Join(dest, "leftover/empty"), 0o755); err != nil {
		t.Fatal(err)
	}

	mustPull(t, fake, dest)
	assertTree(t, dest, files) // fails on any extraneous file or leftover dir
}

func TestPullDownloadFailureLeavesDestUntouched(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	// New manifest references b.txt, but the object is gone: immediate abort.
	pushFixture(t, fake, map[string]string{"a.txt": "alpha", "b.txt": "bravo"})
	fake.Delete(prefix + "/b.txt")

	if _, err := doPull(t, fake, dest); err == nil {
		t.Fatal("Pull succeeded despite a missing object")
	}
	assertTree(t, dest, v1)
	assertNoLeftovers(t, dest)
}

func TestPullMismatchExhaustionLeavesDestUntouched(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"a.txt": "alpha"})
	fake.GetBody = func(key string, body []byte) []byte {
		if strings.HasSuffix(key, "a.txt") {
			return []byte("corrupted-every-time")
		}
		return body
	}
	dest := filepath.Join(t.TempDir(), "dest")

	_, err := doPull(t, fake, dest)
	if err == nil || !strings.Contains(err.Error(), "3 attempts") {
		t.Fatalf("Pull = %v, want exhaustion error naming 3 attempts", err)
	}
	if _, statErr := os.Lstat(dest); statErr == nil {
		t.Error("dest was created despite total failure")
	}
	assertNoLeftovers(t, dest)

	manifestGets := 0
	for _, key := range fake.Gets() {
		if key == manifestKey {
			manifestGets++
		}
	}
	if manifestGets != 3 {
		t.Errorf("manifest fetched %d times, want once per attempt (3)", manifestGets)
	}
}

func TestPullRetryConvergesOnManifestRace(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"a.txt": "old-content"})

	// Overwrite-in-place race: the object already holds new bytes, the
	// manifest still describes the old ones.
	fake.Store(prefix+"/a.txt", []byte("new-content"))
	corrected := encodeManifest(t, map[string]string{"a.txt": "new-content"})
	gets := 0
	fake.BeforeGet = func(f *s3test.Fake, key string) {
		if key != manifestKey {
			return
		}
		gets++
		if gets == 2 { // publisher finished between attempts
			f.Store(manifestKey, corrected)
		}
	}

	dest := filepath.Join(t.TempDir(), "dest")
	stats := mustPull(t, fake, dest)
	if stats.Downloaded != 1 {
		t.Errorf("stats = %+v, want Downloaded=1", stats)
	}
	assertTree(t, dest, map[string]string{"a.txt": "new-content"})
}

func TestPullCrashRecovery(t *testing.T) {
	files := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"}

	t.Run("dest present, stale tmp and old", func(t *testing.T) {
		fake := s3test.NewFake()
		pushFixture(t, fake, files)
		dest := filepath.Join(t.TempDir(), "dest")
		mustPull(t, fake, dest)
		writeFile(t, dest+".pinsync-tmp", "partial.txt", "junk", 0o644)
		writeFile(t, dest+".pinsync-old", "ancient.txt", "junk", 0o644)

		mustPull(t, fake, dest)
		assertTree(t, dest, files)
		assertNoLeftovers(t, dest)
	})

	t.Run("dest missing, old present", func(t *testing.T) {
		fake := s3test.NewFake()
		pushFixture(t, fake, files)
		dest := filepath.Join(t.TempDir(), "dest")
		mustPull(t, fake, dest)
		// Crash between the swap's two renames.
		if err := os.Rename(dest, dest+".pinsync-old"); err != nil {
			t.Fatal(err)
		}
		writeFile(t, dest+".pinsync-tmp", "partial.txt", "junk", 0o644)

		mustPull(t, fake, dest)
		assertTree(t, dest, files)
		assertNoLeftovers(t, dest)
	})
}

func TestPullHardlinkFallbackCopies(t *testing.T) {
	restore := pull.SetLinkFn(func(_, _ string) error { return errors.New("filesystem without hardlinks") })
	defer restore()

	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	stats := mustPull(t, fake, dest)
	if stats.Copied != 2 || stats.Linked != 0 || stats.Downloaded != 0 {
		t.Errorf("stats = %+v, want Copied=2 only", stats)
	}
	assertTree(t, dest, files)
}

func TestPullEmptyManifest(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{}) // empty tree, valid manifest
	dest := filepath.Join(t.TempDir(), "dest")

	stats := mustPull(t, fake, dest)
	if stats.Total != 0 {
		t.Errorf("stats = %+v, want Total=0", stats)
	}
	assertTree(t, dest, map[string]string{})
}

func TestPullAppliesModesFresh(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("push is POSIX-only (P1) and exact modes are a macOS/Linux promise (L12)")
	}
	fake := s3test.NewFake()
	root := t.TempDir()
	writeFile(t, root, "secret.pem", "key", 0o600)
	writeFile(t, root, "run.sh", "#!/bin/sh", 0o755)
	if _, err := push.Push(context.Background(), fake, "bkt", prefix, root, push.Options{}); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	for rel, want := range map[string]fs.FileMode{"secret.pem": 0o600, "run.sh": 0o755} {
		if mode := statMode(t, filepath.Join(dest, rel)); mode != want {
			t.Errorf("%s mode = %#o, want %#o", rel, mode, want)
		}
	}
}

func TestPullUnicodeAndSpacesAndDepth(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{
		"with space/naïve-文件.txt": "unicode",
		"a/b/c/d/e/f/deep.txt":    "deep",
	}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	assertTree(t, dest, files)
}

func TestPullManifestKeyCustomLocation(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"}
	pushFixture(t, fake, files)

	// Relocate the manifest to a key outside the content tree and remove it from
	// the default location, so a pull that ignored ManifestKey would fail.
	const customKey = "manifests/site-a.json"
	body, ok := fake.Object(manifestKey)
	if !ok {
		t.Fatal("fixture manifest missing")
	}
	fake.Store(customKey, body)
	fake.Delete(manifestKey)

	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := pull.Pull(context.Background(), fake, "bkt", prefix, dest,
		pull.Options{ManifestKey: customKey}); err != nil {
		t.Fatalf("Pull: %v", err)
	}

	// The custom key must be the one fetched — proof ManifestKey, not the
	// default, drove the snapshot.
	var read bool
	for _, key := range fake.Gets() {
		if key == customKey {
			read = true
		}
		if key == manifestKey {
			t.Errorf("Pull fetched the default manifest key despite ManifestKey=%q", customKey)
		}
	}
	if !read {
		t.Errorf("custom manifest key never fetched; gets=%v", fake.Gets())
	}
	assertTree(t, dest, files)
	assertNoLeftovers(t, dest)
}

// snapshot records every regular file under root as "content|mode", so a
// before/after comparison proves DryRun touched neither a byte nor a bit.
func snapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	snap := map[string]string{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		body, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		snap[filepath.ToSlash(rel)] = fmt.Sprintf("%s|%04o", body, info.Mode().Perm())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return snap
}

// warnLogger returns a logger writing Warn+ records to buf, for asserting the
// interrupted-pull warning without touching stdout.
func warnLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// TestDryRunClassifiesAndIsReadOnly checks the four outcomes (download a
// content change, copy a mode-only change, count an unchanged file as Linked,
// remove an extra local file), that only the manifest is fetched, that no
// staging trees appear, and that dest is left byte- and mode-for-mode intact.
func TestDryRunClassifiesAndIsReadOnly(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mode-only changes are not meaningful on Windows (L3); push is POSIX-only (P1)")
	}
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "b.txt": "bravo", "c.txt": "charlie"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	// Mutate the live tree: change b's content, c's mode only, add an extra.
	if err := os.WriteFile(filepath.Join(dest, "b.txt"), []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(filepath.Join(dest, "c.txt"), 0o600); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dest, "extra.txt", "junk", 0o644)

	before := snapshot(t, dest)
	fake.ResetCalls()

	plan, err := pull.DryRun(context.Background(), fake, "bkt", prefix, dest, pull.Options{})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if !reflect.DeepEqual(plan.Download, []string{"b.txt"}) {
		t.Errorf("Download = %v, want [b.txt]", plan.Download)
	}
	if !reflect.DeepEqual(plan.Copy, []string{"c.txt"}) {
		t.Errorf("Copy = %v, want [c.txt]", plan.Copy)
	}
	if !reflect.DeepEqual(plan.Remove, []string{"extra.txt"}) {
		t.Errorf("Remove = %v, want [extra.txt]", plan.Remove)
	}
	if plan.Linked != 1 || plan.Total != 3 {
		t.Errorf("plan = %+v, want Linked=1 Total=3", plan)
	}

	// Only the manifest was fetched — no content GetObject.
	for _, key := range fake.Gets() {
		if key != manifestKey {
			t.Errorf("DryRun fetched %q; want the manifest only", key)
		}
	}

	assertNoLeftovers(t, dest) // no .pinsync-tmp / .pinsync-old created

	if after := snapshot(t, dest); !reflect.DeepEqual(after, before) {
		t.Errorf("DryRun mutated dest:\n before %v\n after  %v", before, after)
	}
}

// TestDryRunWarnsOnInterruptedPull checks that leftover crash state (dest gone,
// .pinsync-old present) draws the interrupted-pull warning to the logger,
// reports every entry as a download (the live tree is empty), and mutates
// nothing — the old tree stays and dest stays absent.
func TestDryRunWarnsOnInterruptedPull(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)

	// Crash between the swap's two renames: dest missing, old present.
	old := dest + ".pinsync-old"
	if err := os.Rename(dest, old); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	plan, err := pull.DryRun(context.Background(), fake, "bkt", prefix, dest,
		pull.Options{Logger: warnLogger(&logs)})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if !strings.Contains(logs.String(), "interrupted") {
		t.Errorf("no interrupted-pull warning logged; got %q", logs.String())
	}
	if !reflect.DeepEqual(plan.Download, []string{"a.txt", "b.txt"}) {
		t.Errorf("Download = %v, want [a.txt b.txt]", plan.Download)
	}
	if plan.Linked != 0 || plan.Total != 2 || len(plan.Copy) != 0 || len(plan.Remove) != 0 {
		t.Errorf("plan = %+v, want 2 downloads only", plan)
	}
	if _, err := os.Lstat(old); err != nil {
		t.Errorf(".pinsync-old was mutated by a read-only preview: %v", err)
	}
	if _, err := os.Lstat(dest); err == nil {
		t.Error("dest was created by a read-only preview")
	}
}

// TestDryRunFirstPull checks the empty/missing-dest case: every entry is a
// download, nothing is copied or removed, and no interrupted-pull warning fires.
func TestDryRunFirstPull(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest") // does not exist yet

	var logs bytes.Buffer
	plan, err := pull.DryRun(context.Background(), fake, "bkt", prefix, dest,
		pull.Options{Logger: warnLogger(&logs)})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if !reflect.DeepEqual(plan.Download, []string{"a.txt", "sub/b.txt"}) {
		t.Errorf("Download = %v, want [a.txt sub/b.txt]", plan.Download)
	}
	if plan.Total != 2 || plan.Linked != 0 || len(plan.Copy) != 0 || len(plan.Remove) != 0 {
		t.Errorf("plan = %+v, want 2 downloads only", plan)
	}
	if logs.Len() != 0 {
		t.Errorf("first pull logged a warning: %q", logs.String())
	}
	if _, err := os.Lstat(dest); err == nil {
		t.Error("dest was created by a read-only preview")
	}
}

func TestPullRejectsInvalidManifestBeforeTouchingDest(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"a.txt": "alpha"})
	fake.Store(manifestKey, []byte(fmt.Sprintf(
		`{"version":1,"files":[{"path":"../evil","sha256":"%s","size":1,"mode":"0644"}]}`,
		strings.Repeat("a", 64),
	)))
	dest := filepath.Join(t.TempDir(), "dest")

	if _, err := doPull(t, fake, dest); err == nil {
		t.Fatal("Pull accepted a manifest with a ../ path")
	}
	if _, err := os.Lstat(dest); err == nil {
		t.Error("dest was created for an invalid manifest")
	}
	assertNoLeftovers(t, dest)
}
