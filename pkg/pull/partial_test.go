package pull_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/s3test"
)

// The tests in this file cover the leftover partial tree: a pull that fails
// before its swap keeps the files it staged, so a set too large to finish in
// one time-limited run still converges over several instead of starting from
// zero every time.

func partialDir(dest string) string { return dest + ".pinsync-partial" }

func assertNoPartial(t *testing.T, dest string) {
	t.Helper()
	if _, err := os.Lstat(partialDir(dest)); err == nil {
		t.Errorf("leftover directory %s", partialDir(dest))
	}
}

// contentGets returns the content paths fetched since the last ResetCalls,
// sorted, without the manifest.
func contentGets(fake *s3test.Fake) []string {
	var paths []string
	for _, key := range fake.Gets() {
		if key != manifestKey {
			paths = append(paths, strings.TrimPrefix(key, prefix+"/"))
		}
	}
	slices.Sort(paths)
	return paths
}

// cancelingClient cancels the pull's context when key is requested and, like
// the real SDK, fails every GET once the context is done.
type cancelingClient struct {
	pull.S3API
	key    string
	cancel context.CancelFunc
}

func (c *cancelingClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if aws.ToString(in.Key) == c.key {
		c.cancel()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.S3API.GetObject(ctx, in, opts...)
}

// TestPullCancelledKeepsDownloadedFiles: pulls cut off by their deadline keep
// what they fetched, so successive time-limited runs make progress and the
// last one GETs only what no earlier run finished.
func TestPullCancelledKeepsDownloadedFiles(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "b.txt": "bravo", "c.txt": "charlie", "d.txt": "delta"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")

	// One worker stages in manifest order: the first run lands a and b
	// before c cancels it, the second reuses those and lands c before d does.
	for _, cancelAt := range []string{"c.txt", "d.txt"} {
		ctx, cancel := context.WithCancel(context.Background())
		client := &cancelingClient{S3API: fake, key: prefix + "/" + cancelAt, cancel: cancel}
		_, err := pull.Pull(ctx, client, "bkt", prefix, dest, pull.Options{Parallel: 1})
		cancel()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Pull cancelled at %s = %v, want context.Canceled", cancelAt, err)
		}
		if _, err := os.Lstat(dest); err == nil {
			t.Fatal("a cancelled first pull created dest")
		}
	}
	fake.ResetCalls()

	stats := mustPull(t, fake, dest)
	if got, want := contentGets(fake), []string{"d.txt"}; !slices.Equal(got, want) {
		t.Errorf("content GETs = %v, want only what no cancelled pull finished %v", got, want)
	}
	if stats.Reused != 3 || stats.Downloaded != 1 || stats.Total != 4 {
		t.Errorf("stats = %+v, want Reused=3 Downloaded=1 of 4", stats)
	}
	assertTree(t, dest, files)
	assertNoLeftovers(t, dest)
	assertNoPartial(t, dest)
}

// TestPullReusesKilledPullStaging: a killed pull never gets to keep its
// staging tree itself, so the next pull's recovery does. A complete file is
// reused; one cut off at its final name fails the hash and is fetched again.
func TestPullReusesKilledPullStaging(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"keep.txt": "keep"})
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	v2 := map[string]string{"keep.txt": "keep", "a.txt": "alpha", "b.txt": "bravo"}
	pushFixture(t, fake, v2)

	writeFile(t, dest+".pinsync-tmp", "a.txt", "alpha", 0o644)
	writeFile(t, dest+".pinsync-tmp", "b.txt", "bra", 0o644)

	stats := mustPull(t, fake, dest)
	if got, want := contentGets(fake), []string{"b.txt"}; !slices.Equal(got, want) {
		t.Errorf("content GETs = %v, want %v", got, want)
	}
	if stats.Linked != 1 || stats.Reused != 1 || stats.Downloaded != 1 {
		t.Errorf("stats = %+v, want Linked=1 Reused=1 Downloaded=1", stats)
	}
	assertTree(t, dest, v2)
	assertNoLeftovers(t, dest)
	assertNoPartial(t, dest)
}

// TestPullMatchesLeftoversByContent: a leftover is reused only for the exact
// bytes, wherever it sits (a republish may have moved the file), and never
// through a symlink.
func TestPullMatchesLeftoversByContent(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "b.txt": "bravo", "c.txt": "charlie"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	partial := partialDir(dest)
	writeFile(t, partial, "a.txt", "ALPHA", 0o644)           // right path and size, wrong bytes
	writeFile(t, partial, "moved/old-b.txt", "bravo", 0o644) // right bytes, another path
	target := filepath.Join(t.TempDir(), "c.txt")            // right bytes, reachable only by a link
	writeFile(t, filepath.Dir(target), "c.txt", "charlie", 0o644)
	if err := os.Symlink(target, filepath.Join(partial, "c.txt")); err != nil {
		t.Logf("symlinks unavailable, covering the rest: %v", err)
	}

	stats := mustPull(t, fake, dest)
	if got, want := contentGets(fake), []string{"a.txt", "c.txt"}; !slices.Equal(got, want) {
		t.Errorf("content GETs = %v, want %v", got, want)
	}
	if stats.Reused != 1 || stats.Downloaded != 2 {
		t.Errorf("stats = %+v, want Reused=1 Downloaded=2", stats)
	}
	assertTree(t, dest, files)
	assertNoPartial(t, dest)
	if got := readTree(t, filepath.Dir(target))["c.txt"]; got != "charlie" {
		t.Errorf("symlink target changed to %q", got)
	}
}

// TestPullLinksALeftoverOnce: entries with the same content are staged from
// one leftover, but only one of them is linked to it, so no two paths in
// dest share a file, which a caller may reject.
func TestPullLinksALeftoverOnce(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "same", "b/a-copy.txt": "same", "c.txt": "charlie"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	writeFile(t, partialDir(dest), "old.txt", "same", 0o644)

	stats := mustPull(t, fake, dest)
	if stats.Reused != 2 || stats.Downloaded != 1 {
		t.Errorf("stats = %+v, want Reused=2 Downloaded=1", stats)
	}
	assertTree(t, dest, files)
	assertNoPartial(t, dest)
	if sameFile(t, filepath.Join(dest, "b", "a-copy.txt"), fileID(t, filepath.Join(dest, "a.txt"))) {
		t.Error("a.txt and b/a-copy.txt are one file")
	}
}

// TestPullNeverLinksALeftoverToALiveFile: a leftover may be a live file.
// Recovery drops such names (see TestFailedPullCleansOlderPartial), so this
// one is made after it, standing in for one it missed. Wherever it sits, it
// must not also be linked into another entry with the same content.
func TestPullNeverLinksALeftoverToALiveFile(t *testing.T) {
	for name, leftover := range map[string]string{"same path": "a.txt", "other path": "old.txt"} {
		t.Run(name, func(t *testing.T) {
			fake := s3test.NewFake()
			pushFixture(t, fake, map[string]string{"a.txt": "same"})
			dest := filepath.Join(t.TempDir(), "dest")
			mustPull(t, fake, dest)
			files := map[string]string{"a.txt": "same", "b.txt": "same"}
			pushFixture(t, fake, files)
			client := &afterRecovery{S3API: fake, fn: func() {
				mustLink(t, filepath.Join(dest, "a.txt"), filepath.Join(partialDir(dest), leftover))
			}}

			stats, err := pull.Pull(context.Background(), client, "bkt", prefix, dest, pull.Options{})
			if err != nil {
				t.Fatalf("Pull: %v", err)
			}
			if stats.Linked != 1 || stats.Reused != 1 || stats.Downloaded != 0 {
				t.Errorf("stats = %+v, want Linked=1 Reused=1", stats)
			}
			assertTree(t, dest, files)
			if sameFile(t, filepath.Join(dest, "b.txt"), fileID(t, filepath.Join(dest, "a.txt"))) {
				t.Error("a.txt and b.txt are one file")
			}
		})
	}
}

// TestPullFailureKeepsPartialUntilSuccess: a failed pull leaves dest exactly
// as it was and keeps its staged files beside it; the pull that succeeds
// fetches only what was missing and removes them.
func TestPullFailureKeepsPartialUntilSuccess(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	v2 := map[string]string{"a.txt": "alpha", "b.txt": "bravo", "c.txt": "charlie"}
	pushFixture(t, fake, v2)
	fake.Delete(prefix + "/c.txt")

	if _, err := doPull(t, fake, dest); err == nil {
		t.Fatal("Pull succeeded despite a missing object")
	}
	assertTree(t, dest, v1)
	assertNoLeftovers(t, dest)
	if got := readTree(t, partialDir(dest))["b.txt"]; got != "bravo" {
		t.Errorf("partial b.txt = %q, want the downloaded bytes kept", got)
	}

	fake.Store(prefix+"/c.txt", []byte("charlie"))
	fake.ResetCalls()
	stats := mustPull(t, fake, dest)
	if got, want := contentGets(fake), []string{"c.txt"}; !slices.Equal(got, want) {
		t.Errorf("content GETs = %v, want %v", got, want)
	}
	if stats.Linked != 1 || stats.Reused != 1 || stats.Downloaded != 1 {
		t.Errorf("stats = %+v, want Linked=1 Reused=1 Downloaded=1", stats)
	}
	assertTree(t, dest, v2)
	assertNoPartial(t, dest)
}

// TestPullReusedModeChangeCopies: a leftover whose mode differs from the
// entry is copied, never linked and chmodded — the leftover can share
// its inode with the live tree.
func TestPullReusedModeChangeCopies(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("modes are not meaningful on Windows (L3)")
	}
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, files) // 0644
	dest := filepath.Join(t.TempDir(), "dest")
	writeFile(t, partialDir(dest), "a.txt", "alpha", 0o600)
	leftover := fileID(t, filepath.Join(partialDir(dest), "a.txt"))

	stats := mustPull(t, fake, dest)
	if stats.Reused != 1 || stats.Downloaded != 0 {
		t.Errorf("stats = %+v, want Reused=1", stats)
	}
	if mode := statMode(t, filepath.Join(dest, "a.txt")); mode != 0o644 {
		t.Errorf("a.txt mode = %#o, want 0644", mode)
	}
	if sameFile(t, filepath.Join(dest, "a.txt"), leftover) {
		t.Error("leftover inode was linked and chmodded")
	}
}

// TestPullRetryReusesPreviousAttempt: within one call, a retry after a staged
// mismatch reuses the abandoned attempt's verified files.
func TestPullRetryReusesPreviousAttempt(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	pushFixture(t, fake, files)
	bad := true
	fake.GetBody = func(key string, body []byte) []byte {
		if strings.HasSuffix(key, "b.txt") && bad {
			bad = false
			return []byte("BRAVO")
		}
		return body
	}
	dest := filepath.Join(t.TempDir(), "dest")

	stats, err := pull.Pull(context.Background(), fake, "bkt", prefix, dest, pull.Options{Parallel: 1})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if got, want := contentGets(fake), []string{"a.txt", "b.txt", "b.txt"}; !slices.Equal(got, want) {
		t.Errorf("content GETs = %v, want a once and b twice %v", got, want)
	}
	if stats.Reused != 1 || stats.Downloaded != 1 {
		t.Errorf("stats = %+v, want Reused=1 Downloaded=1", stats)
	}
	assertTree(t, dest, files)
	assertNoPartial(t, dest)
}

// TestRecoverKeepsStagingAsPartial: Recover keeps a killed pull's staging
// tree for reuse and never promotes it, with or without a live dest. A
// partial already beside it means the staging tree never got past reusing
// that partial, so the partial is the one kept.
func TestRecoverKeepsStagingAsPartial(t *testing.T) {
	for _, tc := range []struct {
		name          string
		dest, partial bool
		wantPartial   map[string]string
	}{
		{"dest present", true, false, map[string]string{"tmp.txt": "unverified"}},
		{"dest missing", false, false, map[string]string{"tmp.txt": "unverified"}},
		{"earlier partial", true, true, map[string]string{"earlier.txt": "earlier"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "dest")
			if tc.dest {
				writeFile(t, dest, "live.txt", "live", 0o644)
			}
			if tc.partial {
				writeFile(t, partialDir(dest), "earlier.txt", "earlier", 0o644)
			}
			writeFile(t, dest+".pinsync-tmp", "tmp.txt", "unverified", 0o644)

			if err := pull.Recover(context.Background(), dest, nil); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			assertNoLeftovers(t, dest)
			if tc.dest {
				assertTree(t, dest, map[string]string{"live.txt": "live"})
			} else if _, err := os.Lstat(dest); err == nil {
				t.Error("Recover promoted the unverified staging tree")
			}
			assertTree(t, partialDir(dest), tc.wantPartial)
		})
	}
}

// TestDryRunCountsReuseWithoutTouchingPartial: the preview reports what the
// partial tree would supply and leaves it exactly as it was.
func TestDryRunCountsReuseWithoutTouchingPartial(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"a.txt": "alpha", "b.txt": "bravo"})
	dest := filepath.Join(t.TempDir(), "dest")
	writeFile(t, partialDir(dest), "a.txt", "alpha", 0o644)
	before := snapshot(t, partialDir(dest))

	plan, err := pull.DryRun(context.Background(), fake, "bkt", prefix, dest, pull.Options{})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if !reflect.DeepEqual(plan.Download, []string{"a.txt", "b.txt"}) || plan.Reused != 1 {
		t.Errorf("plan = %+v, want both as content changes, one of them reused", plan)
	}
	if after := snapshot(t, partialDir(dest)); !reflect.DeepEqual(after, before) {
		t.Errorf("DryRun mutated the partial tree:\n before %v\n after  %v", before, after)
	}
}

func TestSiblings(t *testing.T) {
	dest := filepath.Join("srv", "dest")
	want := []string{dest + ".pinsync-tmp", dest + ".pinsync-old", dest + ".pinsync-partial"}
	if got := pull.Siblings(dest + string(filepath.Separator)); !slices.Equal(got, want) {
		t.Errorf("Siblings = %v, want %v", got, want)
	}
}

// TestPullStuckStagingStopsRetry: when a failed attempt's staging tree can be
// neither kept nor deleted (on Windows, a handle held without delete
// sharing), the mismatch retry must not run. That tree holds hardlinks to the
// live files, and staging over it would write through them into dest.
func TestPullStuckStagingStopsRetry(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	v2 := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	pushFixture(t, fake, v2)
	// Every content GET mismatches. a.txt is linked, never fetched, unless a
	// retry has already emptied the live file through the stuck tree's link.
	fake.GetBody = func(key string, body []byte) []byte {
		if key == manifestKey {
			return body
		}
		return bytes.ToUpper(body)
	}
	tmp, held := dest+".pinsync-tmp", errors.New("held open")
	restoreRename := pull.SetRenameFn(func(from, to string) error {
		if from == tmp && to == partialDir(dest) {
			return held
		}
		return os.Rename(from, to)
	})
	restoreRemove := pull.SetRemoveAllFn(func(p string) error {
		if p == tmp {
			return held
		}
		return os.RemoveAll(p)
	})

	_, err := pull.Pull(context.Background(), fake, "bkt", prefix, dest, pull.Options{Parallel: 1})
	restoreRename()
	restoreRemove()
	if !errors.Is(err, held) {
		t.Errorf("Pull = %v, want the error that kept the staging tree in place", err)
	}
	if got := contentGets(fake); !slices.Equal(got, []string{"b.txt"}) {
		t.Errorf("content GETs = %v, want one attempt only", got)
	}
	assertTree(t, dest, v1)

	// Once the tree can move again, the next pull converges.
	fake.GetBody = nil
	mustPull(t, fake, dest)
	assertTree(t, dest, v2)
	assertNoLeftovers(t, dest)
	assertNoPartial(t, dest)
}

// TestPullSurvivesUndeletablePartial: a partial tree that cannot be removed
// (an open handle on Windows, an unreadable subdirectory) only costs its
// reuse; failing on it would fail every later pull the same way.
func TestPullSurvivesUndeletablePartial(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	writeFile(t, partialDir(dest), "a.txt", "alpha", 0o644)
	restore := pull.SetRemoveAllFn(func(p string) error {
		if p == partialDir(dest) {
			return errors.New("held open")
		}
		return os.RemoveAll(p)
	})

	stats, err := doPull(t, fake, dest)
	restore()
	if err != nil {
		t.Fatalf("Pull with an undeletable partial tree: %v", err)
	}
	if stats.Reused != 1 || stats.Downloaded != 1 {
		t.Errorf("stats = %+v, want Reused=1 Downloaded=1", stats)
	}
	assertTree(t, dest, files)
	assertNoLeftovers(t, dest)
	// The leftover it linked is live now, so its name in the stuck tree went.
	assertNoLiveLinks(t, dest)

	// Once it can be removed, the next pull does.
	mustPull(t, fake, dest)
	assertTree(t, dest, files)
	assertNoPartial(t, dest)
}

// TestPullNeverLinksReadOnlyFilesOnWindows: on Windows, removing a read-only
// file can clear the attribute first (os.RemoveAll's fallback when the
// filesystem cannot delete it as is), and every hardlink shares it. So a
// read-only live or partial file is copied, never linked: removing the
// partial or old tree then never clears it on a file in dest.
func TestPullNeverLinksReadOnlyFilesOnWindows(t *testing.T) {
	defer pull.SetGOOS("windows")()
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"a.txt": "alpha"})
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	liveA := filepath.Join(dest, "a.txt")
	if err := os.Chmod(liveA, 0o444); err != nil {
		t.Fatal(err)
	}
	before := fileID(t, liveA)
	leftoverB := filepath.Join(partialDir(dest), "b.txt")
	writeFile(t, partialDir(dest), "b.txt", "bravo", 0o444)
	leftover := fileID(t, leftoverB)
	pushFixture(t, fake, map[string]string{"a.txt": "alpha", "b.txt": "bravo"})

	stats := mustPull(t, fake, dest)
	if stats.Linked != 0 || stats.Copied != 1 || stats.Reused != 1 || stats.Downloaded != 0 {
		t.Errorf("stats = %+v, want Copied=1 Reused=1", stats)
	}
	if sameFile(t, liveA, before) {
		t.Error("read-only live a.txt was linked into the new tree")
	}
	if sameFile(t, filepath.Join(dest, "b.txt"), leftover) {
		t.Error("read-only partial b.txt was linked into dest")
	}
}

// fileID stats p through an open handle, pinning the file p names now. On
// Windows os.Stat records only the path and os.SameFile reads the file ID
// from it when called, so an os.Stat taken before a swap compares the file
// that p names after it (os/types_windows.go, loadFileId).
func fileID(t *testing.T, p string) os.FileInfo {
	t.Helper()
	fh, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fh.Close() }()
	info, err := fh.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func sameFile(t *testing.T, p string, want os.FileInfo) bool {
	t.Helper()
	info, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	return os.SameFile(info, want)
}

// TestPullAfterKillDuringPartialRemoval: a pull killed while removing the
// partial tree leaves its staging tree (links into dest and the partial tree,
// plus a stray unverified file) and a partly deleted partial tree. The next
// pull converges without trusting either, and dest is unchanged until the
// swap.
func TestPullAfterKillDuringPartialRemoval(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	v2 := map[string]string{"a.txt": "alpha", "b.txt": "bravo", "c.txt": "charlie", "d.txt": "delta"}
	pushFixture(t, fake, v2)

	tmp, partial := dest+".pinsync-tmp", partialDir(dest)
	writeFile(t, partial, "b.txt", "bravo", 0o644)
	writeFile(t, partial, "c.txt", "charlie", 0o644)
	for src, dst := range map[string]string{
		filepath.Join(dest, "a.txt"):    filepath.Join(tmp, "a.txt"),
		filepath.Join(partial, "b.txt"): filepath.Join(tmp, "b.txt"),
	} {
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(src, dst); err != nil {
			t.Skipf("hardlinks unavailable: %v", err)
		}
	}
	writeFile(t, tmp, "d.txt", "DELTA", 0o644) // never verified
	if err := os.Remove(filepath.Join(partial, "b.txt")); err != nil {
		t.Fatal(err) // removal got this far
	}
	before := snapshot(t, dest)

	stats, err := pull.Pull(context.Background(), fake, "bkt", prefix, dest, pull.Options{
		BeforeReplace: func(context.Context, []byte) error {
			if got := snapshot(t, dest); !reflect.DeepEqual(got, before) {
				t.Errorf("dest changed before the swap:\n before %v\n after  %v", before, got)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if stats.Reused != 1 || stats.Linked != 1 {
		t.Errorf("stats = %+v, want c.txt reused and a.txt linked", stats)
	}
	assertTree(t, dest, v2)
	assertNoLeftovers(t, dest)
	assertNoPartial(t, dest)
}

// TestPullPartialIsRegularFile: a regular file where the partial tree belongs
// must not wedge pulls. A failed pull replaces it with a real partial tree
// and the next one converges.
func TestPullPartialIsRegularFile(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	v2 := map[string]string{"a.txt": "alpha", "b.txt": "bravo", "c.txt": "charlie"}
	pushFixture(t, fake, v2)
	fake.Delete(prefix + "/c.txt")
	if err := os.WriteFile(partialDir(dest), []byte("bravo"), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := doPull(t, fake, dest); err == nil {
		t.Fatal("Pull succeeded despite a missing object")
	}
	assertTree(t, dest, v1)
	assertNoLeftovers(t, dest)
	if got := readTree(t, partialDir(dest))["b.txt"]; got != "bravo" {
		t.Errorf("partial b.txt = %q, want the staged bytes kept", got)
	}

	fake.Store(prefix+"/c.txt", []byte("charlie"))
	mustPull(t, fake, dest)
	assertTree(t, dest, v2)
	assertNoLeftovers(t, dest)
	assertNoPartial(t, dest)
}

// regularFiles lists every regular file under root, none if it is missing.
func regularFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if p == root && errors.Is(err, fs.ErrNotExist) {
			return fs.SkipAll
		}
		if err == nil && d.Type().IsRegular() {
			files = append(files, p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// assertNoLiveLinks asserts no live file has a second name in the partial
// tree, or anywhere: a caller may reject a live file with a second link, as
// osconfig-sync does on Windows.
func assertNoLiveLinks(t *testing.T, dest string) {
	t.Helper()
	kept := regularFiles(t, partialDir(dest))
	for _, live := range regularFiles(t, dest) {
		if n := linkCount(t, live); n != 1 {
			t.Errorf("live %s has %d links, want 1", live, n)
		}
		id := fileID(t, live)
		for _, p := range kept {
			if sameFile(t, p, id) {
				t.Errorf("partial %s is live %s", p, live)
			}
		}
	}
}

// failBeforeReplace runs a pull that fails in BeforeReplace, after staging.
func failBeforeReplace(t *testing.T, fake *s3test.Fake, dest string) {
	t.Helper()
	refused := errors.New("refused")
	_, err := pull.Pull(context.Background(), fake, "bkt", prefix, dest, pull.Options{
		BeforeReplace: func(context.Context, []byte) error { return refused },
	})
	if !errors.Is(err, refused) {
		t.Fatalf("Pull = %v, want the BeforeReplace error", err)
	}
}

// TestPullFailureKeepsNoLiveLinks: a pull that fails after staging keeps
// what it downloaded, but not the live files it linked, which would give
// each a second name until a pull succeeds. Linking them again costs
// nothing.
func TestPullFailureKeepsNoLiveLinks(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	v2 := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo", "c.txt": "charlie"}
	pushFixture(t, fake, v2)

	failBeforeReplace(t, fake, dest)
	assertTree(t, dest, v1)
	assertNoLeftovers(t, dest)
	assertNoLiveLinks(t, dest)
	assertTree(t, partialDir(dest), map[string]string{"c.txt": "charlie", "sub": "<dir>"})

	fake.ResetCalls()
	stats := mustPull(t, fake, dest)
	if got := contentGets(fake); len(got) != 0 {
		t.Errorf("content GETs = %v, want none", got)
	}
	if stats.Linked != 2 || stats.Reused != 1 || stats.Downloaded != 0 {
		t.Errorf("stats = %+v, want Linked=2 Reused=1", stats)
	}
	assertTree(t, dest, v2)
	assertNoPartial(t, dest)
}

// TestRecoverKeepsNoLiveLinks: recovery after a kill has no record of what
// the killed pull linked, and a staged file can share a live file at
// another path (a leftover it reused) or in the resume directory. None is
// kept; its own downloads and prefixes are.
func TestRecoverKeepsNoLiveLinks(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "dest")
	tmp := dest + ".pinsync-tmp"
	live := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"}
	for rel, content := range live {
		writeFile(t, dest, rel, content, 0o644)
	}
	prefixData := ".pinsync-resume/" + sha(bigContent(bigSize))
	for from, to := range map[string]string{
		"a.txt":     "a.txt",
		"sub/b.txt": "moved.txt",
	} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(tmp, to)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Link(filepath.Join(dest, from), filepath.Join(tmp, to)); err != nil {
			t.Skipf("hardlinks unavailable: %v", err)
		}
	}
	writeFile(t, tmp, prefixData, "cut", 0o644)
	if err := os.Link(filepath.Join(dest, "a.txt"), resumeFile(tmp, "alpha", "")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, tmp, "c.txt", "charlie", 0o644)

	if err := pull.Recover(context.Background(), dest, nil); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertTree(t, dest, live)
	assertNoLeftovers(t, dest)
	assertNoLiveLinks(t, dest)
	assertTree(t, partialDir(dest), map[string]string{"c.txt": "charlie", prefixData: "cut"})
}

// TestPullDiscardsStagingWithAStuckLiveLink: a live file's staged name that
// cannot be removed (on Windows, a consumer holding the file without delete
// sharing) would be kept as a second name, so the staging tree is discarded
// instead, and the live file is left alone.
func TestPullDiscardsStagingWithAStuckLiveLink(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	pushFixture(t, fake, map[string]string{"a.txt": "alpha", "b.txt": "bravo"})
	stuck := filepath.Join(dest+".pinsync-tmp", "a.txt")
	restore := pull.SetRemoveFn(func(p string) error {
		if p == stuck {
			return errors.New("held open")
		}
		return os.Remove(p)
	})

	failBeforeReplace(t, fake, dest)
	restore()
	assertTree(t, dest, v1)
	assertNoLeftovers(t, dest)
	assertNoPartial(t, dest)
	assertNoLiveLinks(t, dest)
}

// TestRecoverDiscardsPrefixesWithAStuckLiveLink: the same holds in the
// resume directory, whose prefixes are otherwise moved into a partial tree
// even when the staging tree is discarded.
func TestRecoverDiscardsPrefixesWithAStuckLiveLink(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "dest")
	tmp := dest + ".pinsync-tmp"
	writeFile(t, dest, "a.txt", "alpha", 0o644)
	stuck := resumeFile(tmp, "alpha", "")
	if err := os.MkdirAll(filepath.Dir(stuck), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(dest, "a.txt"), stuck); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
	restore := pull.SetRemoveFn(func(p string) error {
		if p == stuck {
			return errors.New("held open")
		}
		return os.Remove(p)
	})

	err := pull.Recover(context.Background(), dest, nil)
	restore()
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertTree(t, dest, map[string]string{"a.txt": "alpha"})
	assertNoLeftovers(t, dest)
	assertNoPartial(t, dest)
	assertNoLiveLinks(t, dest)
}

// afterRecovery runs fn when the pull fetches its manifest: after recovery,
// which drops every partial name with a second link, and before staging.
type afterRecovery struct {
	pull.S3API
	fn func()
}

func (c *afterRecovery) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if aws.ToString(in.Key) == manifestKey && c.fn != nil {
		c.fn()
		c.fn = nil
	}
	return c.S3API.GetObject(ctx, in, opts...)
}

// mustLink gives from's file the second name to, skipping the test where
// hardlinks are unavailable.
func mustLink(t *testing.T, from, to string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(from, to); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
}

// TestFailedPullCleansOlderPartial: v0.5.0 and v0.5.1 kept the live files a
// failed pull linked, so their partial tree gives live files a second name,
// with no staging tree beside it. A pull that fails before staging, here on
// a missing manifest, still drops those names and keeps the rest.
func TestFailedPullCleansOlderPartial(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "dest")
	live := map[string]string{"a.txt": "alpha"}
	writeFile(t, dest, "a.txt", "alpha", 0o644)
	writeFile(t, partialDir(dest), "c.txt", "charlie", 0o644)
	mustLink(t, filepath.Join(dest, "a.txt"), filepath.Join(partialDir(dest), "a.txt"))

	if _, err := doPull(t, s3test.NewFake(), dest); err == nil {
		t.Fatal("Pull without a manifest succeeded")
	}
	assertTree(t, dest, live)
	assertNoLeftovers(t, dest)
	assertNoLiveLinks(t, dest)
	assertTree(t, partialDir(dest), map[string]string{"c.txt": "charlie"})
}

// TestRecoverCleansOlderPartialBesideStaging: with both an older partial
// tree and a killed pull's staging tree, recovery drops the staging tree
// first, so a leftover the killed pull linked into it has one name again
// and is kept, while the partial tree's live links go.
func TestRecoverCleansOlderPartialBesideStaging(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "dest")
	tmp, partial := dest+".pinsync-tmp", partialDir(dest)
	live := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	for rel, content := range live {
		writeFile(t, dest, rel, content, 0o644)
	}
	mustLink(t, filepath.Join(dest, "a.txt"), filepath.Join(partial, "a.txt"))
	writeFile(t, partial, "c.txt", "charlie", 0o644)
	mustLink(t, filepath.Join(partial, "c.txt"), filepath.Join(tmp, "c.txt"))
	mustLink(t, filepath.Join(dest, "b.txt"), filepath.Join(tmp, "b.txt"))

	if err := pull.Recover(context.Background(), dest, nil); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertTree(t, dest, live)
	assertNoLeftovers(t, dest)
	assertNoLiveLinks(t, dest)
	assertTree(t, partial, map[string]string{"c.txt": "charlie"})
}

// TestRecoverCleansOlderPartialBesideAStuckOld: a stale old tree that
// cannot be removed fails recovery, but does not keep a partial tree's
// second names of live files, such as one v0.5.0 or v0.5.1 left.
func TestRecoverCleansOlderPartialBesideAStuckOld(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "dest")
	old, partial := dest+".pinsync-old", partialDir(dest)
	live := map[string]string{"a.txt": "alpha"}
	writeFile(t, dest, "a.txt", "alpha", 0o644)
	writeFile(t, old, "a.txt", "older", 0o644)
	writeFile(t, partial, "c.txt", "charlie", 0o644)
	mustLink(t, filepath.Join(dest, "a.txt"), filepath.Join(partial, "a.txt"))
	restore := pull.SetRemoveAllFn(func(p string) error {
		if p == old {
			return errors.New("held open")
		}
		return os.RemoveAll(p)
	})

	err := pull.Recover(context.Background(), dest, nil)
	restore()
	if err == nil || !strings.Contains(err.Error(), old) {
		t.Fatalf("Recover = %v, want the error removing %s", err, old)
	}
	assertTree(t, dest, live)
	assertNoLiveLinks(t, dest)
	assertTree(t, partial, map[string]string{"c.txt": "charlie"})
}

// TestRecoverFailsOnAStuckPartialLink: a live file's name in the partial
// tree that cannot be removed costs the whole tree; if that cannot be
// removed either, Recover fails rather than report a tree a caller may
// reject.
func TestRecoverFailsOnAStuckPartialLink(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "dest")
	writeFile(t, dest, "a.txt", "alpha", 0o644)
	writeFile(t, partialDir(dest), "c.txt", "charlie", 0o644)
	stuck := filepath.Join(partialDir(dest), "a.txt")
	mustLink(t, filepath.Join(dest, "a.txt"), stuck)
	defer pull.SetRemoveFn(func(p string) error {
		if p == stuck {
			return errors.New("held open")
		}
		return os.Remove(p)
	})()
	restore := pull.SetRemoveAllFn(func(p string) error {
		if p == partialDir(dest) {
			return errors.New("held open")
		}
		return os.RemoveAll(p)
	})

	err := pull.Recover(context.Background(), dest, nil)
	restore()
	if err == nil {
		t.Fatal("Recover left a live file with a second name and succeeded")
	}
	if err := pull.Recover(context.Background(), dest, nil); err != nil {
		t.Fatalf("Recover with a removable partial tree: %v", err)
	}
	assertTree(t, dest, map[string]string{"a.txt": "alpha"})
	assertNoPartial(t, dest)
	assertNoLiveLinks(t, dest)
}

// TestPullFailsOnAStuckPartialLink: a leftover linked into the staging tree
// becomes live at the swap, so if the partial tree cannot be removed and
// neither can that leftover's name in it, the pull fails before the swap.
func TestPullFailsOnAStuckPartialLink(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	writeFile(t, partialDir(dest), "a.txt", "alpha", 0o644)
	stuck := filepath.Join(partialDir(dest), "a.txt")
	restoreRemove := pull.SetRemoveFn(func(p string) error {
		if p == stuck {
			return errors.New("held open")
		}
		return os.Remove(p)
	})
	restoreAll := pull.SetRemoveAllFn(func(p string) error {
		if p == partialDir(dest) {
			return errors.New("held open")
		}
		return os.RemoveAll(p)
	})

	_, err := doPull(t, fake, dest)
	restoreAll()
	restoreRemove()
	if err == nil {
		t.Fatal("Pull left a live file with a second name and succeeded")
	}
	if _, err := os.Lstat(dest); err == nil {
		t.Error("failed pull created dest")
	}
	assertNoLeftovers(t, dest)

	mustPull(t, fake, dest)
	assertTree(t, dest, files)
	assertNoPartial(t, dest)
}
