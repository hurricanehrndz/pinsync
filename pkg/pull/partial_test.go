package pull_test

import (
	"bytes"
	"context"
	"errors"
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
	control := filepath.Join(t.TempDir(), "control")
	if err := os.Link(filepath.Join(partialDir(dest), "a.txt"), control); err != nil {
		t.Fatal(err)
	}

	stats := mustPull(t, fake, dest)
	if stats.Reused != 1 || stats.Downloaded != 0 {
		t.Errorf("stats = %+v, want Reused=1", stats)
	}
	if mode := statMode(t, filepath.Join(dest, "a.txt")); mode != 0o644 {
		t.Errorf("a.txt mode = %#o, want 0644", mode)
	}
	if mode := statMode(t, control); mode != 0o600 {
		t.Errorf("leftover inode was chmodded: control mode = %#o, want 0600", mode)
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
	writeFile(t, partialDir(dest), "b.txt", "bravo", 0o644)
	control := filepath.Join(t.TempDir(), "control")
	if err := os.Link(leftoverB, control); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(leftoverB, 0o444); err != nil {
		t.Fatal(err)
	}
	pushFixture(t, fake, map[string]string{"a.txt": "alpha", "b.txt": "bravo"})

	stats := mustPull(t, fake, dest)
	if stats.Linked != 0 || stats.Copied != 1 || stats.Reused != 1 || stats.Downloaded != 0 {
		t.Errorf("stats = %+v, want Copied=1 Reused=1", stats)
	}
	if sameFile(t, liveA, before) {
		t.Error("read-only live a.txt was linked into the new tree")
	}
	if ctl, err := os.Stat(control); err != nil || sameFile(t, filepath.Join(dest, "b.txt"), ctl) {
		t.Errorf("read-only partial b.txt was linked into dest (stat err %v)", err)
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
