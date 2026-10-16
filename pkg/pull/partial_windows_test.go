//go:build windows

package pull_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/s3test"
)

// holdOpen opens p the way a consumer that never asked for delete sharing
// does: until release, p cannot be deleted, and whether its directory can be
// renamed is up to NTFS.
func holdOpen(t *testing.T, p string) (release func()) {
	t.Helper()
	name, err := syscall.UTF16PtrFromString(p)
	if err != nil {
		t.Fatal(err)
	}
	h, err := syscall.CreateFile(name, syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE, nil,
		syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatalf("opening %s: %v", p, err)
	}
	var once sync.Once
	release = func() { once.Do(func() { _ = syscall.CloseHandle(h) }) }
	t.Cleanup(release) // runs before the TempDir cleanups registered earlier
	return release
}

// TestPullWithPartialFileHeldOpen: a handle without delete sharing on a file
// of the partial tree, directly or through a live file linked into it, may
// fail the pull but never leaves dest half-changed, and the next pull after
// it closes converges.
func TestPullWithPartialFileHeldOpen(t *testing.T) {
	v1 := map[string]string{"a.txt": "alpha"}
	v2 := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	for _, tc := range []struct {
		name string
		hold func(t *testing.T, dest string) string // arranges the partial tree, returns the path to hold
	}{
		{"partial file", func(t *testing.T, dest string) string {
			writeFile(t, partialDir(dest), "b.txt", "bravo", 0o644)
			return filepath.Join(partialDir(dest), "b.txt")
		}},
		{"live file linked into partial", func(t *testing.T, dest string) string {
			live := filepath.Join(dest, "a.txt")
			if err := os.MkdirAll(partialDir(dest), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(live, filepath.Join(partialDir(dest), "a.txt")); err != nil {
				t.Fatal(err)
			}
			return live
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := s3test.NewFake()
			pushFixture(t, fake, v1)
			dest := filepath.Join(t.TempDir(), "dest")
			mustPull(t, fake, dest)
			pushFixture(t, fake, v2)
			release := holdOpen(t, tc.hold(t, dest))
			before := snapshot(t, dest)

			if _, err := doPull(t, fake, dest); err != nil {
				t.Logf("pull with the handle held failed: %v", err)
				if got := snapshot(t, dest); !reflect.DeepEqual(got, before) {
					t.Errorf("failed pull changed dest:\n before %v\n after  %v", before, got)
				}
			} else {
				assertTree(t, dest, v2)
			}

			release()
			mustPull(t, fake, dest)
			assertTree(t, dest, v2)
			assertNoLeftovers(t, dest)
			assertNoPartial(t, dest)
		})
	}
}

// TestPullHardlinksPartialOnNTFS: on NTFS a leftover is reused as a hardlink,
// not copied.
func TestPullHardlinksPartialOnNTFS(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	writeFile(t, partialDir(dest), "a.txt", "alpha", 0o644)
	control := filepath.Join(filepath.Dir(dest), "control")
	if err := os.Link(filepath.Join(partialDir(dest), "a.txt"), control); err != nil {
		t.Fatal(err)
	}
	ctl, err := os.Stat(control)
	if err != nil {
		t.Fatal(err)
	}

	stats := mustPull(t, fake, dest)
	if stats.Reused != 1 || stats.Downloaded != 0 {
		t.Errorf("stats = %+v, want Reused=1", stats)
	}
	if !sameFile(t, filepath.Join(dest, "a.txt"), ctl) {
		t.Error("leftover a.txt was copied, not linked")
	}
	assertTree(t, dest, files)
	assertNoPartial(t, dest)
}

// storeManifest publishes files with mode for each into the fake.
func storeManifest(t *testing.T, fake *s3test.Fake, files map[string]string, modes map[string]string) {
	t.Helper()
	m := &manifest.Manifest{Version: 1}
	for rel, content := range files {
		sum := sha256.Sum256([]byte(content))
		m.Files = append(m.Files, manifest.File{
			Path: rel, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(content)), Mode: modes[rel],
		})
	}
	var buf bytes.Buffer
	if err := m.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	fake.Store(manifestKey, buf.Bytes())
	fake.ResetCalls()
}

func readOnly(t *testing.T, p string) bool {
	t.Helper()
	return statMode(t, p)&0o200 == 0
}

// TestPullKeepsReadOnlyAttribute: a read-only file in dest is never shared
// with the staging, partial or old tree, so removing them cannot clear its
// attribute (see TestPullNeverLinksReadOnlyFilesOnWindows for the rule).
func TestPullKeepsReadOnlyAttribute(t *testing.T) {
	fake := s3test.NewFake()
	modes := map[string]string{"a.txt": "0444", "b.txt": "0644", "c.txt": "0644"}
	fake.Store(prefix+"/a.txt", []byte("alpha"))
	storeManifest(t, fake, map[string]string{"a.txt": "alpha"}, modes)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	liveA := filepath.Join(dest, "a.txt")
	if !readOnly(t, liveA) {
		t.Fatal("a 0444 entry was not made read-only")
	}

	v2 := map[string]string{"a.txt": "alpha", "b.txt": "bravo", "c.txt": "charlie"}
	fake.Store(prefix+"/b.txt", []byte("bravo"))
	storeManifest(t, fake, v2, modes) // c.txt not uploaded yet
	if _, err := doPull(t, fake, dest); err == nil {
		t.Fatal("Pull succeeded despite a missing object")
	}
	info, err := os.Stat(liveA)
	if err != nil {
		t.Fatal(err)
	}
	if sameFile(t, filepath.Join(partialDir(dest), "a.txt"), info) {
		t.Error("read-only a.txt is shared between dest and the partial tree")
	}

	fake.Store(prefix+"/c.txt", []byte("charlie"))
	stats := mustPull(t, fake, dest)
	if stats.Copied != 1 || stats.Linked != 0 || stats.Reused != 1 || stats.Downloaded != 1 {
		t.Errorf("stats = %+v, want a copied, b reused, c downloaded", stats)
	}
	if !readOnly(t, liveA) {
		t.Error("a.txt lost its read-only attribute")
	}
	assertTree(t, dest, v2)
	assertNoPartial(t, dest)
}

// TestRecoverWithStagingFileHeldOpen: keeping a staging tree that holds a
// file open without delete sharing either renames it whole or fails with the
// tree still in place; either way nothing reaches dest, and once the handle
// closes the tree is kept.
func TestRecoverWithStagingFileHeldOpen(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "dest")
	tmp := dest + ".pinsync-tmp"
	writeFile(t, dest, "live.txt", "live", 0o644)
	writeFile(t, tmp, "a.txt", "staged", 0o644)
	release := holdOpen(t, filepath.Join(tmp, "a.txt"))

	if err := pull.Recover(context.Background(), dest, nil); err != nil {
		t.Logf("Recover with the handle held failed: %v", err)
		assertTree(t, tmp, map[string]string{"a.txt": "staged"})
		if _, err := os.Lstat(partialDir(dest)); err == nil {
			t.Error("a failed keep left a partial tree beside the staging tree")
		}
	} else {
		t.Log("the staging tree was renamed with the handle held")
		assertTree(t, partialDir(dest), map[string]string{"a.txt": "staged"})
	}
	assertTree(t, dest, map[string]string{"live.txt": "live"})

	release()
	if err := pull.Recover(context.Background(), dest, nil); err != nil {
		t.Fatalf("Recover after the handle closed: %v", err)
	}
	assertNoLeftovers(t, dest)
	assertTree(t, partialDir(dest), map[string]string{"a.txt": "staged"})
	assertTree(t, dest, map[string]string{"live.txt": "live"})
}
