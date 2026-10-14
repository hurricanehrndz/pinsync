package pull_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/s3test"
)

// countingClient wraps an S3API and counts the body bytes pull actually
// reads, to prove oversized bodies are not drained to EOF.
type countingClient struct {
	pull.S3API
	read atomic.Int64
}

func (c *countingClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	out, err := c.S3API.GetObject(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	out.Body = countingBody{out.Body, &c.read}
	return out, nil
}

type countingBody struct {
	io.ReadCloser
	n *atomic.Int64
}

func (b countingBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.n.Add(int64(n))
	return n, err
}

// failingRename returns a rename that fails whenever the destination is
// to, and passes everything else through.
func failingRename(to string) func(oldpath, newpath string) error {
	return func(oldpath, newpath string) error {
		if newpath == to {
			return errors.New("injected rename failure")
		}
		return os.Rename(oldpath, newpath)
	}
}

func TestPullWithManifestReturnsInstalledBytes(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, files)
	want, _ := fake.Object(manifestKey)

	var seen [][]byte
	res, err := pull.PullWithManifest(context.Background(), fake, "bkt", prefix,
		filepath.Join(t.TempDir(), "dest"), pull.Options{
			BeforeReplace: func(_ context.Context, b []byte) error {
				seen = append(seen, b)
				return nil
			},
		})
	if err != nil {
		t.Fatalf("PullWithManifest: %v", err)
	}
	if !bytes.Equal(res.Manifest, want) {
		t.Errorf("Result.Manifest = %q, want the published bytes %q", res.Manifest, want)
	}
	if len(seen) != 1 || !bytes.Equal(seen[0], want) {
		t.Errorf("BeforeReplace saw %q, want one call with the published bytes", seen)
	}
	if res.Stats.Downloaded != 1 || res.Stats.Total != 1 {
		t.Errorf("stats = %+v, want 1 downloaded of 1", res.Stats)
	}
}

// TestPullWithManifestBytesAcrossRetries: the first attempt's manifest is
// stale, so it mismatches and is retried; the returned bytes and the callback
// must belong to the attempt that installed, never the abandoned one.
func TestPullWithManifestBytesAcrossRetries(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"a.txt": "old-content"})
	stale, _ := fake.Object(manifestKey)
	fake.Store(prefix+"/a.txt", []byte("new-content"))
	corrected := encodeManifest(t, map[string]string{"a.txt": "new-content"})
	gets := 0
	fake.BeforeGet = func(f *s3test.Fake, key string) {
		if key != manifestKey {
			return
		}
		if gets++; gets == 2 {
			f.Store(manifestKey, corrected)
		}
	}

	var seen [][]byte
	dest := filepath.Join(t.TempDir(), "dest")
	res, err := pull.PullWithManifest(context.Background(), fake, "bkt", prefix, dest, pull.Options{
		BeforeReplace: func(_ context.Context, b []byte) error {
			seen = append(seen, b)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("PullWithManifest: %v", err)
	}
	if bytes.Equal(stale, corrected) {
		t.Fatal("fixture error: stale and corrected manifests are identical")
	}
	if !bytes.Equal(res.Manifest, corrected) {
		t.Errorf("Result.Manifest = %q, want the installed attempt's %q", res.Manifest, corrected)
	}
	if len(seen) != 1 || !bytes.Equal(seen[0], corrected) {
		t.Errorf("BeforeReplace saw %q, want exactly the installed attempt's bytes", seen)
	}
	if gets != 2 {
		t.Errorf("manifest fetched %d times, want 2 (one retry, no refetch after replacement)", gets)
	}
	assertTree(t, dest, map[string]string{"a.txt": "new-content"})
}

func TestPullBeforeReplaceFailureLeavesDestUntouched(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha", "b.txt": "bravo"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	pushFixture(t, fake, map[string]string{"a.txt": "alpha", "b.txt": "BRAVO!", "c.txt": "charlie"})

	sentinel := errors.New("pending record not written")
	calls := 0
	_, err := pull.PullWithManifest(context.Background(), fake, "bkt", prefix, dest, pull.Options{
		BeforeReplace: func(context.Context, []byte) error {
			calls++
			assertTree(t, dest, v1) // the callback runs before dest is touched
			return sentinel
		},
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("PullWithManifest = %v, want the callback's error", err)
	}
	if calls != 1 {
		t.Errorf("BeforeReplace called %d times, want 1 (no retry)", calls)
	}
	assertTree(t, dest, v1)
	assertNoLeftovers(t, dest)
}

// TestRecoverInterruptedReplacement interrupts a replacement after
// BeforeReplace, between the swap's renames, with the rollback failing too —
// the crash window. The error must return without another attempt, and
// Recover alone (no client) must restore the previous tree.
func TestRecoverInterruptedReplacement(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	pushFixture(t, fake, map[string]string{"a.txt": "alpha2"})

	restore := pull.SetRenameFn(failingRename(dest))
	calls := 0
	_, err := pull.PullWithManifest(context.Background(), fake, "bkt", prefix, dest, pull.Options{
		BeforeReplace: func(context.Context, []byte) error { calls++; return nil },
	})
	restore()
	if err == nil {
		t.Fatal("PullWithManifest succeeded despite the injected rename failure")
	}
	if calls != 1 || len(fake.Gets()) != 2 {
		t.Errorf("BeforeReplace calls = %d, gets = %v; want one attempt only", calls, fake.Gets())
	}
	if _, err := os.Lstat(dest); err == nil {
		t.Fatal("fixture error: dest survived; the swap was not interrupted")
	}

	if err := pull.Recover(context.Background(), dest, nil); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	assertTree(t, dest, v1)
	assertNoLeftovers(t, dest)
}

func TestRecoverLeftoverSiblings(t *testing.T) {
	for _, tc := range []struct {
		name           string
		dest, tmp, old bool
		want           map[string]string // nil: dest stays absent
	}{
		{"dest with stale tmp and old", true, true, true, map[string]string{"live.txt": "live"}},
		{"dest missing, old and tmp", false, true, true, map[string]string{"old.txt": "old"}},
		{"dest missing, old only", false, false, true, map[string]string{"old.txt": "old"}},
		{"dest missing, tmp only", false, true, false, nil},
		{"nothing to do", true, false, false, map[string]string{"live.txt": "live"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "dest")
			if tc.dest {
				writeFile(t, dest, "live.txt", "live", 0o644)
			}
			if tc.tmp {
				writeFile(t, dest+".pinsync-tmp", "tmp.txt", "unverified", 0o644)
			}
			if tc.old {
				writeFile(t, dest+".pinsync-old", "old.txt", "old", 0o644)
			}
			if err := pull.Recover(context.Background(), dest, nil); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			assertNoLeftovers(t, dest)
			if tc.want == nil {
				if _, err := os.Lstat(dest); err == nil {
					t.Error("Recover promoted the unverified staging tree")
				}
				return
			}
			assertTree(t, dest, tc.want)
		})
	}
}

func TestPullRejectsOversizedBody(t *testing.T) {
	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	pushFixture(t, fake, map[string]string{"a.txt": "bravo"})
	huge := bytes.Repeat([]byte("x"), 1<<20)
	fake.GetBody = func(key string, body []byte) []byte {
		if strings.HasSuffix(key, "a.txt") {
			return huge
		}
		return body
	}
	client := &countingClient{S3API: fake}

	_, err := pull.Pull(context.Background(), client, "bkt", prefix, dest, pull.Options{})
	if err == nil || !strings.Contains(err.Error(), "attempts") {
		t.Fatalf("Pull = %v, want mismatch exhaustion", err)
	}
	// Three attempts, each reading the manifest plus at most size+1 bytes.
	manifestBody, _ := fake.Object(manifestKey)
	if limit := 3 * int64(len(manifestBody)+len("bravo")+1); client.read.Load() > limit {
		t.Errorf("read %d body bytes, want at most %d (oversized body drained)", client.read.Load(), limit)
	}
	assertTree(t, dest, v1)
	assertNoLeftovers(t, dest)
}

func TestPullRejectsShortBody(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"a.txt": "alpha"})
	fake.GetBody = func(key string, body []byte) []byte {
		if strings.HasSuffix(key, "a.txt") {
			return body[:2]
		}
		return body
	}
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := doPull(t, fake, dest); err == nil || !strings.Contains(err.Error(), "attempts") {
		t.Fatalf("Pull = %v, want mismatch exhaustion", err)
	}
	if _, err := os.Lstat(dest); err == nil {
		t.Error("dest created from a truncated body")
	}
	assertNoLeftovers(t, dest)
}

func TestPullManifestSizeLimit(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"a.txt": "alpha"})
	body, _ := fake.Object(manifestKey)
	// Trailing whitespace keeps the manifest valid but far over the limit, so
	// draining it to EOF would show in the byte count.
	padded := append(bytes.Clone(body), bytes.Repeat([]byte(" "), 1<<20)...)
	fake.Store(manifestKey, padded)
	client := &countingClient{S3API: fake}
	dest := filepath.Join(t.TempDir(), "dest")

	limit := int64(len(body))
	_, err := pull.Pull(context.Background(), client, "bkt", prefix, dest, pull.Options{MaxManifestBytes: limit})
	if err == nil || !strings.Contains(err.Error(), "manifest limit") {
		t.Fatalf("Pull = %v, want manifest limit error", err)
	}
	if got := client.read.Load(); got > limit+1 {
		t.Errorf("read %d manifest bytes, want at most %d", got, limit+1)
	}
	if _, err := os.Lstat(dest); err == nil {
		t.Error("dest created despite an oversized manifest")
	}

	// Exactly at the limit is allowed.
	if _, err := pull.Pull(context.Background(), fake, "bkt", prefix, dest,
		pull.Options{MaxManifestBytes: int64(len(padded))}); err != nil {
		t.Fatalf("Pull at the limit: %v", err)
	}
}

func TestPullContentSizeLimit(t *testing.T) {
	fake := s3test.NewFake()
	pushFixture(t, fake, map[string]string{"a.txt": "alpha", "b.txt": "bravo"})
	dest := filepath.Join(t.TempDir(), "dest")

	_, err := pull.Pull(context.Background(), fake, "bkt", prefix, dest, pull.Options{MaxContentBytes: 9})
	if err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("Pull = %v, want content limit error", err)
	}
	if gets := fake.Gets(); len(gets) != 1 {
		t.Errorf("gets = %v, want the manifest only", gets)
	}
	if _, err := pull.Pull(context.Background(), fake, "bkt", prefix, dest, pull.Options{MaxContentBytes: 10}); err != nil {
		t.Fatalf("Pull at the limit: %v", err)
	}
}

// sourceTree writes files (0644) plus a top-level manifest.json — as a local
// publication would carry — and returns the root and the manifest bytes.
func sourceTree(t *testing.T, files map[string]string) (string, []byte) {
	t.Helper()
	root := t.TempDir()
	for rel, content := range files {
		writeFile(t, root, rel, content, 0o644)
	}
	m := encodeManifest(t, files)
	writeFile(t, root, "manifest.json", string(m), 0o644)
	return root, m
}

func TestReplaceFromDirectory(t *testing.T) {
	files := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"}
	source, m := sourceTree(t, files)
	before := snapshot(t, source)
	dest := filepath.Join(t.TempDir(), "dest")

	var seen []byte
	res, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{
		BeforeReplace: func(_ context.Context, b []byte) error { seen = b; return nil },
	})
	if err != nil {
		t.Fatalf("ReplaceFromDirectory: %v", err)
	}
	assertTree(t, dest, files) // the source's manifest.json is not listed, so not copied
	assertNoLeftovers(t, dest)
	if !bytes.Equal(res.Manifest, m) || !bytes.Equal(seen, m) {
		t.Errorf("Result.Manifest = %q, callback saw %q; want the supplied bytes", res.Manifest, seen)
	}
	res.Manifest[0] = 'X'
	if m[0] == 'X' {
		t.Error("Result.Manifest aliases the caller's slice")
	}
	if res.Stats.Downloaded != 2 {
		t.Errorf("stats = %+v, want 2 copied from source", res.Stats)
	}
	srcInfo, _ := os.Stat(filepath.Join(source, "a.txt"))
	dstInfo, _ := os.Stat(filepath.Join(dest, "a.txt"))
	if os.SameFile(srcInfo, dstInfo) {
		t.Error("dest hardlinks the source; later source edits would leak into dest")
	}

	// A second run reuses the live files instead of the source.
	res, err = pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{})
	if err != nil {
		t.Fatalf("second ReplaceFromDirectory: %v", err)
	}
	if res.Stats.Linked != 2 || res.Stats.Downloaded != 0 {
		t.Errorf("second stats = %+v, want Linked=2", res.Stats)
	}
	if after := snapshot(t, source); !maps.Equal(before, after) {
		t.Errorf("source changed:\n before %v\n after  %v", before, after)
	}
}

func TestReplaceFromDirectoryCopyFallback(t *testing.T) {
	files := map[string]string{"a.txt": "alpha"}
	source, m := sourceTree(t, files)
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{}); err != nil {
		t.Fatal(err)
	}
	restore := pull.SetLinkFn(func(_, _ string) error { return errors.New("no hardlinks") })
	defer restore()
	res, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Stats.Copied != 1 || res.Stats.Downloaded != 0 {
		t.Errorf("stats = %+v, want Copied=1", res.Stats)
	}
}

func TestReplaceFromDirectoryVerificationFailure(t *testing.T) {
	v1 := map[string]string{"a.txt": "alpha"}
	source, m1 := sourceTree(t, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m1, pull.Options{}); err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(root string){
		"content differs": func(root string) { writeFile(t, root, "b.txt", "BRAVO", 0o644) },
		"entry missing":   func(string) {},
		// Points at the right bytes, so only refusing to follow catches it.
		"symlink": func(root string) {
			target := filepath.Join(t.TempDir(), "b.txt")
			writeFile(t, filepath.Dir(target), "b.txt", "bravo", 0o644)
			if err := os.Symlink(target, filepath.Join(root, "b.txt")); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad, _ := sourceTree(t, v1)
			mutate(bad)
			m2 := encodeManifest(t, map[string]string{"a.txt": "alpha", "b.txt": "bravo"})
			before := snapshot(t, bad)
			called := false
			_, err := pull.ReplaceFromDirectory(context.Background(), bad, dest, m2, pull.Options{
				BeforeReplace: func(context.Context, []byte) error { called = true; return nil },
			})
			if err == nil {
				t.Fatal("ReplaceFromDirectory accepted a source that does not match its manifest")
			}
			if called {
				t.Error("BeforeReplace ran for an unverified tree")
			}
			assertTree(t, dest, v1)
			assertNoLeftovers(t, dest)
			if after := snapshot(t, bad); !maps.Equal(before, after) {
				t.Error("source changed on failure")
			}
		})
	}
}

func TestReplaceFromDirectoryRejectsOverlap(t *testing.T) {
	files := map[string]string{"a.txt": "alpha"}
	source, m := sourceTree(t, files)
	for name, dest := range map[string]string{
		"source is dest":     source,
		"source inside dest": filepath.Dir(source),
		"dest inside source": filepath.Join(source, "sub"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{})
			if err == nil || !strings.Contains(err.Error(), "overlaps") {
				t.Fatalf("ReplaceFromDirectory = %v, want overlap error", err)
			}
			assertTree(t, source, map[string]string{"a.txt": "alpha", "manifest.json": string(m)})
		})
	}

	// Recovery deletes a stale .pinsync-old next to a live dest; a source
	// there must be refused before recovery runs.
	t.Run("source is the old sibling", func(t *testing.T) {
		dest := filepath.Join(t.TempDir(), "dest")
		writeFile(t, dest, "live.txt", "live", 0o644)
		src := dest + ".pinsync-old"
		writeFile(t, src, "a.txt", "alpha", 0o644)
		_, err := pull.ReplaceFromDirectory(context.Background(), src, dest, encodeManifest(t, files), pull.Options{})
		if err == nil || !strings.Contains(err.Error(), "overlaps") {
			t.Fatalf("ReplaceFromDirectory = %v, want overlap error", err)
		}
		assertTree(t, src, files)
	})

	// Sources that only a symlink leads out of dest or its old sibling:
	// recovery or the swap would delete the path out from under the copy.
	for name, parent := range map[string]func(dest string) string{
		"source behind a symlink in the old sibling": func(dest string) string { return dest + ".pinsync-old" },
		"source behind a symlink in dest":            func(dest string) string { return dest },
	} {
		t.Run(name, func(t *testing.T) {
			dest := filepath.Join(t.TempDir(), "dest")
			writeFile(t, dest, "live.txt", "live", 0o644)
			ext := t.TempDir()
			writeFile(t, ext, "a.txt", "alpha", 0o644)
			if err := os.MkdirAll(parent(dest), 0o755); err != nil {
				t.Fatal(err)
			}
			src := filepath.Join(parent(dest), "link")
			if err := os.Symlink(ext, src); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
			_, err := pull.ReplaceFromDirectory(context.Background(), src, dest, encodeManifest(t, files), pull.Options{})
			if err == nil || !strings.Contains(err.Error(), "overlaps") {
				t.Fatalf("ReplaceFromDirectory = %v, want overlap error", err)
			}
			if _, err := os.Lstat(src); err != nil {
				t.Errorf("source link removed: %v", err)
			}
			assertTree(t, ext, files)
		})
	}
}

func TestReplaceFromDirectoryRejectsInvalidManifest(t *testing.T) {
	source, _ := sourceTree(t, map[string]string{"a.txt": "alpha"})
	dest := filepath.Join(t.TempDir(), "dest")
	for name, m := range map[string][]byte{
		"garbage":   []byte("not json"),
		"bad paths": []byte(`{"version":1,"files":[{"path":"../evil","sha256":"` + strings.Repeat("a", 64) + `","size":1,"mode":"0644"}]}`),
	} {
		if _, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{}); err == nil {
			t.Errorf("%s: ReplaceFromDirectory accepted an invalid manifest", name)
		}
	}
	m := encodeManifest(t, map[string]string{"a.txt": "alpha"})
	if _, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m,
		pull.Options{MaxManifestBytes: int64(len(m) - 1)}); err == nil {
		t.Error("ReplaceFromDirectory ignored MaxManifestBytes")
	}
	if _, err := os.Lstat(dest); err == nil {
		t.Error("dest created for a rejected manifest")
	}
}

// TestPullBeforeReplaceMismatchErrorIsTerminal: a callback error is never
// retried, even one that wraps a staged-content mismatch from another call.
func TestPullBeforeReplaceMismatchErrorIsTerminal(t *testing.T) {
	bad, _ := sourceTree(t, map[string]string{"a.txt": "ALPHA"})
	_, mismatchErr := pull.ReplaceFromDirectory(context.Background(), bad,
		filepath.Join(t.TempDir(), "other"), encodeManifest(t, map[string]string{"a.txt": "alpha"}), pull.Options{})
	if mismatchErr == nil {
		t.Fatal("fixture error: ReplaceFromDirectory accepted mismatching bytes")
	}

	fake := s3test.NewFake()
	v1 := map[string]string{"a.txt": "alpha"}
	pushFixture(t, fake, v1)
	dest := filepath.Join(t.TempDir(), "dest")
	mustPull(t, fake, dest)
	pushFixture(t, fake, map[string]string{"a.txt": "bravo"})

	calls := 0
	_, err := pull.PullWithManifest(context.Background(), fake, "bkt", prefix, dest, pull.Options{
		BeforeReplace: func(context.Context, []byte) error { calls++; return mismatchErr },
	})
	if err != mismatchErr { //nolint:errorlint // the callback's error must come back as-is
		t.Fatalf("PullWithManifest = %v, want the callback's error as-is", err)
	}
	if calls != 1 {
		t.Errorf("BeforeReplace called %d times, want 1 (callback errors are terminal)", calls)
	}
	assertTree(t, dest, v1)
	assertNoLeftovers(t, dest)
}

func TestNegativeLimitsRejected(t *testing.T) {
	for name, opts := range map[string]pull.Options{
		"MaxManifestBytes": {MaxManifestBytes: -1},
		"MaxContentBytes":  {MaxContentBytes: -1},
	} {
		t.Run(name, func(t *testing.T) {
			fake := s3test.NewFake()
			pushFixture(t, fake, map[string]string{"a.txt": "alpha"})
			dest := filepath.Join(t.TempDir(), "dest")
			writeFile(t, dest, "live.txt", "live", 0o644)
			// Recovery would delete this stale sibling.
			writeFile(t, dest+".pinsync-tmp", "stale.txt", "stale", 0o644)

			_, err := pull.PullWithManifest(context.Background(), fake, "bkt", prefix, dest, opts)
			if err == nil || !strings.Contains(err.Error(), "negative") {
				t.Errorf("PullWithManifest = %v, want negative limit error", err)
			}
			if gets := fake.Gets(); len(gets) != 0 {
				t.Errorf("gets = %v, want none", gets)
			}
			source, m := sourceTree(t, map[string]string{"a.txt": "alpha"})
			_, err = pull.ReplaceFromDirectory(context.Background(), source, dest, m, opts)
			if err == nil || !strings.Contains(err.Error(), "negative") {
				t.Errorf("ReplaceFromDirectory = %v, want negative limit error", err)
			}
			assertTree(t, dest, map[string]string{"live.txt": "live"})
			assertTree(t, dest+".pinsync-tmp", map[string]string{"stale.txt": "stale"})
		})
	}
}

// TestReplaceFromDirectoryOwnsManifestBytes: neither the callback mutating its
// copy nor the callback mutating the caller's slice changes Result.Manifest.
func TestReplaceFromDirectoryOwnsManifestBytes(t *testing.T) {
	files := map[string]string{"a.txt": "alpha"}
	source, want := sourceTree(t, files)
	for name, mutate := range map[string]func(supplied, received []byte){
		"callback's copy": func(_, received []byte) { received[0] = 'X' },
		"caller's slice":  func(supplied, _ []byte) { supplied[0] = 'X' },
	} {
		t.Run(name, func(t *testing.T) {
			supplied := bytes.Clone(want)
			res, err := pull.ReplaceFromDirectory(context.Background(), source,
				filepath.Join(t.TempDir(), "dest"), supplied, pull.Options{
					BeforeReplace: func(_ context.Context, b []byte) error { mutate(supplied, b); return nil },
				})
			if err != nil {
				t.Fatalf("ReplaceFromDirectory: %v", err)
			}
			if !bytes.Equal(res.Manifest, want) {
				t.Errorf("Result.Manifest = %q, want the pre-callback bytes %q", res.Manifest, want)
			}
			if name == "callback's copy" && !bytes.Equal(supplied, want) {
				t.Errorf("caller's bytes = %q, want unchanged %q", supplied, want)
			}
		})
	}
}

// TestReplaceFromDirectoryDoesNotReuseWrongSize: a live file with the
// manifest's hash but a different size is not reused (here a mode change
// would copy it), so the bounded source read rejects it.
func TestReplaceFromDirectoryDoesNotReuseWrongSize(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	source, m := sourceTree(t, map[string]string{"a.txt": big})
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{}); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(big))
	undersized := []byte(`{"version":1,"files":[{"path":"a.txt","sha256":"` + hex.EncodeToString(sum[:]) +
		`","size":5,"mode":"0755"}]}`)

	_, err := pull.ReplaceFromDirectory(context.Background(), source, dest, undersized, pull.Options{})
	if err == nil || !strings.Contains(err.Error(), "copying source") || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("ReplaceFromDirectory = %v, want the bounded source copy to reject the oversized body", err)
	}
	assertTree(t, dest, map[string]string{"a.txt": big})
	assertNoLeftovers(t, dest)
}

// TestReplaceFromDirectoryBoundsLiveCopy grows the live file after it was
// hashed, then forces the copy fallback: the copy must stop one byte past the
// declared size instead of copying the whole file.
func TestReplaceFromDirectoryBoundsLiveCopy(t *testing.T) {
	files := map[string]string{"a.txt": "alpha"}
	source, m := sourceTree(t, files)
	dest := filepath.Join(t.TempDir(), "dest")
	if _, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{}); err != nil {
		t.Fatal(err)
	}
	restore := pull.SetLinkFn(func(oldname, _ string) error {
		writeFile(t, filepath.Dir(oldname), filepath.Base(oldname), "alpha"+strings.Repeat("x", 1<<20), 0o644)
		return errors.New("no hardlinks")
	})
	defer restore()
	_, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{})
	if err == nil || !strings.Contains(err.Error(), "copying live") || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("ReplaceFromDirectory = %v, want the bounded live copy to reject the grown file", err)
	}
	assertNoLeftovers(t, dest)
}

// TestStagedTreeReverifiedBeforeCallback corrupts a linked entry after
// classification (as a live file rewritten after hashing would): the staged
// re-verification must fail before BeforeReplace runs.
func TestStagedTreeReverifiedBeforeCallback(t *testing.T) {
	v1 := map[string]string{"a.txt": "alpha"}
	corruptLink := func(_, newname string) error {
		return os.WriteFile(newname, []byte("ALPHA"), 0o644) // same size and mode
	}
	for name, run := range map[string]func(t *testing.T, opts pull.Options) (string, error){
		"PullWithManifest": func(t *testing.T, opts pull.Options) (string, error) {
			fake := s3test.NewFake()
			pushFixture(t, fake, v1)
			dest := filepath.Join(t.TempDir(), "dest")
			mustPull(t, fake, dest)
			defer pull.SetLinkFn(corruptLink)()
			_, err := pull.PullWithManifest(context.Background(), fake, "bkt", prefix, dest, opts)
			return dest, err
		},
		"ReplaceFromDirectory": func(t *testing.T, opts pull.Options) (string, error) {
			source, m := sourceTree(t, v1)
			dest := filepath.Join(t.TempDir(), "dest")
			if _, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, pull.Options{}); err != nil {
				t.Fatal(err)
			}
			defer pull.SetLinkFn(corruptLink)()
			_, err := pull.ReplaceFromDirectory(context.Background(), source, dest, m, opts)
			return dest, err
		},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			dest, err := run(t, pull.Options{
				BeforeReplace: func(context.Context, []byte) error { calls++; return nil },
			})
			if err == nil || !strings.Contains(err.Error(), "hash") {
				t.Fatalf("%s = %v, want a staged verification hash error", name, err)
			}
			if calls != 0 {
				t.Errorf("BeforeReplace called %d times for an unverified tree", calls)
			}
			assertTree(t, dest, v1)
			assertNoLeftovers(t, dest)
		})
	}
}
