package pull_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/s3test"
)

// The tests in this file cover resuming a download cut off mid-object: an
// object too large to arrive within one time-limited run must still arrive
// over several, so the next pull asks only for the bytes it does not have.

const (
	bigPath = "pkg/engine.msi"
	bigSize = 3<<20 + 517 // above the resume threshold, not block aligned
	cutAt   = 1<<20 + 123 // where a cut-off pull stops
)

var bigKey = prefix + "/" + bigPath

// bigContent is deterministic, non-repeating-looking content of n bytes.
func bigContent(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return string(b)
}

func sha(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// resumeFile is a kept prefix's data file (or, with a suffix, its sidecar or
// copy-forward source) in the tree rooted at root.
func resumeFile(root, content, suffix string) string {
	return filepath.Join(root, ".pinsync-resume", sha(content)+suffix)
}

// getCall is one content GET as the client saw it.
type getCall struct {
	key, rng, ifMatch string
	mode              types.ChecksumMode
	validation        aws.ResponseChecksumValidation // what the per-call options set
	read              int64                          // body bytes read
}

// recordingClient records content GETs and can cut a body off: after cut
// bytes of a GET for cutKey it calls onCut and returns its error.
type recordingClient struct {
	pull.S3API
	cutKey string
	cut    int64
	onCut  func() error
	// rewrite, when set, alters a successful content GET's output.
	rewrite func(in *s3.GetObjectInput, out *s3.GetObjectOutput)

	mu    sync.Mutex
	calls []*getCall
}

func (c *recordingClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(in.Key)
	if key == manifestKey {
		return c.S3API.GetObject(ctx, in, opts...)
	}
	var o s3.Options
	for _, fn := range opts {
		fn(&o)
	}
	call := &getCall{
		key: key, rng: aws.ToString(in.Range), ifMatch: aws.ToString(in.IfMatch),
		mode: in.ChecksumMode, validation: o.ResponseChecksumValidation,
	}
	c.mu.Lock()
	c.calls = append(c.calls, call)
	c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out, err := c.S3API.GetObject(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	if c.rewrite != nil {
		c.rewrite(in, out)
	}
	body := &cuttableBody{ReadCloser: out.Body, call: call, mu: &c.mu, left: -1}
	if key == c.cutKey && c.onCut != nil {
		body.left, body.onCut = c.cut, c.onCut
		c.onCut = nil // cut only the first GET
	}
	out.Body = body
	return out, nil
}

// gets returns the recorded GETs for key.
func (c *recordingClient) gets(key string) []getCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []getCall
	for _, call := range c.calls {
		if call.key == key {
			out = append(out, *call)
		}
	}
	return out
}

// bytesRead totals the body bytes read for key.
func (c *recordingClient) bytesRead(key string) int64 {
	var n int64
	for _, call := range c.gets(key) {
		n += call.read
	}
	return n
}

type cuttableBody struct {
	io.ReadCloser
	call  *getCall
	mu    *sync.Mutex
	left  int64 // bytes until the cut, -1 for none
	onCut func() error
}

func (b *cuttableBody) Read(p []byte) (int, error) {
	if b.left == 0 {
		return 0, b.onCut()
	}
	if b.left > 0 && int64(len(p)) > b.left {
		p = p[:b.left]
	}
	n, err := b.ReadCloser.Read(p)
	if b.left > 0 {
		b.left -= int64(n)
	}
	b.mu.Lock()
	b.call.read += int64(n)
	b.mu.Unlock()
	return n, err
}

var errReset = errors.New("connection reset by peer")

// resumeFixture publishes a big object and a small one and returns the
// files and a fresh dest.
func resumeFixture(t *testing.T, fake *s3test.Fake) (map[string]string, string) {
	t.Helper()
	files := map[string]string{bigPath: bigContent(bigSize), "a.txt": "alpha"}
	pushFixture(t, fake, files)
	return files, filepath.Join(t.TempDir(), "dest")
}

// cutOffPull runs a pull whose big download is cut off after cutAt bytes by
// a failed read, and returns its client.
func cutOffPull(t *testing.T, fake *s3test.Fake, dest string) *recordingClient {
	t.Helper()
	c := &recordingClient{S3API: fake, cutKey: bigKey, cut: cutAt, onCut: func() error { return errReset }}
	if _, err := pull.Pull(context.Background(), c, "bkt", prefix, dest, pull.Options{Parallel: 1}); !errors.Is(err, errReset) {
		t.Fatalf("cut-off Pull = %v, want %v", err, errReset)
	}
	return c
}

// resumedPull runs a pull through a recording client and returns both.
func resumedPull(t *testing.T, fake *s3test.Fake, dest string) (pull.Stats, *recordingClient) {
	t.Helper()
	fake.ResetCalls()
	c := &recordingClient{S3API: fake}
	stats, err := pull.Pull(context.Background(), c, "bkt", prefix, dest, pull.Options{Parallel: 1})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	return stats, c
}

// manifestGets counts manifest fetches since the last ResetCalls: more than
// one means the staged-mismatch retry ran.
func manifestGets(fake *s3test.Fake) int {
	n := 0
	for _, key := range fake.Gets() {
		if key == manifestKey {
			n++
		}
	}
	return n
}

// TestPullResumesCutOffDownload: a download cut off mid-body, by a failed
// read or by the pull's deadline, resumes on the next pull from where it
// stopped, with a ranged GET conditional on the ETag, and the object crosses
// the wire about once in total.
func TestPullResumesCutOffDownload(t *testing.T) {
	for _, tc := range []struct {
		name  string
		onCut func(cancel context.CancelFunc) func() error
		want  error
	}{
		{"read error", func(context.CancelFunc) func() error { return func() error { return errReset } }, errReset},
		{"deadline", func(cancel context.CancelFunc) func() error {
			return func() error { cancel(); return context.Canceled }
		}, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := s3test.NewFake()
			files, dest := resumeFixture(t, fake)
			etag := etagOf(t, fake, bigKey)

			ctx, cancel := context.WithCancel(context.Background())
			first := &recordingClient{S3API: fake, cutKey: bigKey, cut: cutAt, onCut: tc.onCut(cancel)}
			_, err := pull.Pull(ctx, first, "bkt", prefix, dest, pull.Options{Parallel: 1})
			cancel()
			if !errors.Is(err, tc.want) {
				t.Fatalf("cut-off Pull = %v, want %v", err, tc.want)
			}
			if g := first.gets(bigKey); len(g) != 1 || g[0].rng != "" || g[0].mode != types.ChecksumModeEnabled {
				t.Errorf("first GETs = %+v, want one whole-object GET with checksum mode enabled", g)
			}
			if info, err := os.Stat(resumeFile(partialDir(dest), files[bigPath], "")); err != nil || info.Size() != cutAt {
				t.Fatalf("kept prefix: %v (size %d), want %d bytes", err, sizeOf(info), cutAt)
			}

			stats, c := resumedPull(t, fake, dest)
			g := c.gets(bigKey)
			if len(g) != 1 || g[0].rng != "bytes=1048699-" || g[0].ifMatch != etag {
				t.Fatalf("resumed GETs = %+v, want one for bytes=%d- if-match %s", g, cutAt, etag)
			}
			// A ranged GET cannot carry a full-object checksum: neither ask
			// for one nor let the SDK's default add it.
			if g[0].mode != "" || g[0].validation != aws.ResponseChecksumValidationWhenRequired {
				t.Errorf("ranged GET checksum mode %q, validation %v; want none and WhenRequired", g[0].mode, g[0].validation)
			}
			if total := first.bytesRead(bigKey) + c.bytesRead(bigKey); total != bigSize {
				t.Errorf("object bytes fetched over both pulls = %d, want %d", total, bigSize)
			}
			if stats.Resumed != 1 || stats.Downloaded != 1 || stats.Reused != 1 {
				t.Errorf("stats = %+v, want Downloaded=1 (Resumed=1) Reused=1", stats)
			}
			assertTree(t, dest, files)
			assertNoLeftovers(t, dest)
			assertNoPartial(t, dest)
		})
	}
}

func sizeOf(info os.FileInfo) int64 {
	if info == nil {
		return -1
	}
	return info.Size()
}

func etagOf(t *testing.T, fake *s3test.Fake, key string) string {
	t.Helper()
	out, err := fake.GetObject(context.Background(), &s3.GetObjectInput{Key: aws.String(key)})
	if err != nil {
		t.Fatal(err)
	}
	_ = out.Body.Close()
	fake.ResetCalls()
	return aws.ToString(out.ETag)
}

// TestPullResumesAfterKill: a killed pull leaves its staging tree in place,
// possibly mid-write. The next pull's recovery keeps it, and the prefix is
// trusted only as far as it is re-hashed: a shorter one resumes from where it
// ends, and anything that cannot be the object's leading bytes is fetched
// whole within the same pull.
func TestPullResumesAfterKill(t *testing.T) {
	for _, tc := range []struct {
		name string
		// kill turns the cut-off pull's kept prefix into what a kill leaves.
		kill      func(t *testing.T, data, side, prev, content string)
		wantRange string // "" means one whole-object GET
		wantGets  int
		resumed   int
	}{
		{"as cut off", func(*testing.T, string, string, string, string) {}, "bytes=1048699-", 1, 1},
		{"prefix shorter", func(t *testing.T, data, _, _, _ string) {
			mustTruncate(t, data, 1000)
		}, "bytes=1000-", 1, 1},
		{"killed while copying forward", func(t *testing.T, data, _, prev, _ string) {
			if err := os.Rename(data, prev); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(data, []byte("half-copied"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, "bytes=1048699-", 1, 1},
		{"complete before its rename", func(t *testing.T, data, _, _, content string) {
			if err := os.WriteFile(data, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
		}, "", 0, 1},
		{"no sidecar", func(t *testing.T, _, side, _, _ string) {
			mustRemove(t, side)
		}, "", 1, 0},
		{"garbage sidecar", func(t *testing.T, _, side, _, _ string) {
			if err := os.WriteFile(side, []byte("{\"sha256\":"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, "", 1, 0},
		{"empty prefix", func(t *testing.T, data, _, _, _ string) {
			mustTruncate(t, data, 0)
		}, "", 1, 0},
		{"longer than the object", func(t *testing.T, data, _, _, content string) {
			if err := os.WriteFile(data, []byte(content+"junk"), 0o644); err != nil {
				t.Fatal(err)
			}
		}, "", 1, 0},
		{"junk past the written bytes", func(t *testing.T, data, _, _, _ string) {
			appendTo(t, data, make([]byte, 4096)) // zeros, as after a power cut
		}, "bytes=1052795-", 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := s3test.NewFake()
			files, dest := resumeFixture(t, fake)
			cutOffPull(t, fake, dest)
			tmp := dest + ".pinsync-tmp"
			// A killed pull never kept its staging tree.
			if err := os.Rename(partialDir(dest), tmp); err != nil {
				t.Fatal(err)
			}
			c := files[bigPath]
			tc.kill(t, resumeFile(tmp, c, ""), resumeFile(tmp, c, ".json"), resumeFile(tmp, c, ".prev"), c)

			stats, rc := resumedPull(t, fake, dest)
			g := rc.gets(bigKey)
			if len(g) != tc.wantGets || (len(g) > 0 && g[0].rng != tc.wantRange) {
				t.Errorf("GETs = %+v, want %d starting with range %q", g, tc.wantGets, tc.wantRange)
			}
			if len(g) > 0 && g[len(g)-1].rng == "" && g[len(g)-1].mode != types.ChecksumModeEnabled {
				t.Errorf("whole-object GET without checksum mode: %+v", g[len(g)-1])
			}
			if stats.Resumed != tc.resumed || manifestGets(fake) != 1 {
				t.Errorf("stats = %+v after %d manifest GETs, want Resumed=%d and no re-sync", stats, manifestGets(fake), tc.resumed)
			}
			assertTree(t, dest, files)
			assertNoLeftovers(t, dest)
			assertNoPartial(t, dest)
		})
	}
}

func mustTruncate(t *testing.T, p string, n int64) {
	t.Helper()
	if err := os.Truncate(p, n); err != nil {
		t.Fatal(err)
	}
}

func mustRemove(t *testing.T, p string) {
	t.Helper()
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
}

func appendTo(t *testing.T, p string, b []byte) {
	t.Helper()
	fh, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fh.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestPullResumeFallsBack: whenever the kept prefix cannot be shown to
// belong to the object, the pull discards it and downloads the object whole
// in the same pull — without the manifest re-fetch reserved for a bucket
// changing mid-publish.
func TestPullResumeFallsBack(t *testing.T) {
	for _, tc := range []struct {
		name     string
		arrange  func(t *testing.T, fake *s3test.Fake, dest, content string) func(*s3.GetObjectInput, *s3.GetObjectOutput)
		wantGets int // GETs of the big object
	}{
		{"object re-uploaded (412)", func(_ *testing.T, fake *s3test.Fake, _, _ string) func(*s3.GetObjectInput, *s3.GetObjectOutput) {
			fake.SetETag(bigKey, `"re-uploaded"`)
			return nil
		}, 2},
		{"server ignores Range", func(_ *testing.T, fake *s3test.Fake, _, _ string) func(*s3.GetObjectInput, *s3.GetObjectOutput) {
			fake.IgnoreRange = true
			return nil
		}, 1},
		{"prefix bytes corrupted", func(t *testing.T, _ *s3test.Fake, dest, content string) func(*s3.GetObjectInput, *s3.GetObjectOutput) {
			p := resumeFile(partialDir(dest), content, "")
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			b[len(b)/2] ^= 0xff
			if err := os.WriteFile(p, b, 0o644); err != nil {
				t.Fatal(err)
			}
			return nil
		}, 2},
		{"Content-Range for other bytes", func(*testing.T, *s3test.Fake, string, string) func(*s3.GetObjectInput, *s3.GetObjectOutput) {
			return func(in *s3.GetObjectInput, out *s3.GetObjectOutput) {
				if in.Range != nil {
					out.ContentRange = aws.String("bytes 0-99/3146245")
				}
			}
		}, 2},
		{"Content-Range missing on a partial body", func(*testing.T, *s3test.Fake, string, string) func(*s3.GetObjectInput, *s3.GetObjectOutput) {
			return func(in *s3.GetObjectInput, out *s3.GetObjectOutput) {
				if in.Range != nil {
					out.ContentRange = nil // and the body is still the range
				}
			}
		}, 2},
		{"IfMatch ignored", func(*testing.T, *s3test.Fake, string, string) func(*s3.GetObjectInput, *s3.GetObjectOutput) {
			return func(in *s3.GetObjectInput, out *s3.GetObjectOutput) {
				if in.Range != nil {
					out.ETag = aws.String(`"another"`)
				}
			}
		}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := s3test.NewFake()
			files, dest := resumeFixture(t, fake)
			cutOffPull(t, fake, dest)
			rewrite := tc.arrange(t, fake, dest, files[bigPath])

			fake.ResetCalls()
			c := &recordingClient{S3API: fake, rewrite: rewrite}
			stats, err := pull.Pull(context.Background(), c, "bkt", prefix, dest, pull.Options{Parallel: 1})
			if err != nil {
				t.Fatalf("Pull: %v", err)
			}
			g := c.gets(bigKey)
			if len(g) != tc.wantGets || g[0].rng != "bytes=1048699-" {
				t.Fatalf("GETs = %+v, want %d, the first resuming at %d", g, tc.wantGets, cutAt)
			}
			if last := g[len(g)-1]; tc.wantGets > 1 && (last.rng != "" || last.ifMatch != "" || last.mode != types.ChecksumModeEnabled) {
				t.Errorf("fallback GET = %+v, want a plain whole-object GET with checksum mode", last)
			}
			if n := manifestGets(fake); n != 1 {
				t.Errorf("manifest fetched %d times, want once: the fallback must not re-sync", n)
			}
			if stats.Resumed != 0 || stats.Downloaded != 1 {
				t.Errorf("stats = %+v, want Downloaded=1 Resumed=0", stats)
			}
			assertTree(t, dest, files)
			assertNoLeftovers(t, dest)
			assertNoPartial(t, dest)
		})
	}
}

// TestPullFallbackMismatchStillReSyncs: a whole-object download that
// mismatches after a discarded prefix is the bucket changing mid-publish, so
// it keeps its re-sync; nothing unverified is installed.
func TestPullFallbackMismatchStillReSyncs(t *testing.T) {
	fake := s3test.NewFake()
	files, dest := resumeFixture(t, fake)
	cutOffPull(t, fake, dest)
	fake.ResetCalls()
	// Overwritten in place with other bytes: the ETag changes, so the
	// ranged GET fails its precondition and the whole object mismatches.
	fake.Store(bigKey, []byte(strings.ToUpper(files[bigPath])))

	_, err := pull.Pull(context.Background(), fake, "bkt", prefix, dest, pull.Options{Parallel: 1})
	if err == nil || !strings.Contains(err.Error(), "kept mismatching") {
		t.Fatalf("Pull = %v, want the re-sync to give up", err)
	}
	if n := manifestGets(fake); n != 3 {
		t.Errorf("manifest fetched %d times, want 3 re-sync attempts", n)
	}
	if _, err := os.Lstat(dest); err == nil {
		t.Error("a failed first pull created dest")
	}
}

// TestPullKeepsNoPrefixForSmallObjects: objects under the threshold restart
// from zero, as before; a prefix of a small file saves too little to keep.
func TestPullKeepsNoPrefixForSmallObjects(t *testing.T) {
	fake := s3test.NewFake()
	small := bigContent(1<<20 - 1)
	files := map[string]string{"small.bin": small}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	c := &recordingClient{S3API: fake, cutKey: prefix + "/small.bin", cut: 1000, onCut: func() error { return errReset }}
	if _, err := pull.Pull(context.Background(), c, "bkt", prefix, dest, pull.Options{}); !errors.Is(err, errReset) {
		t.Fatalf("cut-off Pull = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(partialDir(dest), ".pinsync-resume")); err == nil {
		t.Error("kept a prefix of an object under the threshold")
	}

	stats, rc := resumedPull(t, fake, dest)
	if g := rc.gets(prefix + "/small.bin"); len(g) != 1 || g[0].rng != "" {
		t.Errorf("GETs = %+v, want one whole-object GET", g)
	}
	if stats.Resumed != 0 {
		t.Errorf("stats = %+v, want Resumed=0", stats)
	}
	assertTree(t, dest, files)
}

// barrierClient holds every content GET until n have been made, so the
// downloads they serve are in flight together.
type barrierClient struct {
	pull.S3API
	wg sync.WaitGroup
}

func (c *barrierClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if aws.ToString(in.Key) != manifestKey {
		c.wg.Done()
		c.wg.Wait()
	}
	return c.S3API.GetObject(ctx, in, opts...)
}

// TestPullDuplicateContentResumesOnce: two entries with the same big content
// share one prefix slot, so only one of them continues it even when both
// download at once; the other downloads whole, and both land.
func TestPullDuplicateContentResumesOnce(t *testing.T) {
	fake := s3test.NewFake()
	c := bigContent(bigSize)
	files := map[string]string{"a/engine.msi": c, "b/engine.msi": c}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	cut := &recordingClient{S3API: fake, cutKey: prefix + "/a/engine.msi", cut: cutAt, onCut: func() error { return errReset }}
	if _, err := pull.Pull(context.Background(), cut, "bkt", prefix, dest, pull.Options{Parallel: 1}); !errors.Is(err, errReset) {
		t.Fatalf("cut-off Pull = %v", err)
	}

	bc := &barrierClient{S3API: fake}
	bc.wg.Add(2)
	stats, err := pull.Pull(context.Background(), bc, "bkt", prefix, dest, pull.Options{Parallel: 2})
	if err != nil {
		t.Fatalf("Pull: %v", err)
	}
	if stats.Downloaded != 2 || stats.Resumed != 1 {
		t.Errorf("stats = %+v, want Downloaded=2 Resumed=1", stats)
	}
	assertTree(t, dest, files)
	assertNoPartial(t, dest)
}

// TestPullKeepsNoPrefixWithoutETag: a response without an ETag gives a
// resumed GET nothing to be conditional on, so its cut-off download is not
// kept.
func TestPullKeepsNoPrefixWithoutETag(t *testing.T) {
	fake := s3test.NewFake()
	files, dest := resumeFixture(t, fake)
	c := &recordingClient{
		S3API: fake, cutKey: bigKey, cut: cutAt, onCut: func() error { return errReset },
		rewrite: func(_ *s3.GetObjectInput, out *s3.GetObjectOutput) { out.ETag = nil },
	}
	if _, err := pull.Pull(context.Background(), c, "bkt", prefix, dest, pull.Options{Parallel: 1}); !errors.Is(err, errReset) {
		t.Fatalf("cut-off Pull = %v", err)
	}
	if _, err := os.Lstat(resumeFile(partialDir(dest), files[bigPath], ".json")); err == nil {
		t.Error("kept a prefix without an ETag")
	}

	stats, rc := resumedPull(t, fake, dest)
	if g := rc.gets(bigKey); len(g) != 1 || g[0].rng != "" || stats.Resumed != 0 {
		t.Errorf("GETs = %+v, stats %+v; want one whole-object GET", g, stats)
	}
	assertTree(t, dest, files)
}

// TestDryRunCountsResumable: the preview reports how many downloads a kept
// prefix would shorten, and leaves the partial tree as it was.
func TestDryRunCountsResumable(t *testing.T) {
	fake := s3test.NewFake()
	_, dest := resumeFixture(t, fake)
	cutOffPull(t, fake, dest)
	before := snapshot(t, partialDir(dest))

	plan, err := pull.DryRun(context.Background(), fake, "bkt", prefix, dest, pull.Options{})
	if err != nil {
		t.Fatalf("DryRun: %v", err)
	}
	if !reflect.DeepEqual(plan.Download, []string{"a.txt", bigPath}) || plan.Reused != 1 || plan.Resumed != 1 {
		t.Errorf("plan = %+v, want a reused and %s resumed", plan, bigPath)
	}
	if after := snapshot(t, partialDir(dest)); !reflect.DeepEqual(after, before) {
		t.Errorf("DryRun mutated the partial tree:\n before %v\n after  %v", before, after)
	}

	// Killed while copying forward, before the new data file existed: the
	// previous one still counts.
	data := resumeFile(partialDir(dest), bigContent(bigSize), "")
	if err = os.Rename(data, data+".prev"); err != nil {
		t.Fatal(err)
	}
	if plan, err = pull.DryRun(context.Background(), fake, "bkt", prefix, dest, pull.Options{}); err != nil || plan.Resumed != 1 {
		t.Errorf("DryRun = %+v, %v; want Resumed=1 from the .prev file", plan, err)
	}
}

// TestPullReservedResumeNameInManifest: a manifest that itself has files
// under the resume directory's name turns resuming off for that pull rather
// than mixing its files with prefixes.
func TestPullReservedResumeNameInManifest(t *testing.T) {
	fake := s3test.NewFake()
	files := map[string]string{".pinsync-resume/x.txt": "mine", bigPath: bigContent(bigSize)}
	pushFixture(t, fake, files)
	dest := filepath.Join(t.TempDir(), "dest")
	cut := &recordingClient{S3API: fake, cutKey: bigKey, cut: cutAt, onCut: func() error { return errReset }}
	if _, err := pull.Pull(context.Background(), cut, "bkt", prefix, dest, pull.Options{Parallel: 1}); !errors.Is(err, errReset) {
		t.Fatalf("cut-off Pull = %v", err)
	}

	stats, rc := resumedPull(t, fake, dest)
	if g := rc.gets(bigKey); len(g) != 1 || g[0].rng != "" {
		t.Errorf("GETs = %+v, want one whole-object GET", g)
	}
	if stats.Resumed != 0 {
		t.Errorf("stats = %+v, want Resumed=0", stats)
	}
	assertTree(t, dest, files)
	assertNoPartial(t, dest)
}

// TestPullResumeNeverWritesThroughALink: a prefix is never written in place.
// If its name is a hardlink to some other file (here, one outside the
// trees), resuming copies the prefix forward into a fresh file and the other
// file keeps its bytes.
func TestPullResumeNeverWritesThroughALink(t *testing.T) {
	fake := s3test.NewFake()
	files, dest := resumeFixture(t, fake)
	cutOffPull(t, fake, dest)
	data := resumeFile(partialDir(dest), files[bigPath], "")
	before, err := os.ReadFile(data)
	if err != nil {
		t.Fatal(err)
	}
	control := filepath.Join(t.TempDir(), "control")
	if err := os.Link(data, control); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}

	stats, _ := resumedPull(t, fake, dest)
	if stats.Resumed != 1 {
		t.Errorf("stats = %+v, want Resumed=1", stats)
	}
	if after, err := os.ReadFile(control); err != nil || !bytes.Equal(after, before) {
		t.Errorf("the linked file changed (%d -> %d bytes, err %v)", len(before), len(after), err)
	}
	assertTree(t, dest, files)
}
