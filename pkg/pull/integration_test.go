//go:build integration

package pull_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/push"
	"github.com/hurricanehrndz/pinsync/pkg/s3test"
)

// TestPullRustFS exercises the full loop against a real S3 implementation:
// push → pull → corrupt/delete/add-extraneous → pull → exact convergence.
func TestPullRustFS(t *testing.T) {
	client := s3test.StartRustFS(t)
	bucket := s3test.CreateBucket(t, client, "pinsync-pull")
	ctx := context.Background()

	files := map[string]string{
		"a.txt":     "alpha",
		"sub/b.txt": "bravo",
		"empty.txt": "",
	}
	root := t.TempDir()
	for rel, content := range files {
		writeFile(t, root, rel, content, 0o644)
	}
	if _, err := push.Push(ctx, client, bucket, prefix, root, push.Options{}); err != nil {
		t.Fatalf("Push: %v", err)
	}

	dest := filepath.Join(t.TempDir(), "dest")
	stats, err := pull.Pull(ctx, client, bucket, prefix, dest, pull.Options{})
	if err != nil {
		t.Fatalf("fresh Pull: %v", err)
	}
	if stats.Downloaded != len(files) {
		t.Errorf("fresh stats = %+v, want Downloaded=%d", stats, len(files))
	}
	assertTree(t, dest, files)

	// Corrupt in place (same size), delete a file, add extraneous junk.
	if err = os.WriteFile(filepath.Join(dest, "a.txt"), []byte("XXXXX"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dest, "sub", "b.txt")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, dest, "extraneous.txt", "junk", 0o644)

	stats, err = pull.Pull(ctx, client, bucket, prefix, dest, pull.Options{})
	if err != nil {
		t.Fatalf("repair Pull: %v", err)
	}
	if stats.Downloaded != 2 || stats.Linked != 1 {
		t.Errorf("repair stats = %+v, want Downloaded=2 Linked=1", stats)
	}
	assertTree(t, dest, files)
	assertNoLeftovers(t, dest)
}

// raceClient re-publishes the manifest after the first manifest fetch,
// simulating a publisher finishing between attempts.
type raceClient struct {
	*s3.Client
	bucket    string
	corrected []byte
	gets      int
}

func (c *raceClient) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if aws.ToString(in.Key) == manifestKey {
		if c.gets++; c.gets == 2 {
			if _, err := c.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(c.bucket), Key: in.Key, Body: bytes.NewReader(c.corrected),
			}); err != nil {
				return nil, err
			}
		}
	}
	return c.Client.GetObject(ctx, in, opts...)
}

// TestPullWithManifestRustFS covers the manifest-persistence contract against
// a real S3 implementation: exact bytes, BeforeReplace failure, interrupted
// swap with offline Recover, unchanged-data reuse, bounded bodies and
// manifests, and retries returning the installed attempt's bytes.
func TestPullWithManifestRustFS(t *testing.T) {
	client := s3test.StartRustFS(t)
	bucket := s3test.CreateBucket(t, client, "pinsync-manifest")
	ctx := context.Background()

	put := func(key string, body []byte) {
		t.Helper()
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
		}); err != nil {
			t.Fatalf("PutObject(%s): %v", key, err)
		}
	}
	get := func(key string) []byte {
		t.Helper()
		out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("GetObject(%s): %v", key, err)
		}
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		if err != nil {
			t.Fatal(err)
		}
		return body
	}
	publish := func(files map[string]string) []byte {
		t.Helper()
		root := t.TempDir()
		for rel, content := range files {
			writeFile(t, root, rel, content, 0o644)
		}
		if _, err := push.Push(ctx, client, bucket, prefix, root, push.Options{}); err != nil {
			t.Fatalf("Push: %v", err)
		}
		return get(manifestKey)
	}

	v1 := map[string]string{"a.txt": "alpha", "sub/b.txt": "bravo"}
	m1 := publish(v1)
	dest := filepath.Join(t.TempDir(), "dest")
	res, err := pull.PullWithManifest(ctx, client, bucket, prefix, dest, pull.Options{})
	if err != nil {
		t.Fatalf("PullWithManifest: %v", err)
	}
	if !bytes.Equal(res.Manifest, m1) {
		t.Errorf("Result.Manifest differs from the published manifest")
	}
	assertTree(t, dest, v1)

	v2 := map[string]string{"a.txt": "alpha", "sub/b.txt": "BRAVO2"}
	m2 := publish(v2)

	t.Run("BeforeReplace failure", func(t *testing.T) {
		sentinel := errors.New("refused")
		_, err := pull.PullWithManifest(ctx, client, bucket, prefix, dest, pull.Options{
			BeforeReplace: func(_ context.Context, b []byte) error {
				if !bytes.Equal(b, m2) {
					t.Errorf("callback got %q, want the published manifest", b)
				}
				return sentinel
			},
		})
		if !errors.Is(err, sentinel) {
			t.Fatalf("PullWithManifest = %v, want the callback error", err)
		}
		assertTree(t, dest, v1)
		assertNoLeftovers(t, dest)
		// The verified v2 tree is kept for the next pull to reuse.
		assertTree(t, partialDir(dest), v2)
	})

	t.Run("interrupted swap recovered offline", func(t *testing.T) {
		restore := pull.SetRenameFn(func(from, to string) error {
			if to == dest {
				return errors.New("injected crash")
			}
			return os.Rename(from, to)
		})
		_, err := pull.PullWithManifest(ctx, client, bucket, prefix, dest, pull.Options{})
		restore()
		if err == nil {
			t.Fatal("PullWithManifest succeeded despite the injected crash")
		}
		if err := pull.Recover(ctx, dest, nil); err != nil {
			t.Fatalf("Recover: %v", err)
		}
		assertTree(t, dest, v1)
		assertNoLeftovers(t, dest)
	})

	t.Run("bounded bodies", func(t *testing.T) {
		// Earlier failures kept the good v2 bytes, which would be reused instead of fetching the bad body.
		if err := os.RemoveAll(partialDir(dest)); err != nil {
			t.Fatal(err)
		}
		key := prefix + "/sub/b.txt"
		for name, body := range map[string]string{"truncated": "BRA", "oversized": "BRAVO2" + strings.Repeat("x", 1<<20)} {
			put(key, []byte(body))
			if _, err := pull.Pull(ctx, client, bucket, prefix, dest, pull.Options{}); err == nil {
				t.Errorf("%s body: Pull succeeded", name)
			}
			assertTree(t, dest, v1)
			assertNoLeftovers(t, dest)
		}
		put(key, []byte(v2["sub/b.txt"]))
		// The partial now holds the cut-off bad body; it fails the hash, so
		// the next pull must download sub/b.txt rather than reuse it.
		plan, err := pull.DryRun(ctx, client, bucket, prefix, dest, pull.Options{})
		if err != nil {
			t.Fatalf("DryRun: %v", err)
		}
		if plan.Reused != 0 || len(plan.Download) != 1 || plan.Download[0] != "sub/b.txt" {
			t.Errorf("plan = %+v, want sub/b.txt downloaded, nothing reused", plan)
		}
	})

	t.Run("manifest limit", func(t *testing.T) {
		_, err := pull.Pull(ctx, client, bucket, prefix, dest, pull.Options{MaxManifestBytes: int64(len(m2) - 1)})
		if err == nil || !strings.Contains(err.Error(), "manifest limit") {
			t.Fatalf("Pull = %v, want manifest limit error", err)
		}
		assertTree(t, dest, v1)
	})

	t.Run("unchanged data reused", func(t *testing.T) {
		res, err := pull.PullWithManifest(ctx, client, bucket, prefix, dest, pull.Options{})
		if err != nil {
			t.Fatalf("PullWithManifest: %v", err)
		}
		if res.Stats.Linked != 1 || res.Stats.Downloaded != 1 || !bytes.Equal(res.Manifest, m2) {
			t.Errorf("stats = %+v, manifest match = %v; want Linked=1 Downloaded=1 and the v2 bytes",
				res.Stats, bytes.Equal(res.Manifest, m2))
		}
		assertTree(t, dest, v2)
	})

	t.Run("retry returns the installed attempt's bytes", func(t *testing.T) {
		// Overwrite-in-place race: content is v3, the manifest still v2.
		v3 := map[string]string{"a.txt": "alpha", "sub/b.txt": "charlie"}
		put(prefix+"/sub/b.txt", []byte(v3["sub/b.txt"]))
		m3 := encodeManifest(t, v3)
		rc := &raceClient{Client: client, bucket: bucket, corrected: m3}
		fresh := filepath.Join(t.TempDir(), "dest") // nothing to reuse, so the stale entry is fetched
		res, err := pull.PullWithManifest(ctx, rc, bucket, prefix, fresh, pull.Options{})
		if err != nil {
			t.Fatalf("PullWithManifest: %v", err)
		}
		if rc.gets != 2 || !bytes.Equal(res.Manifest, m3) {
			t.Errorf("manifest gets = %d, bytes match = %v; want 2 and the corrected manifest",
				rc.gets, bytes.Equal(res.Manifest, m3))
		}
		assertTree(t, fresh, v3)
	})
}
