//go:build integration

package push_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
	"github.com/hurricanehrndz/pinsync/pkg/push"
	"github.com/hurricanehrndz/pinsync/pkg/s3test"
)

func TestPushRustFS(t *testing.T) {
	client := s3test.StartRustFS(t)
	bucket := s3test.CreateBucket(t, client, "pinsync-push")
	ctx := context.Background()

	root := fixtureTree(t)
	want := map[string]string{
		"cfg/prod/a.txt":     "alpha",
		"cfg/prod/b.txt":     "bravo",
		"cfg/prod/sub/c.txt": "charlie",
	}

	get := func(key string) []byte {
		t.Helper()
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			t.Fatalf("GetObject(%s): %v", key, err)
		}
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		if err != nil {
			t.Fatalf("reading %s: %v", key, err)
		}
		return body
	}

	stats, err := push.Push(ctx, client, bucket, "cfg/prod", root, push.Options{})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if stats.Uploaded != len(want) {
		t.Errorf("Uploaded = %d, want %d", stats.Uploaded, len(want))
	}

	for key, content := range want {
		if got := get(key); !bytes.Equal(got, []byte(content)) {
			t.Errorf("object %s = %q, want %q", key, got, content)
		}
	}
	m, err := manifest.Decode(bytes.NewReader(get("cfg/prod/manifest.json")))
	if err != nil {
		t.Fatalf("uploaded manifest invalid: %v", err)
	}
	if len(m.Files) != len(want) {
		t.Errorf("manifest lists %d files, want %d", len(m.Files), len(want))
	}
}

// manifestFailClient wraps the real client, recording put keys and failing the
// manifest put while failManifest is set.
type manifestFailClient struct {
	*s3.Client
	manifestKey  string
	failManifest bool

	mu   sync.Mutex
	puts []string
}

func (c *manifestFailClient) PutObject(ctx context.Context, in *s3.PutObjectInput,
	opts ...func(*s3.Options),
) (*s3.PutObjectOutput, error) {
	c.mu.Lock()
	c.puts = append(c.puts, aws.ToString(in.Key))
	c.mu.Unlock()
	if c.failManifest && aws.ToString(in.Key) == c.manifestKey {
		return nil, errors.New("injected manifest failure")
	}
	return c.Client.PutObject(ctx, in, opts...)
}

// TestPushRepairsAfterFailedManifestPutRustFS replays the partial-failure
// sequence against real RustFS: push A, push B with the manifest put failing
// (B lands under A's manifest), push A again. The third push must re-upload f,
// relying on the server's stored SHA256 checksum, and leave the identical
// manifest alone.
func TestPushRepairsAfterFailedManifestPutRustFS(t *testing.T) {
	server := s3test.StartRustFS(t)
	bucket := s3test.CreateBucket(t, server, "pinsync-repair")
	client := &manifestFailClient{Client: server, manifestKey: "cfg/prod/manifest.json"}
	ctx := context.Background()
	root := t.TempDir()

	get := func(key string) []byte {
		t.Helper()
		out, err := server.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			t.Fatalf("GetObject(%s): %v", key, err)
		}
		defer out.Body.Close()
		body, err := io.ReadAll(out.Body)
		if err != nil {
			t.Fatalf("reading %s: %v", key, err)
		}
		return body
	}

	writeFile(t, root, "f", "AAAA")
	if _, err := push.Push(ctx, client, bucket, "cfg/prod", root, push.Options{}); err != nil {
		t.Fatalf("push A: %v", err)
	}

	writeFile(t, root, "f", "BBBB")
	client.failManifest = true
	if _, err := push.Push(ctx, client, bucket, "cfg/prod", root, push.Options{}); err == nil {
		t.Fatal("push B succeeded despite the failed manifest put")
	}
	client.failManifest = false
	if got := get("cfg/prod/f"); string(got) != "BBBB" {
		t.Fatalf("arrange: object = %q, want BBBB under the old manifest", got)
	}

	writeFile(t, root, "f", "AAAA")
	client.puts = nil
	stats, err := push.Push(ctx, client, bucket, "cfg/prod", root, push.Options{})
	if err != nil {
		t.Fatalf("push A again: %v", err)
	}
	if want := (push.Stats{Uploaded: 1, Repaired: 1, Total: 1}); stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
	if want := []string{"cfg/prod/f"}; !slices.Equal(client.puts, want) {
		t.Errorf("puts = %v, want %v (identical manifest is not re-put)", client.puts, want)
	}
	if got := get("cfg/prod/f"); string(got) != "AAAA" {
		t.Errorf("object = %q after repair, want AAAA", got)
	}

	head, err := server.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("cfg/prod/f"), ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	sum := sha256.Sum256([]byte("AAAA"))
	if got, want := aws.ToString(head.ChecksumSHA256), base64.StdEncoding.EncodeToString(sum[:]); got != want {
		t.Errorf("ChecksumSHA256 = %q, want %q", got, want)
	}
}
