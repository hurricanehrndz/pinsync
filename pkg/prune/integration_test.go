//go:build integration

package prune_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/internal/s3test"
	"github.com/hurricanehrndz/pinsync/pkg/prune"
	"github.com/hurricanehrndz/pinsync/pkg/push"
)

// seed pushes a small tree (referenced content + manifest) and drops a couple
// of orphan objects the manifest does not name, returning the referenced keys
// plus the manifest key that prune must never delete.
func seed(t *testing.T, client *s3.Client, bucket string, orphans ...string) []string {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	for rel, content := range map[string]string{
		"a.txt":     "alpha",
		"sub/b.txt": "bravo",
	} {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := push.Push(ctx, client, bucket, "cfg/prod", root, push.Options{}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	for _, key := range orphans {
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader([]byte("stale")),
		}); err != nil {
			t.Fatalf("PutObject(%s): %v", key, err)
		}
	}
	return []string{"cfg/prod/a.txt", "cfg/prod/sub/b.txt", "cfg/prod/manifest.json"}
}

// listKeys returns every object key under the prefix, sorted.
func listKeys(t *testing.T, client *s3.Client, bucket string) []string {
	t.Helper()
	out, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String("cfg/prod/"),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	var keys []string
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	slices.Sort(keys)
	return keys
}

// TestPruneMinIO exercises prune against a real S3 implementation: orphan
// objects the manifest doesn't reference are deleted while referenced content
// and the manifest survive.
func TestPruneMinIO(t *testing.T) {
	client := s3test.StartMinIO(t)
	bucket := s3test.CreateBucket(t, client, "pinsync-prune")

	keep := seed(t, client, bucket, "cfg/prod/orphan1.txt", "cfg/prod/sub/orphan2.txt")

	stats, err := prune.Prune(context.Background(), client, bucket, "cfg/prod", prune.Options{MinAge: 0})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if stats.Deleted != 2 {
		t.Errorf("Deleted = %d, want 2", stats.Deleted)
	}

	slices.Sort(keep)
	if got := listKeys(t, client, bucket); !slices.Equal(got, keep) {
		t.Errorf("remaining keys = %v, want %v", got, keep)
	}
}

// TestPruneMinAgeProtects verifies the grace window end-to-end: a fresh orphan
// survives a min-age larger than its real S3 LastModified age.
func TestPruneMinAgeProtects(t *testing.T) {
	client := s3test.StartMinIO(t)
	bucket := s3test.CreateBucket(t, client, "pinsync-prune-minage")

	keep := seed(t, client, bucket, "cfg/prod/orphan.txt")

	stats, err := prune.Prune(context.Background(), client, bucket, "cfg/prod", prune.Options{MinAge: time.Hour})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if stats.Deleted != 0 || stats.Protected != 1 {
		t.Errorf("stats = %+v, want Deleted=0 Protected=1", stats)
	}

	want := slices.Concat(keep, []string{"cfg/prod/orphan.txt"})
	slices.Sort(want)
	if got := listKeys(t, client, bucket); !slices.Equal(got, want) {
		t.Errorf("remaining keys = %v, want %v", got, want)
	}
}
