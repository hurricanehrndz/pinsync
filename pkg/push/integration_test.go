//go:build integration

package push_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/internal/s3test"
	"github.com/hurricanehrndz/pinsync/pkg/manifest"
	"github.com/hurricanehrndz/pinsync/pkg/push"
)

func TestPushMinIO(t *testing.T) {
	client := s3test.StartMinIO(t)
	bucket := s3test.CreateBucket(t, client, "pinsync-push")
	ctx := context.Background()

	root := fixtureTree(t)
	want := map[string]string{
		"cfg/prod/a.txt":     "alpha",
		"cfg/prod/b.txt":     "bravo",
		"cfg/prod/sub/c.txt": "charlie",
	}

	stats, err := push.Push(ctx, client, bucket, "cfg/prod", root, push.Options{})
	if err != nil {
		t.Fatalf("Push: %v", err)
	}
	if stats.Uploaded != len(want) {
		t.Errorf("Uploaded = %d, want %d", stats.Uploaded, len(want))
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
