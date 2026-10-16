package s3test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// get fetches key with an optional Range and IfMatch and returns the output
// and body.
func get(t *testing.T, f *Fake, key, rng, ifMatch string) (*s3.GetObjectOutput, string, error) {
	t.Helper()
	in := &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String(key)}
	if rng != "" {
		in.Range = aws.String(rng)
	}
	if ifMatch != "" {
		in.IfMatch = aws.String(ifMatch)
	}
	out, err := f.GetObject(context.Background(), in)
	if err != nil {
		return nil, "", err
	}
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatal(err)
	}
	return out, string(body), nil
}

// statusOf returns the HTTP status an SDK-shaped error carries, or 0.
func statusOf(err error) int {
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// TestGetObjectRange: a ranged GET serves the requested bytes with a
// Content-Range, as S3 does, so a resumed download can check it got the
// bytes it asked for.
func TestGetObjectRange(t *testing.T) {
	f := NewFake()
	f.Store("k", []byte("0123456789"))

	full, body, err := get(t, f, "k", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if body != "0123456789" || full.ContentRange != nil || aws.ToString(full.ETag) == "" {
		t.Errorf("full GET = %q, Content-Range %v, ETag %q; want the body, no range, an ETag",
			body, full.ContentRange, aws.ToString(full.ETag))
	}
	for rng, want := range map[string]struct{ body, cr string }{
		"bytes=4-":   {"456789", "bytes 4-9/10"},
		"bytes=4-6":  {"456", "bytes 4-6/10"},
		"bytes=4-99": {"456789", "bytes 4-9/10"},
	} {
		out, body, err := get(t, f, "k", rng, aws.ToString(full.ETag))
		if err != nil {
			t.Fatalf("%s: %v", rng, err)
		}
		if body != want.body || aws.ToString(out.ContentRange) != want.cr ||
			aws.ToInt64(out.ContentLength) != int64(len(want.body)) || aws.ToString(out.ETag) != aws.ToString(full.ETag) {
			t.Errorf("%s = %q, Content-Range %q, length %d; want %q, %q",
				rng, body, aws.ToString(out.ContentRange), aws.ToInt64(out.ContentLength), want.body, want.cr)
		}
	}
}

// TestGetObjectPreconditions: a stale IfMatch fails with 412 and a range past
// the end with 416, in the SDK's error shape (an API error code and an HTTP
// status).
func TestGetObjectPreconditions(t *testing.T) {
	f := NewFake()
	f.Store("k", []byte("0123456789"))
	for _, tc := range []struct {
		name, rng, ifMatch, code string
		status                   int
	}{
		{"stale ETag", "bytes=4-", `"stale"`, "PreconditionFailed", http.StatusPreconditionFailed},
		{"range past the end", "bytes=10-", "", "InvalidRange", http.StatusRequestedRangeNotSatisfiable},
	} {
		_, _, err := get(t, f, "k", tc.rng, tc.ifMatch)
		var ae smithy.APIError
		if !errors.As(err, &ae) || ae.ErrorCode() != tc.code || statusOf(err) != tc.status {
			t.Errorf("%s: GetObject = %v (status %d), want %s / %d", tc.name, err, statusOf(err), tc.code, tc.status)
		}
	}
}

// TestGetObjectIgnoreRange: with IgnoreRange the fake behaves like a server
// that answers a ranged GET with the whole object and no Content-Range.
func TestGetObjectIgnoreRange(t *testing.T) {
	f := NewFake()
	f.Store("k", []byte("0123456789"))
	f.IgnoreRange = true
	out, body, err := get(t, f, "k", "bytes=4-", "")
	if err != nil {
		t.Fatal(err)
	}
	if body != "0123456789" || out.ContentRange != nil {
		t.Errorf("GET = %q, Content-Range %v; want the whole object and no range", body, out.ContentRange)
	}
}

// TestSetETag: SetETag stands for a re-upload that changes the ETag but not
// the bytes (multipart, SSE-KMS); a later Store resets it.
func TestSetETag(t *testing.T) {
	f := NewFake()
	f.Store("k", []byte("0123456789"))
	before, _, err := get(t, f, "k", "", "")
	if err != nil {
		t.Fatal(err)
	}
	f.SetETag("k", `"reuploaded"`)
	if _, _, err = get(t, f, "k", "bytes=4-", aws.ToString(before.ETag)); statusOf(err) != http.StatusPreconditionFailed {
		t.Errorf("GET with the old ETag = %v, want 412", err)
	}
	f.Store("k", []byte("0123456789"))
	after, _, err := get(t, f, "k", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(after.ETag) != aws.ToString(before.ETag) {
		t.Errorf("ETag after Store = %s, want the content ETag %s", aws.ToString(after.ETag), aws.ToString(before.ETag))
	}
}
