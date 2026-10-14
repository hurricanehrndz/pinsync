package s3test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestListHonorsPrefixMaxKeysAndToken checks the three listing knobs in
// isolation: only prefixed keys are returned, MaxKeys caps a page and marks it
// truncated, and a ContinuationToken resumes strictly after the given key.
func TestListHonorsPrefixMaxKeysAndToken(t *testing.T) {
	f := NewFake()
	for _, k := range []string{"cfg/a", "cfg/b", "cfg/c", "other/z"} {
		f.Store(k, []byte(k))
	}

	out, err := f.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Prefix:  aws.String("cfg/"),
		MaxKeys: aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if got := keysOf(out); !slices.Equal(got, []string{"cfg/a", "cfg/b"}) {
		t.Errorf("first page = %v, want [cfg/a cfg/b]", got)
	}
	if !aws.ToBool(out.IsTruncated) {
		t.Error("first page not marked truncated")
	}
	if got := aws.ToString(out.NextContinuationToken); got != "cfg/b" {
		t.Errorf("NextContinuationToken = %q, want cfg/b", got)
	}

	out2, err := f.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Prefix:            aws.String("cfg/"),
		MaxKeys:           aws.Int32(2),
		ContinuationToken: out.NextContinuationToken,
	})
	if err != nil {
		t.Fatalf("ListObjectsV2: %v", err)
	}
	if got := keysOf(out2); !slices.Equal(got, []string{"cfg/c"}) {
		t.Errorf("second page = %v, want [cfg/c]", got)
	}
	if aws.ToBool(out2.IsTruncated) {
		t.Error("last page marked truncated")
	}
}

// TestListDrivesRealPaginator drives the SDK's own paginator against the fake
// with a small page size and asserts every key comes back exactly once.
func TestListDrivesRealPaginator(t *testing.T) {
	f := NewFake()
	want := []string{"cfg/a", "cfg/b", "cfg/c", "cfg/d", "cfg/e"}
	for _, k := range want {
		f.Store(k, []byte(k))
	}
	f.Store("skip/x", nil)

	p := s3.NewListObjectsV2Paginator(f, &s3.ListObjectsV2Input{
		Prefix: aws.String("cfg/"),
	}, func(o *s3.ListObjectsV2PaginatorOptions) { o.Limit = 2 })

	var got []string
	for p.HasMorePages() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			t.Fatalf("NextPage: %v", err)
		}
		got = append(got, keysOf(page)...)
	}
	if !slices.Equal(got, want) {
		t.Errorf("paginated keys = %v, want %v", got, want)
	}
}

// TestDeleteObjectsRemovesAndRecords verifies DeleteObjects drops the objects
// and records the deleted keys in call order.
func TestDeleteObjectsRemovesAndRecords(t *testing.T) {
	f := NewFake()
	f.Store("cfg/a", nil)
	f.Store("cfg/b", nil)

	out, err := f.DeleteObjects(context.Background(), delInput("cfg/a", "cfg/b"))
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if len(out.Errors) != 0 {
		t.Errorf("Errors = %v, want none", out.Errors)
	}
	if _, ok := f.Object("cfg/a"); ok {
		t.Error("cfg/a still present after delete")
	}
	if got := f.Deletes(); !slices.Equal(got, []string{"cfg/a", "cfg/b"}) {
		t.Errorf("Deletes() = %v, want [cfg/a cfg/b]", got)
	}
}

// TestDeleteErrLeavesKeyInPlace verifies a per-key DeleteErr is reported as a
// types.Error without failing the request, and that key survives.
func TestDeleteErrLeavesKeyInPlace(t *testing.T) {
	f := NewFake()
	f.Store("cfg/a", nil)
	f.Store("cfg/keep", nil)
	f.DeleteErr = func(key string) error {
		if key == "cfg/keep" {
			return errors.New("boom")
		}
		return nil
	}

	out, err := f.DeleteObjects(context.Background(), delInput("cfg/a", "cfg/keep"))
	if err != nil {
		t.Fatalf("DeleteObjects: %v", err)
	}
	if len(out.Errors) != 1 || aws.ToString(out.Errors[0].Key) != "cfg/keep" {
		t.Errorf("Errors = %v, want one for cfg/keep", out.Errors)
	}
	if _, ok := f.Object("cfg/keep"); !ok {
		t.Error("cfg/keep removed despite DeleteErr")
	}
	if got := f.Deletes(); !slices.Equal(got, []string{"cfg/a"}) {
		t.Errorf("Deletes() = %v, want [cfg/a]", got)
	}
}

func keysOf(out *s3.ListObjectsV2Output) []string {
	keys := make([]string, len(out.Contents))
	for i, o := range out.Contents {
		keys[i] = aws.ToString(o.Key)
	}
	return keys
}

func delInput(keys ...string) *s3.DeleteObjectsInput {
	ids := make([]types.ObjectIdentifier, len(keys))
	for i, k := range keys {
		ids[i] = types.ObjectIdentifier{Key: aws.String(k)}
	}
	return &s3.DeleteObjectsInput{Delete: &types.Delete{Objects: ids}}
}
