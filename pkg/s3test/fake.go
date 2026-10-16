// Package s3test provides an in-memory fake of the narrow S3 surface pinsync
// uses, plus a RustFS test harness (StartRustFS, CreateBucket) that only
// builds under -tags integration. It is public so other modules can reuse
// both in their tests.
package s3test

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // S3's single-part ETag is an MD5; not a security use
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Fake is an in-memory S3 store implementing the PutObject, GetObject and
// HeadObject shapes push and pull depend on. Create it with NewFake.
type Fake struct {
	mu          sync.Mutex
	objects     map[string][]byte
	mod         map[string]time.Time
	sums        map[string]string // base64 SHA256, only for SHA256-checksummed puts
	etags       map[string]string // SetETag overrides; the default is the body's MD5
	putAlgo     map[string]types.ChecksumAlgorithm
	putSum      map[string]string // ChecksumSHA256 supplied by the caller, "" if none
	getMode     map[string]types.ChecksumMode
	headMode    map[string]types.ChecksumMode
	putOrder    []string
	getOrder    []string
	headOrder   []string
	delOrder    []string
	inFlight    int
	maxInFlight int

	// Fault-injection hooks, all optional.
	PutErr       func(key string, seq int) error      // non-nil return fails that put (seq is 1-based)
	BeforePut    func(key string)                     // runs before a put reads its body (mutate the source)
	PutDelay     time.Duration                        // hold each put open, for concurrency tests
	GetBody      func(key string, body []byte) []byte // transform served bytes (corruption)
	BeforeGet    func(f *Fake, key string)            // mutate the store between fetches
	DeleteErr    func(key string) error               // non-nil return reports that key as a types.Error, leaving it in place
	HeadErr      func(key string) error               // non-nil return fails that head
	HeadChecksum func(key, sum string) string         // transform the ChecksumSHA256 a head returns
	HeadDelay    time.Duration                        // hold each head open, for concurrency tests
	IgnoreRange  bool                                 // answer ranged GETs with the whole object and no Content-Range
}

// NewFake returns an empty in-memory store.
func NewFake() *Fake {
	return &Fake{
		objects:  map[string][]byte{},
		mod:      map[string]time.Time{},
		sums:     map[string]string{},
		etags:    map[string]string{},
		putAlgo:  map[string]types.ChecksumAlgorithm{},
		putSum:   map[string]string{},
		getMode:  map[string]types.ChecksumMode{},
		headMode: map[string]types.ChecksumMode{},
	}
}

// PutObject stores the body under the key, recording call order, checksum
// algorithm, supplied ChecksumSHA256, and peak concurrency. A SHA256
// ChecksumAlgorithm stores the body's full-object SHA256 for HeadObject to
// return, as S3 does. Like S3, a supplied ContentLength or ChecksumSHA256 that
// does not match the body fails the put (InvalidRequest or BadDigest) and
// leaves the stored object untouched.
func (f *Fake) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := aws.ToString(in.Key)
	if f.BeforePut != nil {
		f.BeforePut(key)
	}
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(body)
	b64 := base64.StdEncoding.EncodeToString(sum[:])

	f.mu.Lock()
	f.putOrder = append(f.putOrder, key)
	seq := len(f.putOrder)
	hook, delay := f.PutErr, f.PutDelay
	f.mu.Unlock()
	defer f.enter()()

	if err := sleep(ctx, delay); err != nil {
		return nil, err
	}
	if hook != nil {
		if err := hook(key, seq); err != nil {
			return nil, err
		}
	}
	if n := in.ContentLength; n != nil && *n != int64(len(body)) {
		return nil, &types.InvalidRequest{Message: aws.String(
			fmt.Sprintf("ContentLength %d does not match the %d-byte body", *n, len(body)))}
	}
	if want := aws.ToString(in.ChecksumSHA256); want != "" && want != b64 {
		return nil, &smithy.GenericAPIError{Code: "BadDigest", Message: "ChecksumSHA256 does not match the body"}
	}

	f.mu.Lock()
	f.objects[key] = body
	delete(f.etags, key)
	f.putAlgo[key] = in.ChecksumAlgorithm
	f.putSum[key] = aws.ToString(in.ChecksumSHA256)
	if in.ChecksumAlgorithm == types.ChecksumAlgorithmSha256 {
		f.sums[key] = b64
	} else {
		delete(f.sums, key)
	}
	f.mu.Unlock()
	return &s3.PutObjectOutput{}, nil
}

// HeadObject reports the stored object's length and, when it was put with a
// SHA256 ChecksumAlgorithm, its base64 SHA256 (as S3 does under ChecksumMode
// ENABLED), passed through HeadChecksum when set. It records call order,
// checksum mode, and peak concurrency (shared with PutObject). A missing key
// returns *types.NotFound, as the SDK does.
func (f *Fake) HeadObject(ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	key := aws.ToString(in.Key)
	f.mu.Lock()
	f.headOrder = append(f.headOrder, key)
	f.headMode[key] = in.ChecksumMode
	hook, delay, sumHook := f.HeadErr, f.HeadDelay, f.HeadChecksum
	f.mu.Unlock()
	defer f.enter()()

	if err := sleep(ctx, delay); err != nil {
		return nil, err
	}
	if hook != nil {
		if err := hook(key); err != nil {
			return nil, err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.objects[key]
	if !ok {
		return nil, &types.NotFound{}
	}
	out := &s3.HeadObjectOutput{ContentLength: aws.Int64(int64(len(body)))}
	if sum, ok := f.sums[key]; ok {
		if sumHook != nil {
			sum = sumHook(key, sum)
		}
		out.ChecksumSHA256 = aws.String(sum)
	}
	return out, nil
}

// enter counts one in-flight call toward the peak and returns its exit.
func (f *Fake) enter() func() {
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxInFlight {
		f.maxInFlight = f.inFlight
	}
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}
}

// sleep waits d (if positive) or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// GetObject serves the stored body, recording call order and checksum mode.
// Like S3 it returns an ETag (the body's MD5 unless SetETag overrides it),
// fails a stale IfMatch with 412 PreconditionFailed, and serves a
// "bytes=N-" or "bytes=N-M" Range with a Content-Range, or 416 InvalidRange
// when N is past the end; errors carry the HTTP status as the SDK's do.
// IgnoreRange serves the whole object instead, as a server without range
// support does. GetBody transforms the whole body before any range is cut.
func (f *Fake) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(in.Key)
	if f.BeforeGet != nil {
		f.BeforeGet(f, key)
	}
	f.mu.Lock()
	f.getOrder = append(f.getOrder, key)
	f.getMode[key] = in.ChecksumMode
	body, ok := f.objects[key]
	etag := f.etagLocked(key)
	ignoreRange := f.IgnoreRange
	f.mu.Unlock()
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	if m := aws.ToString(in.IfMatch); m != "" && m != etag {
		return nil, httpError(http.StatusPreconditionFailed, "PreconditionFailed",
			"At least one of the pre-conditions you specified did not hold")
	}
	if f.GetBody != nil {
		body = f.GetBody(key, body)
	}
	out := &s3.GetObjectOutput{ETag: aws.String(etag), AcceptRanges: aws.String("bytes")}
	if start, end, ok := parseRange(aws.ToString(in.Range)); ok && !ignoreRange {
		if start >= int64(len(body)) {
			return nil, httpError(http.StatusRequestedRangeNotSatisfiable, "InvalidRange",
				"The requested range is not satisfiable")
		}
		end = min(end, int64(len(body))-1)
		out.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
		body = body[start : end+1]
	}
	out.Body = io.NopCloser(bytes.NewReader(body))
	out.ContentLength = aws.Int64(int64(len(body)))
	return out, nil
}

// parseRange reads a single "bytes=N-" or "bytes=N-M" range; anything else is
// ignored, as HTTP servers ignore a Range they cannot parse.
func parseRange(h string) (start, end int64, ok bool) {
	spec, found := strings.CutPrefix(h, "bytes=")
	from, to, dash := strings.Cut(spec, "-")
	if !found || !dash {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(from, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}
	if to == "" {
		return start, 1<<62 - 1, true
	}
	end, err = strconv.ParseInt(to, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	return start, end, true
}

// httpError builds an error shaped like the SDK's for an S3 error response:
// an API error code wrapped in a response error that reports the status.
func httpError(status int, code, msg string) error {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
		Err:      &smithy.GenericAPIError{Code: code, Message: msg},
	}
}

// etagLocked returns key's ETag; f.mu must be held.
func (f *Fake) etagLocked(key string) string {
	if e, ok := f.etags[key]; ok {
		return e
	}
	sum := md5.Sum(f.objects[key]) //nolint:gosec // see the import
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// SetETag makes key report etag until its next put or Store, standing for a
// re-upload that changes the ETag but not the bytes (multipart, SSE-KMS).
func (f *Fake) SetETag(key, etag string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.etags[key] = etag
}

// Store seeds or overwrites an object directly, bypassing the hooks. Like an
// upload without a checksum algorithm, it leaves no stored checksum.
func (f *Fake) Store(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), body...)
	delete(f.sums, key)
	delete(f.etags, key)
}

// StoreAt seeds or overwrites an object with a LastModified time, so listing
// and age checks see it. Objects seeded with Store have a zero mod time. Like
// Store, it leaves no stored checksum.
func (f *Fake) StoreAt(key string, body []byte, mod time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), body...)
	f.mod[key] = mod
	delete(f.sums, key)
	delete(f.etags, key)
}

// Delete removes an object directly.
func (f *Fake) Delete(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
	delete(f.mod, key)
	delete(f.sums, key)
	delete(f.etags, key)
}

// ListObjectsV2 lists stored keys under the requested Prefix in lexical order,
// honoring MaxKeys (default 1000) and a key-based ContinuationToken (keys
// strictly greater than the token). It can drive s3.NewListObjectsV2Paginator.
func (f *Fake) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefix := aws.ToString(in.Prefix)
	token := aws.ToString(in.ContinuationToken)
	max := int(aws.ToInt32(in.MaxKeys))
	if max <= 0 {
		max = 1000
	}

	f.mu.Lock()
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, prefix) && k > token {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	truncated := len(keys) > max
	if truncated {
		keys = keys[:max]
	}
	contents := make([]types.Object, len(keys))
	for i, k := range keys {
		contents[i] = types.Object{Key: aws.String(k), LastModified: aws.Time(f.mod[k])}
	}
	f.mu.Unlock()

	out := &s3.ListObjectsV2Output{
		Contents:    contents,
		KeyCount:    aws.Int32(int32(len(contents))),
		IsTruncated: aws.Bool(truncated),
	}
	if truncated {
		out.NextContinuationToken = aws.String(keys[len(keys)-1])
	}
	return out, nil
}

// DeleteObjects removes each requested key from the store, recording deleted
// keys in call order. A non-nil DeleteErr for a key is reported as a
// types.Error (the key is left in place) instead of failing the whole request.
// Quiet suppresses the Deleted entries; errors are always reported.
func (f *Fake) DeleteObjects(_ context.Context, in *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	quiet := in.Delete != nil && aws.ToBool(in.Delete.Quiet)
	out := &s3.DeleteObjectsOutput{}

	f.mu.Lock()
	defer f.mu.Unlock()
	if in.Delete == nil {
		return out, nil
	}
	for _, obj := range in.Delete.Objects {
		key := aws.ToString(obj.Key)
		if f.DeleteErr != nil {
			if err := f.DeleteErr(key); err != nil {
				out.Errors = append(out.Errors, types.Error{
					Key:     aws.String(key),
					Message: aws.String(err.Error()),
				})
				continue
			}
		}
		delete(f.objects, key)
		delete(f.mod, key)
		delete(f.sums, key)
		delete(f.etags, key)
		f.delOrder = append(f.delOrder, key)
		if !quiet {
			out.Deleted = append(out.Deleted, types.DeletedObject{Key: aws.String(key)})
		}
	}
	return out, nil
}

// Object returns the stored body for key.
func (f *Fake) Object(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.objects[key]
	return body, ok
}

// ResetCalls forgets the recorded call order and peak concurrency, so a caller
// can seed the store as an arrange step and then observe only the calls its
// act makes. Stored objects and fault hooks are left in place.
func (f *Fake) ResetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putOrder = nil
	f.getOrder = nil
	f.headOrder = nil
	f.delOrder = nil
	f.maxInFlight = 0
}

// Puts returns every attempted PutObject key in call order.
func (f *Fake) Puts() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.putOrder...)
}

// Gets returns every GetObject key in call order.
func (f *Fake) Gets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.getOrder...)
}

// Heads returns every HeadObject key in call order.
func (f *Fake) Heads() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.headOrder...)
}

// Deletes returns every deleted key in call order.
func (f *Fake) Deletes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.delOrder...)
}

// PutAlgorithm returns the ChecksumAlgorithm recorded for key's last put.
func (f *Fake) PutAlgorithm(key string) types.ChecksumAlgorithm {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putAlgo[key]
}

// PutChecksum returns the ChecksumSHA256 the caller supplied on key's last
// successful put, or "" if it supplied none.
func (f *Fake) PutChecksum(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putSum[key]
}

// GetMode returns the ChecksumMode recorded for key's last get.
func (f *Fake) GetMode(key string) types.ChecksumMode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getMode[key]
}

// HeadMode returns the ChecksumMode recorded for key's last head.
func (f *Fake) HeadMode(key string) types.ChecksumMode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.headMode[key]
}

// MaxInFlight returns the peak number of concurrent PutObject and HeadObject
// calls observed.
func (f *Fake) MaxInFlight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInFlight
}
