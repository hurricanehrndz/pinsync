// Package s3test provides an in-memory fake of the narrow S3 surface pinsync
// uses, plus (behind -tags integration) a MinIO test harness. It is shared by
// the push and pull tests.
package s3test

import (
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Fake is an in-memory S3 store implementing the PutObject and GetObject
// shapes push and pull depend on. Create it with NewFake.
type Fake struct {
	mu          sync.Mutex
	objects     map[string][]byte
	putAlgo     map[string]types.ChecksumAlgorithm
	getMode     map[string]types.ChecksumMode
	putOrder    []string
	getOrder    []string
	inFlight    int
	maxInFlight int

	// Fault-injection hooks, all optional.
	PutErr    func(key string, seq int) error      // non-nil return fails that put (seq is 1-based)
	PutDelay  time.Duration                        // hold each put open, for concurrency tests
	GetBody   func(key string, body []byte) []byte // transform served bytes (corruption)
	BeforeGet func(f *Fake, key string)            // mutate the store between fetches
}

// NewFake returns an empty in-memory store.
func NewFake() *Fake {
	return &Fake{
		objects: map[string][]byte{},
		putAlgo: map[string]types.ChecksumAlgorithm{},
		getMode: map[string]types.ChecksumMode{},
	}
}

// PutObject stores the body under the key, recording call order, checksum
// algorithm, and peak concurrency.
func (f *Fake) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	key := aws.ToString(in.Key)

	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxInFlight {
		f.maxInFlight = f.inFlight
	}
	f.putOrder = append(f.putOrder, key)
	seq := len(f.putOrder)
	hook, delay := f.PutErr, f.PutDelay
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.inFlight--
		f.mu.Unlock()
	}()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	if hook != nil {
		if err := hook(key, seq); err != nil {
			return nil, err
		}
	}

	f.mu.Lock()
	f.objects[key] = body
	f.putAlgo[key] = in.ChecksumAlgorithm
	f.mu.Unlock()
	return &s3.PutObjectOutput{}, nil
}

// GetObject serves the stored body, recording call order and checksum mode.
func (f *Fake) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	key := aws.ToString(in.Key)
	if f.BeforeGet != nil {
		f.BeforeGet(f, key)
	}
	f.mu.Lock()
	f.getOrder = append(f.getOrder, key)
	f.getMode[key] = in.ChecksumMode
	body, ok := f.objects[key]
	f.mu.Unlock()
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	if f.GetBody != nil {
		body = f.GetBody(key, body)
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: aws.Int64(int64(len(body))),
	}, nil
}

// Store seeds or overwrites an object directly, bypassing the hooks.
func (f *Fake) Store(key string, body []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), body...)
}

// Delete removes an object directly.
func (f *Fake) Delete(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
}

// Object returns the stored body for key.
func (f *Fake) Object(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, ok := f.objects[key]
	return body, ok
}

// ResetCalls forgets the recorded PutObject/GetObject order, so a caller can
// seed the store as an arrange step and then observe only the calls its act
// makes. Stored objects and fault hooks are left in place.
func (f *Fake) ResetCalls() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.putOrder = nil
	f.getOrder = nil
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

// PutAlgorithm returns the ChecksumAlgorithm recorded for key's last put.
func (f *Fake) PutAlgorithm(key string) types.ChecksumAlgorithm {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putAlgo[key]
}

// GetMode returns the ChecksumMode recorded for key's last get.
func (f *Fake) GetMode(key string) types.ChecksumMode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getMode[key]
}

// MaxInFlight returns the peak number of concurrent PutObject calls observed.
func (f *Fake) MaxInFlight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInFlight
}
