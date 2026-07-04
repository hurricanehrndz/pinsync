// Package pull mirrors a manifest snapshot from S3 into a destination
// directory it exclusively owns. It never lists, never deletes, and never
// modifies the live tree: every run fetches the manifest, fully re-hashes
// the live tree, assembles a verified sibling tree (hardlink unchanged /
// copy mode-changed / download changed), and atomically swaps it in. On any
// failure the destination is left byte-for-byte untouched.
package pull

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// S3API is the slice of the S3 client Pull needs; *s3.Client satisfies it.
type S3API interface {
	GetObject(ctx context.Context, in *s3.GetObjectInput,
		opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Options configures Pull.
type Options struct {
	Parallel int          // concurrent downloads; 0 means 16
	Logger   *slog.Logger // nil means discard

	// Fixed in v1; kept internal until a caller needs them.
	mismatchRetries int // full re-sync attempts on staged-hash mismatch; 0 means 3
	swapRetries     int // attempts per swap rename; 0 means 5
}

// Stats reports how each manifest entry was staged.
type Stats struct {
	Downloaded int // fetched from S3
	Linked     int // hardlink into the live tree's file
	Copied     int // copied locally (mode change or hardlink fallback)
	Total      int // manifest entries
}

const (
	tmpSuffix = ".pinsync-tmp"
	oldSuffix = ".pinsync-old"
)

// goos is a test seam for Windows-specific behavior.
var goos = runtime.GOOS

// mismatchBackoff is the pause before re-sync attempts 2 and 3.
var mismatchBackoff = []time.Duration{time.Second, 2 * time.Second}

// Pull converges dest to exactly the manifest's tree, or returns a non-nil
// error leaving dest untouched.
func Pull(ctx context.Context, client S3API, bucket, prefix, dest string, opts Options) (Stats, error) {
	s := &syncer{
		client:       client,
		bucket:       bucket,
		prefix:       strings.TrimSuffix(prefix, "/"),
		dest:         filepath.Clean(dest),
		parallel:     max(opts.Parallel, 0),
		logger:       opts.Logger,
		swapAttempts: opts.swapRetries,
	}
	if s.parallel == 0 {
		s.parallel = 16
	}
	if s.logger == nil {
		s.logger = slog.New(slog.DiscardHandler)
	}
	if s.swapAttempts == 0 {
		s.swapAttempts = 5
	}
	s.tmp, s.old = s.dest+tmpSuffix, s.dest+oldSuffix

	if err := recoverInterrupted(s.dest, s.tmp, s.old, s.logger); err != nil {
		return Stats{}, err
	}

	attempts := opts.mismatchRetries
	if attempts == 0 {
		attempts = 3
	}
	var mismatch *hashMismatchError
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if err := sleepCtx(ctx, mismatchBackoff[min(attempt-2, len(mismatchBackoff)-1)]); err != nil {
				return Stats{}, err
			}
		}
		stats, err := s.syncOnce(ctx)
		if err == nil {
			return stats, nil
		}
		if !errors.As(err, &mismatch) {
			return Stats{}, err
		}
		s.logger.Warn("staged object mismatched the manifest; re-fetching manifest and re-syncing",
			"path", mismatch.Path, "attempt", attempt)
	}
	return Stats{}, fmt.Errorf(
		"pull: %s kept mismatching its manifest hash after %d attempts (the bucket may be mid-publish); giving up",
		mismatch.Path, attempts,
	)
}

// syncer carries one Pull invocation's parameters.
type syncer struct {
	client         S3API
	bucket, prefix string
	dest, tmp, old string
	parallel       int
	logger         *slog.Logger
	swapAttempts   int
}

// syncOnce runs one fetch → hash → stage → swap cycle. On any error the
// staging tree is discarded and dest is untouched.
func (s *syncer) syncOnce(ctx context.Context) (Stats, error) {
	m, err := s.fetchManifest(ctx)
	if err != nil {
		return Stats{}, err
	}
	live, err := hashTree(s.dest)
	if err != nil {
		return Stats{}, err
	}
	stats, err := s.stage(ctx, m, live)
	if err != nil {
		_ = os.RemoveAll(s.tmp)
		return Stats{}, err
	}
	if err := s.swap(); err != nil {
		_ = os.RemoveAll(s.tmp)
		return Stats{}, err
	}
	stats.Total = len(m.Files)
	s.logger.Info("pull complete", "total", stats.Total,
		"downloaded", stats.Downloaded, "linked", stats.Linked, "copied", stats.Copied)
	return stats, nil
}

// fetchManifest gets and validates <prefix>/manifest.json before anything
// under dest is touched.
func (s *syncer) fetchManifest(ctx context.Context) (*manifest.Manifest, error) {
	key := path.Join(s.prefix, manifest.Name)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return nil, fmt.Errorf("pull: fetching %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	return manifest.Decode(out.Body)
}

// sleepCtx sleeps for d unless ctx is canceled first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
