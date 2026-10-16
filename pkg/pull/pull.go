// Package pull mirrors a manifest snapshot from S3 into a destination
// directory it exclusively owns. It never lists, never deletes, and never
// modifies the live tree: every run fetches the manifest, fully re-hashes
// the live tree, assembles a verified sibling tree (hardlink unchanged /
// copy mode-changed / download changed), re-verifies that tree against the
// manifest, and atomically swaps it in. On any failure before the swap the
// destination is left byte-for-byte untouched, and the files staged so far
// are kept beside it so the next run downloads only what is still missing.
//
// PullWithManifest also returns the exact manifest bytes it installed, and
// Options.BeforeReplace lets a caller persist them before the swap.
// ReplaceFromDirectory does the same from a local source tree, and Recover
// repairs an interrupted swap offline. Manifest and content reads are bounded
// by Options.MaxManifestBytes and Options.MaxContentBytes and by each entry's
// declared size.
package pull

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
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

// Options configures Pull, PullWithManifest, and ReplaceFromDirectory.
type Options struct {
	Parallel    int          // concurrent downloads; 0 means 16
	Logger      *slog.Logger // nil means discard
	ManifestKey string       // "" means <prefix>/manifest.json

	// BeforeReplace, when set, is called with a copy of the exact manifest
	// bytes once the staged tree has been verified against them and before
	// the destination is touched. An error prevents replacement, leaves the
	// destination untouched, and is returned as-is. Bounded retries cover any
	// staged-content mismatch (hash or size) observed before BeforeReplace;
	// nothing retries after it.
	BeforeReplace func(context.Context, []byte) error

	MaxManifestBytes int64 // manifest size cap; 0 means DefaultMaxManifestBytes, negative is an error
	MaxContentBytes  int64 // cap on the manifest's total declared size; 0 means DefaultMaxContentBytes, negative is an error

	// Fixed in v1; kept internal until a caller needs them.
	mismatchRetries int // full re-sync attempts on staged-content mismatch; 0 means 3
	swapRetries     int // attempts per swap rename; 0 means 5
}

// Library defaults for the Options limits.
const (
	DefaultMaxManifestBytes int64 = 64 << 20 // 64 MiB
	DefaultMaxContentBytes  int64 = 64 << 30 // 64 GiB
)

// Stats reports how each manifest entry was staged.
type Stats struct {
	Downloaded int // fetched from S3 (or, for ReplaceFromDirectory, the source tree); excludes Reused, unlike Plan.Download
	Linked     int // hardlink into the live tree's file
	Copied     int // copied locally (mode change or hardlink fallback)
	Reused     int // staged from a failed pull's leftover files instead of downloaded
	Total      int // manifest entries
}

// Result is a successful replacement: its stats and the exact manifest bytes
// that describe the installed tree.
type Result struct {
	Stats    Stats
	Manifest []byte
}

const (
	tmpSuffix     = ".pinsync-tmp"
	oldSuffix     = ".pinsync-old"
	partialSuffix = ".pinsync-partial"
)

// Siblings returns every path pull creates beside dest: the staging tree,
// the previous tree held aside during a swap, and the files kept from a pull
// that failed before its swap. Each is a directory pull owns outright; a
// caller can keep its own paths clear of them or walk them.
func Siblings(dest string) []string {
	dest = filepath.Clean(dest)
	return []string{dest + tmpSuffix, dest + oldSuffix, dest + partialSuffix}
}

// goos is a test seam for Windows-specific behavior.
var goos = runtime.GOOS

// mismatchBackoff is the pause before re-sync attempts 2 and 3.
var mismatchBackoff = []time.Duration{time.Second, 2 * time.Second}

// Pull converges dest to exactly the manifest's tree, or returns a non-nil
// error leaving dest untouched. It is PullWithManifest without the manifest.
func Pull(ctx context.Context, client S3API, bucket, prefix, dest string, opts Options) (Stats, error) {
	res, err := PullWithManifest(ctx, client, bucket, prefix, dest, opts)
	return res.Stats, err
}

// PullWithManifest converges dest to exactly the manifest's tree and returns
// the manifest bytes of the attempt that installed it; the manifest is never
// refetched after replacement. A staged object that mismatches its manifest
// entry re-fetches the manifest and retries the whole sync, reusing whatever
// the abandoned attempt already staged; any other error, including any error
// from BeforeReplace or the swap, is returned at once.
func PullWithManifest(ctx context.Context, client S3API, bucket, prefix, dest string, opts Options) (Result, error) {
	s, err := newSyncer(dest, opts)
	if err != nil {
		return Result{}, err
	}
	s.client, s.bucket, s.prefix = client, bucket, strings.TrimSuffix(prefix, "/")
	s.fetch = s.download

	if err := recoverInterrupted(s.dest, s.tmp, s.old, s.partial, s.logger); err != nil {
		return Result{}, err
	}

	attempts := opts.mismatchRetries
	if attempts == 0 {
		attempts = 3
	}
	var mismatch *stagedMismatchError
	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if err := sleepCtx(ctx, mismatchBackoff[min(attempt-2, len(mismatchBackoff)-1)]); err != nil {
				return Result{}, err
			}
		}
		res, staged, err := s.syncOnce(ctx)
		if err == nil {
			return res, nil
		}
		if staged == nil {
			return Result{}, err
		}
		mismatch = staged
		s.logger.Warn("staged object mismatched the manifest; re-fetching manifest and re-syncing",
			"path", mismatch.Path, "attempt", attempt)
	}
	return Result{}, fmt.Errorf(
		"pull: %s kept mismatching its manifest entry after %d attempts (the bucket may be mid-publish); giving up",
		mismatch.Path, attempts,
	)
}

// syncer carries one invocation's parameters.
type syncer struct {
	client         S3API
	bucket, prefix string
	source         string // ReplaceFromDirectory's source tree
	dest, tmp, old string
	partial        string // files kept from a pull that failed before its swap
	parallel       int
	logger         *slog.Logger
	swapAttempts   int
	manifestKey    string
	beforeReplace  func(context.Context, []byte) error
	maxManifest    int64
	maxContent     int64

	// fetch stages one entry's bytes from the source (S3 or a directory).
	fetch func(ctx context.Context, f manifest.File, dst string, mode fs.FileMode) error
}

// newSyncer validates opts and applies the defaults shared by every entry
// point.
func newSyncer(dest string, opts Options) (*syncer, error) {
	if opts.MaxManifestBytes < 0 || opts.MaxContentBytes < 0 {
		return nil, fmt.Errorf("pull: negative limit (MaxManifestBytes %d, MaxContentBytes %d)",
			opts.MaxManifestBytes, opts.MaxContentBytes)
	}
	s := &syncer{
		dest:          filepath.Clean(dest),
		parallel:      max(opts.Parallel, 0),
		logger:        opts.Logger,
		swapAttempts:  opts.swapRetries,
		manifestKey:   opts.ManifestKey,
		beforeReplace: opts.BeforeReplace,
		maxManifest:   opts.MaxManifestBytes,
		maxContent:    opts.MaxContentBytes,
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
	if s.maxManifest == 0 {
		s.maxManifest = DefaultMaxManifestBytes
	}
	if s.maxContent == 0 {
		s.maxContent = DefaultMaxContentBytes
	}
	s.tmp, s.old, s.partial = s.dest+tmpSuffix, s.dest+oldSuffix, s.dest+partialSuffix
	return s, nil
}

// syncOnce fetches the manifest and runs one replace cycle against it; see
// replace for the mismatch result.
func (s *syncer) syncOnce(ctx context.Context) (Result, *stagedMismatchError, error) {
	raw, m, err := s.fetchManifest(ctx)
	if err != nil {
		return Result{}, nil, err
	}
	return s.replace(ctx, raw, m)
}

// replace runs one hash → stage → verify → BeforeReplace → swap cycle for the
// manifest m encoded as raw, which the caller owns. On any error before the
// swap dest is untouched and the staging tree is kept as the partial tree
// (see keepStaged). mismatch is non-nil only when staging failed on a
// staged-content mismatch and the staging tree was cleared — the one
// retryable failure; errors from BeforeReplace and the swap never set it,
// whatever they wrap.
func (s *syncer) replace(ctx context.Context, raw []byte, m *manifest.Manifest) (Result, *stagedMismatchError, error) {
	if err := s.checkContentSize(m); err != nil {
		return Result{}, nil, err
	}
	live, err := hashTree(s.dest)
	if err != nil {
		return Result{}, nil, err
	}
	var mismatch *stagedMismatchError
	stats, err := s.stage(ctx, m, live, s.hashPartial())
	if err != nil {
		errors.As(err, &mismatch)
	} else {
		// Re-read the whole staged tree: linked and copied entries were
		// trusted from the live or partial hash, which may have changed
		// since.
		// CEILING: this reads every staged byte a second time, including
		// fresh downloads already hashed while streaming. Fine for config
		// trees; if pull time on large trees matters, re-verify only the
		// linked and copied entries.
		err = manifest.VerifyTree(ctx, s.tmp, m)
	}
	if err == nil && s.beforeReplace != nil {
		err = s.beforeReplace(ctx, bytes.Clone(raw))
	}
	if err == nil {
		err = s.swap()
	}
	if err != nil {
		// A staging tree that can be neither kept nor deleted rules out a
		// retry, which would have to stage over it; the next pull's recovery
		// tries again.
		if keepErr := keepStaged(s.tmp, s.partial, s.logger); keepErr != nil {
			return Result{}, nil, errors.Join(err, keepErr)
		}
		return Result{}, mismatch, err
	}
	stats.Total = len(m.Files)
	s.logger.Info("pull complete", "total", stats.Total, "downloaded", stats.Downloaded,
		"linked", stats.Linked, "copied", stats.Copied, "reused", stats.Reused)
	return Result{Stats: stats, Manifest: raw}, nil, nil
}

// hashPartial indexes the partial tree by content. The partial tree only
// saves downloads, so one that cannot be read is logged and treated as empty;
// either way staging removes it once nothing more can come from it.
// CEILING: this reads every partial byte, including files that were linked
// from the live tree and so were just hashed there. Bounded by one manifest's
// content and far cheaper than downloading; if it matters, skip inodes that
// are already in the live tree.
func (s *syncer) hashPartial() map[contentKey]leftover {
	files, err := hashTree(s.partial)
	if err != nil {
		s.logger.Warn("ignoring unreadable files kept from a failed pull", "path", s.partial, "error", err)
		return nil
	}
	return byContent(files)
}

// checkContentSize rejects a manifest whose total declared size exceeds the
// configured cap, before anything is staged.
func (s *syncer) checkContentSize(m *manifest.Manifest) error {
	var total int64
	for _, f := range m.Files {
		// Validate guarantees nonnegative sizes; comparing before adding
		// keeps the sum from overflowing.
		if f.Size > s.maxContent-total {
			return fmt.Errorf("pull: manifest content exceeds the %d-byte limit", s.maxContent)
		}
		total += f.Size
	}
	return nil
}

// fetchManifest gets and validates the manifest key (default
// <prefix>/manifest.json) before anything under dest is touched, returning
// its exact bytes alongside the decoded form. A manifest larger than
// maxManifest fails without being read to EOF.
func (s *syncer) fetchManifest(ctx context.Context) ([]byte, *manifest.Manifest, error) {
	key := s.manifestKey
	if key == "" {
		key = path.Join(s.prefix, manifest.Name)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("pull: fetching %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(out.Body, s.maxManifest+1))
	if err != nil {
		return nil, nil, fmt.Errorf("pull: fetching %s: %w", key, err)
	}
	if int64(len(raw)) > s.maxManifest {
		return nil, nil, fmt.Errorf("pull: %s exceeds the %d-byte manifest limit", key, s.maxManifest)
	}
	m, err := manifest.Decode(bytes.NewReader(raw))
	if err != nil {
		return nil, nil, err
	}
	return raw, m, nil
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
