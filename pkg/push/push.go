// Package push publishes a local tree to S3 as an atomic manifest snapshot:
// every content file is uploaded first, the manifest last, so readers only
// ever see a complete snapshot. Push is POSIX-only — on Windows the recorded
// mode bits would be synthetic.
package push

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// S3API is the slice of the S3 client Push needs; *s3.Client satisfies it.
type S3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput,
		opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput,
		opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Options configures Push.
type Options struct {
	Parallel    int          // concurrent uploads; 0 means 16
	Logger      *slog.Logger // nil means discard
	ManifestKey string       // "" means <prefix>/manifest.json
	Full        bool         // re-upload every file, skipping the remote-manifest diff
}

// Stats reports what Push did (the manifest itself is not counted in Uploaded).
type Stats struct {
	Uploaded int // content files uploaded
	Skipped  int // content files unchanged since the remote manifest
	Total    int // manifest entry count
}

// goos is a test seam for the Windows guard.
var goos = runtime.GOOS

// Push hashes root, validates the resulting manifest, and uploads only the
// content files that are new or changed relative to the remote manifest to
// <prefix>/<path>; only after all of them succeed does it upload the manifest
// to Options.ManifestKey (default <prefix>/manifest.json). An unchanged tree is
// a no-op that uploads nothing, not even the manifest. Options.Full forces a
// complete re-upload. On any failure the manifest is left untouched, so readers
// keep the previous complete snapshot.
func Push(ctx context.Context, client S3API, bucket, prefix, root string, opts Options) (Stats, error) {
	if goos == "windows" {
		return Stats{}, errors.New("push: not supported on Windows (POSIX mode bits would be synthetic)")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	parallel := opts.Parallel
	if parallel <= 0 {
		parallel = 16
	}
	prefix = strings.TrimSuffix(prefix, "/")

	m, err := manifest.Build(root)
	if err != nil {
		return Stats{}, err
	}

	manifestKey, err := resolveManifestKey(prefix, opts.ManifestKey, m.Files)
	if err != nil {
		return Stats{}, err
	}

	var buf bytes.Buffer
	if err := m.Encode(&buf); err != nil {
		return Stats{}, err
	}

	// Diff against the remote manifest unless -full forces a complete re-upload.
	// A byte-identical remote manifest means the tree is unchanged: upload
	// nothing, not even the manifest, so an unchanged push is a true no-op. Any
	// other change — including mode-only edits or deletions, which leave content
	// untouched but alter the manifest bytes — still republishes the manifest.
	var remote map[string]string
	if !opts.Full {
		diffMap, noop, diffErr := remoteDiff(ctx, client, bucket, manifestKey, buf.Bytes())
		if diffErr != nil {
			return Stats{}, diffErr
		}
		if noop {
			logger.Info("push up to date", "files", len(m.Files), "bucket", bucket, "prefix", prefix)
			return Stats{Skipped: len(m.Files), Total: len(m.Files)}, nil
		}
		remote = diffMap
	}

	skipped, upErr := uploadChanged(ctx, client, bucket, prefix, root, m.Files, remote, parallel, logger)
	if upErr != nil {
		return Stats{}, upErr
	}

	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(manifestKey),
		Body:              bytes.NewReader(buf.Bytes()),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}); err != nil {
		return Stats{}, fmt.Errorf("push: uploading %s: %w", manifest.Name, err)
	}
	uploaded := len(m.Files) - skipped
	logger.Info("push complete", "uploaded", uploaded, "skipped", skipped,
		"total", len(m.Files), "bucket", bucket, "prefix", prefix)
	return Stats{Uploaded: uploaded, Skipped: skipped, Total: len(m.Files)}, nil
}

// resolveManifestKey returns the manifest key (default <prefix>/manifest.json).
// A custom key that names an uploaded content file would let the manifest
// overwrite that file (or vice versa), breaking the atomic-snapshot invariant,
// so it is rejected before anything is uploaded and nothing lands on failure.
func resolveManifestKey(prefix, custom string, files []manifest.File) (string, error) {
	if custom == "" {
		return path.Join(prefix, manifest.Name), nil
	}
	for _, f := range files {
		if path.Join(prefix, f.Path) == custom {
			return "", fmt.Errorf("push: manifest key %q collides with a content file; choose a location outside the uploaded tree", custom)
		}
	}
	return custom, nil
}

// remoteDiff compares the local manifest bytes against the remote manifest at
// manifestKey. It returns the remote path→SHA256 map to upload against; noop is
// true when the remote manifest is byte-identical to local (nothing to do). A
// nil map with noop false means upload everything (first push, no remote yet).
func remoteDiff(ctx context.Context, client S3API, bucket, manifestKey string, local []byte) (remote map[string]string, noop bool, err error) {
	rm, err := remoteManifest(ctx, client, bucket, manifestKey)
	if err != nil {
		return nil, false, err
	}
	if rm == nil {
		return nil, false, nil
	}
	var rbuf bytes.Buffer
	if err := rm.Encode(&rbuf); err != nil {
		return nil, false, err
	}
	if bytes.Equal(local, rbuf.Bytes()) {
		return nil, true, nil
	}
	remote = make(map[string]string, len(rm.Files))
	for _, f := range rm.Files {
		remote[f.Path] = f.SHA256
	}
	return remote, false, nil
}

// remoteManifest fetches and decodes the manifest at manifestKey to diff
// against. A missing manifest (first push) returns nil, nil; any other failure
// is returned wrapped, noting that -full skips this fetch.
func remoteManifest(ctx context.Context, client S3API, bucket, manifestKey string) (*manifest.Manifest, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(manifestKey),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		var missing *types.NoSuchKey
		if errors.As(err, &missing) {
			return nil, nil
		}
		return nil, fmt.Errorf("push: fetching remote %s (pass -full to skip this diff): %w", manifestKey, err)
	}
	defer func() { _ = out.Body.Close() }()
	m, err := manifest.Decode(out.Body)
	if err != nil {
		return nil, fmt.Errorf("push: reading remote %s (pass -full to skip this diff): %w", manifestKey, err)
	}
	return m, nil
}

// uploadChanged uploads, with at most parallel in flight, every file whose
// SHA256 differs from remote (a nil remote uploads all — first push or -full).
// It returns the count of files skipped as unchanged.
func uploadChanged(ctx context.Context, client S3API, bucket, prefix, root string,
	files []manifest.File, remote map[string]string, parallel int, logger *slog.Logger,
) (int, error) {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallel)
	skipped := 0
	for _, f := range files {
		if remote != nil && remote[f.Path] == f.SHA256 {
			skipped++
			logger.Debug("unchanged", "path", f.Path)
			continue
		}
		g.Go(func() error {
			if err := putFile(gctx, client, bucket, prefix, root, f); err != nil {
				return err
			}
			logger.Debug("uploaded", "path", f.Path, "size", f.Size)
			return nil
		})
	}
	return skipped, g.Wait()
}

// putFile uploads one content file as a single-part put with a full-object
// SHA256 checksum.
func putFile(ctx context.Context, client S3API, bucket, prefix, root string, f manifest.File) error {
	fh, err := os.Open(filepath.Join(root, filepath.FromSlash(f.Path)))
	if err != nil {
		return err
	}
	defer func() { _ = fh.Close() }() // read-only; nothing to do about a close error
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(path.Join(prefix, f.Path)),
		Body:              fh,
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}); err != nil {
		return fmt.Errorf("push: uploading %s: %w", f.Path, err)
	}
	return nil
}
