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
}

// Options configures Push.
type Options struct {
	Parallel int          // concurrent uploads; 0 means 16
	Logger   *slog.Logger // nil means discard
}

// Stats reports what Push uploaded (the manifest itself is not counted).
type Stats struct {
	Uploaded int
}

// goos is a test seam for the Windows guard.
var goos = runtime.GOOS

// Push hashes root, validates the resulting manifest, uploads every content
// file to <prefix>/<path>, and only after all of them succeed uploads the
// manifest to <prefix>/manifest.json. On any failure the manifest is left
// untouched, so readers keep the previous complete snapshot.
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

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallel)
	for _, f := range m.Files {
		g.Go(func() error {
			if err := putFile(gctx, client, bucket, prefix, root, f); err != nil {
				return err
			}
			logger.Debug("uploaded", "path", f.Path, "size", f.Size)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return Stats{}, err
	}

	var buf bytes.Buffer
	if err := m.Encode(&buf); err != nil {
		return Stats{}, err
	}
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(path.Join(prefix, manifest.Name)),
		Body:              bytes.NewReader(buf.Bytes()),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}); err != nil {
		return Stats{}, fmt.Errorf("push: uploading %s: %w", manifest.Name, err)
	}
	logger.Info("push complete", "files", len(m.Files), "bucket", bucket, "prefix", prefix)
	return Stats{Uploaded: len(m.Files)}, nil
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
