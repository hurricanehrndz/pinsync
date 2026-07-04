package pull

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// linkFn is a test seam for forcing hardlink failures.
var linkFn = os.Link

// hashMismatchError marks a downloaded object whose bytes do not match the
// manifest — the one error class that triggers a full re-sync attempt.
type hashMismatchError struct {
	Path      string
	Want, Got string
}

func (e *hashMismatchError) Error() string {
	return fmt.Sprintf("pull: %s: downloaded bytes hash %s, manifest says %s", e.Path, e.Got, e.Want)
}

// counters aggregates per-entry outcomes across the staging workers.
type counters struct {
	downloaded, linked, copied atomic.Int64
}

// stage assembles the complete fresh tree in s.tmp, a sibling of dest on the
// same filesystem. Every manifest entry is staged and verified; nothing under
// dest is ever modified.
func (s *syncer) stage(ctx context.Context, m *manifest.Manifest, live map[string]localFile) (Stats, error) {
	if err := os.MkdirAll(s.tmp, 0o755); err != nil {
		return Stats{}, err
	}
	var c counters
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.parallel)
	for _, f := range m.Files {
		g.Go(func() error { return s.stageEntry(gctx, f, live, &c) })
	}
	if err := g.Wait(); err != nil {
		return Stats{}, err
	}
	return Stats{
		Downloaded: int(c.downloaded.Load()),
		Linked:     int(c.linked.Load()),
		Copied:     int(c.copied.Load()),
	}, nil
}

// stageEntry stages one manifest entry: hardlink when content and mode match
// the live file (content only on Windows), copy locally on a mode-only
// change (never hardlink-then-chmod, which would mutate the live inode), and
// download otherwise. Hardlink failures fall back to a copy.
func (s *syncer) stageEntry(ctx context.Context, f manifest.File, live map[string]localFile, c *counters) error {
	mode, err := f.FileMode()
	if err != nil {
		return err
	}
	dst := filepath.Join(s.tmp, filepath.FromSlash(f.Path))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	lf, ok := live[f.Path]
	if !ok || lf.hash != f.SHA256 {
		if err := s.download(ctx, f, dst, mode); err != nil {
			return err
		}
		c.downloaded.Add(1)
		return nil
	}
	src := filepath.Join(s.dest, filepath.FromSlash(f.Path))
	if goos != "windows" && lf.mode != mode {
		if err := copyFile(src, dst, mode); err != nil {
			return err
		}
		c.copied.Add(1)
		return nil
	}
	if err := linkFn(src, dst); err != nil {
		s.logger.Debug("hardlink failed; copying instead", "path", f.Path, "error", err)
		if err := copyFile(src, dst, mode); err != nil {
			return err
		}
		c.copied.Add(1)
		return nil
	}
	c.linked.Add(1)
	return nil
}

// download fetches one object whole (never ranged — a full-object checksum
// needs full-object bytes), streams it into the staging file while hashing,
// and independently verifies it against the manifest entry.
func (s *syncer) download(ctx context.Context, f manifest.File, dst string, mode fs.FileMode) error {
	key := path.Join(s.prefix, f.Path)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return fmt.Errorf("pull: fetching %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()

	fh, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(fh, io.TeeReader(out.Body, h)); err != nil {
		_ = fh.Close()
		return fmt.Errorf("pull: downloading %s: %w", key, err)
	}
	if err := fh.Close(); err != nil {
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return &hashMismatchError{Path: f.Path, Want: f.SHA256, Got: got}
	}
	return chmod(dst, mode)
}

// copyFile copies src into the staging tree with the manifest's mode.
func copyFile(src, dst string, mode fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }() // read-only; nothing to do about a close error
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return chmod(dst, mode)
}

// chmod applies the exact permission bits; on Windows modes are not
// meaningful and a chmod failure is never an error.
func chmod(p string, mode fs.FileMode) error {
	if err := os.Chmod(p, mode); err != nil && goos != "windows" {
		return err
	}
	return nil
}
