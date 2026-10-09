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

// stagedMismatchError marks staged bytes whose hash or length does not match
// the manifest entry — the one error class that triggers a full re-sync
// attempt (an object overwritten mid-publish can differ in either). Detail
// says which.
type stagedMismatchError struct {
	Path   string
	Detail string
}

func (e *stagedMismatchError) Error() string {
	return fmt.Sprintf("pull: %s: %s", e.Path, e.Detail)
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

// action is how one manifest entry is reconciled against the live tree.
type action int

const (
	actDownload action = iota // live file is absent or its content or size differs
	actCopy                   // content matches but the mode does not (never on Windows)
	actLink                   // content and mode match; hardlink the live file
)

// classify decides, without touching the filesystem, how a manifest entry
// should be staged against the live tree, and returns the parsed target mode.
// It is the single decision shared by real staging and the dry-run preview:
// download when the live file is absent or its content or size differs, copy on a
// mode-only change (modes are not meaningful on Windows, so skipped there),
// otherwise hardlink. The hardlink→copy runtime fallback is the stager's, not
// classify's — classify reports only the intended action.
func classify(f manifest.File, live map[string]localFile) (action, fs.FileMode, error) {
	mode, err := f.FileMode()
	if err != nil {
		return 0, 0, err
	}
	lf, ok := live[f.Path]
	if !ok || lf.hash != f.SHA256 || lf.size != f.Size {
		return actDownload, mode, nil
	}
	if goos != "windows" && lf.mode != mode {
		return actCopy, mode, nil
	}
	return actLink, mode, nil
}

// stageEntry stages one manifest entry: hardlink when content and mode match
// the live file (content only on Windows), copy locally on a mode-only
// change (never hardlink-then-chmod, which would mutate the live inode), and
// download otherwise. Hardlink failures fall back to a copy.
func (s *syncer) stageEntry(ctx context.Context, f manifest.File, live map[string]localFile, c *counters) error {
	act, mode, err := classify(f, live)
	if err != nil {
		return err
	}
	dst := filepath.Join(s.tmp, filepath.FromSlash(f.Path))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if act == actDownload {
		if err := s.fetch(ctx, f, dst, mode); err != nil {
			return err
		}
		c.downloaded.Add(1)
		return nil
	}
	src := filepath.Join(s.dest, filepath.FromSlash(f.Path))
	if act == actCopy {
		if err := copyFile(src, f, dst, mode); err != nil {
			return err
		}
		c.copied.Add(1)
		return nil
	}
	if err := linkFn(src, dst); err != nil {
		s.logger.Debug("hardlink failed; copying instead", "path", f.Path, "error", err)
		if err := copyFile(src, f, dst, mode); err != nil {
			return err
		}
		c.copied.Add(1)
		return nil
	}
	c.linked.Add(1)
	return nil
}

// download fetches one object whole (never ranged — a full-object checksum
// needs full-object bytes) and writes it into staging with writeVerified.
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
	if err := writeVerified(out.Body, f, dst, mode); err != nil {
		return fmt.Errorf("pull: downloading %s: %w", key, err)
	}
	return nil
}

// copyFromSource stages one entry from ReplaceFromDirectory's source tree.
// The source file must be a regular file; its bytes are verified like a
// download.
func (s *syncer) copyFromSource(_ context.Context, f manifest.File, dst string, mode fs.FileMode) error {
	src := filepath.Join(s.source, filepath.FromSlash(f.Path))
	info, err := os.Lstat(src)
	if err != nil {
		return fmt.Errorf("pull: source %s: %w", f.Path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("pull: source %s: not a regular file", f.Path)
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }() // read-only; nothing to do about a close error
	if err := writeVerified(in, f, dst, mode); err != nil {
		return fmt.Errorf("pull: copying source %s: %w", f.Path, err)
	}
	return nil
}

// writeVerified streams r into the staging file dst while hashing, reading at
// most one byte past the declared size, and verifies length and SHA256
// against the manifest entry. A long body fails without being read to EOF.
func writeVerified(r io.Reader, f manifest.File, dst string, mode fs.FileMode) error {
	fh, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(fh, io.TeeReader(io.LimitReader(r, f.Size+1), h))
	if err != nil {
		_ = fh.Close()
		return err
	}
	if err := fh.Close(); err != nil {
		return err
	}
	switch {
	case n > f.Size:
		return &stagedMismatchError{Path: f.Path, Detail: fmt.Sprintf("body exceeds the manifest's %d bytes", f.Size)}
	case n < f.Size:
		return &stagedMismatchError{Path: f.Path, Detail: fmt.Sprintf("body is %d bytes, manifest says %d", n, f.Size)}
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return &stagedMismatchError{Path: f.Path, Detail: fmt.Sprintf("bytes hash %s, manifest says %s", got, f.SHA256)}
	}
	return chmod(dst, mode)
}

// copyFile copies the live file src into the staging tree with
// writeVerified, so a live file that changed since it was hashed is never
// read past the entry's declared size.
func copyFile(src string, f manifest.File, dst string, mode fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }() // read-only; nothing to do about a close error
	if err := writeVerified(in, f, dst, mode); err != nil {
		return fmt.Errorf("pull: copying live %s: %w", f.Path, err)
	}
	return nil
}

// chmod applies the exact permission bits; on Windows modes are not
// meaningful and a chmod failure is never an error.
func chmod(p string, mode fs.FileMode) error {
	if err := os.Chmod(p, mode); err != nil && goos != "windows" {
		return err
	}
	return nil
}
