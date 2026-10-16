package pull

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
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
	downloaded, linked, copied, reused, resumed atomic.Int64
}

// job is one manifest entry and how stage reconciles it.
type job struct {
	f    manifest.File
	act  action
	mode fs.FileMode
	live fs.FileMode // the live file's mode, for actLink and actCopy

	// resume lets an actDownload keep a prefix if cut off and continue a
	// kept one (see resumeSlots).
	resume bool
}

// stage assembles the complete fresh tree in s.tmp, a sibling of dest on the
// same filesystem. Every manifest entry is staged and verified; nothing under
// dest is ever modified. Entries already on disk, live or partial, are staged
// before any download, and the partial tree is removed between the two
// phases: from then on s.tmp holds everything it could supply, so a failure
// can keep s.tmp in its place without losing anything (see keepStaged; a
// partial tree that cannot be removed is kept instead). That includes the
// kept prefixes of cut-off downloads, which move into s.tmp's resume
// directory first; that directory is removed once staging succeeds.
func (s *syncer) stage(ctx context.Context, m *manifest.Manifest, live map[string]localFile,
	partial map[contentKey]leftover,
) (Stats, error) {
	resumable := resumeSlots(m)
	var local, remote []job
	for _, f := range m.Files {
		act, mode, err := classify(f, live, partial)
		if err != nil {
			return Stats{}, err
		}
		j := job{f: f, act: act, mode: mode, live: live[f.Path].mode}
		if act == actDownload {
			// Only S3 downloads resume; ReplaceFromDirectory copies locally.
			j.resume = s.client != nil && resumable(f)
			remote = append(remote, j)
		} else {
			local = append(local, j)
		}
	}
	// A staging tree that is already there may hold hardlinks to live files;
	// staging into it could write through them, so it is refused, never
	// reused. Recovery and keepStaged clear it before any attempt.
	if err := os.MkdirAll(filepath.Dir(s.tmp), 0o755); err != nil {
		return Stats{}, err
	}
	if err := os.Mkdir(s.tmp, 0o755); err != nil {
		return Stats{}, fmt.Errorf("pull: creating staging directory: %w", err)
	}
	var c counters
	if err := s.stageAll(ctx, local, partial, &c); err != nil {
		return Stats{}, err
	}
	s.adoptPrefixes(remote)
	// A partial tree that cannot be removed costs only what it would save: if
	// this attempt fails, keepStaged sees it still there and deletes s.tmp
	// rather than mixing the two, and a later pull removes it.
	if err := removeAllFn(s.partial); err != nil {
		s.logger.Warn("could not remove files kept from a failed pull; continuing", "path", s.partial, "error", err)
	}
	if err := s.stageAll(ctx, remote, partial, &c); err != nil {
		return Stats{}, err
	}
	// Every resumable download installed its file and dropped its sidecar,
	// so this removes at most an empty directory, which must go before
	// VerifyTree.
	if slices.ContainsFunc(remote, func(j job) bool { return j.resume }) {
		if err := os.RemoveAll(filepath.Join(s.tmp, resumeDir)); err != nil {
			return Stats{}, fmt.Errorf("pull: removing the staging tree's resume directory: %w", err)
		}
	}
	return Stats{
		Downloaded: int(c.downloaded.Load()),
		Linked:     int(c.linked.Load()),
		Copied:     int(c.copied.Load()),
		Reused:     int(c.reused.Load()),
		Resumed:    int(c.resumed.Load()),
	}, nil
}

// stageAll stages jobs on up to s.parallel workers.
func (s *syncer) stageAll(ctx context.Context, jobs []job, partial map[contentKey]leftover, c *counters) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.parallel)
	for _, j := range jobs {
		g.Go(func() error { return s.stageEntry(gctx, j, partial, c) })
	}
	return g.Wait()
}

// action is how one manifest entry is reconciled against the live tree.
type action int

const (
	actDownload action = iota // neither the live file nor the partial tree has the content
	actCopy                   // content matches but the mode does not (never on Windows)
	actLink                   // content and mode match; hardlink the live file
	actReuse                  // only the partial tree has the content; stage it from there
)

// classify decides, without touching the filesystem, how a manifest entry
// should be staged against the live tree and the partial tree's content
// index, and returns the parsed target mode. It is the single decision shared
// by real staging and the dry-run preview: when the live file is absent or
// its content or size differs, reuse a partial file with the same content or
// else download; copy on a mode-only change (modes are not meaningful on
// Windows, so skipped there), otherwise hardlink. The hardlink→copy runtime
// fallback is the stager's, not classify's — classify reports only the
// intended action.
func classify(f manifest.File, live map[string]localFile, partial map[contentKey]leftover) (action, fs.FileMode, error) {
	mode, err := f.FileMode()
	if err != nil {
		return 0, 0, err
	}
	lf, ok := live[f.Path]
	if !ok || lf.hash != f.SHA256 || lf.size != f.Size {
		if _, ok := partial[contentKey{f.SHA256, f.Size}]; ok {
			return actReuse, mode, nil
		}
		return actDownload, mode, nil
	}
	if goos != "windows" && lf.mode != mode {
		return actCopy, mode, nil
	}
	return actLink, mode, nil
}

// stageEntry stages one manifest entry: hardlink when content and mode match
// the live file or a partial file (content only on Windows), copy locally on
// a mode-only change (never hardlink-then-chmod, which would mutate an inode
// the live tree or another staged entry may share) or a read-only file on
// Windows, and download otherwise. Hardlink failures fall back to a copy.
func (s *syncer) stageEntry(ctx context.Context, j job, partial map[contentKey]leftover, c *counters) error {
	dst := filepath.Join(s.tmp, filepath.FromSlash(j.f.Path))
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	switch j.act {
	case actDownload:
		resumed, err := s.fetch(ctx, j, dst)
		if err != nil {
			return err
		}
		c.downloaded.Add(1)
		if resumed {
			c.resumed.Add(1)
		}
	case actReuse:
		lo := partial[contentKey{j.f.SHA256, j.f.Size}]
		src := filepath.Join(s.partial, filepath.FromSlash(lo.path))
		copyOnly := (goos != "windows" && lo.mode != j.mode) || readOnlyOnWindows(lo.mode)
		if _, err := s.linkOrCopy(src, "partial", j, dst, copyOnly); err != nil {
			return err
		}
		c.reused.Add(1)
	default:
		src := filepath.Join(s.dest, filepath.FromSlash(j.f.Path))
		linked, err := s.linkOrCopy(src, "live", j, dst, j.act == actCopy || readOnlyOnWindows(j.live))
		if err != nil {
			return err
		}
		if linked {
			c.linked.Add(1)
		} else {
			c.copied.Add(1)
		}
	}
	return nil
}

// readOnlyOnWindows reports a file that must be copied rather than linked
// because it carries the Windows read-only attribute (Go reports it as a
// missing owner-write bit). Every hardlink shares that attribute, and removing
// a read-only file can clear it first (os.RemoveAll's fallback where the
// filesystem cannot delete it as is), so removing the partial or old tree
// would clear it on the file in dest. Never sharing a read-only file between
// trees rules that out, at the cost of copying such files on every pull.
func readOnlyOnWindows(mode fs.FileMode) bool {
	return goos == "windows" && mode&0o200 == 0
}

// linkOrCopy stages j from src, a file in the tree named from, at dst: a
// hardlink unless copyOnly, falling back to a copy when the link fails. It
// reports whether it linked.
func (s *syncer) linkOrCopy(src, from string, j job, dst string, copyOnly bool) (bool, error) {
	if !copyOnly {
		err := linkFn(src, dst)
		if err == nil {
			return true, nil
		}
		s.logger.Debug("hardlink failed; copying instead", "path", j.f.Path, "error", err)
	}
	return false, copyFile(src, from, j.f, dst, j.mode)
}

// download fetches one object into staging and reports whether it resumed a
// kept prefix. A whole-object GET asks S3 to check its full-object checksum
// as well as writeVerified's SHA256. Objects of at least minResumeSize go
// through downloadResumable instead: a cut-off one is kept and continued by a
// ranged GET, which carries no full-object checksum, so the manifest's SHA256
// over the kept and the new bytes is then the only check (VerifyTree still
// re-reads the file before the swap).
func (s *syncer) download(ctx context.Context, j job, dst string) (bool, error) {
	if j.resume {
		return s.downloadResumable(ctx, j.f, dst, j.mode)
	}
	key := path.Join(s.prefix, j.f.Path)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(s.bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return false, fmt.Errorf("pull: fetching %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	if err := writeVerified(out.Body, j.f, dst, j.mode); err != nil {
		return false, fmt.Errorf("pull: downloading %s: %w", key, err)
	}
	return false, nil
}

// copyFromSource stages one entry from ReplaceFromDirectory's source tree.
// The source file must be a regular file; its bytes are verified like a
// download.
func (s *syncer) copyFromSource(_ context.Context, j job, dst string) (bool, error) {
	src := filepath.Join(s.source, filepath.FromSlash(j.f.Path))
	info, err := os.Lstat(src)
	if err != nil {
		return false, fmt.Errorf("pull: source %s: %w", j.f.Path, err)
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("pull: source %s: not a regular file", j.f.Path)
	}
	in, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer func() { _ = in.Close() }() // read-only; nothing to do about a close error
	if err := writeVerified(in, j.f, dst, j.mode); err != nil {
		return false, fmt.Errorf("pull: copying source %s: %w", j.f.Path, err)
	}
	return false, nil
}

// writeVerified streams r into the staging file dst while hashing, reading at
// most one byte past the declared size, and verifies length and SHA256
// against the manifest entry. A long body fails without being read to EOF.
// dst must not exist: an existing name in staging may be a hardlink to a
// live file, and opening it for writing would change dest.
func writeVerified(r io.Reader, f manifest.File, dst string, mode fs.FileMode) error {
	fh, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
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
	if err := checkStaged(f, n, h); err != nil {
		return err
	}
	return chmod(dst, mode)
}

// checkStaged compares n staged bytes hashed into h with f's size and SHA256.
func checkStaged(f manifest.File, n int64, h hash.Hash) error {
	switch {
	case n > f.Size:
		return &stagedMismatchError{Path: f.Path, Detail: fmt.Sprintf("body exceeds the manifest's %d bytes", f.Size)}
	case n < f.Size:
		return &stagedMismatchError{Path: f.Path, Detail: fmt.Sprintf("body is %d bytes, manifest says %d", n, f.Size)}
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return &stagedMismatchError{Path: f.Path, Detail: fmt.Sprintf("bytes hash %s, manifest says %s", got, f.SHA256)}
	}
	return nil
}

// copyFile copies src, a file in the live or partial tree (named by from),
// into the staging tree with writeVerified, so a file that changed since it
// was hashed is never read past the entry's declared size.
func copyFile(src, from string, f manifest.File, dst string, mode fs.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }() // read-only; nothing to do about a close error
	if err := writeVerified(in, f, dst, mode); err != nil {
		return fmt.Errorf("pull: copying %s %s: %w", from, f.Path, err)
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
