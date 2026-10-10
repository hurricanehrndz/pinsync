package pull

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// ReplaceFromDirectory converges dest to exactly the tree manifestBytes
// describes, copying changed entries from the local source tree instead of
// S3. Staging, verification, BeforeReplace, swap, and crash recovery are the
// same as PullWithManifest; files in source that the manifest does not list
// are ignored. source is only read: it is never renamed or deleted, its
// files are copied rather than linked into dest, and it may not overlap dest
// or its staging siblings.
// A mismatching source file fails at once — there is no retry. manifestBytes
// is copied on entry; the returned Result holds that copy.
func ReplaceFromDirectory(ctx context.Context, source, dest string, manifestBytes []byte, opts Options) (Result, error) {
	s, err := newSyncer(dest, opts)
	if err != nil {
		return Result{}, err
	}
	if int64(len(manifestBytes)) > s.maxManifest {
		return Result{}, fmt.Errorf("pull: manifest exceeds the %d-byte manifest limit", s.maxManifest)
	}
	raw := bytes.Clone(manifestBytes)
	m, err := manifest.Decode(bytes.NewReader(raw))
	if err != nil {
		return Result{}, err
	}
	s.source = filepath.Clean(source)
	info, err := os.Stat(s.source)
	if err != nil {
		return Result{}, fmt.Errorf("pull: source: %w", err)
	}
	if !info.IsDir() {
		return Result{}, fmt.Errorf("pull: source %s is not a directory", s.source)
	}
	// Checked before recovery, which may delete the staging siblings.
	for _, p := range []string{s.dest, s.tmp, s.old} {
		if within(s.source, p) || within(p, s.source) {
			return Result{}, fmt.Errorf("pull: source %s overlaps %s", s.source, p)
		}
	}
	s.fetch = s.copyFromSource

	if err = recoverInterrupted(s.dest, s.tmp, s.old, s.logger); err != nil {
		return Result{}, err
	}
	res, _, err := s.replace(ctx, raw, m)
	return res, err
}

// within reports whether p is, or lies inside, the existing directory outer.
// It compares file identity along the ancestors of both p's lexical absolute
// path and its symlink-resolved form, so a path that reaches outer only
// through a symlink, a path lexically inside outer that a symlink leads
// elsewhere, and case-insensitive aliases are all caught.
func within(p, outer string) bool {
	oi, err := os.Stat(outer)
	if err != nil {
		return false
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		abs = p
	}
	for _, q := range []string{abs, resolve(p)} {
		for ; ; q = filepath.Dir(q) {
			if qi, err := os.Stat(q); err == nil && os.SameFile(oi, qi) {
				return true
			}
			if filepath.Dir(q) == q {
				break
			}
		}
	}
	return false
}

// resolve makes p absolute and evaluates symlinks in its longest existing
// prefix; the missing remainder is appended unchanged.
func resolve(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	rest := ""
	for q := abs; ; q = filepath.Dir(q) {
		if r, err := filepath.EvalSymlinks(q); err == nil {
			return filepath.Join(r, rest)
		}
		if filepath.Dir(q) == q {
			return abs
		}
		rest = filepath.Join(filepath.Base(q), rest)
	}
}
