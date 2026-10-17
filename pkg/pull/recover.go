package pull

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
)

// Recover finishes or rolls back a swap interrupted by a crash, using only
// dest and its siblings (see Siblings) — no S3 client and no source.
// Afterwards dest is either the tree from before the interrupted replacement
// or the fully swapped-in new one (a crash after both renames), and only the
// partial tree may remain beside it: a leftover staging tree is kept there
// for the next pull to reuse by content, with any download it cut off
// partway for the next pull to resume (see resumeDir). Nothing kept there is
// a live file under a second name (see unsharePartial). It never promotes the
// unverified staging tree. Pull, PullWithManifest, and ReplaceFromDirectory run it first
// themselves.
func Recover(ctx context.Context, dest string, logger *slog.Logger) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	dest = filepath.Clean(dest)
	return recoverInterrupted(dest, dest+tmpSuffix, dest+oldSuffix, dest+partialSuffix, logger)
}

// recoverInterrupted applies the crash-recovery rules before any sync work.
// With a live dest, a stale old tree from an interrupted swap is deleted.
// With dest missing but the old tree present — a crash between the swap's two
// renames — the old tree is renamed back to dest. Rolling back and re-syncing
// converges to the same end state as rolling forward, without ever trusting
// an unverified leftover staging tree. Either way a leftover staging tree is
// kept as the partial tree (see keepStaged): never promoted, only re-hashed
// and reused by content, its cut-off downloads continued only when the
// object still has the ETag they were fetched with and the result hashes
// right. Then the partial tree, whether or not it came from tmp, is cleared
// of second names of live files (see unsharePartial). That runs after tmp is
// gone: a leftover the failed pull linked into tmp has one name again and is
// kept, where clearing first would drop it and then lose tmp's name with tmp.
// A stale old tree that cannot be removed, or restored, does not stop either
// step: the error is returned with theirs, first. A stuck old tree would
// otherwise keep live files' second names in the partial tree, and with
// dest missing there are no live files, so the cleanup is harmless.
func recoverInterrupted(dest, tmp, old, partial string, logger *slog.Logger) error {
	var oldErr error
	switch {
	case !exists(old): // nothing to roll back or clean up
	case exists(dest):
		logger.Warn("removing stale directory from an interrupted pull", "path", old)
		if err := removeAllFn(old); err != nil {
			oldErr = fmt.Errorf("pull: removing stale %s: %w", old, err)
		}
	default:
		logger.Warn("restoring previous tree after an interrupted swap", "from", old, "to", dest)
		if err := os.Rename(old, dest); err != nil {
			oldErr = fmt.Errorf("pull: restoring %s from %s: %w", dest, old, err)
		}
	}
	err := keepStaged(tmp, partial, logger)
	return errors.Join(oldErr, err, unsharePartial(partial, logger))
}

// unsharePartial drops every name in the partial tree whose file has another
// name (see dropShared), so none is a live file: a caller may reject a live
// file with a second link, as osconfig-sync does on Windows. v0.5.0 and
// v0.5.1 kept the live files a failed pull linked, and a successful pull
// whose partial tree was stuck kept the leftovers it linked into the new
// tree. If one cannot be dropped, the partial tree is deleted. If it cannot
// be deleted either, the error is returned: a pull would only report
// success for a tree some callers reject, and stage would fail on it anyway.
func unsharePartial(partial string, logger *slog.Logger) error {
	if !exists(partial) {
		return nil
	}
	err := dropShared(partial)
	if err == nil {
		return nil
	}
	logger.Warn("could not drop kept files the live tree may share; discarding them",
		"path", partial, "error", err)
	if err := removeAllFn(partial); err != nil {
		return fmt.Errorf("pull: removing %s, which may share live files: %w", partial, err)
	}
	return nil
}

// keepStaged keeps a staging tree that never reached its swap as the partial
// tree, so a later pull can reuse its files instead of downloading them
// again and resume the downloads in its resume directory. A kill can leave
// that directory mid-write; downloadResumable only trusts what it re-hashes.
// If the partial tree still exists, staging either never got past
// reusing it (stage removes it before the first download) or could not
// remove it; tmp is deleted instead, costing at most the downloads since.
// A tmp that cannot be renamed is deleted too; the error is returned only
// when it can be neither kept nor deleted. Before tmp is deleted, its kept
// prefixes move into the partial tree (see stashPrefixes): they include the
// ones stage adopted from it, so while that tree is stuck a slow link would
// otherwise lose every run's progress on a large object.
//
// First, every staged file with a second name is dropped (see dropShared),
// so nothing kept is also a live file: a caller may reject a live file with
// a second link, as osconfig-sync does on Windows. If one cannot be dropped,
// tmp is deleted without keeping anything from it.
func keepStaged(tmp, partial string, logger *slog.Logger) error {
	if !exists(tmp) {
		return nil
	}
	shared := dropShared(tmp)
	if shared != nil {
		logger.Warn("could not drop staged files the live tree may share; discarding them",
			"path", tmp, "error", shared)
	} else if !exists(partial) {
		err := renameFn(tmp, partial)
		if err == nil {
			logger.Info("kept staged files for the next pull to reuse", "path", partial)
			return nil
		}
		logger.Warn("could not keep staged files for the next pull; discarding them",
			"from", tmp, "to", partial, "error", err)
	}
	if shared == nil {
		stashPrefixes(tmp, partial, logger)
	}
	if err := removeAllFn(tmp); err != nil {
		return fmt.Errorf("pull: removing stale %s: %w", tmp, err)
	}
	return nil
}

// dropShared removes every name under root, a staging or partial tree,
// whose file has another name, wherever that is. Staging links a live file at its own path (actLink),
// but a leftover it links (actReuse) or a prefix it adopts can share one at
// another path: a leftover is only copied when its content hashed the same
// as a live file's, and prefixes are not checked at all. Counting links
// catches every case and needs nothing from the failed attempt, which
// recovery after a kill does not have. A linked live file costs nothing to
// link again; downloads, copies and prefixes have one name and stay.
// Removing a name leaves the file as it is: os.Remove clears the read-only
// attribute only of a file it cannot otherwise delete, and a read-only file
// is never linked (see readOnlyOnWindows). It returns the first file it
// could neither show to have one name nor remove.
func dropShared(root string) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		sole, err := soleName(p)
		if err == nil && sole {
			return nil
		}
		return removeFn(p)
	})
}

// stashPrefixes moves the files in tmp's resume directory into partial's,
// best effort, never replacing one already there. It keeps no state, so
// recovery after a kill mid-move just runs it again. A sidecar and data file
// that end up from different attempts are safe: nothing is trusted before
// the whole object hashes right (see downloadResumable).
func stashPrefixes(tmp, partial string, logger *slog.Logger) {
	from, to := filepath.Join(tmp, resumeDir), filepath.Join(partial, resumeDir)
	entries, err := os.ReadDir(from)
	if err != nil || len(entries) == 0 {
		return
	}
	if err := os.MkdirAll(to, 0o755); err != nil {
		logger.Debug("could not keep partial downloads", "path", to, "error", err)
		return
	}
	for _, e := range entries {
		dst := filepath.Join(to, e.Name())
		if exists(dst) {
			continue
		}
		if err := renameFn(filepath.Join(from, e.Name()), dst); err != nil {
			logger.Debug("could not keep a partial download", "path", dst, "error", err)
		}
	}
}

// exists reports whether the path itself exists (without following links).
func exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}
