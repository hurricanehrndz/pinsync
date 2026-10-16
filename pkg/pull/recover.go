package pull

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// Recover finishes or rolls back a swap interrupted by a crash, using only
// dest and its siblings (see Siblings) — no S3 client and no source.
// Afterwards dest is either the tree from before the interrupted replacement
// or the fully swapped-in new one (a crash after both renames), and only the
// partial tree may remain beside it: a leftover staging tree is kept there
// for the next pull to reuse by content. It never promotes the unverified
// staging tree. Pull, PullWithManifest, and ReplaceFromDirectory run it first
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
// and reused by content.
func recoverInterrupted(dest, tmp, old, partial string, logger *slog.Logger) error {
	switch {
	case !exists(old): // nothing to roll back or clean up
	case exists(dest):
		logger.Warn("removing stale directory from an interrupted pull", "path", old)
		if err := os.RemoveAll(old); err != nil {
			return fmt.Errorf("pull: removing stale %s: %w", old, err)
		}
	default:
		logger.Warn("restoring previous tree after an interrupted swap", "from", old, "to", dest)
		if err := os.Rename(old, dest); err != nil {
			return fmt.Errorf("pull: restoring %s from %s: %w", dest, old, err)
		}
	}
	return keepStaged(tmp, partial, logger)
}

// keepStaged keeps a staging tree that never reached its swap as the partial
// tree, so a later pull can reuse its files instead of downloading them
// again. If the partial tree still exists, staging either never got past
// reusing it (stage removes it before the first download) or could not
// remove it; tmp is deleted instead, never merged into it, costing at most
// the downloads since. A tmp that cannot be renamed is deleted too; the error
// is returned only when it can be neither kept nor deleted.
func keepStaged(tmp, partial string, logger *slog.Logger) error {
	if !exists(tmp) {
		return nil
	}
	if !exists(partial) {
		err := renameFn(tmp, partial)
		if err == nil {
			logger.Info("kept staged files for the next pull to reuse", "path", partial)
			return nil
		}
		logger.Warn("could not keep staged files for the next pull; discarding them",
			"from", tmp, "to", partial, "error", err)
	}
	if err := removeAllFn(tmp); err != nil {
		return fmt.Errorf("pull: removing stale %s: %w", tmp, err)
	}
	return nil
}

// exists reports whether the path itself exists (without following links).
func exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}
