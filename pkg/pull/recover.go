package pull

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
)

// Recover finishes or rolls back a swap interrupted by a crash, using only
// dest and its .pinsync-tmp and .pinsync-old siblings — no S3 client and no
// source. Afterwards dest is either the tree from before the interrupted
// replacement or the fully swapped-in new one (a crash after both renames),
// and no sibling remains. It never promotes the unverified staging tree.
// Pull, PullWithManifest, and ReplaceFromDirectory run it first themselves.
func Recover(ctx context.Context, dest string, logger *slog.Logger) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	dest = filepath.Clean(dest)
	return recoverInterrupted(dest, dest+tmpSuffix, dest+oldSuffix, logger)
}

// recoverInterrupted applies the crash-recovery rules before any sync work.
// With a live dest, stale staging/old trees from an interrupted pull are
// deleted (v1 discards partial staging rather than resuming). With dest
// missing but the old tree present — a crash between the swap's two renames —
// the old tree is renamed back to dest. Rolling back and re-syncing converges
// to the same end state as rolling forward, without ever trusting an
// unverified leftover staging tree.
func recoverInterrupted(dest, tmp, old string, logger *slog.Logger) error {
	if exists(dest) {
		for _, stale := range []string{tmp, old} {
			if !exists(stale) {
				continue
			}
			logger.Warn("removing stale directory from an interrupted pull", "path", stale)
			if err := os.RemoveAll(stale); err != nil {
				return fmt.Errorf("pull: removing stale %s: %w", stale, err)
			}
		}
		return nil
	}
	if exists(old) {
		logger.Warn("restoring previous tree after an interrupted swap", "from", old, "to", dest)
		if err := os.Rename(old, dest); err != nil {
			return fmt.Errorf("pull: restoring %s from %s: %w", dest, old, err)
		}
	}
	if exists(tmp) {
		if err := os.RemoveAll(tmp); err != nil {
			return fmt.Errorf("pull: removing stale %s: %w", tmp, err)
		}
	}
	return nil
}

// exists reports whether the path itself exists (without following links).
func exists(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}
