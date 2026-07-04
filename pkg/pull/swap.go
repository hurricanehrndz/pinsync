package pull

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// swapBackoff is the initial pause between rename retries; it doubles per
// attempt. A test seam.
var swapBackoff = 100 * time.Millisecond

// swap atomically replaces dest with the fully verified staging tree:
// rename dest aside, rename staging in, delete the old tree. Each rename is
// retried with bounded backoff to ride out Windows sharing violations from
// consumers holding open handles; Go surfaces those as wrapped syscall
// errors not worth enumerating in v1, so any rename error is retried.
func (s *syncer) swap() error {
	destExisted := exists(s.dest)
	if destExisted {
		if err := s.renameRetry(s.dest, s.old); err != nil {
			return err
		}
	}
	if err := s.renameRetry(s.tmp, s.dest); err != nil {
		if destExisted {
			if rbErr := os.Rename(s.old, s.dest); rbErr != nil {
				return errors.Join(err, fmt.Errorf(
					"pull: rolling %s back also failed (the next pull will recover it): %w", s.dest, rbErr,
				))
			}
		}
		return err
	}
	if destExisted {
		if err := os.RemoveAll(s.old); err != nil {
			// The swap itself succeeded; the next pull's crash recovery
			// removes the leftover old tree.
			s.logger.Warn("could not remove the previous tree; the next pull will", "path", s.old, "error", err)
		}
	}
	syncDir(filepath.Dir(s.dest))
	return nil
}

// renameRetry renames with bounded, doubling backoff.
func (s *syncer) renameRetry(from, to string) error {
	backoff := swapBackoff
	var err error
	for attempt := 1; attempt <= s.swapAttempts; attempt++ {
		if err = os.Rename(from, to); err == nil {
			return nil
		}
		if attempt < s.swapAttempts {
			s.logger.Warn("rename failed; retrying", "from", from, "to", to, "attempt", attempt, "error", err)
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return fmt.Errorf(
		"pull: rename %s -> %s failed after %d attempts — most likely a consumer is holding files open under the destination: %w",
		from, to, s.swapAttempts, err,
	)
}

// syncDir best-effort fsyncs a directory so the renames are durable.
func syncDir(dir string) {
	fh, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = fh.Sync()
	_ = fh.Close()
}
