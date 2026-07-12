package prune

import "time"

// SetNow overrides the min-age clock for deterministic tests, restoring the
// real clock via t.Cleanup-style callers.
func SetNow(t time.Time) (restore func()) {
	orig := now
	now = func() time.Time { return t }
	return func() { now = orig }
}
