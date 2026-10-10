package pull

import "time"

// Shrink the retry backoffs for the whole test binary so mismatch-retry and
// exhaustion tests run in milliseconds.
func init() {
	mismatchBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	swapBackoff = time.Millisecond
}

// SetLinkFn swaps the hardlink implementation and returns a restore func —
// lets black-box tests force the copy fallback (L4).
func SetLinkFn(fn func(oldname, newname string) error) (restore func()) {
	orig := linkFn
	linkFn = fn
	return func() { linkFn = orig }
}

// SetRenameFn swaps the rename used by the swap and returns a restore func —
// lets black-box tests interrupt a replacement between its renames.
func SetRenameFn(fn func(oldpath, newpath string) error) (restore func()) {
	orig := renameFn
	renameFn = fn
	return func() { renameFn = orig }
}
