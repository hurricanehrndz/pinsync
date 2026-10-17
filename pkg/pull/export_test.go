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

// SetRemoveAllFn swaps the RemoveAll used on the staging, partial and old trees
// and returns a restore func — lets black-box tests make a sibling tree
// undeletable, as an open handle does on Windows.
func SetRemoveAllFn(fn func(path string) error) (restore func()) {
	orig := removeAllFn
	removeAllFn = fn
	return func() { removeAllFn = orig }
}

// SetRemoveFn swaps the Remove used on kept prefixes and on staged files the
// live tree shares, and returns a restore func — lets black-box tests make
// such a file undeletable.
func SetRemoveFn(fn func(name string) error) (restore func()) {
	orig := removeFn
	removeFn = fn
	return func() { removeFn = orig }
}

// SetGOOS makes pull behave as if it ran on goos and returns a restore func.
func SetGOOS(name string) (restore func()) {
	orig := goos
	goos = name
	return func() { goos = orig }
}
