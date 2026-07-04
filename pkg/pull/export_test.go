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
