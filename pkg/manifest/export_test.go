package manifest

import "os"

// SetLstatFn swaps verifyFile's Lstat and returns a restore func — lets
// black-box tests replace an entry between its Lstat and open.
func SetLstatFn(fn func(name string) (os.FileInfo, error)) (restore func()) {
	orig := lstatFn
	lstatFn = fn
	return func() { lstatFn = orig }
}
