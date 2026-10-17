//go:build unix

package pull

import (
	"os"
	"syscall"
)

// soleName reports whether p, not followed if a link, is its file's only
// name.
func soleName(p string) (bool, error) {
	info, err := os.Lstat(p)
	if err != nil {
		return false, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && st.Nlink == 1, nil
}
