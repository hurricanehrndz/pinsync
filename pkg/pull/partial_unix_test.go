//go:build unix

package pull_test

import (
	"os"
	"syscall"
	"testing"
)

// linkCount is p's number of names.
func linkCount(t *testing.T, p string) uint64 {
	t.Helper()
	info, err := os.Lstat(p)
	if err != nil {
		t.Fatal(err)
	}
	return uint64(info.Sys().(*syscall.Stat_t).Nlink) //nolint:unconvert // uint16 on darwin
}
