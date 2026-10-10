//go:build unix

package manifest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// TestVerifyTreeRejectsSymlinkAfterLstat swaps a listed file for a symlink to
// the same inode between verifyFile's Lstat and its open. The identity check
// alone would pass; only a no-follow open catches it.
func TestVerifyTreeRejectsSymlinkAfterLstat(t *testing.T) {
	root, m := verifyFixture(t)
	p := filepath.Join(root, "a.txt")
	moved := filepath.Join(t.TempDir(), "a.txt")
	restore := manifest.SetLstatFn(func(name string) (os.FileInfo, error) {
		info, err := os.Lstat(name)
		if name == p && err == nil {
			if renameErr := os.Rename(p, moved); renameErr != nil {
				t.Fatal(renameErr)
			}
			if linkErr := os.Symlink(moved, p); linkErr != nil {
				t.Fatal(linkErr)
			}
		}
		return info, err
	})
	defer restore()
	verifyErr(t, root, m, "symlink")
}
