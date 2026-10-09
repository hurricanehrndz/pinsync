package manifest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
)

// VerifyTree checks that root holds exactly m's file set: every entry present
// as a regular file with the manifest's size, SHA256 and (except on Windows,
// where modes are not meaningful) permission bits, and no other file.
// Symlinks and other non-regular entries fail and are never followed;
// directories are accepted wherever they appear. root must exist and be a
// directory; an empty directory matches an empty manifest. m itself must be a
// valid schema-version manifest. The first problem found is returned.
func VerifyTree(ctx context.Context, root string, m *Manifest) error {
	if m.Version != Version {
		return fmt.Errorf("manifest: unsupported version %d (this build reads version %d)", m.Version, Version)
	}
	if err := m.Validate(); err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("manifest: verify: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("manifest: verify: %s is not a directory (%s)", root, typeName(info.Mode().Type()))
	}

	want := make(map[string]File, len(m.Files))
	for _, f := range m.Files {
		want[f.Path] = f
	}
	seen := make(map[string]bool, len(m.Files))
	walk := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if d.IsDir() {
			return nil
		}
		return verifyEntry(root, p, d, want, seen)
	}
	if err := filepath.WalkDir(root, walk); err != nil {
		return err
	}
	if len(seen) == len(want) {
		return nil
	}
	var missing []string
	for p := range want {
		if !seen[p] {
			missing = append(missing, p)
		}
	}
	slices.Sort(missing)
	return fmt.Errorf("manifest: verify: %d manifest entries missing, first %s", len(missing), missing[0])
}

// verifyEntry checks one walked non-directory entry against want and marks
// it seen.
func verifyEntry(root, p string, d fs.DirEntry, want map[string]File, seen map[string]bool) error {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return err
	}
	relPath := filepath.ToSlash(rel)
	if !d.Type().IsRegular() {
		return fmt.Errorf("manifest: verify: %s: not a regular file (%s)", relPath, typeName(d.Type()))
	}
	f, ok := want[relPath]
	if !ok {
		return fmt.Errorf("manifest: verify: %s: not in the manifest", relPath)
	}
	seen[relPath] = true
	return verifyFile(p, f)
}

// lstatFn is a test seam for swapping an entry between Lstat and open.
var lstatFn = os.Lstat

// verifyFile checks one regular file against its entry. It opens the file
// without following a final symlink or blocking on a FIFO (openFlags),
// requires the handle to be a regular file, and compares it to the walked
// path, so a file swapped mid-walk is caught rather than followed. It reads
// at most one byte past the declared size.
func verifyFile(p string, f File) error {
	before, err := lstatFn(p)
	if err != nil {
		return err
	}
	fh, err := os.OpenFile(p, os.O_RDONLY|openFlags, 0)
	if err != nil {
		return fmt.Errorf("manifest: verify: %s: open without following symlinks: %w", f.Path, err)
	}
	defer func() { _ = fh.Close() }() // read-only; nothing to do about a close error
	info, err := fh.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("manifest: verify: %s: not a regular file (%s)", f.Path, typeName(info.Mode().Type()))
	}
	if !os.SameFile(before, info) {
		return fmt.Errorf("manifest: verify: %s changed while being verified", f.Path)
	}
	if info.Size() != f.Size {
		return fmt.Errorf("manifest: verify: %s: size %d, manifest says %d", f.Path, info.Size(), f.Size)
	}
	if runtime.GOOS != "windows" {
		mode, _ := f.FileMode() // Validate already rejected bad modes
		if got := info.Mode().Perm(); got != mode {
			return fmt.Errorf("manifest: verify: %s: mode %#o, manifest says %#o", f.Path, got, mode)
		}
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(fh, f.Size+1))
	if err != nil {
		return fmt.Errorf("manifest: verify: hashing %s: %w", f.Path, err)
	}
	if n != f.Size {
		return fmt.Errorf("manifest: verify: %s: read %d bytes, manifest says %d", f.Path, n, f.Size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return fmt.Errorf("manifest: verify: %s: hash %s, manifest says %s", f.Path, got, f.SHA256)
	}
	return nil
}
