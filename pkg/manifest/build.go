package manifest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Build walks root and returns a validated manifest of its regular files.
// Any non-regular entry (symlink, fifo, socket, device) is an error, as is a
// top-level file named Name: that key is reserved for the manifest itself.
func Build(root string) (*Manifest, error) {
	m := &Manifest{Version: Version}
	walk := func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		relPath := filepath.ToSlash(rel)
		if !d.Type().IsRegular() {
			return fmt.Errorf("manifest: %s: not a regular file (%s)", relPath, typeName(d.Type()))
		}
		if relPath == Name {
			return fmt.Errorf("manifest: root contains a top-level %s; that name is reserved for the manifest itself", Name)
		}
		f, err := hashFile(p, relPath)
		if err != nil {
			return err
		}
		m.Files = append(m.Files, f)
		return nil
	}
	if err := filepath.WalkDir(root, walk); err != nil {
		return nil, err
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// hashFile streams path through SHA256 and records its size and permission
// bits under the manifest path rel.
func hashFile(path, rel string) (File, error) {
	fh, err := os.Open(path)
	if err != nil {
		return File{}, err
	}
	defer func() { _ = fh.Close() }() // read-only; nothing to do about a close error
	info, err := fh.Stat()
	if err != nil {
		return File{}, err
	}
	h := sha256.New()
	size, err := io.Copy(h, fh)
	if err != nil {
		return File{}, fmt.Errorf("manifest: hashing %s: %w", rel, err)
	}
	return File{
		Path:   rel,
		SHA256: hex.EncodeToString(h.Sum(nil)),
		Size:   size,
		Mode:   fmt.Sprintf("%#o", info.Mode().Perm()),
	}, nil
}

// typeName names a non-regular file type for error messages.
func typeName(m fs.FileMode) string {
	switch {
	case m&fs.ModeSymlink != 0:
		return "symlink"
	case m&fs.ModeNamedPipe != 0:
		return "fifo"
	case m&fs.ModeSocket != 0:
		return "socket"
	case m&fs.ModeDevice != 0:
		return "device"
	default:
		return m.String()
	}
}
