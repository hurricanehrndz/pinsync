// Package manifest defines the pinsync manifest schema (version 1): a
// deterministic JSON listing of every regular file under a sync root, each
// with its SHA256, size, and POSIX permission bits. The manifest is the only
// source of truth for push and pull decisions.
package manifest

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strconv"
)

// Name is the fixed manifest object name. It is reserved: a sync root must
// not contain a top-level file with this name.
const Name = "manifest.json"

// Version is the schema version this package reads and writes.
const Version = 1

// File describes one regular file in the tree.
type File struct {
	Path   string `json:"path"`   // relative, forward-slash separated
	SHA256 string `json:"sha256"` // lowercase hex of the full content
	Size   int64  `json:"size"`   // bytes
	Mode   string `json:"mode"`   // octal permission bits, e.g. "0644"
}

// FileMode parses the entry's octal mode string into permission bits.
func (f File) FileMode() (fs.FileMode, error) {
	n, err := strconv.ParseUint(f.Mode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("manifest: %s: invalid mode %q: %w", f.Path, f.Mode, err)
	}
	if n&^uint64(fs.ModePerm) != 0 {
		return 0, fmt.Errorf("manifest: %s: mode %q carries more than permission bits", f.Path, f.Mode)
	}
	return fs.FileMode(n), nil
}

// Manifest is a schema v1 manifest.
type Manifest struct {
	Version int    `json:"version"`
	Files   []File `json:"files"` // sorted by Path
}

// Encode writes m as indented JSON with a trailing newline, sorting Files by
// Path first so identical trees always produce identical bytes.
func (m *Manifest) Encode(w io.Writer) error {
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(m)
}

// Decode reads a manifest, rejects any version other than Version, and
// validates it.
func Decode(r io.Reader) (*Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("manifest: decode: %w", err)
	}
	if m.Version != Version {
		return nil, fmt.Errorf("manifest: unsupported version %d (this build reads version %d)", m.Version, Version)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &m, nil
}
