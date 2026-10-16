package pull

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// localFile is what a local tree knows about one regular file.
type localFile struct {
	hash string // lowercase hex SHA256 of the content
	size int64
	mode fs.FileMode
}

// hashTree fully re-hashes a local tree (the live tree or the partial one) —
// no hash cache, no mtime fast-path; content integrity can only be
// established by reading the bytes. A missing root yields an empty map
// (first pull). Non-regular entries are skipped, not errors: they cannot
// serve as link/copy sources and simply vanish at swap.
func hashTree(root string) (map[string]localFile, error) {
	files := map[string]localFile{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == root && os.IsNotExist(err) {
				return fs.SkipAll
			}
			return err
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		hash, err := hashContent(p)
		if err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = localFile{hash: hash, size: info.Size(), mode: info.Mode().Perm()}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}

// contentKey identifies a file's bytes regardless of its path.
type contentKey struct {
	hash string
	size int64
}

// leftover is one partial-tree file that can stand in for a download.
type leftover struct {
	path string // slash-separated, relative to the partial root
	mode fs.FileMode
}

// byContent indexes a hashed tree by content, so a leftover matches any
// manifest entry with the same bytes wherever either sits. Among duplicates
// the lexically first path wins, keeping staging deterministic.
func byContent(files map[string]localFile) map[contentKey]leftover {
	index := make(map[contentKey]leftover, len(files))
	for p, lf := range files {
		k := contentKey{lf.hash, lf.size}
		if have, ok := index[k]; !ok || p < have.path {
			index[k] = leftover{path: p, mode: lf.mode}
		}
	}
	return index
}

// hashContent streams one file through SHA256.
func hashContent(path string) (string, error) {
	fh, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = fh.Close() }() // read-only; nothing to do about a close error
	h := sha256.New()
	if _, err := io.Copy(h, fh); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
