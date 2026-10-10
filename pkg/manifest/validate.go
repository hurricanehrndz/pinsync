package manifest

import (
	"errors"
	"fmt"
	"strings"
)

// Validate checks every entry against the schema's rules: a lowercase hex
// SHA256, a nonnegative size, permission-only mode bits, well-formed relative
// slash-separated paths, no two paths differing only in case (default APFS
// and NTFS are case-insensitive), and Windows compatibility (no reserved
// device names, no components ending in a dot or space). All violations are
// aggregated into a single error so callers report everything at once.
func (m *Manifest) Validate() error {
	var errs []error
	lower := make(map[string]string, len(m.Files))
	for _, f := range m.Files {
		if !isSHA256(f.SHA256) {
			errs = append(errs, fmt.Errorf("manifest: %s: sha256 %q is not 64 lowercase hex digits", f.Path, f.SHA256))
		}
		if f.Size < 0 {
			errs = append(errs, fmt.Errorf("manifest: %s: negative size %d", f.Path, f.Size))
		}
		if _, err := f.FileMode(); err != nil {
			errs = append(errs, err)
		}
		if err := validatePath(f.Path); err != nil {
			errs = append(errs, err)
			continue
		}
		key := strings.ToLower(f.Path)
		prev, seen := lower[key]
		switch {
		case !seen:
			lower[key] = f.Path
		case prev == f.Path:
			errs = append(errs, fmt.Errorf("manifest: duplicate path %s", f.Path))
		default:
			errs = append(errs, fmt.Errorf("manifest: %s and %s collide on case-insensitive filesystems", prev, f.Path))
		}
	}
	return errors.Join(errs...)
}

// isSHA256 reports whether s is a lowercase hex SHA256 digest.
func isSHA256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// validatePath enforces the path rules on a single manifest path.
func validatePath(p string) error {
	switch {
	case p == "":
		return errors.New("manifest: empty path")
	case strings.Contains(p, `\`):
		return fmt.Errorf("manifest: %s: backslashes are not allowed; paths are forward-slash separated", p)
	case strings.HasPrefix(p, "/"):
		return fmt.Errorf("manifest: %s: absolute paths are not allowed", p)
	}
	for _, seg := range strings.Split(p, "/") {
		if err := validateSegment(p, seg); err != nil {
			return err
		}
	}
	return nil
}

// validateSegment enforces the per-component rules.
func validateSegment(p, seg string) error {
	switch {
	case seg == "":
		return fmt.Errorf("manifest: %s: empty path segment", p)
	case seg == "." || seg == "..":
		return fmt.Errorf("manifest: %s: %q segments are not allowed", p, seg)
	case strings.HasSuffix(seg, "."), strings.HasSuffix(seg, " "):
		return fmt.Errorf("manifest: %s: component %q ends in a dot or space, which Windows cannot store", p, seg)
	}
	if base, reserved := reservedName(seg); reserved {
		return fmt.Errorf("manifest: %s: component %q uses the reserved Windows device name %s", p, seg, base)
	}
	return nil
}

// reservedName reports whether a path component is a Windows reserved device
// name, bare or with any extension (e.g. "NUL", "nul.txt", "COM1.tar.gz").
func reservedName(seg string) (string, bool) {
	base := seg
	if i := strings.IndexByte(seg, '.'); i >= 0 {
		base = seg[:i]
	}
	base = strings.ToUpper(base)
	switch base {
	case "CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
		return base, true
	}
	return "", false
}
