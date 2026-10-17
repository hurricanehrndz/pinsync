//go:build !unix && !windows

package pull

import "errors"

// soleName cannot count links here, so keepStaged drops every staged file
// rather than risk keeping one the live tree shares.
func soleName(string) (bool, error) {
	return false, errors.ErrUnsupported
}
