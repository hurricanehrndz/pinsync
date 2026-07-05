//go:build !(darwin || windows)

package rolesanywhere

import (
	"crypto/x509"
	"errors"
	"log/slog"
	"regexp"
)

// FindIdentity is unavailable off macOS and Windows: the underlying
// certstore library only binds those platforms' certificate stores.
func FindIdentity(_ *slog.Logger, _ CertField, _ *regexp.Regexp, _ StoreLoc) (Identity, *x509.Certificate, error) {
	return nil, nil, errors.New("IAM Roles Anywhere support requires macOS or Windows")
}
