//go:build darwin || windows

package rolesanywhere

import (
	"crypto/x509"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/tailscale/certstore"
)

// FindIdentity opens the certificate store at loc and returns the first
// identity whose selected CN (Subject or Issuer, per field) matches re, along
// with its parsed leaf certificate. The caller owns the returned Identity and
// must Close() it; every other identity is closed here.
//
// The store handle is closed before returning: certstore's identities own
// duplicated handles (winIdentity duplicates each CertContext; macIdentity
// retains its own CoreFoundation refs and macStore.Close is a no-op), so the
// returned Identity stays usable after Store.Close.
func FindIdentity(logger *slog.Logger, field CertField, re *regexp.Regexp, loc StoreLoc) (Identity, *x509.Certificate, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	store, err := openStore(loc)
	if err != nil {
		return nil, nil, err
	}
	defer store.Close()

	idents, err := store.Identities()
	if err != nil {
		return nil, nil, fmt.Errorf("listing certificate identities: %w", err)
	}

	logger.Debug("rolesanywhere: scanning certificate store", "store", loc.String(), "field", field.String(), "pattern", re.String())

	for i, ident := range idents {
		cert, err := ident.Certificate()
		if err != nil {
			// An identity whose leaf will not parse cannot be selected; skip
			// it rather than failing the whole scan.
			logger.Debug("rolesanywhere: skipping identity; certificate did not parse", "error", err)
			ident.Close()
			continue
		}
		if matchCert(cert, field, re) {
			// Hand this identity to the caller; close the ones we rejected.
			closeIdentities(idents[i+1:])
			logger.Debug("rolesanywhere: selected certificate", "subject", cert.Subject.CommonName, "issuer", cert.Issuer.CommonName, "serial", cert.SerialNumber, "field", field.String(), "store", loc.String())
			return ident, cert, nil
		}
		ident.Close()
	}

	return nil, nil, fmt.Errorf("no certificate with %s CN matching %q (scanned %d identities)", field, re, len(idents))
}

// openStore opens the store for loc, wrapping errors so a machine-store
// permission denial reads as such instead of resurfacing later as "no cert
// found".
func openStore(loc StoreLoc) (certstore.Store, error) {
	if loc == StoreMachine {
		store, err := certstore.Open(certstore.System)
		if err != nil {
			return nil, fmt.Errorf("opening machine certificate store: %w (machine store may require admin/SYSTEM)", err)
		}
		return store, nil
	}
	store, err := certstore.Open(certstore.User)
	if err != nil {
		return nil, fmt.Errorf("opening user certificate store: %w", err)
	}
	return store, nil
}

// closeIdentities releases every identity in the slice.
func closeIdentities(idents []certstore.Identity) {
	for _, ident := range idents {
		ident.Close()
	}
}
