package rolesanywhere

import (
	"crypto"
	"crypto/x509"
)

// Identity is the subset of github.com/tailscale/certstore's Identity that
// pinsync uses. certstore.Identity satisfies it implicitly, so pure files in
// this package (and their tests) never need to import certstore — which
// matters because certstore refuses to compile on Linux. We omit Delete()
// deliberately: pinsync only reads the store, it never mutates it.
type Identity interface {
	Certificate() (*x509.Certificate, error)
	CertificateChain() ([]*x509.Certificate, error)
	Signer() (crypto.Signer, error)
	Close()
}
