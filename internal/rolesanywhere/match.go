package rolesanywhere

import (
	"crypto/x509"
	"fmt"
	"regexp"
)

// CertField selects which certificate name is matched against the selection
// regex: the leaf's Subject CN or its Issuer CN.
type CertField int

const (
	// FieldSubject matches against the certificate's Subject CommonName.
	FieldSubject CertField = iota
	// FieldIssuer matches against the certificate's Issuer CommonName.
	FieldIssuer
)

// String returns the wire name of the field ("subject" or "issuer"), matching
// the value ParseCertField accepts. It is used in selection error messages.
func (f CertField) String() string {
	switch f {
	case FieldSubject:
		return "subject"
	case FieldIssuer:
		return "issuer"
	default:
		return fmt.Sprintf("CertField(%d)", int(f))
	}
}

// ParseCertField parses the config value naming which CN to match. Only
// "subject" and "issuer" are accepted.
func ParseCertField(s string) (CertField, error) {
	switch s {
	case "subject":
		return FieldSubject, nil
	case "issuer":
		return FieldIssuer, nil
	default:
		return 0, fmt.Errorf("invalid certificate field %q: want \"subject\" or \"issuer\"", s)
	}
}

// StoreLoc selects which system certificate store to scan.
type StoreLoc int

const (
	// StoreUser is the per-user certificate store.
	StoreUser StoreLoc = iota
	// StoreMachine is the machine/system certificate store.
	StoreMachine
)

// ParseStoreLoc parses the config value naming which store to scan. Only
// "user" and "machine" are accepted.
func ParseStoreLoc(s string) (StoreLoc, error) {
	switch s {
	case "user":
		return StoreUser, nil
	case "machine":
		return StoreMachine, nil
	default:
		return 0, fmt.Errorf("invalid certificate store %q: want \"user\" or \"machine\"", s)
	}
}

// matchCert reports whether the certificate's selected CN matches re. The
// match is unanchored (plain re.MatchString), so the regex is a substring
// selector — this is the caddy-certstore selection contract. First-wins
// semantics across multiple identities live in the caller.
func matchCert(cert *x509.Certificate, f CertField, re *regexp.Regexp) bool {
	var cn string
	switch f {
	case FieldSubject:
		cn = cert.Subject.CommonName
	case FieldIssuer:
		cn = cert.Issuer.CommonName
	}
	return re.MatchString(cn)
}
