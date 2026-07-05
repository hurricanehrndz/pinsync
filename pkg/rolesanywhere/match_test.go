package rolesanywhere

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"regexp"
	"strings"
	"testing"
	"time"
)

// testLeaf builds a leaf certificate signed by a CA whose CNs differ, so a
// test can prove that field selection targets the right name: the Subject CN
// is "pinsync-test-device" and the Issuer CN is "Pinsync Test CA".
func testLeaf(t *testing.T) *x509.Certificate {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Pinsync Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "pinsync-test-device"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}
	return leaf
}

func TestMatchCert(t *testing.T) {
	leaf := testLeaf(t)

	cases := []struct {
		name  string
		field CertField
		re    string
		want  bool
		why   string
	}{
		{
			name:  "subject exact",
			field: FieldSubject,
			re:    "^pinsync-test-device$",
			want:  true,
			why:   "Subject CN selection must see the leaf's own CN",
		},
		{
			name:  "issuer exact",
			field: FieldIssuer,
			re:    "^Pinsync Test CA$",
			want:  true,
			why:   "Issuer CN selection must see the signing CA's CN, not the leaf's",
		},
		{
			name:  "no match",
			field: FieldSubject,
			re:    "some-other-device",
			want:  false,
			why:   "a regex matching neither CN must not select the cert",
		},
		{
			name:  "unanchored substring",
			field: FieldSubject,
			re:    "test-device",
			want:  true,
			// This is the load-bearing contract: selection uses plain
			// MatchString, so an operator's substring regex matches anywhere
			// in the CN (the caddy-certstore selection contract). If someone
			// anchored the match, this case would fail.
			why: "unanchored substrings must match, matching caddy-certstore",
		},
		{
			name:  "subject regex does not leak into issuer",
			field: FieldSubject,
			re:    "Pinsync Test CA",
			want:  false,
			why:   "FieldSubject must not match against the Issuer CN",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			re := regexp.MustCompile(tc.re)
			if got := matchCert(leaf, tc.field, re); got != tc.want {
				t.Errorf("matchCert(field=%s, re=%q) = %v, want %v — %s", tc.field, tc.re, got, tc.want, tc.why)
			}
		})
	}
}

func TestParseCertField(t *testing.T) {
	valid := map[string]CertField{
		"subject": FieldSubject,
		"issuer":  FieldIssuer,
	}
	for in, want := range valid {
		got, err := ParseCertField(in)
		if err != nil {
			t.Errorf("ParseCertField(%q) errored: %v", in, err)
		}
		if got != want {
			t.Errorf("ParseCertField(%q) = %v, want %v", in, got, want)
		}
	}

	// Bad values must be rejected, and the error must name the offending value
	// so a misconfiguration is diagnosable from the message alone.
	for _, bad := range []string{"", "Subject", "san", "commonName"} {
		_, err := ParseCertField(bad)
		if err == nil {
			t.Errorf("ParseCertField(%q) accepted an invalid field", bad)
			continue
		}
		if !strings.Contains(err.Error(), bad) && bad != "" {
			t.Errorf("ParseCertField(%q) error %q does not name the bad value", bad, err)
		}
	}
}

func TestParseStoreLoc(t *testing.T) {
	valid := map[string]StoreLoc{
		"user":    StoreUser,
		"machine": StoreMachine,
	}
	for in, want := range valid {
		got, err := ParseStoreLoc(in)
		if err != nil {
			t.Errorf("ParseStoreLoc(%q) errored: %v", in, err)
		}
		if got != want {
			t.Errorf("ParseStoreLoc(%q) = %v, want %v", in, got, want)
		}
	}

	for _, bad := range []string{"", "User", "system", "local"} {
		_, err := ParseStoreLoc(bad)
		if err == nil {
			t.Errorf("ParseStoreLoc(%q) accepted an invalid store", bad)
			continue
		}
		if !strings.Contains(err.Error(), bad) && bad != "" {
			t.Errorf("ParseStoreLoc(%q) error %q does not name the bad value", bad, err)
		}
	}
}
