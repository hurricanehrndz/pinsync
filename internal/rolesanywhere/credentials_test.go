package rolesanywhere

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeIdentity is an in-memory Identity backed by a standard-contract
// crypto.Signer (an rsa or ecdsa private key), so the SigV4-X509 path can be
// exercised on Linux without a real certificate store.
type fakeIdentity struct {
	leaf  *x509.Certificate
	chain []*x509.Certificate
	key   crypto.Signer
}

func (f *fakeIdentity) Certificate() (*x509.Certificate, error)        { return f.leaf, nil }
func (f *fakeIdentity) CertificateChain() ([]*x509.Certificate, error) { return f.chain, nil }
func (f *fakeIdentity) Signer() (crypto.Signer, error)                 { return f.key, nil }
func (f *fakeIdentity) Close()                                         {}

// selfSign creates a self-signed leaf certificate for key.
func selfSign(t *testing.T, key crypto.Signer, serial int64) *x509.Certificate {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: "pinsync-x509-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatalf("self-sign cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse self-signed cert: %v", err)
	}
	return cert
}

func newRSAIdentity(t *testing.T) *fakeIdentity {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return &fakeIdentity{leaf: selfSign(t, key, 1001), key: key}
}

func newECDSAIdentity(t *testing.T) *fakeIdentity {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ECDSA key: %v", err)
	}
	return &fakeIdentity{leaf: selfSign(t, key, 1002), key: key}
}

// newChainedIdentity builds a leaf signed by a CA and returns an identity whose
// CertificateChain is [leaf, CA] plus the CA certificate for assertions.
func newChainedIdentity(t *testing.T) (*fakeIdentity, *x509.Certificate) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2001),
		Subject:               pkix.Name{CommonName: "Pinsync X509 Test CA"},
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
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2002),
		Subject:      pkix.Name{CommonName: "pinsync-x509-leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, ca, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}
	// certstore hands back the full path; include the leaf so the signer must
	// strip it out of the chain header.
	return &fakeIdentity{leaf: leaf, chain: []*x509.Certificate{leaf, ca}, key: leafKey}, ca
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// parseAuthHeader splits an AWS4-X509 Authorization header into its parts.
func parseAuthHeader(t *testing.T, auth string) (algorithm, scope, signedHeaders, signature string) {
	t.Helper()
	head, rest, ok := strings.Cut(auth, " ")
	if !ok {
		t.Fatalf("Authorization header has no algorithm: %q", auth)
	}
	algorithm = head
	for _, part := range strings.Split(rest, ", ") {
		switch {
		case strings.HasPrefix(part, "Credential="):
			cred := strings.TrimPrefix(part, "Credential=")
			// Credential = <serial>/<scope>; scope is everything after the serial.
			_, scope, ok = strings.Cut(cred, "/")
			if !ok {
				t.Fatalf("Credential missing scope: %q", cred)
			}
		case strings.HasPrefix(part, "SignedHeaders="):
			signedHeaders = strings.TrimPrefix(part, "SignedHeaders=")
		case strings.HasPrefix(part, "Signature="):
			signature = strings.TrimPrefix(part, "Signature=")
		}
	}
	return algorithm, scope, signedHeaders, signature
}

// rebuildCanonical reconstructs the canonical request server-side using only
// the headers named in SignedHeaders (host comes from r.Host, which Go strips
// out of r.Header). This is an independent reimplementation of the signing
// contract so a bug in sign.go cannot mask itself.
func rebuildCanonical(r *http.Request, body []byte, signedHeaders string) string {
	names := strings.Split(signedHeaders, ";")
	lines := make([]string, len(names))
	for i, name := range names {
		var v string
		if name == "host" {
			v = r.Host
		} else {
			v = strings.Join(strings.Fields(r.Header.Get(name)), " ")
		}
		lines[i] = name + ":" + v
	}
	uri := r.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	query := strings.ReplaceAll(r.URL.Query().Encode(), "+", "%20")
	return strings.Join([]string{
		r.Method,
		uri,
		query,
		strings.Join(lines, "\n"),
		"",
		signedHeaders,
		sha256Hex(body),
	}, "\n")
}

// verifyRARequest asserts the required headers are present, then
// cryptographically verifies the Authorization signature against the leaf's
// public key (taken from the X-Amz-X509 header the client sent).
func verifyRARequest(t *testing.T, r *http.Request, body []byte, expectAlg string) {
	t.Helper()

	x509Header := r.Header.Get("X-Amz-X509")
	if x509Header == "" {
		t.Error("request missing X-Amz-X509 header")
	}
	if r.Header.Get("X-Amz-Date") == "" {
		t.Error("request missing X-Amz-Date header")
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, expectAlg+" ") {
		t.Errorf("Authorization = %q, want prefix %q", auth, expectAlg+" ")
		return
	}

	algorithm, scope, signedHeaders, sigHex := parseAuthHeader(t, auth)
	canonical := rebuildCanonical(r, body, signedHeaders)
	sts := strings.Join([]string{
		algorithm,
		r.Header.Get("X-Amz-Date"),
		scope,
		sha256Hex([]byte(canonical)),
	}, "\n")
	digest := sha256.Sum256([]byte(sts))

	sig, err := hex.DecodeString(sigHex)
	if err != nil {
		t.Fatalf("decode signature hex: %v", err)
	}

	der, err := base64.StdEncoding.DecodeString(x509Header)
	if err != nil {
		t.Fatalf("decode X-Amz-X509: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf from X-Amz-X509: %v", err)
	}

	switch pub := leaf.PublicKey.(type) {
	case *rsa.PublicKey:
		if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig); err != nil {
			t.Errorf("RSA signature verification failed: %v", err)
		}
	case *ecdsa.PublicKey:
		if !ecdsa.VerifyASN1(pub, digest[:], sig) {
			t.Error("ECDSA signature verification failed")
		}
	default:
		t.Errorf("unexpected leaf key type %T", pub)
	}
}

// credsJSON is the canned CreateSession success body.
func credsJSON(expiration string) string {
	body, _ := json.Marshal(map[string]any{
		"credentialSet": []map[string]any{{
			"credentials": map[string]any{
				"accessKeyId":     "AKIATESTACCESSKEY",
				"secretAccessKey": "test-secret-access-key",
				"sessionToken":    "test-session-token",
				"expiration":      expiration,
			},
		}},
	})
	return string(body)
}

// TestFetchSignsAndReturnsCredentials is the end-to-end + middleware-parity
// test: if the stock SigV4 finalizers were still active the call would fail
// (no credentials provider is configured), so a successful, signature-verified
// exchange proves our X509 finalizer replaced them.
func TestFetchSignsAndReturnsCredentials(t *testing.T) {
	cases := []struct {
		name string
		id   *fakeIdentity
		alg  string
	}{
		{"rsa", newRSAIdentity(t), algRSA},
		{"ecdsa", newECDSAIdentity(t), algECDSA},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expiration := time.Now().Add(time.Hour).UTC().Truncate(time.Second).Format(time.RFC3339)

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read body: %v", err)
				}
				verifyRARequest(t, r, body, tc.alg)
				_, _ = io.WriteString(w, credsJSON(expiration))
			}))
			defer srv.Close()

			creds, err := Fetch(context.Background(), tc.id, Options{
				TrustAnchorARN: "arn:aws:rolesanywhere:us-west-2:111122223333:trust-anchor/abc",
				ProfileARN:     "arn:aws:rolesanywhere:us-west-2:111122223333:profile/def",
				RoleARN:        "arn:aws:iam::111122223333:role/ra",
				Region:         "us-west-2",
				baseEndpoint:   srv.URL,
			})
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}

			if creds.AccessKeyID != "AKIATESTACCESSKEY" {
				t.Errorf("AccessKeyID = %q, want AKIATESTACCESSKEY", creds.AccessKeyID)
			}
			if creds.SecretAccessKey != "test-secret-access-key" {
				t.Errorf("SecretAccessKey = %q", creds.SecretAccessKey)
			}
			if creds.SessionToken != "test-session-token" {
				t.Errorf("SessionToken = %q", creds.SessionToken)
			}
			if creds.Source != "RolesAnywhere" {
				t.Errorf("Source = %q, want RolesAnywhere", creds.Source)
			}
			if !creds.CanExpire {
				t.Error("CanExpire = false, want true (RA credentials are temporary)")
			}
			want, _ := time.Parse(time.RFC3339, expiration)
			if !creds.Expires.Equal(want) {
				t.Errorf("Expires = %v, want %v", creds.Expires, want)
			}
		})
	}
}

// TestFetchChainHeader verifies that a chained identity emits X-Amz-X509-Chain
// containing only the intermediate (CA), with the leaf stripped, and that a
// leaf-only identity omits the header entirely.
func TestFetchChainHeader(t *testing.T) {
	t.Run("chained strips leaf", func(t *testing.T) {
		id, ca := newChainedIdentity(t)
		expiration := time.Now().Add(time.Hour).UTC().Truncate(time.Second).Format(time.RFC3339)

		var gotChain string
		var chainSet bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotChain = r.Header.Get("X-Amz-X509-Chain")
			_, chainSet = r.Header["X-Amz-X509-Chain"]
			_, _ = io.WriteString(w, credsJSON(expiration))
		}))
		defer srv.Close()

		_, err := Fetch(context.Background(), id, Options{
			TrustAnchorARN: "arn:aws:rolesanywhere:eu-central-1:111122223333:trust-anchor/abc",
			ProfileARN:     "arn:aws:rolesanywhere:eu-central-1:111122223333:profile/def",
			RoleARN:        "arn:aws:iam::111122223333:role/ra",
			baseEndpoint:   srv.URL,
		})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if !chainSet {
			t.Fatal("X-Amz-X509-Chain header absent, want CA present")
		}
		wantCA := base64.StdEncoding.EncodeToString(ca.Raw)
		if gotChain != wantCA {
			t.Errorf("X-Amz-X509-Chain = %q, want only CA %q (leaf must be stripped)", gotChain, wantCA)
		}
		if strings.Contains(gotChain, base64.StdEncoding.EncodeToString(id.leaf.Raw)) {
			t.Error("X-Amz-X509-Chain contains the leaf; it must be stripped")
		}
	})

	t.Run("leaf-only omits header", func(t *testing.T) {
		id := newRSAIdentity(t)
		expiration := time.Now().Add(time.Hour).UTC().Truncate(time.Second).Format(time.RFC3339)

		var chainSet bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, chainSet = r.Header["X-Amz-X509-Chain"]
			_, _ = io.WriteString(w, credsJSON(expiration))
		}))
		defer srv.Close()

		_, err := Fetch(context.Background(), id, Options{
			TrustAnchorARN: "arn:aws:rolesanywhere:us-east-1:111122223333:trust-anchor/abc",
			ProfileARN:     "arn:aws:rolesanywhere:us-east-1:111122223333:profile/def",
			RoleARN:        "arn:aws:iam::111122223333:role/ra",
			baseEndpoint:   srv.URL,
		})
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if chainSet {
			t.Error("X-Amz-X509-Chain present for a leaf-only identity; it must be omitted")
		}
	})
}

// TestFetchEmptyCredentialSet ensures an empty credentialSet surfaces a clear
// error rather than a nil-pointer panic.
func TestFetchEmptyCredentialSet(t *testing.T) {
	id := newRSAIdentity(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"credentialSet":[]}`)
	}))
	defer srv.Close()

	_, err := Fetch(context.Background(), id, Options{
		TrustAnchorARN: "arn:aws:rolesanywhere:us-west-2:111122223333:trust-anchor/abc",
		ProfileARN:     "arn:aws:rolesanywhere:us-west-2:111122223333:profile/def",
		RoleARN:        "arn:aws:iam::111122223333:role/ra",
		baseEndpoint:   srv.URL,
	})
	if err == nil {
		t.Fatal("Fetch succeeded on empty credentialSet, want error")
	}
	if !strings.Contains(err.Error(), "no credentials") {
		t.Errorf("error = %q, want it to mention missing credentials", err)
	}
}

func TestResolveRegion(t *testing.T) {
	const anchor = "arn:aws:rolesanywhere:ap-southeast-2:111122223333:trust-anchor/abc"

	cases := []struct {
		name    string
		opts    Options
		want    string
		wantErr bool
		why     string
	}{
		{
			name: "explicit region wins",
			opts: Options{Region: "us-west-1", TrustAnchorARN: anchor},
			want: "us-west-1",
			why:  "an explicit Region must override the ARN-derived region",
		},
		{
			name: "region from trust anchor arn",
			opts: Options{TrustAnchorARN: anchor},
			want: "ap-southeast-2",
			why:  "with no explicit region, the trust anchor ARN supplies it",
		},
		{
			name:    "both empty errors",
			opts:    Options{},
			wantErr: true,
			why:     "no region and an empty ARN cannot resolve a region",
		},
		{
			name:    "malformed arn errors",
			opts:    Options{TrustAnchorARN: "not-an-arn"},
			wantErr: true,
			why:     "an unparseable ARN must fail rather than silently defaulting",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveRegion(tc.opts)
			if tc.wantErr {
				if err == nil {
					t.Errorf("resolveRegion() = %q, want error — %s", got, tc.why)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveRegion() errored: %v — %s", err, tc.why)
			}
			if got != tc.want {
				t.Errorf("resolveRegion() = %q, want %q — %s", got, tc.want, tc.why)
			}
		})
	}
}
