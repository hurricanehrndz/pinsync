package rolesanywhere

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"strings"
	"testing"
)

func TestX509Algorithm(t *testing.T) {
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate ECDSA key: %v", err)
	}
	edPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}

	if got, err := x509Algorithm(&rsaKey.PublicKey); err != nil || got != algRSA {
		t.Errorf("x509Algorithm(RSA) = %q, %v; want %q, nil", got, err, algRSA)
	}
	if got, err := x509Algorithm(&ecKey.PublicKey); err != nil || got != algECDSA {
		t.Errorf("x509Algorithm(ECDSA) = %q, %v; want %q, nil", got, err, algECDSA)
	}
	// An unsupported key type must be rejected with a diagnosable error, not
	// silently signed with the wrong algorithm.
	if _, err := x509Algorithm(edPub); err == nil || !strings.Contains(err.Error(), "unsupported certificate key type") {
		t.Errorf("x509Algorithm(ed25519) error = %v, want it to mention unsupported certificate key type", err)
	}
}

func TestStripLeafAndEncodeChain(t *testing.T) {
	id, ca := newChainedIdentity(t)

	// stripLeaf removes the leaf, leaving only the intermediate.
	stripped := stripLeaf(id.leaf, id.chain)
	if len(stripped) != 1 || !stripped[0].Equal(ca) {
		t.Fatalf("stripLeaf = %d certs, want exactly the CA", len(stripped))
	}

	// encodeChain of the stripped chain is the CA's base64 DER only.
	if got := encodeChain(stripped); got == "" || strings.Contains(got, ",") {
		t.Errorf("encodeChain(one CA) = %q, want a single base64 entry", got)
	}

	// An empty chain yields the empty string so the caller omits the header.
	if got := encodeChain(nil); got != "" {
		t.Errorf("encodeChain(nil) = %q, want empty string", got)
	}
}
