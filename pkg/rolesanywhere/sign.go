package rolesanywhere

// The SigV4-X509 request-signing protocol implemented here is adapted from
// AWS's IAM Roles Anywhere credential helper
// (github.com/aws/rolesanywhere-credential-helper, Apache-2.0), specifically
// its aws_signing_helper/signer.go. The canonical request, string-to-sign, and
// Authorization-header layout are defined by that source. One deliberate
// difference: the helper's own signers hash the message inside Sign(), so it
// passes the raw string-to-sign to signer.Sign(). certstore's crypto.Signer
// follows the STANDARD contract (it expects a pre-computed digest), so we
// sha256 the string-to-sign ourselves and pass the digest. See signX509.

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const (
	x509SigningService = "rolesanywhere"
	// x509TimeFormat is the SigV4 X-Amz-Date layout (UTC).
	x509TimeFormat = "20060102T150405Z"
	// x509ShortFormat is the credential-scope date layout (UTC).
	x509ShortFormat = "20060102"

	algRSA   = "AWS4-X509-RSA-SHA256"
	algECDSA = "AWS4-X509-ECDSA-SHA256"
)

// ignoredHeaders are excluded from canonicalization, keyed lowercase. Matches
// the helper's ignoredHeaderKeys set.
var ignoredHeaders = map[string]bool{
	"authorization":   true,
	"user-agent":      true,
	"x-amzn-trace-id": true,
}

// x509Algorithm returns the AWS4-X509 signing algorithm for the leaf's public
// key type. Only RSA and ECDSA leaves can sign a Roles Anywhere session.
func x509Algorithm(pub crypto.PublicKey) (string, error) {
	switch pub.(type) {
	case *rsa.PublicKey:
		return algRSA, nil
	case *ecdsa.PublicKey:
		return algECDSA, nil
	default:
		return "", fmt.Errorf("unsupported certificate key type %T", pub)
	}
}

// newX509SignMiddleware builds the smithy finalize middleware that signs the
// CreateSession request with SigV4-X509. It replaces the SDK's stock "Signing"
// finalizer (same name) after the stock identity/signing finalizers are
// removed, so the request is authenticated by the certificate rather than by
// IAM credentials.
func newX509SignMiddleware(signer crypto.Signer, leaf *x509.Certificate, chain []*x509.Certificate, region string) middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc("Signing", func(
		ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
	) (middleware.FinalizeOutput, middleware.Metadata, error) {
		req, ok := in.Request.(*smithyhttp.Request)
		if !ok {
			return middleware.FinalizeOutput{}, middleware.Metadata{}, fmt.Errorf("unexpected request middleware type %T", in.Request)
		}
		// The payload hash is computed by the upstream ComputePayloadSHA256
		// finalizer, which runs before this one.
		if err := signX509(req.Request, v4.GetPayloadHash(ctx), signer, leaf, chain, region, time.Now()); err != nil {
			return middleware.FinalizeOutput{}, middleware.Metadata{}, err
		}
		return next.HandleFinalize(ctx, in)
	})
}

// signX509 sets the SigV4-X509 headers on req and computes the Authorization
// header. now is passed explicitly so tests are deterministic; production
// passes time.Now().
func signX509(req *http.Request, payloadHash string, signer crypto.Signer, leaf *x509.Certificate, chain []*x509.Certificate, region string, now time.Time) error {
	algorithm, err := x509Algorithm(leaf.PublicKey)
	if err != nil {
		return err
	}

	now = now.UTC()
	amzDate := now.Format(x509TimeFormat)
	scope := now.Format(x509ShortFormat) + "/" + region + "/" + x509SigningService + "/aws4_request"

	req.Header.Set("Host", req.URL.Host)
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-X509", base64.StdEncoding.EncodeToString(leaf.Raw))
	// The chain header carries only intermediates: the leaf travels in
	// X-Amz-X509. Omit the header entirely when nothing is left after stripping.
	if chainHeader := encodeChain(stripLeaf(leaf, chain)); chainHeader != "" {
		req.Header.Set("X-Amz-X509-Chain", chainHeader)
	}

	canonical, signedHeaders := canonicalRequest(req, payloadHash)
	sts := stringToSign(canonical, algorithm, amzDate, scope)

	// Standard crypto.Signer contract: hash first, then sign the digest.
	digest := sha256.Sum256([]byte(sts))
	sig, err := signer.Sign(rand.Reader, digest[:], crypto.SHA256)
	if err != nil {
		return fmt.Errorf("signing CreateSession request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf(
		"%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, leaf.SerialNumber.String(), scope, signedHeaders, hex.EncodeToString(sig),
	))
	return nil
}

// canonicalRequest builds the SigV4 canonical request string and the
// semicolon-joined signed-headers list. It returns the raw canonical string
// (unhashed); stringToSign hashes it.
func canonicalRequest(r *http.Request, payloadHash string) (canonical, signedHeaders string) {
	headers, signed := canonicalHeaders(r)
	uri := r.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	query := strings.ReplaceAll(r.URL.Query().Encode(), "+", "%20")
	return strings.Join([]string{
		r.Method,
		uri,
		query,
		headers,
		"", // blank line between canonical headers and signed-headers list
		signed,
		payloadHash,
	}, "\n"), signed
}

// canonicalHeaders returns the canonical header block and signed-headers list.
// Keys are lowercased and sorted; the ignored set is excluded; values are
// space-collapsed and trimmed.
func canonicalHeaders(r *http.Request) (block, signed string) {
	keys := make([]string, 0, len(r.Header))
	values := make(map[string]string, len(r.Header))
	for k, v := range r.Header {
		lower := strings.ToLower(k)
		if ignoredHeaders[lower] {
			continue
		}
		keys = append(keys, lower)
		values[lower] = trimHeaderValue(strings.Join(v, ","))
	}
	sort.Strings(keys)

	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(values[k])
	}
	return b.String(), strings.Join(keys, ";")
}

// trimHeaderValue trims and collapses internal whitespace runs to a single
// space, matching SigV4 header value normalization.
func trimHeaderValue(v string) string {
	return strings.Join(strings.Fields(v), " ")
}

// stringToSign builds the AWS4-X509 string-to-sign over the (unhashed)
// canonical request.
func stringToSign(canonical, algorithm, amzDate, scope string) string {
	h := sha256.Sum256([]byte(canonical))
	return strings.Join([]string{
		algorithm,
		amzDate,
		scope,
		hex.EncodeToString(h[:]),
	}, "\n")
}

// stripLeaf returns chain with any entry equal to leaf removed, so the chain
// header carries only intermediates.
func stripLeaf(leaf *x509.Certificate, chain []*x509.Certificate) []*x509.Certificate {
	out := make([]*x509.Certificate, 0, len(chain))
	for _, c := range chain {
		if c.Equal(leaf) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// encodeChain joins the base64 DER of each certificate with commas. An empty
// chain yields the empty string, signalling the caller to omit the header.
func encodeChain(chain []*x509.Certificate) string {
	if len(chain) == 0 {
		return ""
	}
	parts := make([]string, len(chain))
	for i, c := range chain {
		parts[i] = base64.StdEncoding.EncodeToString(c.Raw)
	}
	return strings.Join(parts, ",")
}
