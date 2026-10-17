package rolesanywhere

import (
	"context"
	"crypto/x509"
	"log/slog"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Provider is an aws.CredentialsProvider that exchanges a device certificate
// for Roles Anywhere credentials on every Retrieve. It does not cache: wrap it
// in aws.NewCredentialsCache, which calls Retrieve again once the credentials
// it holds have expired, and set an ExpiryWindow there so it refreshes early
// and a request is not signed with credentials about to lapse.
type Provider struct {
	field CertField
	re    *regexp.Regexp
	loc   StoreLoc
	opts  Options

	// find selects the identity. It is FindIdentity in production; tests
	// replace it so they need no certificate store.
	find func(*slog.Logger, CertField, *regexp.Regexp, StoreLoc) (Identity, *x509.Certificate, error)
}

var _ aws.CredentialsProvider = (*Provider)(nil)

// NewProvider returns a Provider that selects the certificate whose field CN
// matches re in the store at loc, and exchanges it per opts. opts.Logger
// receives the diagnostics of both the store scan and the exchange.
func NewProvider(field CertField, re *regexp.Regexp, loc StoreLoc, opts Options) *Provider {
	return &Provider{field: field, re: re, loc: loc, opts: opts, find: FindIdentity}
}

// Retrieve finds the identity afresh, exchanges it with Fetch, and closes it.
// Holding a keychain or CNG handle open between refreshes would buy nothing,
// and re-finding leaves callers no lifecycle to manage.
func (p *Provider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	id, _, err := p.find(p.opts.Logger, p.field, p.re, p.loc)
	if err != nil {
		return aws.Credentials{}, err
	}
	defer id.Close()
	return Fetch(ctx, id, p.opts)
}
