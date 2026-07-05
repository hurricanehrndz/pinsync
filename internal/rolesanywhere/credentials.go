package rolesanywhere

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/rolesanywhere-credential-helper/rolesanywhere"
	"github.com/aws/smithy-go/middleware"
)

// Options configures a Roles Anywhere CreateSession exchange. The three ARNs
// identify the trust anchor, profile, and role; Region overrides the region
// otherwise inferred from the trust anchor ARN.
type Options struct {
	TrustAnchorARN string
	ProfileARN     string
	RoleARN        string
	Region         string

	// baseEndpoint, when set, overrides the resolved Roles Anywhere endpoint.
	// It exists for tests, which point CreateSession at an httptest server;
	// production leaves it empty and lets the SDK resolve the real endpoint.
	baseEndpoint string
}

// Fetch performs a Roles Anywhere CreateSession using SigV4-X509 signing over
// the identity's certificate, and returns the vended temporary credentials.
// The identity's private key never leaves its store: signing goes through the
// crypto.Signer returned by id.Signer().
func Fetch(ctx context.Context, id Identity, opts Options) (aws.Credentials, error) {
	region, err := resolveRegion(opts)
	if err != nil {
		return aws.Credentials{}, err
	}

	leaf, err := id.Certificate()
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("reading certificate: %w", err)
	}
	chain, err := id.CertificateChain()
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("reading certificate chain: %w", err)
	}
	signer, err := id.Signer()
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("obtaining signer: %w", err)
	}

	clientOpts := rolesanywhere.Options{Region: region}
	if opts.baseEndpoint != "" {
		clientOpts.BaseEndpoint = aws.String(opts.baseEndpoint)
	}
	clientOpts.APIOptions = append(clientOpts.APIOptions, func(stack *middleware.Stack) error {
		// Drop the stock SigV4 identity/signing finalizers: CreateSession is
		// authenticated by the certificate via SigV4-X509, not by IAM
		// credentials (there is no credentials provider configured). Remove
		// tolerates a missing name so SDK codegen drift can't hard-fail here,
		// so its not-found error is intentionally ignored.
		_, _ = stack.Finalize.Remove("Signing")
		_, _ = stack.Finalize.Remove("setLegacyContextSigningOptions")
		_, _ = stack.Finalize.Remove("GetIdentity")
		return stack.Finalize.Add(newX509SignMiddleware(signer, leaf, chain, region), middleware.After)
	})

	client := rolesanywhere.New(clientOpts)

	out, err := client.CreateSession(ctx, &rolesanywhere.CreateSessionInput{
		Cert:           aws.String(base64.StdEncoding.EncodeToString(leaf.Raw)),
		ProfileArn:     aws.String(opts.ProfileARN),
		RoleArn:        aws.String(opts.RoleARN),
		TrustAnchorArn: aws.String(opts.TrustAnchorARN),
	})
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("rolesanywhere CreateSession: %w", err)
	}
	return toCredentials(out)
}

// toCredentials maps a CreateSession response's first credential set to an
// aws.Credentials, surfacing clear errors for empty or incomplete responses.
func toCredentials(out *rolesanywhere.CreateSessionOutput) (aws.Credentials, error) {
	if len(out.CredentialSet) == 0 || out.CredentialSet[0].Credentials == nil {
		return aws.Credentials{}, errors.New("rolesanywhere CreateSession returned no credentials")
	}
	c := out.CredentialSet[0].Credentials
	if c.AccessKeyId == nil || c.SecretAccessKey == nil || c.SessionToken == nil || c.Expiration == nil {
		return aws.Credentials{}, errors.New("rolesanywhere CreateSession returned incomplete credentials")
	}
	exp, err := time.Parse(time.RFC3339, *c.Expiration)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("parsing credential expiration %q: %w", *c.Expiration, err)
	}

	return aws.Credentials{
		AccessKeyID:     *c.AccessKeyId,
		SecretAccessKey: *c.SecretAccessKey,
		SessionToken:    *c.SessionToken,
		Source:          "RolesAnywhere",
		CanExpire:       true,
		Expires:         exp,
	}, nil
}

// resolveRegion returns opts.Region when set, otherwise the region parsed from
// the trust anchor ARN. It errors when neither yields a region.
func resolveRegion(opts Options) (string, error) {
	if opts.Region != "" {
		return opts.Region, nil
	}
	parsed, err := arn.Parse(opts.TrustAnchorARN)
	if err != nil {
		return "", fmt.Errorf("resolving region from trust anchor ARN: %w", err)
	}
	if parsed.Region == "" {
		return "", errors.New("no region: set Options.Region or use a trust anchor ARN that carries a region")
	}
	return parsed.Region, nil
}
