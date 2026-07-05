// Package awsclient builds the AWS config and S3 client pinsync uses,
// resolving credentials either through the standard SDK default chain or, when
// configured, through IAM Roles Anywhere. It keeps all session construction out
// of the command so the two credential paths share one contract.
package awsclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/pkg/rolesanywhere"
)

// Config configures the AWS session. Region overrides the default chain's
// region; Endpoint, when set, points the S3 client at a custom endpoint (MinIO)
// and is session-agnostic, so it is applied by NewS3, not Load. RolesAnywhere,
// when non-nil, switches credential resolution to IAM Roles Anywhere.
type Config struct {
	Region        string
	Endpoint      string
	Logger        *slog.Logger
	RolesAnywhere *RAConfig
}

// RAConfig configures IAM Roles Anywhere credential resolution: the three ARNs
// identify the trust anchor, profile, and role, while CertPattern, CertField,
// and CertStore select the device certificate to authenticate with.
type RAConfig struct {
	TrustAnchorARN string
	ProfileARN     string
	RoleARN        string
	CertPattern    *regexp.Regexp
	CertField      rolesanywhere.CertField
	CertStore      rolesanywhere.StoreLoc
}

// Load resolves an aws.Config. Without RolesAnywhere it uses the standard SDK
// default chain, applying Region only when set. With RolesAnywhere it selects
// the device certificate, exchanges it for temporary credentials via a Roles
// Anywhere CreateSession, and builds the config from them; the region is
// resolved once so the CreateSession exchange and the resulting config agree,
// and the certificate's private key never leaves its store. The endpoint is not
// applied here: it is session-agnostic and belongs to the S3 client.
func Load(ctx context.Context, cfg Config) (aws.Config, error) {
	if cfg.RolesAnywhere == nil {
		var loadOpts []func(*awsconfig.LoadOptions) error
		if cfg.Region != "" {
			loadOpts = append(loadOpts, awsconfig.WithRegion(cfg.Region))
		}
		return awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	}

	ra := cfg.RolesAnywhere
	region, err := raRegion(cfg.Region, ra.TrustAnchorARN)
	if err != nil {
		return aws.Config{}, err
	}
	id, _, err := rolesanywhere.FindIdentity(cfg.Logger, ra.CertField, ra.CertPattern, ra.CertStore)
	if err != nil {
		return aws.Config{}, err
	}
	defer id.Close()
	creds, err := rolesanywhere.Fetch(ctx, id, rolesanywhere.Options{
		TrustAnchorARN: ra.TrustAnchorARN,
		ProfileARN:     ra.ProfileARN,
		RoleARN:        ra.RoleARN,
		Region:         region,
		Logger:         cfg.Logger,
	})
	if err != nil {
		return aws.Config{}, err
	}
	return awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken,
		)),
	)
}

// NewS3 loads the config with Load and returns an S3 client. A custom endpoint
// switches the client to path-style addressing (MinIO).
func NewS3(ctx context.Context, cfg Config) (*s3.Client, error) {
	awsCfg, err := Load(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	}), nil
}

// raRegion resolves the region for a Roles Anywhere invocation: an explicit
// region wins, otherwise it is read from the trust anchor ARN.
func raRegion(region, trustAnchorARN string) (string, error) {
	if region != "" {
		return region, nil
	}
	parsed, err := arn.Parse(trustAnchorARN)
	if err != nil {
		return "", fmt.Errorf("resolving region from -ra-trust-anchor-arn: %w", err)
	}
	if parsed.Region == "" {
		return "", errors.New("no region: pass -region or use a trust anchor ARN that carries a region")
	}
	return parsed.Region, nil
}
