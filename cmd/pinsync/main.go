// Command pinsync publishes a local tree to S3 as an atomic manifest
// snapshot (push) and mirrors it back down with full verification (pull).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/push"
	"github.com/hurricanehrndz/pinsync/pkg/rolesanywhere"
)

const usage = `usage:
  pinsync push -bucket B [flags] <root>   publish root to s3://B/<prefix>
  pinsync pull -bucket B [flags] <dest>   mirror s3://B/<prefix> into dest

run "pinsync push -h" or "pinsync pull -h" for flags`

func main() {
	err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr)
	if err == nil {
		return
	}
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	fmt.Fprintln(os.Stderr, "pinsync:", err)
	os.Exit(1)
}

// cli holds the parsed command line.
type cli struct {
	sub      string
	bucket   string
	prefix   string
	region   string
	endpoint string
	parallel int
	verbose  bool
	dir      string

	// IAM Roles Anywhere flags (pull only, macOS/Windows). The raw flag
	// values are parsed into raMode/raRegex/raField/raStore by parseArgs so
	// bad input fails before any store or AWS work.
	raTrustAnchor string
	raProfile     string
	raRole        string
	raCertPattern string
	raCertField   string
	raCertStore   string

	raMode  bool
	raRegex *regexp.Regexp
	raField rolesanywhere.CertField
	raStore rolesanywhere.StoreLoc
}

// parseArgs parses the subcommand and its flags; the one positional argument
// is the local root (push) or destination (pull).
func parseArgs(args []string, stderr io.Writer) (*cli, error) {
	if len(args) == 0 {
		return nil, errors.New("missing subcommand\n" + usage)
	}
	c := &cli{sub: args[0]}
	if c.sub != "push" && c.sub != "pull" {
		return nil, fmt.Errorf("unknown subcommand %q\n%s", c.sub, usage)
	}
	fs := flag.NewFlagSet("pinsync "+c.sub, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&c.bucket, "bucket", "", "S3 bucket (required)")
	fs.StringVar(&c.prefix, "prefix", "", "key prefix under the bucket")
	fs.IntVar(&c.parallel, "parallel", 16, "concurrent transfers")
	fs.StringVar(&c.region, "region", "", "AWS region (overrides the default chain)")
	fs.StringVar(&c.endpoint, "endpoint-url", "", "custom S3 endpoint, e.g. MinIO; implies path-style addressing")
	fs.BoolVar(&c.verbose, "v", false, "log progress to stderr")
	// IAM Roles Anywhere is a pull-only device flow; registering these on pull
	// alone makes push reject them as unknown flags for free.
	if c.sub == "pull" {
		fs.StringVar(&c.raTrustAnchor, "ra-trust-anchor-arn", "", "IAM Roles Anywhere trust anchor ARN")
		fs.StringVar(&c.raProfile, "ra-profile-arn", "", "IAM Roles Anywhere profile ARN")
		fs.StringVar(&c.raRole, "ra-role-arn", "", "IAM role ARN to assume via Roles Anywhere")
		fs.StringVar(&c.raCertPattern, "ra-cert-pattern", "", "regex selecting the device certificate by CN")
		fs.StringVar(&c.raCertField, "ra-cert-field", "subject", "certificate CN to match: subject|issuer")
		fs.StringVar(&c.raCertStore, "ra-cert-store", "user", "windows only: user|machine; ignored on macOS")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return nil, err
	}
	if c.bucket == "" {
		return nil, errors.New("-bucket is required")
	}
	if fs.NArg() != 1 {
		what := "root directory"
		if c.sub == "pull" {
			what = "destination directory"
		}
		return nil, fmt.Errorf("expected exactly one positional argument: the %s", what)
	}
	c.dir = fs.Arg(0)
	if err := parseRAFlags(c, fs); err != nil {
		return nil, err
	}
	return c, nil
}

// parseRAFlags detects whether IAM Roles Anywhere mode is active — any -ra-*
// flag explicitly passed (even the defaulted -ra-cert-field/-ra-cert-store) —
// and, when so, validates every required ARN together and parses the pattern,
// field, and store so all bad input fails before any store or AWS work.
func parseRAFlags(c *cli, fs *flag.FlagSet) error {
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "ra-") {
			set[f.Name] = true
		}
	})
	if len(set) == 0 {
		return nil
	}
	c.raMode = true

	var missing []string
	if c.raTrustAnchor == "" {
		missing = append(missing, "-ra-trust-anchor-arn")
	}
	if c.raProfile == "" {
		missing = append(missing, "-ra-profile-arn")
	}
	if c.raRole == "" {
		missing = append(missing, "-ra-role-arn")
	}
	if c.raCertPattern == "" {
		missing = append(missing, "-ra-cert-pattern")
	}
	if len(missing) > 0 {
		return fmt.Errorf("IAM Roles Anywhere requires: %s", strings.Join(missing, ", "))
	}

	re, err := regexp.Compile(c.raCertPattern)
	if err != nil {
		return fmt.Errorf("invalid -ra-cert-pattern %q: %w", c.raCertPattern, err)
	}
	c.raRegex = re

	c.raField, err = rolesanywhere.ParseCertField(c.raCertField)
	if err != nil {
		return err
	}
	c.raStore, err = rolesanywhere.ParseStoreLoc(c.raCertStore)
	if err != nil {
		return err
	}
	return nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	c, err := parseArgs(args, stderr)
	if err != nil {
		return err
	}
	var logger *slog.Logger
	if c.verbose {
		logger = slog.New(slog.NewTextHandler(stderr, nil))
	}
	client, err := newClient(ctx, c)
	if err != nil {
		return err
	}
	summary, err := execute(ctx, c, client, logger)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, summary)
	return err
}

// execute dispatches to the library and renders the one-line summary.
func execute(ctx context.Context, c *cli, client *s3.Client, logger *slog.Logger) (string, error) {
	if c.sub == "push" {
		stats, err := push.Push(ctx, client, c.bucket, c.prefix, c.dir, push.Options{
			Parallel: c.parallel, Logger: logger,
		})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("pushed %d files to s3://%s/%s", stats.Uploaded, c.bucket, c.prefix), nil
	}
	stats, err := pull.Pull(ctx, client, c.bucket, c.prefix, c.dir, pull.Options{
		Parallel: c.parallel, Logger: logger,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pulled %d files: %d downloaded, %d linked, %d copied",
		stats.Total, stats.Downloaded, stats.Linked, stats.Copied), nil
}

// newClient builds the S3 client. Without Roles Anywhere it resolves
// credentials and region via the standard SDK default chain; a custom endpoint
// (MinIO) switches to path-style addressing.
func newClient(ctx context.Context, c *cli) (*s3.Client, error) {
	if c.raMode {
		return newRAClient(ctx, c)
	}
	var loadOpts []func(*awsconfig.LoadOptions) error
	if c.region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(c.region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if c.endpoint != "" {
			o.BaseEndpoint = aws.String(c.endpoint)
			o.UsePathStyle = true
		}
	}), nil
}

// newRAClient selects the device certificate, exchanges it for temporary
// credentials via IAM Roles Anywhere, and builds the S3 client from them. The
// region is resolved once and shared by the CreateSession exchange and the S3
// client so both agree; the certificate's private key never leaves its store.
func newRAClient(ctx context.Context, c *cli) (*s3.Client, error) {
	region, err := raRegion(c.region, c.raTrustAnchor)
	if err != nil {
		return nil, err
	}
	id, _, err := rolesanywhere.FindIdentity(c.raField, c.raRegex, c.raStore)
	if err != nil {
		return nil, err
	}
	defer id.Close()
	creds, err := rolesanywhere.Fetch(ctx, id, rolesanywhere.Options{
		TrustAnchorARN: c.raTrustAnchor,
		ProfileARN:     c.raProfile,
		RoleARN:        c.raRole,
		Region:         region,
	})
	if err != nil {
		return nil, err
	}
	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken,
		)),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if c.endpoint != "" {
			o.BaseEndpoint = aws.String(c.endpoint)
			o.UsePathStyle = true
		}
	}), nil
}

// raRegion resolves the region for a Roles Anywhere invocation: the -region
// flag wins, otherwise it is read from the trust anchor ARN.
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
