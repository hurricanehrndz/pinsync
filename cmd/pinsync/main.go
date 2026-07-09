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

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/pkg/awsclient"
	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/push"
	"github.com/hurricanehrndz/pinsync/pkg/rolesanywhere"
)

const usage = `usage:
  pinsync push    -bucket B [flags] <root>   publish root to s3://B/<prefix>
  pinsync pull    -bucket B [flags] <dest>   mirror s3://B/<prefix> into dest
  pinsync version                        print the pinsync version and exit

run "pinsync push -h" or "pinsync pull -h" for flags`

// version is the pinsync release version, managed by `go tool versionbump`
// (see versionbump.yaml).
const version = "0.1.0"

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
	sub         string
	bucket      string
	prefix      string
	region      string
	endpoint    string
	parallel    int
	dir         string
	manifestKey string
	full        bool

	// Logging flags. The raw -log-level value is parsed into logLevelVal by
	// parseArgs so bad input fails before any store or AWS work.
	logLevel  string
	logFormat string

	logLevelVal slog.Level

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

// posArgName returns the human-readable name for the required positional
// argument of the given subcommand.
func posArgName(sub string) string {
	if sub == "pull" {
		return "destination directory"
	}
	return "root directory"
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
	fs.StringVar(&c.manifestKey, "manifest-key", "", "S3 key for the manifest; default <prefix>/manifest.json")
	fs.IntVar(&c.parallel, "parallel", 16, "concurrent transfers")
	fs.StringVar(&c.region, "region", "", "AWS region (overrides the default chain)")
	fs.StringVar(&c.endpoint, "endpoint-url", "", "custom S3 endpoint, e.g. MinIO; implies path-style addressing")
	fs.StringVar(&c.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	fs.StringVar(&c.logFormat, "log-format", "text", "log format: text|json")
	// -full is a push-only override; registering it on push alone makes pull
	// reject it as an unknown flag for free.
	if c.sub == "push" {
		fs.BoolVar(&c.full, "full", false, "re-upload every file, skipping the remote-manifest diff")
	}
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
		return nil, fmt.Errorf("expected exactly one positional argument: the %s", posArgName(c.sub))
	}
	c.dir = fs.Arg(0)
	lvl, err := parseLogLevel(c.logLevel)
	if err != nil {
		return nil, err
	}
	c.logLevelVal = lvl
	if c.logFormat != "text" && c.logFormat != "json" {
		return nil, fmt.Errorf("invalid -log-format %q: want text|json", c.logFormat)
	}
	if err := parseRAFlags(c, fs); err != nil {
		return nil, err
	}
	return c, nil
}

// parseLogLevel maps a -log-level string to its slog.Level.
func parseLogLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	}
	return 0, fmt.Errorf("invalid -log-level %q: want debug|info|warn|error", s)
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
	if len(args) > 0 && args[0] == "version" {
		_, err := fmt.Fprintln(stdout, version)
		return err
	}
	c, err := parseArgs(args, stderr)
	if err != nil {
		return err
	}
	logger := newLogger(c, stderr)
	acfg := awsclient.Config{Region: c.region, Endpoint: c.endpoint, Logger: logger}
	if c.raMode {
		acfg.RolesAnywhere = &awsclient.RAConfig{
			TrustAnchorARN: c.raTrustAnchor, ProfileARN: c.raProfile, RoleARN: c.raRole,
			CertPattern: c.raRegex, CertField: c.raField, CertStore: c.raStore,
		}
	}
	client, err := awsclient.NewS3(ctx, acfg)
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

// newLogger builds the always-on logger over w, honoring the parsed -log-level
// and -log-format flags.
func newLogger(c *cli, w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: c.logLevelVal}
	if c.logFormat == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}

// execute dispatches to the library and renders the one-line summary.
func execute(ctx context.Context, c *cli, client *s3.Client, logger *slog.Logger) (string, error) {
	if c.sub == "push" {
		stats, err := push.Push(ctx, client, c.bucket, c.prefix, c.dir, push.Options{
			Parallel: c.parallel, Logger: logger, ManifestKey: c.manifestKey, Full: c.full,
		})
		if err != nil {
			return "", err
		}
		return pushSummary(stats, c.bucket, c.prefix), nil
	}
	stats, err := pull.Pull(ctx, client, c.bucket, c.prefix, c.dir, pull.Options{
		Parallel: c.parallel, Logger: logger, ManifestKey: c.manifestKey,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("pulled %d files: %d downloaded, %d linked, %d copied",
		stats.Total, stats.Downloaded, stats.Linked, stats.Copied), nil
}

// pushSummary renders the one-line differential push result. Uploaded==0 means
// nothing new reached the bucket, so the tree is reported as up to date.
func pushSummary(s push.Stats, bucket, prefix string) string {
	dest := fmt.Sprintf("s3://%s/%s", bucket, prefix)
	if s.Uploaded == 0 {
		return fmt.Sprintf("up to date: %d files unchanged at %s", s.Total, dest)
	}
	return fmt.Sprintf("pushed %d of %d files (%d unchanged) to %s", s.Uploaded, s.Total, s.Skipped, dest)
}
