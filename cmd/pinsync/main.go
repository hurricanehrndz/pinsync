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

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/push"
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
	return c, nil
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
	client, err := newClient(ctx, c.region, c.endpoint)
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

// newClient resolves credentials and region via the standard SDK default
// chain; a custom endpoint (MinIO) switches to path-style addressing.
func newClient(ctx context.Context, region, endpoint string) (*s3.Client, error) {
	var loadOpts []func(*awsconfig.LoadOptions) error
	if region != "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	}), nil
}
