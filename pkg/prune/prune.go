// Package prune deletes S3 objects under a prefix that the published manifest
// no longer references — snapshot garbage left by pushes that changed or
// removed files. It diffs the live object listing against the manifest's
// keep-set: the manifest itself plus every content path it names are protected,
// and so is anything younger than a min-age grace window. A missing or corrupt
// manifest is fatal — prune never treats "no reference set" as "delete
// everything".
package prune

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// S3API is the slice of the S3 client prune needs; *s3.Client satisfies it.
type S3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input,
		opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput,
		opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput,
		opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

// Options configures Prune and DryRun.
type Options struct {
	ManifestKey string        // "" means <prefix>/manifest.json
	MinAge      time.Duration // objects modified within this window are protected
	Logger      *slog.Logger  // nil means discard
}

// Plan is the read-only preview DryRun returns: what Prune would delete without
// touching S3. Delete is sorted lexically.
type Plan struct {
	Delete     []string // unreferenced objects older than MinAge
	Protected  int      // unreferenced but younger than MinAge (kept)
	Referenced int      // objects in the manifest keep-set (kept)
	Listed     int      // total objects listed under the prefix
}

// Stats reports what Prune did.
type Stats struct {
	Deleted    int // objects successfully deleted
	Protected  int // unreferenced but younger than MinAge (kept)
	Referenced int // objects in the manifest keep-set (kept)
	Listed     int // total objects listed under the prefix
}

// now is a test seam for the min-age cutoff.
var now = time.Now

// DryRun previews what Prune would delete without deleting anything: it fetches
// the manifest, lists the prefix, and classifies every object as referenced,
// protected (too young), or a deletion candidate. It performs no DeleteObjects.
func DryRun(ctx context.Context, client S3API, bucket, prefix string, opts Options) (Plan, error) {
	del, protected, referenced, listed, err := classify(ctx, client, bucket, prefix, opts)
	if err != nil {
		return Plan{}, err
	}
	return Plan{Delete: del, Protected: protected, Referenced: referenced, Listed: listed}, nil
}

// Prune deletes every object under prefix that the published manifest no longer
// references and that is older than Options.MinAge. The manifest object and all
// content paths it names are protected, as is anything within the grace window.
// A missing or corrupt manifest is fatal: prune refuses to delete rather than
// treat an unreadable reference set as empty. Deletes go out in batches of up
// to 1000; a whole-request failure aborts, while per-key failures are collected
// and returned as an error naming the keys (the other keys are still deleted).
func Prune(ctx context.Context, client S3API, bucket, prefix string, opts Options) (Stats, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}

	del, protected, referenced, listed, err := classify(ctx, client, bucket, prefix, opts)
	if err != nil {
		return Stats{}, err
	}

	deleted, err := deleteBatched(ctx, client, bucket, del)
	stats := Stats{Deleted: deleted, Protected: protected, Referenced: referenced, Listed: listed}
	if err != nil {
		return stats, err
	}
	logger.Info("prune complete", "deleted", deleted, "protected", protected,
		"referenced", referenced, "listed", listed, "bucket", bucket, "prefix", prefix)
	return stats, nil
}

// manifestKey returns the manifest object key: the default <prefix>/manifest.json
// or the caller's custom key when non-empty.
func manifestKey(prefix, custom string) string {
	if custom == "" {
		return path.Join(prefix, manifest.Name)
	}
	return custom
}

// classify fetches the manifest and lists the prefix, splitting every object
// into referenced (in the manifest keep-set), protected (unreferenced but
// younger than MinAge), or a deletion candidate (returned sorted). A missing or
// corrupt manifest is fatal.
func classify(ctx context.Context, client S3API, bucket, prefix string, opts Options) (del []string, protected, referenced, listed int, err error) {
	prefix = strings.TrimSuffix(prefix, "/")
	key := manifestKey(prefix, opts.ManifestKey)

	m, err := fetchManifest(ctx, client, bucket, key)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	keep := make(map[string]struct{}, len(m.Files)+1)
	keep[key] = struct{}{}
	for _, f := range m.Files {
		keep[path.Join(prefix, f.Path)] = struct{}{}
	}

	cutoff := now().Add(-opts.MinAge)
	listPrefix := ""
	if prefix != "" {
		listPrefix = prefix + "/"
	}
	pager := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(listPrefix),
	})
	for pager.HasMorePages() {
		page, perr := pager.NextPage(ctx)
		if perr != nil {
			return nil, 0, 0, 0, fmt.Errorf("prune: listing %s: %w", bucket, perr)
		}
		for _, obj := range page.Contents {
			k := aws.ToString(obj.Key)
			listed++
			if _, ok := keep[k]; ok {
				referenced++
				continue
			}
			if obj.LastModified != nil && obj.LastModified.After(cutoff) {
				protected++
				continue
			}
			del = append(del, k)
		}
	}
	slices.Sort(del)
	return del, protected, referenced, listed, nil
}

// fetchManifest fetches and decodes the manifest at key. A missing manifest is
// fatal (prune refuses to run without a reference set to diff against), and so
// is a decode/validate failure.
func fetchManifest(ctx context.Context, client S3API, bucket, key string) (*manifest.Manifest, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		var missing *types.NoSuchKey
		if errors.As(err, &missing) {
			return nil, fmt.Errorf("prune: no manifest at %s; refusing to prune (nothing to diff against)", key)
		}
		return nil, fmt.Errorf("prune: fetching manifest %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	m, err := manifest.Decode(out.Body)
	if err != nil {
		return nil, fmt.Errorf("prune: reading manifest %s: %w", key, err)
	}
	return m, nil
}

// deleteBatched deletes keys in batches of up to 1000. A whole-request error
// aborts immediately; per-key errors are collected across batches and returned
// joined, naming the failed keys. It returns the count actually deleted
// (attempted minus per-key failures).
func deleteBatched(ctx context.Context, client S3API, bucket string, keys []string) (int, error) {
	const batchSize = 1000
	deleted := 0
	var errs []error
	for chunk := range slices.Chunk(keys, batchSize) {
		ids := make([]types.ObjectIdentifier, len(chunk))
		for i, k := range chunk {
			ids[i] = types.ObjectIdentifier{Key: aws.String(k)}
		}
		out, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{Objects: ids, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return deleted, fmt.Errorf("prune: deleting objects: %w", err)
		}
		for _, e := range out.Errors {
			errs = append(errs, fmt.Errorf("%s: %s", aws.ToString(e.Key), aws.ToString(e.Message)))
		}
		deleted += len(chunk) - len(out.Errors)
	}
	if len(errs) > 0 {
		return deleted, fmt.Errorf("prune: %d object(s) failed to delete: %w", len(errs), errors.Join(errs...))
	}
	return deleted, nil
}
