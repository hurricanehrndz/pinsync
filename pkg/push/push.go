// Package push publishes a local tree to S3 as a manifest snapshot: content
// files are uploaded first and the manifest last, so a failed push never
// publishes a manifest whose content is missing. A failed push can still leave
// new bytes under the previous manifest (content puts succeeded, the manifest
// put did not), and so can an outside change to an object; pull readers then
// fail their digest check. An incremental push verifies every object it intends
// to skip against its stored SHA256 checksum, so the next push repairs either
// case. Every put carries the manifest's SHA256 and size, so S3 rejects a file
// that changed after the manifest was built instead of publishing bytes the
// manifest does not describe. Push is POSIX-only — on Windows the recorded mode bits would be
// synthetic.
package push

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"golang.org/x/sync/errgroup"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// S3API is the slice of the S3 client Push needs; *s3.Client satisfies it.
type S3API interface {
	PutObject(ctx context.Context, in *s3.PutObjectInput,
		opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput,
		opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, in *s3.HeadObjectInput,
		opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// Options configures Push and DryRun.
type Options struct {
	Parallel    int          // concurrent uploads and verifying HEADs; 0 means 16
	Logger      *slog.Logger // nil means discard
	ManifestKey string       // "" means <prefix>/manifest.json
	Full        bool         // re-upload every file, skipping the remote-manifest diff
	Adopt       bool         // publish only the manifest, claiming the remote tree without diffing or uploading content
}

// Stats reports what Push did (the manifest itself is not counted in Uploaded).
type Stats struct {
	Uploaded int // content files uploaded: new, changed and repaired
	Repaired int // of Uploaded, files the remote manifest already listed whose object failed verification
	Skipped  int // content files unchanged since the remote manifest, verified by checksum; with Adopt, every file, unverified
	Total    int // manifest entry count
}

// Plan is the read-only preview DryRun returns: what Push would do without
// writing to S3. Upload, Repair and Orphan are sorted lexically.
type Plan struct {
	Upload       []string // local paths that are new, changed or repaired (all paths with Full)
	Repair       []string // subset of Upload: listed unchanged in the remote manifest, but the object failed verification
	Orphan       []string // remote-manifest paths with no local counterpart
	Unchanged    int      // local paths already up to date, verified by checksum
	Total        int      // local manifest entry count
	ManifestOnly bool     // Adopt preview: only the manifest would be published
}

// goos is a test seam for the Windows guard.
var goos = runtime.GOOS

// Push hashes root, validates the resulting manifest, and uploads to
// <prefix>/<path> the content files that are new or changed relative to the
// remote manifest. It also uploads any file the remote manifest lists as
// unchanged whose object does not verify: before skipping a file, Push HEADs
// its object and requires the stored SHA256 checksum and size to match the
// manifest entry. A push that died between content and manifest, or an outside
// change to an object, is therefore repaired by the next push. Only after every
// content upload succeeds does Push upload the manifest to Options.ManifestKey
// (default <prefix>/manifest.json), and only if it differs from the remote one.
// An unchanged, verified tree is a no-op that uploads nothing. Options.Full
// forces a complete re-upload without the diff or the HEADs.
func Push(ctx context.Context, client S3API, bucket, prefix, root string, opts Options) (Stats, error) {
	logger, parallel := resolve(opts)
	prefix = strings.TrimSuffix(prefix, "/")

	m, manifestKey, mbytes, err := prepare(prefix, root, opts.ManifestKey)
	if err != nil {
		return Stats{}, err
	}
	total := len(m.Files)

	// Adopt claims an existing remote tree as the current snapshot by publishing
	// only the manifest: no diff, no content loop, no GetObject or HeadObject.
	if opts.Adopt {
		if err := putManifest(ctx, client, bucket, manifestKey, mbytes); err != nil {
			return Stats{}, err
		}
		logger.Info("push adopted", "files", total, "bucket", bucket, "prefix", prefix)
		return Stats{Skipped: total, Total: total}, nil
	}

	// Diff against the remote manifest unless -full forces a complete re-upload.
	// A byte-identical remote manifest needs no rewrite; if every skipped object
	// also verified, the push is a true no-op. Any manifest change, including
	// mode-only edits or deletions that leave content untouched, republishes it.
	todo, st, putM := m.Files, Stats{Total: total}, true
	if !opts.Full {
		d, err := diffRemote(ctx, client, bucket, prefix, manifestKey, mbytes, m.Files, parallel, logger)
		if err != nil {
			return Stats{}, err
		}
		if d.sameManifest && len(d.upload) == 0 {
			logger.Info("push up to date", "files", total, "bucket", bucket, "prefix", prefix)
			return Stats{Skipped: total, Total: total}, nil
		}
		todo, putM = d.upload, !d.sameManifest
		st.Skipped, st.Repaired = d.verified, len(d.repair)
	}

	if err := uploadFiles(ctx, client, bucket, prefix, root, todo, parallel, logger); err != nil {
		return Stats{}, err
	}
	st.Uploaded = len(todo)
	if putM {
		if err := putManifest(ctx, client, bucket, manifestKey, mbytes); err != nil {
			return Stats{}, err
		}
	}
	logger.Info("push complete", "uploaded", st.Uploaded, "repaired", st.Repaired, "skipped", st.Skipped,
		"total", total, "manifest", putM, "bucket", bucket, "prefix", prefix)
	return st, nil
}

// DryRun previews what Push would do without writing to S3. It builds the
// local manifest and applies the same Windows and collision guards as Push,
// then — unless Adopt is set — fetches the remote manifest once and HEADs each
// object it lists as unchanged, as Push does. Each path is then classified as
// an upload (new, changed, or a repair whose object failed verification), an
// orphan (present remotely but gone locally), or unchanged. With Full it
// reports every local path as an upload and makes no S3 calls, so orphans are
// not computed. With Adopt it makes no S3 calls at all, reporting only the
// manifest it would publish. DryRun never performs a PutObject.
func DryRun(ctx context.Context, client S3API, bucket, prefix, root string, opts Options) (Plan, error) {
	logger, parallel := resolve(opts)
	prefix = strings.TrimSuffix(prefix, "/")
	m, manifestKey, mbytes, err := prepare(prefix, root, opts.ManifestKey)
	if err != nil {
		return Plan{}, err
	}
	total := len(m.Files)

	if opts.Adopt {
		return Plan{Total: total, ManifestOnly: true}, nil
	}
	files := m.Files
	var d diff
	if !opts.Full {
		d, err = diffRemote(ctx, client, bucket, prefix, manifestKey, mbytes, m.Files, parallel, logger)
		if err != nil {
			return Plan{}, err
		}
		files = d.upload
	}
	upload := make([]string, 0, len(files))
	for _, f := range files {
		upload = append(upload, f.Path)
	}
	slices.Sort(upload)
	return Plan{Upload: upload, Repair: d.repair, Orphan: d.orphan, Unchanged: d.verified, Total: total}, nil
}

// resolve applies the Options defaults: a discarding logger and 16 workers.
func resolve(opts Options) (*slog.Logger, int) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	parallel := opts.Parallel
	if parallel <= 0 {
		parallel = 16
	}
	return logger, parallel
}

// prepare runs the shared Push/DryRun preamble — the Windows guard, the
// manifest build, and the manifest-key collision guard — and returns the
// manifest, its resolved key, and its encoded bytes. It touches no network:
// nothing is uploaded and nothing is fetched. prefix must already be trimmed.
func prepare(prefix, root, manifestKey string) (*manifest.Manifest, string, []byte, error) {
	if goos == "windows" {
		return nil, "", nil, errors.New("push: not supported on Windows (POSIX mode bits would be synthetic)")
	}
	m, err := manifest.Build(root)
	if err != nil {
		return nil, "", nil, err
	}
	key, err := resolveManifestKey(prefix, manifestKey, m.Files)
	if err != nil {
		return nil, "", nil, err
	}
	var buf bytes.Buffer
	if err := m.Encode(&buf); err != nil {
		return nil, "", nil, err
	}
	return m, key, buf.Bytes(), nil
}

// putManifest uploads the encoded manifest to manifestKey as a single-part put
// carrying its precomputed full-object SHA256 checksum and length.
func putManifest(ctx context.Context, client S3API, bucket, manifestKey string, mbytes []byte) error {
	sum := sha256.Sum256(mbytes)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(manifestKey),
		Body:              bytes.NewReader(mbytes),
		ContentLength:     aws.Int64(int64(len(mbytes))),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256:    aws.String(base64.StdEncoding.EncodeToString(sum[:])),
	}); err != nil {
		return fmt.Errorf("push: uploading %s: %w", manifest.Name, err)
	}
	return nil
}

// resolveManifestKey returns the manifest key (default <prefix>/manifest.json).
// A custom key that names an uploaded content file would let the manifest
// overwrite that file (or vice versa), breaking the atomic-snapshot invariant,
// so it is rejected before anything is uploaded and nothing lands on failure.
func resolveManifestKey(prefix, custom string, files []manifest.File) (string, error) {
	if custom == "" {
		return path.Join(prefix, manifest.Name), nil
	}
	for _, f := range files {
		if path.Join(prefix, f.Path) == custom {
			return "", fmt.Errorf("push: manifest key %q collides with a content file; choose a location outside the uploaded tree", custom)
		}
	}
	return custom, nil
}

// diff is the result of comparing the local manifest with the remote snapshot.
type diff struct {
	upload       []manifest.File // new, changed and repaired files, in local manifest order
	repair       []string        // sorted paths in upload that the remote manifest listed unchanged but failed verification
	orphan       []string        // sorted remote-manifest paths with no local counterpart
	verified     int             // files skipped: same SHA256 in the remote manifest and a verified object
	sameManifest bool            // the remote manifest is byte-identical to local
}

// diffRemote fetches the remote manifest at manifestKey and classifies every
// local file. A file whose path and SHA256 match the remote manifest is only a
// candidate for skipping: its object is HEADed (at most parallel in flight) and
// skipped only if it verifies, otherwise it is a logged repair. With no remote
// manifest (first push) every file is an upload.
func diffRemote(ctx context.Context, client S3API, bucket, prefix, manifestKey string, local []byte,
	files []manifest.File, parallel int, logger *slog.Logger,
) (diff, error) {
	rm, err := remoteManifest(ctx, client, bucket, manifestKey)
	if err != nil {
		return diff{}, err
	}
	var d diff
	remote := map[string]string{}
	if rm != nil {
		var rbuf bytes.Buffer
		if err = rm.Encode(&rbuf); err != nil {
			return diff{}, err
		}
		d.sameManifest = bytes.Equal(local, rbuf.Bytes())
		for _, f := range rm.Files {
			remote[f.Path] = f.SHA256
		}
	}

	var candidates []manifest.File
	for _, f := range files {
		if remote[f.Path] == f.SHA256 {
			candidates = append(candidates, f)
		} else {
			d.upload = append(d.upload, f)
		}
		delete(remote, f.Path)
	}
	for p := range remote {
		d.orphan = append(d.orphan, p)
	}
	slices.Sort(d.orphan)

	verdicts, err := verifyObjects(ctx, client, bucket, prefix, candidates, parallel)
	if err != nil {
		return diff{}, err
	}
	for i, f := range candidates {
		v := verdicts[i]
		if v.reason == "" {
			d.verified++
			logger.Debug("unchanged", "path", f.Path)
			continue
		}
		attrs := []any{"path", f.Path, "reason", v.reason}
		if v.cause != nil {
			attrs = append(attrs, "error", v.cause.Error())
		}
		logger.Warn("remote object failed verification", attrs...)
		d.upload = append(d.upload, f)
		d.repair = append(d.repair, f.Path)
	}
	slices.Sort(d.repair)
	return d, nil
}

// verdict is one verifying HEAD's outcome. reason is "" when the object
// verifies, otherwise why it must be re-uploaded; cause is the HEAD error
// behind a reasonForbidden, logged so a real permission problem stays visible.
type verdict struct {
	reason string
	cause  error
}

// Verification reasons that need more than a word.
const (
	reasonForbidden = "cannot verify (HEAD returned 403; the object may be missing or the caller may lack read access)"
	reasonComposite = "composite checksum from a multipart upload; re-uploading as a single part"
)

// verifyObjects HEADs each file's object, at most parallel in flight, and
// returns per-file verdicts.
func verifyObjects(ctx context.Context, client S3API, bucket, prefix string, files []manifest.File, parallel int) ([]verdict, error) {
	verdicts := make([]verdict, len(files))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallel)
	for i, f := range files {
		g.Go(func() error {
			v, err := verifyObject(gctx, client, bucket, path.Join(prefix, f.Path), f)
			verdicts[i] = v
			return err
		})
	}
	return verdicts, g.Wait()
}

// verifyObject HEADs key with checksum mode enabled and reports an empty
// verdict when its stored full-object SHA256 and size match f, or the reason
// they do not: missing, a 403 (which S3 also returns for a missing key when the
// caller lacks s3:ListBucket), size mismatch, no checksum (an upload without a
// SHA256 checksum algorithm, so nothing to trust), a composite checksum, or
// checksum mismatch. Any other HEAD failure is returned wrapped, noting that
// -full skips the diff.
//
// CEILING: pinsync uploads single-part only, so objects are limited to 5 GB
// and a composite (multipart) checksum is always repaired by re-upload rather
// than verified. The upgrade path is a fixed-part-size multipart putFile plus
// composite verification via GetObjectAttributes(ObjectParts).
func verifyObject(ctx context.Context, client S3API, bucket, key string, f manifest.File) (verdict, error) {
	out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		var missing *types.NotFound
		switch {
		case errors.As(err, &missing):
			return verdict{reason: "missing"}, nil
		case forbidden(err):
			return verdict{reason: reasonForbidden, cause: err}, nil
		}
		return verdict{}, fmt.Errorf("push: verifying remote %s (pass -full to skip this diff): %w", key, err)
	}
	if aws.ToInt64(out.ContentLength) != f.Size {
		return verdict{reason: "size mismatch"}, nil
	}
	got := aws.ToString(out.ChecksumSHA256)
	if got == "" {
		return verdict{reason: "no checksum"}, nil
	}
	// Standard base64 has no '-', so one marks a composite "<sum>-<parts>".
	if out.ChecksumType == types.ChecksumTypeComposite || strings.Contains(got, "-") {
		return verdict{reason: reasonComposite}, nil
	}
	want, err := b64SHA256(f)
	if err != nil {
		return verdict{}, err
	}
	if got != want {
		return verdict{reason: "checksum mismatch"}, nil
	}
	return verdict{}, nil
}

// forbidden reports whether err is an HTTP 403 or an S3 Forbidden/AccessDenied
// error code.
func forbidden(err error) bool {
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.Response != nil && re.HTTPStatusCode() == http.StatusForbidden {
		return true
	}
	var ae smithy.APIError
	return errors.As(err, &ae) && (ae.ErrorCode() == "Forbidden" || ae.ErrorCode() == "AccessDenied")
}

// b64SHA256 returns f's manifest SHA256 in the standard base64 form S3 uses
// for ChecksumSHA256.
func b64SHA256(f manifest.File) (string, error) {
	sum, err := hex.DecodeString(f.SHA256)
	if err != nil {
		return "", fmt.Errorf("push: manifest SHA256 for %s: %w", f.Path, err)
	}
	return base64.StdEncoding.EncodeToString(sum), nil
}

// remoteManifest fetches and decodes the manifest at manifestKey to diff
// against. A missing manifest (first push) returns nil, nil; any other failure
// is returned wrapped, noting that -full skips this fetch.
func remoteManifest(ctx context.Context, client S3API, bucket, manifestKey string) (*manifest.Manifest, error) {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(manifestKey),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		var missing *types.NoSuchKey
		if errors.As(err, &missing) {
			return nil, nil
		}
		return nil, fmt.Errorf("push: fetching remote %s (pass -full to skip this diff): %w", manifestKey, err)
	}
	defer func() { _ = out.Body.Close() }()
	m, err := manifest.Decode(out.Body)
	if err != nil {
		return nil, fmt.Errorf("push: reading remote %s (pass -full to skip this diff): %w", manifestKey, err)
	}
	return m, nil
}

// uploadFiles uploads every file, with at most parallel in flight.
func uploadFiles(ctx context.Context, client S3API, bucket, prefix, root string,
	files []manifest.File, parallel int, logger *slog.Logger,
) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallel)
	for _, f := range files {
		g.Go(func() error {
			if err := putFile(gctx, client, bucket, prefix, root, f); err != nil {
				return err
			}
			logger.Debug("uploaded", "path", f.Path, "size", f.Size)
			return nil
		})
	}
	return g.Wait()
}

// putFile uploads one content file as a single-part put carrying the manifest's
// SHA256 and size rather than letting the SDK hash the file as it is now, so S3
// rejects bytes that changed after the manifest was built before replacing the
// object.
func putFile(ctx context.Context, client S3API, bucket, prefix, root string, f manifest.File) error {
	sum, err := b64SHA256(f)
	if err != nil {
		return err
	}
	fh, err := os.Open(filepath.Join(root, filepath.FromSlash(f.Path)))
	if err != nil {
		return err
	}
	defer func() { _ = fh.Close() }() // read-only; nothing to do about a close error
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(path.Join(prefix, f.Path)),
		Body:              fh,
		ContentLength:     aws.Int64(f.Size),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256:    aws.String(sum),
	}); err != nil {
		return fmt.Errorf("push: uploading %s: %w", f.Path, err)
	}
	return nil
}
