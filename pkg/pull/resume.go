package pull

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/hurricanehrndz/pinsync/pkg/manifest"
)

// resumeDir is the directory at the root of the staging tree, and so of the
// partial tree it becomes, that holds downloads cut off mid-object for the
// next pull to continue. It is removed before the staged tree is verified,
// so it never reaches dest. A manifest with files of its own under this name
// turns resuming off for that pull (see resumeSlots).
const resumeDir = ".pinsync-resume"

// minResumeSize is the smallest object whose cut-off download is kept to
// resume. Below it a restart costs little more than the ranged GET and local
// re-hash a resume needs, so smaller downloads go straight to their staged
// path. The limit: an object under 1 MiB that cannot arrive within one run
// never arrives; lower this if links that slow matter.
const minResumeSize = 1 << 20

// errDiscardPrefix marks a kept prefix that cannot be shown to belong to the
// object; the caller discards it and downloads the whole object instead.
var errDiscardPrefix = errors.New("kept prefix does not continue the object")

// errPrefixStuck marks a prefix file that cannot be created, renamed or
// removed (an open handle on Windows, an ACL). The caller downloads the
// object without resuming instead, so the stuck file costs its resume, not
// the pull, however many pulls it outlives (but see clearResumeDir).
var errPrefixStuck = errors.New("cannot use the kept partial download")

// stuck wraps a failure of the prefix files in errPrefixStuck.
func stuck(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %w", errPrefixStuck, err)
}

// prefixInfo is a kept prefix's sidecar: the manifest content its bytes are
// the start of, and the object they came from. Its length is the data file's
// own; every byte is re-hashed before anything trusts it.
type prefixInfo struct {
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
	ETag      string `json:"etag"`
	VersionID string `json:"version_id,omitempty"`
}

// prefixFiles names one content's prefix files in a tree's resume directory:
// the data, its sidecar, and the data's previous name while it is copied
// forward (see extend). Content-addressed, so a prefix outlives a rename of
// its entry; resumeSlots gives each content to one entry at a time.
type prefixFiles struct{ data, prev, info string }

func prefixFilesIn(root, sum string) prefixFiles {
	base := filepath.Join(root, resumeDir, sum)
	return prefixFiles{data: base, prev: base + ".prev", info: base + ".json"}
}

// resumeSlots returns a function reporting whether an entry about to be
// downloaded may keep and continue a prefix: it must be at least
// minResumeSize and the first entry with its content. A manifest with its own
// files under resumeDir resumes nothing, rather than mixing them with
// prefixes.
func resumeSlots(m *manifest.Manifest) func(manifest.File) bool {
	for _, f := range m.Files {
		if first, _, _ := strings.Cut(f.Path, "/"); strings.EqualFold(first, resumeDir) {
			return func(manifest.File) bool { return false }
		}
	}
	claimed := map[string]bool{}
	return func(f manifest.File) bool {
		if f.Size < minResumeSize || claimed[f.SHA256] {
			return false
		}
		claimed[f.SHA256] = true
		return true
	}
}

// adoptPrefixes moves the prefixes the jobs can continue from the partial
// tree into the staging tree, just before stage removes the partial tree;
// prefixes no job wants go with it, so disk use stays within one manifest's
// content. A file that cannot be moved only costs its resume.
func (s *syncer) adoptPrefixes(jobs []job) {
	for _, j := range jobs {
		if !j.resume {
			continue
		}
		from, to := prefixFilesIn(s.partial, j.f.SHA256), prefixFilesIn(s.tmp, j.f.SHA256)
		for _, mv := range [][2]string{{from.data, to.data}, {from.prev, to.prev}, {from.info, to.info}} {
			if !exists(mv[0]) {
				continue
			}
			err := os.MkdirAll(filepath.Dir(mv[1]), 0o755)
			if err == nil {
				err = renameFn(mv[0], mv[1])
			}
			if err != nil {
				s.logger.Debug("could not keep a partial download", "path", j.f.Path, "error", err)
			}
		}
	}
}

// inspect reports, without changing anything, the prefix kept for f and its
// length: its sidecar must name f's content and an ETag, and its data must
// be a regular file of 1 to f.Size bytes. p.prev, if present, is the data
// (see extend).
func (p prefixFiles) inspect(f manifest.File) (int64, prefixInfo, bool) {
	data := p.data
	if exists(p.prev) {
		data = p.prev
	}
	fh, err := os.Open(p.info)
	if err != nil {
		return 0, prefixInfo{}, false
	}
	raw, err := io.ReadAll(io.LimitReader(fh, 4096))
	_ = fh.Close() // read-only; nothing to do about a close error
	var info prefixInfo
	if err != nil || json.Unmarshal(raw, &info) != nil ||
		info.SHA256 != f.SHA256 || info.Size != f.Size || info.ETag == "" {
		return 0, prefixInfo{}, false
	}
	st, err := os.Lstat(data)
	if err != nil || !st.Mode().IsRegular() || st.Size() == 0 || st.Size() > f.Size {
		return 0, prefixInfo{}, false
	}
	return st.Size(), info, true
}

// load is inspect after moving a p.prev back to p.data: a pull killed while
// copying forward leaves p.prev whole and p.data incomplete.
func (p prefixFiles) load(f manifest.File) (int64, prefixInfo, bool) {
	if exists(p.prev) {
		if err := renameFn(p.prev, p.data); err != nil {
			return 0, prefixInfo{}, false
		}
	}
	return p.inspect(f)
}

// discard removes every prefix file, so a fresh download starts from none.
func (p prefixFiles) discard() error {
	for _, name := range []string{p.data, p.prev, p.info, p.info + ".tmp"} {
		if err := removeFn(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

// writeInfo writes the sidecar atomically: a kill leaves the old one, the new
// one, or a stray .tmp that discard removes.
func (p prefixFiles) writeInfo(info prefixInfo) error {
	raw, err := json.Marshal(info)
	if err != nil {
		return err
	}
	tmp := p.info + ".tmp"
	if err = removeFn(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	fh, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, err = fh.Write(raw)
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return renameFn(tmp, p.info)
}

// install moves the verified data to its staged path dst, drops the sidecar
// and applies the entry's mode. A sidecar that cannot be removed is left: it
// is never trusted without its data, and stage clears the resume directory
// (see clearResumeDir). Only a failed chmod, as for any staged file, is an
// error once dst exists.
func (p prefixFiles) install(dst string, mode fs.FileMode) error {
	if err := renameFn(p.data, dst); err != nil {
		return stuck(err)
	}
	_ = removeFn(p.info)
	return chmod(dst, mode)
}

// rangedGet stops the SDK asking for a full-object checksum on a ranged GET.
// It validates one only on a 200 (a 206 never carries the object's
// checksum), so ChecksumMode is left unset and the SDK default
// (WhenSupported), which would set it anyway, is lowered for the call. The
// manifest's SHA256 over prefix and remainder is the check.
func rangedGet(o *s3.Options) {
	o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
}

// rangeRefused reports a ranged, conditional GET the server turned down: 412
// (the object changed since the prefix was fetched) or 416 (the range is past
// its end).
func rangeRefused(err error) bool {
	var re interface{ HTTPStatusCode() int }
	if errors.As(err, &re) && (re.HTTPStatusCode() == 412 || re.HTTPStatusCode() == 416) {
		return true
	}
	var ae smithy.APIError
	return errors.As(err, &ae) && (ae.ErrorCode() == "PreconditionFailed" || ae.ErrorCode() == "InvalidRange")
}

// downloadResumable downloads an object large enough to resume (see
// resumeSlots) through a prefix file in the staging tree's resume directory,
// which a failure leaves for the next pull (see keepStaged). A kept prefix is
// continued with a ranged GET; when it cannot be shown to belong to the
// object, or the result fails the hash, it is discarded and the whole object
// downloaded in the same call. When the prefix files themselves cannot be
// created, renamed or removed, the object is downloaded whole straight to
// dst, as one below minResumeSize is. It reports whether f was completed
// from a prefix.
func (s *syncer) downloadResumable(ctx context.Context, f manifest.File, dst string, mode fs.FileMode) (bool, error) {
	resumed, err := s.resumeOrFetch(ctx, f, dst, mode)
	if !errors.Is(err, errPrefixStuck) {
		return resumed, err
	}
	s.logger.Warn("cannot keep a partial download; downloading it without resuming", "path", f.Path, "error", err)
	return false, s.downloadWhole(ctx, f, dst, mode)
}

// resumeOrFetch is downloadResumable through the prefix files. An error
// wrapping errPrefixStuck leaves dst absent.
func (s *syncer) resumeOrFetch(ctx context.Context, f manifest.File, dst string, mode fs.FileMode) (bool, error) {
	key := path.Join(s.prefix, f.Path)
	p := prefixFilesIn(s.tmp, f.SHA256)
	if err := os.MkdirAll(filepath.Dir(p.data), 0o755); err != nil {
		return false, stuck(err)
	}
	var whole *s3.GetObjectOutput
	if n, info, ok := p.load(f); ok {
		var err error
		whole, err = s.continuePrefix(ctx, key, f, p, n, info, dst, mode)
		switch {
		case err == nil && whole == nil:
			return true, nil
		case err != nil && !errors.Is(err, errDiscardPrefix):
			return false, err
		}
		reason := "the server ignored the range"
		if err != nil {
			reason = err.Error()
		}
		s.logger.Info("discarding a partial download; fetching it whole", "path", f.Path, "reason", reason)
	}
	if err := p.discard(); err != nil {
		if whole != nil {
			_ = whole.Body.Close()
		}
		return false, stuck(err)
	}
	return false, s.fetchWhole(ctx, key, f, p, whole, dst, mode)
}

// continuePrefix fetches the rest of f after a prefix of n bytes with a GET
// for bytes=n- conditional on the prefix's ETag, and completes the file with
// extend. It returns nil when f is staged from the prefix; the server's
// output when it ignored the Range and sent the whole object; an error
// wrapping errDiscardPrefix when the prefix cannot be shown to continue the
// object or the result fails the hash; one wrapping errPrefixStuck when the
// prefix files cannot be used; and any other error (the pull fails, the
// prefix is kept) as is.
func (s *syncer) continuePrefix(ctx context.Context, key string, f manifest.File, p prefixFiles, n int64,
	info prefixInfo, dst string, mode fs.FileMode,
) (*s3.GetObjectOutput, error) {
	if n == f.Size { // killed before install: nothing left to GET
		return nil, extendErr(key, p.extend(f, n, strings.NewReader(""), dst, mode))
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket:  aws.String(s.bucket),
		Key:     aws.String(key),
		Range:   aws.String(fmt.Sprintf("bytes=%d-", n)),
		IfMatch: aws.String(info.ETag),
	}, rangedGet)
	if err != nil {
		if rangeRefused(err) {
			return nil, fmt.Errorf("%w: %w", errDiscardPrefix, err)
		}
		return nil, fmt.Errorf("pull: fetching %s: %w", key, err)
	}
	if out.ContentRange == nil && aws.ToInt64(out.ContentLength) == f.Size {
		return out, nil
	}
	defer func() { _ = out.Body.Close() }()
	if err := checkContinues(out, f, n, info); err != nil {
		return nil, err
	}
	return nil, extendErr(key, p.extend(f, n, out.Body, dst, mode))
}

// extendErr names key in an error from extend, leaving errDiscardPrefix
// bare for the caller to recognise.
func extendErr(key string, err error) error {
	if err == nil || errors.Is(err, errDiscardPrefix) {
		return err
	}
	return fmt.Errorf("pull: resuming %s: %w", key, err)
}

// checkContinues accepts a ranged response only for exactly the bytes after
// the prefix, of the same object.
func checkContinues(out *s3.GetObjectOutput, f manifest.File, n int64, info prefixInfo) error {
	want := fmt.Sprintf("bytes %d-%d/%d", n, f.Size-1, f.Size)
	switch vid := aws.ToString(out.VersionId); {
	case aws.ToString(out.ContentRange) != want:
		return fmt.Errorf("%w: Content-Range %q, want %q", errDiscardPrefix, aws.ToString(out.ContentRange), want)
	case aws.ToString(out.ETag) != info.ETag:
		return fmt.Errorf("%w: ETag %s, the prefix is from %s", errDiscardPrefix, aws.ToString(out.ETag), info.ETag)
	case vid != "" && info.VersionID != "" && vid != info.VersionID:
		return fmt.Errorf("%w: version %s, the prefix is from %s", errDiscardPrefix, vid, info.VersionID)
	}
	return nil
}

// extend completes a prefix of n bytes with rest, the object's bytes from
// offset n, and installs it at dst. It never writes into the prefix file,
// which another name may share: the prefix moves to p.prev and is copied
// forward into a new p.data, so a kill at any point leaves either p.prev
// (which load prefers) or a p.data that is again a prefix. The prefix is
// hashed together with rest; a mismatch wraps errDiscardPrefix, and a
// prefix file that cannot be renamed, created, read or removed wraps
// errPrefixStuck.
// CEILING: the copy rewrites up to one object's bytes locally per resume;
// far cheaper than the network, and the read is needed for the hash anyway.
// While it runs, p.prev and p.data together take up to twice the prefix on
// disk, which MaxContentBytes does not count; one object at most.
func (p prefixFiles) extend(f manifest.File, n int64, rest io.Reader, dst string, mode fs.FileMode) error {
	if err := renameFn(p.data, p.prev); err != nil {
		return stuck(err)
	}
	fh, err := createData(p.data, mode)
	if err != nil {
		return stuck(err)
	}
	h := sha256.New()
	err = copyPrefix(fh, p.prev, n, h)
	if err == nil {
		// From here a cut-off leaves p.data as the one, longer, prefix.
		err = removeFn(p.prev)
	}
	if err != nil && !errors.Is(err, errDiscardPrefix) {
		err = stuck(err)
	}
	var got int64
	if err == nil {
		got, err = io.Copy(fh, io.TeeReader(io.LimitReader(rest, f.Size-n+1), h))
	}
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := checkStaged(f, n+got, h); err != nil {
		return fmt.Errorf("%w: %v", errDiscardPrefix, err) //nolint:errorlint // must not read as a staged mismatch
	}
	return p.install(dst, mode)
}

// copyPrefix appends the first n bytes of src to fh through h, closing src
// before it returns so src can be removed on Windows.
func copyPrefix(fh io.Writer, src string, n int64, h hash.Hash) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }() // read-only; nothing to do about a close error
	copied, err := io.Copy(fh, io.TeeReader(io.LimitReader(in, n), h))
	if err == nil && copied != n {
		err = fmt.Errorf("%w: it shrank to %d bytes", errDiscardPrefix, copied)
	}
	return err
}

// fetchWhole downloads all of f into p.data, from whole if the server
// already sent it, recording the object's ETag in the sidecar first so a
// cut-off leaves a prefix the next pull can continue. Without an ETag there
// is nothing to make that GET conditional on, so it is written like a small
// download. A mismatch is a staged mismatch, as for any whole download;
// prefix files that cannot be written wrap errPrefixStuck.
func (s *syncer) fetchWhole(ctx context.Context, key string, f manifest.File, p prefixFiles,
	whole *s3.GetObjectOutput, dst string, mode fs.FileMode,
) error {
	out := whole
	if out == nil {
		var err error
		if out, err = s.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket:       aws.String(s.bucket),
			Key:          aws.String(key),
			ChecksumMode: types.ChecksumModeEnabled,
		}); err != nil {
			return fmt.Errorf("pull: fetching %s: %w", key, err)
		}
	}
	defer func() { _ = out.Body.Close() }()
	if aws.ToString(out.ETag) == "" {
		if err := writeVerified(out.Body, f, dst, mode); err != nil {
			return fmt.Errorf("pull: downloading %s: %w", key, err)
		}
		return nil
	}
	err := p.writeInfo(prefixInfo{
		SHA256: f.SHA256, Size: f.Size, ETag: aws.ToString(out.ETag), VersionID: aws.ToString(out.VersionId),
	})
	if err != nil {
		return stuck(err)
	}
	fh, err := createData(p.data, mode)
	if err != nil {
		return stuck(err)
	}
	h := sha256.New()
	n, err := io.Copy(fh, io.TeeReader(io.LimitReader(out.Body, f.Size+1), h))
	if cerr := fh.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("pull: downloading %s: %w", key, err)
	}
	if err := checkStaged(f, n, h); err != nil {
		_ = p.discard() // known-bad bytes; never a prefix
		return fmt.Errorf("pull: downloading %s: %w", key, err)
	}
	return p.install(dst, mode)
}

// createData creates a prefix data file. Never an existing one: the name may
// be a hardlink to a file elsewhere. It is owner-writable so it can be
// removed on Windows; install applies the exact mode.
func createData(name string, mode fs.FileMode) (*os.File, error) {
	return os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode|0o200)
}
