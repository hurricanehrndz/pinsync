package pull

import (
	"context"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
)

// Plan is the read-only preview DryRun returns: what Pull would do without
// touching the live tree. Download, Copy, and Remove are sorted lexically;
// Linked counts the entries that already match (content and mode) and would be
// staged as a hardlink; Total is the manifest entry count.
type Plan struct {
	Download, Copy, Remove []string
	Linked, Total          int
}

// DryRun previews what Pull would converge dest to, without writing anything:
// it fetches the manifest once, re-hashes the live tree, and classifies every
// entry with the same decision real staging uses (download / copy / hardlink),
// plus the live paths absent from the manifest that Pull would remove at swap.
// It performs at most one GetObject (the manifest — never content), creates no
// staging tree, and skips crash recovery: leftover state from an interrupted
// pull is reported as a warning rather than repaired, so the preview may be
// inaccurate until the next real pull recovers.
func DryRun(ctx context.Context, client S3API, bucket, prefix, dest string, opts Options) (Plan, error) {
	s := &syncer{
		client:      client,
		bucket:      bucket,
		prefix:      strings.TrimSuffix(prefix, "/"),
		dest:        filepath.Clean(dest),
		logger:      opts.Logger,
		manifestKey: opts.ManifestKey,
	}
	if s.logger == nil {
		s.logger = slog.New(slog.DiscardHandler)
	}
	s.tmp, s.old = s.dest+tmpSuffix, s.dest+oldSuffix

	// Mirror recoverInterrupted's trigger conditions without acting on them: a
	// stale staging tree or a leftover old tree both mean a previous pull was
	// interrupted. A read-only preview cannot recover, only warn.
	if exists(s.tmp) || exists(s.old) {
		s.logger.Warn("a previous pull was interrupted; this preview may be inaccurate until the next pull recovers",
			"tmp", s.tmp, "old", s.old)
	}

	m, err := s.fetchManifest(ctx)
	if err != nil {
		return Plan{}, err
	}
	// A missing dest yields an empty live map (first pull); hashTree handles it.
	live, err := hashTree(s.dest)
	if err != nil {
		return Plan{}, err
	}

	var plan Plan
	inManifest := make(map[string]struct{}, len(m.Files))
	for _, f := range m.Files {
		inManifest[f.Path] = struct{}{}
		act, _, err := classify(f, live)
		if err != nil {
			return Plan{}, err
		}
		switch act {
		case actDownload:
			plan.Download = append(plan.Download, f.Path)
		case actCopy:
			plan.Copy = append(plan.Copy, f.Path)
		case actLink:
			plan.Linked++
		}
	}
	for p := range live {
		if _, ok := inManifest[p]; !ok {
			plan.Remove = append(plan.Remove, p)
		}
	}
	slices.Sort(plan.Download)
	slices.Sort(plan.Copy)
	slices.Sort(plan.Remove)
	plan.Total = len(m.Files)
	return plan, nil
}
