package pull

import (
	"context"
	"slices"
	"strings"
)

// Plan is the read-only preview DryRun returns: what Pull would do without
// touching the live tree. Download, Copy, and Remove are sorted lexically;
// Linked counts the entries that already match (content and mode) and would be
// staged as a hardlink; Total is the manifest entry count. Download lists every
// entry whose content changes, including the Reused ones that the partial tree
// kept from a failed pull would supply without a GET; Stats.Downloaded, unlike
// Download, leaves those out.
type Plan struct {
	Download, Copy, Remove []string
	Linked, Reused, Total  int
}

// DryRun previews what Pull would converge dest to, without writing anything:
// it fetches the manifest once, re-hashes the live tree and any partial tree,
// and classifies every entry with the same decision real staging uses
// (download / reuse / copy / hardlink), plus the live paths absent from the
// manifest that Pull would remove at swap. It performs at most one GetObject
// (the manifest — never content), creates or changes no sibling tree, and
// skips crash recovery: leftover state from an interrupted pull is reported as
// a warning rather than repaired, so the preview may be inaccurate until the
// next real pull recovers.
func DryRun(ctx context.Context, client S3API, bucket, prefix, dest string, opts Options) (Plan, error) {
	s, err := newSyncer(dest, opts)
	if err != nil {
		return Plan{}, err
	}
	s.client, s.bucket, s.prefix = client, bucket, strings.TrimSuffix(prefix, "/")

	// Mirror recoverInterrupted's trigger conditions without acting on them: a
	// stale staging tree or a leftover old tree both mean a previous pull was
	// interrupted. A read-only preview cannot recover, only warn.
	if exists(s.tmp) || exists(s.old) {
		s.logger.Warn("a previous pull was interrupted; this preview may be inaccurate until the next pull recovers",
			"tmp", s.tmp, "old", s.old)
	}

	_, m, err := s.fetchManifest(ctx)
	if err != nil {
		return Plan{}, err
	}
	// A missing dest yields an empty live map (first pull); hashTree handles it.
	live, err := hashTree(s.dest)
	if err != nil {
		return Plan{}, err
	}
	partial := s.hashPartial()

	var plan Plan
	inManifest := make(map[string]struct{}, len(m.Files))
	for _, f := range m.Files {
		inManifest[f.Path] = struct{}{}
		act, _, err := classify(f, live, partial)
		if err != nil {
			return Plan{}, err
		}
		switch act {
		case actDownload:
			plan.Download = append(plan.Download, f.Path)
		case actReuse:
			plan.Download = append(plan.Download, f.Path)
			plan.Reused++
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
