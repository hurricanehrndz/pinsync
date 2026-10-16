//go:build windows

package pull_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/pull"
	"github.com/hurricanehrndz/pinsync/pkg/s3test"
)

// TestPullResumeWithPrefixHeldOpen: another process (a scanner, an indexer)
// holding the prefix open without delete sharing when the pull is cut off
// may stop the staging tree from being kept, but never leaves dest changed,
// and once the handle closes the next pull converges, resuming if the prefix
// and its sidecar survived. The pull's own handle must be closed before it
// renames anything.
func TestPullResumeWithPrefixHeldOpen(t *testing.T) {
	fake := s3test.NewFake()
	files, dest := resumeFixture(t, fake)
	tmp := dest + ".pinsync-tmp"
	data := resumeFile(tmp, files[bigPath], "")
	var release func()
	c := &recordingClient{S3API: fake, cutKey: bigKey, cut: cutAt, onCut: func() error {
		release = holdOpen(t, data)
		return errReset
	}}
	_, err := pull.Pull(context.Background(), c, "bkt", prefix, dest, pull.Options{Parallel: 1})
	if !errors.Is(err, errReset) {
		t.Fatalf("cut-off Pull = %v, want %v", err, errReset)
	}
	t.Logf("cut-off pull with the prefix held open: %v", err)
	if release == nil {
		t.Fatal("the cut never happened")
	}
	release()
	if _, err := os.Lstat(dest); err == nil {
		t.Error("a failed first pull created dest")
	}
	survived := false
	for _, root := range []string{tmp, partialDir(dest)} {
		_, dErr := os.Stat(resumeFile(root, files[bigPath], ""))
		_, sErr := os.Stat(resumeFile(root, files[bigPath], ".json"))
		if dErr == nil && sErr == nil {
			t.Logf("prefix survived in %s", filepath.Base(root))
			survived = true
		}
	}

	stats, rc := resumedPull(t, fake, dest)
	t.Logf("next pull: %+v, GETs %+v", stats, rc.gets(bigKey))
	if survived && stats.Resumed != 1 {
		t.Errorf("stats = %+v, want the surviving prefix resumed", stats)
	}
	assertTree(t, dest, files)
	assertNoLeftovers(t, dest)
	assertNoPartial(t, dest)
}
