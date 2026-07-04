package push

import (
	"context"
	"strings"
	"testing"

	"github.com/hurricanehrndz/pinsync/internal/s3test"
)

func TestPushRefusesWindows(t *testing.T) {
	orig := goos
	goos = "windows"
	t.Cleanup(func() { goos = orig })

	fake := s3test.NewFake()
	_, err := Push(context.Background(), fake, "bkt", "p", t.TempDir(), Options{})
	if err == nil || !strings.Contains(err.Error(), "Windows") {
		t.Fatalf("Push on windows = %v, want refusal naming Windows", err)
	}
	if puts := fake.Puts(); len(puts) != 0 {
		t.Errorf("Push attempted uploads on Windows: %v", puts)
	}
}
