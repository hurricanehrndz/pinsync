package awsclient

import (
	"context"
	"regexp"
	"testing"

	"github.com/hurricanehrndz/pinsync/pkg/rolesanywhere"
)

// TestLoadRolesAnywhereFailsEarly pins that Load retrieves Roles Anywhere
// credentials before returning. The SDK cache would otherwise defer the first
// exchange to the first S3 request, so a missing certificate would surface
// mid-command instead of before any work starts. The pattern can match no
// string (a position cannot be both a word boundary and not one), so this
// fails at selection on macOS and Windows and at the stub elsewhere, and never
// signs with a real key or contacts AWS.
func TestLoadRolesAnywhereFailsEarly(t *testing.T) {
	_, err := Load(context.Background(), Config{RolesAnywhere: &RAConfig{
		TrustAnchorARN: "arn:aws:rolesanywhere:us-west-2:111122223333:trust-anchor/abc",
		ProfileARN:     "arn:aws:rolesanywhere:us-west-2:111122223333:profile/def",
		RoleARN:        "arn:aws:iam::111122223333:role/ra",
		CertPattern:    regexp.MustCompile(`\b\B`),
		CertField:      rolesanywhere.FieldSubject,
		CertStore:      rolesanywhere.StoreUser,
	}})
	if err == nil {
		t.Fatal("Load succeeded with no matching certificate; want the selection error up front")
	}
}
