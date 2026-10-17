package rolesanywhere

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// closeCounter wraps an Identity and counts Close calls, so a test can tell
// whether Retrieve released every handle it found.
type closeCounter struct {
	Identity
	closes *atomic.Int32
}

func (c closeCounter) Close() { c.closes.Add(1) }

func testProviderOptions(endpoint string) Options {
	return Options{
		TrustAnchorARN: "arn:aws:rolesanywhere:us-west-2:111122223333:trust-anchor/abc",
		ProfileARN:     "arn:aws:rolesanywhere:us-west-2:111122223333:profile/def",
		RoleARN:        "arn:aws:iam::111122223333:role/ra",
		baseEndpoint:   endpoint,
	}
}

// TestProviderRefreshesThroughCache pins the bug this provider exists to fix:
// a static provider reports CanExpire false, so the SDK cache never refreshes
// and a long run fails with ExpiredToken. Through aws.CredentialsCache, credentials
// inside the expiry window must trigger a fresh CreateSession, and each
// Retrieve must re-find the identity and close it.
func TestProviderRefreshesThroughCache(t *testing.T) {
	id := newRSAIdentity(t)
	// Ten minutes out, inside the fifteen-minute window below, so the cache
	// treats every result as already due for refresh.
	expiration := time.Now().Add(10 * time.Minute).UTC().Truncate(time.Second).Format(time.RFC3339)

	var sessions atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessions.Add(1)
		_, _ = io.WriteString(w, credsJSON(expiration))
	}))
	defer srv.Close()

	var finds, closes atomic.Int32
	p := NewProvider(FieldIssuer, regexp.MustCompile("^Test CA$"), StoreMachine, testProviderOptions(srv.URL))
	p.find = func(_ *slog.Logger, field CertField, _ *regexp.Regexp, loc StoreLoc) (Identity, *x509.Certificate, error) {
		finds.Add(1)
		if field != FieldIssuer || loc != StoreMachine {
			t.Errorf("find got field %v, store %v; want the provider's selection", field, loc)
		}
		return closeCounter{Identity: id, closes: &closes}, id.leaf, nil
	}
	// The provider itself must pass the upstream expiry through untouched;
	// the cache reports Expires less its window, so check before wrapping.
	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Retrieve: %v", err)
	}
	if !creds.CanExpire {
		t.Fatal("CanExpire = false; the cache would never refresh")
	}
	want, _ := time.Parse(time.RFC3339, expiration)
	if !creds.Expires.Equal(want) {
		t.Errorf("Expires = %v, want upstream %v", creds.Expires, want)
	}

	cache := aws.NewCredentialsCache(p, func(o *aws.CredentialsCacheOptions) {
		o.ExpiryWindow = 15 * time.Minute
	})
	for range 2 {
		if _, err := cache.Retrieve(context.Background()); err != nil {
			t.Fatalf("cache Retrieve: %v", err)
		}
	}

	// One direct Retrieve plus one per cache refresh.
	if got := sessions.Load(); got != 3 {
		t.Errorf("CreateSession calls = %d, want 3 (the cache must refresh each time)", got)
	}
	if got := finds.Load(); got != 3 {
		t.Errorf("identity finds = %d, want 3 (re-find on every Retrieve)", got)
	}
	if got := closes.Load(); got != 3 {
		t.Errorf("identity closes = %d, want 3 (no handle held between refreshes)", got)
	}
}

// TestProviderFetchErrorClosesIdentity ensures a rejected exchange still
// releases the identity, so repeated refresh failures on a long run do not
// leak keychain or CNG handles.
func TestProviderFetchErrorClosesIdentity(t *testing.T) {
	id := newRSAIdentity(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"message":"denied"}`, http.StatusForbidden)
	}))
	defer srv.Close()

	var closes atomic.Int32
	p := NewProvider(FieldSubject, regexp.MustCompile("x"), StoreUser, testProviderOptions(srv.URL))
	p.find = func(*slog.Logger, CertField, *regexp.Regexp, StoreLoc) (Identity, *x509.Certificate, error) {
		return closeCounter{Identity: id, closes: &closes}, id.leaf, nil
	}

	if _, err := p.Retrieve(context.Background()); err == nil {
		t.Fatal("Retrieve succeeded on a 403, want error")
	}
	if got := closes.Load(); got != 1 {
		t.Errorf("identity closes = %d, want 1 after a failed exchange", got)
	}
}

// TestProviderFindError ensures a selection failure (no matching certificate,
// store access denied) reaches the caller and no exchange is attempted.
func TestProviderFindError(t *testing.T) {
	var sessions atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sessions.Add(1)
	}))
	defer srv.Close()

	findErr := errors.New("no certificate matched")
	p := NewProvider(FieldSubject, regexp.MustCompile("x"), StoreUser, testProviderOptions(srv.URL))
	p.find = func(*slog.Logger, CertField, *regexp.Regexp, StoreLoc) (Identity, *x509.Certificate, error) {
		return nil, nil, findErr
	}

	if _, err := p.Retrieve(context.Background()); !errors.Is(err, findErr) {
		t.Errorf("Retrieve error = %v, want %v", err, findErr)
	}
	if got := sessions.Load(); got != 0 {
		t.Errorf("CreateSession calls = %d, want 0 after a failed find", got)
	}
}
