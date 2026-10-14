//go:build integration

package s3test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	rustfsUser     = "pinsync"
	rustfsPassword = "pinsync-secret"
)

// StartRustFS execs a `rustfs server` on an ephemeral port backed by a test
// temp dir and returns an S3 client pointed at it (path-style, static
// test credentials). The server is killed on test cleanup. A missing rustfs
// binary fails the test rather than skipping it, so integration runs cannot
// pass vacuously.
func StartRustFS(t *testing.T) *s3.Client {
	t.Helper()
	bin, err := exec.LookPath("rustfs")
	if err != nil {
		t.Fatalf("rustfs binary not on PATH (provided by devenv): %v", err)
	}
	addr := freeAddr(t)
	cmd := exec.Command(bin, "server",
		"--address", addr,
		"--access-key", rustfsUser,
		"--secret-key", rustfsPassword,
		t.TempDir(),
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting rustfs: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	waitLive(t, addr)

	return s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(rustfsUser, rustfsPassword, ""),
		BaseEndpoint: aws.String("http://" + addr),
		UsePathStyle: true,
	})
}

// CreateBucket creates the named bucket and returns the name for chaining.
func CreateBucket(t *testing.T, client *s3.Client, name string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("creating bucket %s: %v", name, err)
	}
	return name
}

// freeAddr reserves an ephemeral localhost port and returns host:port.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving port: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// waitLive polls the rustfs health endpoint until the server accepts requests.
func waitLive(t *testing.T, addr string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/health/live", addr)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("rustfs at %s did not become ready within 30s", addr)
}
