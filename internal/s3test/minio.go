//go:build integration

package s3test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	minioUser     = "pinsync"
	minioPassword = "pinsync-secret"
)

// StartMinIO execs a `minio server` on an ephemeral port backed by a test
// temp dir and returns an S3 client pointed at it (path-style, static test
// credentials). The server is killed on test cleanup. Skips the test with a
// clear message if the minio binary is not on PATH.
func StartMinIO(t *testing.T) *s3.Client {
	t.Helper()
	bin, err := exec.LookPath("minio")
	if err != nil {
		t.Skip("minio binary not on PATH (provided by devenv); skipping integration test")
	}
	addr := freeAddr(t)
	cmd := exec.Command(bin, "server", "--address", addr, "--quiet", t.TempDir())
	cmd.Env = append(
		os.Environ(),
		"MINIO_ROOT_USER="+minioUser,
		"MINIO_ROOT_PASSWORD="+minioPassword,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting minio: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	waitLive(t, addr)

	return s3.New(s3.Options{
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider(minioUser, minioPassword, ""),
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

// waitLive polls the MinIO health endpoint until the server accepts requests.
func waitLive(t *testing.T, addr string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/minio/health/live", addr)
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
	t.Fatalf("minio at %s did not become ready within 30s", addr)
}
