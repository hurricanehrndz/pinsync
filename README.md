# pinsync

Manifest-based S3 sync: `push` publishes a config tree to S3 as an atomic
snapshot described by a `manifest.json` (content first, manifest last);
`pull` mirrors it back down, verifying every byte against the manifest's
SHA256 entries and atomically swapping the destination — no listing, no
delete phase, corrupted or extraneous local files converge by construction.

## Build

```sh
go build ./cmd/pinsync
```

## Push (POSIX CI only)

```sh
pinsync push -bucket my-bucket -prefix cfg/prod ./config-tree
```

## Pull (macOS / Windows devices)

```sh
pinsync pull -bucket my-bucket -prefix cfg/prod /etc/myapp
```

Common flags: `-parallel N` (default 16), `-region`, `-endpoint-url`
(e.g. MinIO; enables path-style addressing), `-v` (progress logging).
Credentials resolve via the standard AWS SDK default chain.

Import the library directly from services:

```go
import "github.com/hurricanehrndz/pinsync/pkg/pull"

stats, err := pull.Pull(ctx, s3Client, bucket, prefix, dest, pull.Options{})
```
