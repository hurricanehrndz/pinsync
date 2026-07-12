# pinsync

Manifest-based S3 sync: `push` publishes a config tree to S3 as an atomic
snapshot described by a `manifest.json` (content first, manifest last);
`pull` mirrors it back down, verifying every byte against the manifest's
SHA256 entries and atomically swapping the destination — no listing, no
delete phase, corrupted or extraneous local files converge by construction.

## Build

```sh
go build ./cmd/pinsync   # or: just build
```

The linux and windows targets are pure Go and need no C toolchain
(`just build`, `just build-windows [arch]`). The darwin target links the
system certificate store via cgo, so it cross-builds with `zig cc` against a
pinned macOS SDK:

```sh
just build-darwin arm64   # or amd64
```

Run it inside the devshell (`direnv exec .` / `devenv shell`), which supplies
both `zig` and `MACOS_SDK`. It also works anywhere else with `zig` on `PATH`
and `MACOS_SDK` pointing at a macOS SDK root.

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

## Prune

```sh
pinsync prune -bucket my-bucket -prefix cfg/prod -apply
```

`prune` deletes objects under the prefix that the published `manifest.json` no
longer references — snapshot garbage left behind when a push changes or removes
files. It previews by default (listing what it *would* delete and touching
nothing); pass `-apply` to actually delete.

`-min-age` (default `24h`) protects any object modified within the window from
deletion. This closes the push race: a concurrent push uploads content first
and publishes the manifest last, so freshly uploaded objects are briefly absent
from the reference set — the grace window keeps them until the manifest catches
up.

A missing or corrupt manifest is fatal: prune refuses to delete rather than
treat "no reference set" as "everything is orphan".

**Slow-pull residual caveat:** a pull slow enough to still be mirroring a
snapshot that a newer push has already superseded can have the objects it is
reading deleted out from under it once they age past `-min-age`. This is bounded
by the threshold and deliberately left uncoordinated in code — raise `-min-age`
if your slowest pull can exceed the default.

## Credentials

By default credentials resolve via the standard AWS SDK default chain, in
order: environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_SESSION_TOKEN`), the shared config/credentials files (including named
profiles and SSO), and finally the EC2/container instance metadata service
(IMDS). There are deliberately no static-credential flags — configure the
environment or shared config instead.

## IAM Roles Anywhere (macOS / Windows)

On managed devices, `pull` can authenticate with a device certificate held in
the OS certificate store (macOS Keychain / Windows CNG) instead of long-lived
keys. The private key never leaves the store: signing goes through the store's
key handle, and the vended credentials are short-lived.

Selection uses three ARNs:

- **trust anchor** validates the device certificate's chain,
- **profile** bounds the set of roles the device may assume,
- **role** is the specific role picked from that allow-list.

This flow is macOS/Windows only; the flags are rejected on other platforms.
`-ra-cert-store` (`user`|`machine`) is Windows only and ignored on macOS.

```sh
pinsync pull -bucket dist \
  -ra-trust-anchor-arn arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/ta-id \
  -ra-profile-arn      arn:aws:rolesanywhere:us-east-1:123456789012:profile/p-id \
  -ra-role-arn         arn:aws:iam::123456789012:role/device-pull \
  -ra-cert-pattern     'MDM Device CA' \
  -ra-cert-field       issuer \
  /opt/dist
```

The region comes from `-region` when set, otherwise from the trust anchor ARN,
and applies to both the credential exchange and the S3 client.

Import the library directly from services:

```go
import "github.com/hurricanehrndz/pinsync/pkg/pull"

stats, err := pull.Pull(ctx, s3Client, bucket, prefix, dest, pull.Options{})
```

`push` and `prune` are importable the same way
(`github.com/hurricanehrndz/pinsync/pkg/push`,
`github.com/hurricanehrndz/pinsync/pkg/prune`); `prune` also exposes `DryRun`
for the preview.
