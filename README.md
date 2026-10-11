# pinsync

pinsync publishes a directory to S3 and mirrors it onto other machines, byte
for byte. `push` uploads the tree plus a `manifest.json` that lists each file's
SHA256, size and mode. `pull` reads that manifest, checks every byte against
it, and swaps the finished tree into place with a rename. A machine that was
offline, half-synced or had a file corrupted gets the right tree on its next
pull.

One machine or a thousand, a laptop or a CI job, it works the same. Pushing
config from CI to managed devices is the use it was built for, not the only one.

## Why not `aws s3 sync`

`aws s3 sync` copies files. pinsync publishes a snapshot and guarantees every
copy matches it.

| | `aws s3 sync` | pinsync |
|---|---|---|
| Deciding what changed | Size and modification time | SHA256 of the contents |
| File permissions | Not stored | Recorded by push, applied by pull on Linux and macOS |
| Interrupted download | Leaves a mix of old and new files | Destination untouched, and the next run recovers |
| Download during an upload | Can mix files from two uploads | Detects the mismatch and fetches the newer snapshot |
| Local file corrupted, same size | Kept if its mtime is newer | Caught by the hash and downloaded again |
| Stray local files | Stay unless you pass `--delete` | Gone after the next pull |
| Run with nothing new | Lists every object under the prefix | One manifest GET, and unchanged files are hardlinked |
| Object altered in the bucket | Not noticed | The next push sees the wrong SHA256 and uploads it again |
| Cleaning up old objects | `--delete` removes them at once | `prune` keeps anything newer than `-min-age` |
| Device credentials | Keys or profiles | Also IAM Roles Anywhere, with the private key kept in Keychain or Windows CNG |

Already using `aws s3 sync`? `push -adopt` publishes a manifest for the existing
bucket, so machines can start pulling right away. Objects uploaded by
`aws s3 sync` usually have no stored SHA256, so the first normal push after that
re-uploads each of them once. See
[Migrating from aws s3 sync](https://github.com/hurricanehrndz/pinsync/wiki/Migrating-from-aws-s3-sync).

The [wiki](https://github.com/hurricanehrndz/pinsync/wiki) covers the
details, including the [sync model](https://github.com/hurricanehrndz/pinsync/wiki/Sync-Model)
and every [flag and output line](https://github.com/hurricanehrndz/pinsync/wiki/Push-and-Pull).

## What goes where

Say you push this tree:

```
config-tree/
├── app.conf
├── logging.yaml
└── certs/
    └── ca.pem
```

`pinsync push -bucket my-bucket -prefix cfg/prod ./config-tree` uploads each
file under the prefix, then writes the manifest last:

```
s3://my-bucket/
└── cfg/
    └── prod/
        ├── app.conf
        ├── logging.yaml
        ├── certs/
        │   └── ca.pem
        └── manifest.json    written after every file above
```

```json
{
  "version": 1,
  "files": [
    { "path": "app.conf",     "sha256": "…", "size": 128,  "mode": "0644" },
    { "path": "certs/ca.pem", "sha256": "…", "size": 2202, "mode": "0644" },
    { "path": "logging.yaml", "sha256": "…", "size": 341,  "mode": "0644" }
  ]
}
```

A reader that fetches the manifest therefore never sees half a push. It gets the
old snapshot or the new one.

`pinsync pull -bucket my-bucket -prefix cfg/prod /etc/myapp` builds
the new tree next to the destination, then renames it into place:

```
/etc/
├── myapp/               live tree, untouched until the swap
└── myapp.pinsync-tmp/   new tree: unchanged files hardlinked from myapp/,
                         changed ones downloaded and hash-checked
```

Pull owns `/etc/myapp`. A file you drop in there by hand isn't in the manifest,
so the next pull removes it.

The manifest can live elsewhere with `-manifest-key`. See
[Layout](https://github.com/hurricanehrndz/pinsync/wiki/Layout).

## Push

```sh
pinsync push -bucket my-bucket -prefix cfg/prod ./config-tree
```

```
pushed 1 of 3 files (2 unchanged) to s3://my-bucket/cfg/prod
```

Push diffs the tree against the remote manifest and uploads only new and changed
files. For each file it would skip, it asks S3 for the object's stored SHA256 and
size with `HeadObject`. A missing or mismatched object gets re-uploaded and
counted as repaired. That fixes a prefix left behind by a push that died before
writing its manifest, or by someone editing the bucket directly. `-full`
re-uploads everything without checking, and `-dry-run` prints what would change.

Push runs on Linux and macOS. It refuses Windows, where the recorded permission
bits would be made up.

## Pull

```sh
pinsync pull -bucket my-bucket -prefix cfg/prod /etc/myapp
```

```
pulled 3 files: 1 downloaded, 2 linked, 0 copied
```

Pull runs on Linux, macOS and Windows. A pull with nothing to do downloads
nothing, so cron or launchd can run it as often as you like.

Common flags: `-parallel N` (default 16), `-region`,
`-endpoint-url` for MinIO and other S3-compatible stores (turns on path-style
addressing), and `-log-level`/`-log-format`.

## Prune

Push never deletes. When a file changes or leaves the tree, its old object stays
in the bucket with nothing pointing at it. `prune` removes those:

```sh
pinsync prune -bucket my-bucket -prefix cfg/prod          # preview
pinsync prune -bucket my-bucket -prefix cfg/prod -apply   # delete
```

`-min-age` (default `24h`) keeps any object modified inside that window. A push
in progress has uploaded content its manifest doesn't reference yet, and this
window stops prune from deleting it. If prune can't read a valid manifest, it
deletes nothing.

One gap is left open on purpose. A pull that's still downloading an old snapshot
after a newer push replaced it can lose objects to prune once they're older than
`-min-age`. If your slowest pull can outlast the default, raise `-min-age`.

## Credentials

pinsync uses the AWS SDK's default chain: environment variables, shared
config and credentials files (profiles and SSO), then instance metadata. It has
no flags for static keys.

On managed macOS and Windows devices, `pull` can sign in with
[IAM Roles Anywhere](https://github.com/hurricanehrndz/pinsync/wiki/Push-and-Pull#iam-roles-anywhere)
using a device certificate from the Keychain or Windows CNG store. The private
key never leaves the store.

## Install

Each `v*` tag publishes three artifacts plus `SHA256SUMS`:

- `pinsync-<version>-x86_64.msi` is an unsigned Windows x86-64 installer. It
  installs under `C:\Program Files\pinsync` and does not add that directory to
  `PATH`.
- `pinsync-<version>-universal.pkg` is a signed, notarized macOS installer with
  Intel and Apple silicon binaries. Install it with Installer.app or
  `sudo installer -pkg <file> -target /`. It writes `/usr/local/bin/pinsync`.
- `pinsync-<version>-linux-x86_64.tar.gz` contains a static Linux x86-64 binary.

[embala](https://github.com/hurricanehrndz/embala) builds both installers. The
macOS package is signed with a Developer ID, notarized, and stapled, so
Gatekeeper accepts it offline.

## Build

```sh
go build ./cmd/pinsync   # or: just build
```

Linux and Windows builds are pure Go (`just build`, `just build-windows [arch]`).
The darwin build links the system certificate store through cgo, so it
cross-compiles with `zig cc` against a pinned macOS SDK:

```sh
just build-darwin arm64   # or amd64
```

The devshell (`direnv exec .` or `devenv shell`) provides `zig` and
`MACOS_SDK`. Outside it, put `zig` on `PATH` and point `MACOS_SDK` at a macOS SDK
root.

## Go library

The CLI is a thin wrapper around `pkg/pull`, `pkg/push` and `pkg/prune`, so an
agent or service can embed the same guarantees without running a separate
process. The library also does things the CLI doesn't expose:

- Save the manifest you're about to install, before the swap, with a
  `BeforeReplace` hook. Your service then always knows which snapshot is on disk.
- Install a snapshot from a local directory instead of S3, such as a cache or a
  USB stick, with the same hash checks and swap.
- Check that a directory still matches a manifest without touching the network.
- Pass in your own S3 client, or a fake in tests. Each package takes a small
  interface that `*s3.Client` already satisfies. Pull needs only `GetObject`.

```go
import "github.com/hurricanehrndz/pinsync/pkg/pull"

stats, err := pull.Pull(ctx, s3Client, bucket, prefix, dest, pull.Options{})
```

`pkg/push` and `pkg/prune` work the same way, and `prune` also has `DryRun` for
the preview.

### Manifest persistence and offline repair

If you need to remember which manifest describes an installed tree, call
`PullWithManifest`. It returns the exact manifest bytes it installed.

```go
res, err := pull.PullWithManifest(ctx, s3Client, bucket, prefix, dest, pull.Options{
    BeforeReplace: func(ctx context.Context, m []byte) error { return savePending(m) },
})
// res.Manifest describes dest; res.Stats is what Pull returns.
```

`Options.BeforeReplace` gets a copy of those bytes after the staged tree passes
verification and before the swap. Return an error to cancel the swap and leave
the destination as recovery left it. Recovery of an earlier interrupted swap
runs first, so it may already have renamed the destination's siblings by the
time the callback runs. Pull retries only a staged-content mismatch (hash or
size) seen before `BeforeReplace`, by fetching the manifest again. Nothing is
retried after the callback.

- `pull.Recover(ctx, dest, logger)` finishes or rolls back an interrupted swap
  using only `dest`, `dest.pinsync-tmp` and `dest.pinsync-old`. It never
  promotes the unverified staging tree.
- `pull.ReplaceFromDirectory(ctx, source, dest, manifestBytes, opts)` installs
  the tree described by `manifestBytes` from a local directory, with the same
  verification, callback, swap and recovery. It only reads `source`.
- `manifest.VerifyTree(ctx, root, m)` checks that `root` holds exactly `m`'s
  files (hash, size, and mode except on Windows) and nothing else. Symlinks and
  other non-regular files fail.

`Options.MaxManifestBytes` (default 64 MiB) caps the manifest read, and
`Options.MaxContentBytes` (default 64 GiB) caps the total size the manifest
declares. Zero means the default, and a negative limit is an error. Pull reads
each body to at most its declared size plus one byte.
