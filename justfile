# pinsync task runner. Later phases add build/test recipes here.

# Stand up the Roles Anywhere test infrastructure (idempotent).
ra-infra-up:
    ./hack/ra-test/up.sh

# Tear down the Roles Anywhere test infrastructure.
ra-infra-down:
    ./hack/ra-test/down.sh

# Native build (linux: pure Go, Roles Anywhere store stubbed out).
build:
    go build ./cmd/pinsync

# Windows build: pure Go (CNG store is called via syscall, no cgo).
build-windows arch='amd64':
    GOOS=windows GOARCH={{ arch }} CGO_ENABLED=0 \
        go build -o pinsync-windows-{{ arch }}.exe ./cmd/pinsync

# certstore's certstore_darwin.go is objective-c cgo linking CoreFoundation +
# Security, so this needs a clang that can target darwin: `zig cc` is that
# clang, and MACOS_SDK (from devenv.nix) supplies headers, frameworks and the
# .tbd stub libs. arm64->aarch64-macos, amd64->x86_64-macos.

# Darwin cross-build via zig cc (needs MACOS_SDK). arm64 or amd64.
build-darwin arch='arm64':
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -z "${MACOS_SDK:-}" ]; then
        echo "error: MACOS_SDK is unset; run inside the devenv shell (direnv exec .) or export it manually" >&2
        exit 1
    fi
    case "{{ arch }}" in
        arm64) zigtarget=aarch64-macos ;;
        amd64) zigtarget=x86_64-macos ;;
        *) echo "error: unsupported arch '{{ arch }}' (use arm64 or amd64)" >&2; exit 1 ;;
    esac
    # Keep zig's cache out of $HOME so unattended builds don't touch it.
    export ZIG_GLOBAL_CACHE_DIR="${ZIG_GLOBAL_CACHE_DIR:-$PWD/.zig-cache}"
    export ZIG_LOCAL_CACHE_DIR="${ZIG_LOCAL_CACHE_DIR:-$PWD/.zig-cache}"
    # -Wno-incompatible-sysroot: clang reads the SDK platform from the sysroot
    # dir basename; the nix store path (<hash>-MacOSX15.2.sdk) trips its
    # heuristic, and zig promotes that warning to an error by default.
    GOOS=darwin GOARCH={{ arch }} CGO_ENABLED=1 \
        CC="zig cc -target ${zigtarget}" \
        CGO_CFLAGS="-isysroot $MACOS_SDK -isystem $MACOS_SDK/usr/include -iframework $MACOS_SDK/System/Library/Frameworks -Wno-incompatible-sysroot" \
        CGO_LDFLAGS="-isysroot $MACOS_SDK -L$MACOS_SDK/usr/lib -F$MACOS_SDK/System/Library/Frameworks -Wno-incompatible-sysroot" \
        go build -ldflags=-w -o pinsync-darwin-{{ arch }} ./cmd/pinsync
    # -ldflags=-w omits DWARF: Go's external link step otherwise shells out to
    # dsymutil to bundle debug info, which does not exist on a linux host.

# bump the version, commit, and tag (patch|minor|major|...)
bump part="patch":
    go tool versionbump {{part}}
