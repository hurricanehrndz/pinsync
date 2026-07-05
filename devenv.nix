{ pkgs, ... }:

let
  # Community-mirrored macOS SDK, used to cross-build the darwin cgo target
  # (tailscale/certstore links CoreFoundation + Security) with `zig cc` as the
  # C compiler. Fixed-output derivation: hash pins the fetched tarball.
  macosSdk = pkgs.stdenvNoCC.mkDerivation {
    name = "MacOSX15.2.sdk";
    src = pkgs.fetchzip {
      url = "https://github.com/joseluisq/macosx-sdks/releases/download/15.2/MacOSX15.2.sdk.tar.xz";
      hash = "sha256-I35yUUjM8zGSaqE/vYz03YCUhpR8uIG1uwJTJvcF8jk=";
    };
    dontBuild = true;
    installPhase = "cp -r . $out";
  };
in
{
  # Exposed to `just build-darwin` as -isysroot / framework search root.
  env.MACOS_SDK = "${macosSdk}";

  # https://devenv.sh/packages/
  packages = with pkgs; [
    awscli2
    just
    golangci-lint
    zig # C compiler (clang-based) for the darwin cgo cross-build
    minio # spawned by `go test -tags integration` (internal/s3test)
  ];

  languages.rust.enable = true;

  treefmt = {
    enable = true;
    config.programs = {
      nixfmt.enable = true;
      gofumpt.enable = true;
      yamlfmt.enable = true;
    };
  };

  # https://devenv.sh/git-hooks/
  # Run treefmt on commit. Enabling the `treefmt` module above already wires
  # its config-baked wrapper into this hook (git-hooks.hooks.treefmt.package),
  # so we only need to switch the hook on.
  git-hooks.hooks.treefmt.enable = true;

  # Lint Go on commit. Pin the hook to the same golangci-lint from `packages`
  # above so the CLI and the hook never drift in version; config lives in
  # .golangci.yml (v2 schema).
  git-hooks.hooks.golangci-lint = {
    enable = true;
    package = pkgs.golangci-lint;
  };
}
