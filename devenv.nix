{ pkgs, ... }:

{
  # https://devenv.sh/packages/
  packages = with pkgs; [
    just
    golangci-lint
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
