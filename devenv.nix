{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

{
  enterShell = ''
    export GEN=ninja
    # VCPKG disabled - using system packages instead
    # export VCPKG_TOOLCHAIN_PATH=${pkgs.vcpkg}/share/vcpkg/scripts/buildsystems/vcpkg.cmake
  '';

  # https://devenv.sh/packages/
  packages = with pkgs; [
    git
    gnumake

    # For faster compilation
    ninja

    # C/C++ tools
    autoconf
    automake
    pkg-config

    clang-tools

    # Build dependencies for web_archive_cdx extension
    zlib
    openssl
  ];

  # https://devenv.sh/languages/
  languages.cplusplus.enable = true;

  git-hooks.hooks = {
    # Nix files
    nixfmt.enable = true;
    # Github Actions
    actionlint.enable = true;
    # Markdown files
    markdownlint = {
      enable = true;
      settings.configuration = {
        # Ignore line lengths for now
        MD013 = false;
      };
    };
    # Leaking secrets
    ripsecrets.enable = true;
  };

  # Security hardening to prevent malicious takeover of Github Actions:
  # https://news.ycombinator.com/item?id=43367987
  # Replaces tags like "v4" in 3rd party Github Actions to the commit hashes
  git-hooks.hooks.lock-github-action-tags = {
    enable = true;
    files = "^.github/workflows/";
    types = [ "yaml" ];
    entry =
      let
        script_path = pkgs.writeShellScript "lock-github-action-tags" ''
          for workflow in "$@"; do
            grep -E "uses:[[:space:]]+[A-Za-z0-9._-]+/[A-Za-z0-9._-]+@v[0-9]+" "$workflow" | while read -r line; do
              repo=$(echo "$line" | sed -E 's/.*uses:[[:space:]]+([A-Za-z0-9._-]+\/[A-Za-z0-9._-]+)@v[0-9]+.*/\1/')
              tag=$(echo "$line" | sed -E 's/.*@((v[0-9]+)).*/\1/')
              commit_hash=$(git ls-remote "https://github.com/$repo.git" "refs/tags/$tag" | cut -f1)
              [ -n "$commit_hash" ] && sed -i.bak -E "s|(uses:[[:space:]]+$repo@)$tag|\1$commit_hash #$tag|g" "$workflow" && rm -f "$workflow.bak"
            done
          done
        '';
      in
      toString script_path;
  };

  # Run clang-tidy and clang-format on commits
  git-hooks.hooks = {
    clang-format = {
      enable = true;
      types_or = [
        "c++"
        "c"
      ];
    };
    clang-tidy = {
      enable = false;
      types_or = [
        "c++"
        "c"
      ];
      entry = "clang-tidy -p build --fix";
    };
  };
}
