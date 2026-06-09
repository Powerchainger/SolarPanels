{
  description = "Solar panel scraper service (EnPhase + Growatt → MongoDB → Output API)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        python = pkgs.python313;
      in {
        devShells.default = pkgs.mkShell {
          packages = [
            python
            pkgs.uv
          ];

          env = {
            # Pin uv to the nix-provided Python so it never downloads its own.
            UV_PYTHON = "${python}/bin/python3.13";
            LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";
          };

          shellHook = ''
            if [ ! -d .venv ]; then
              uv venv --python ${python}/bin/python3.13
            fi
            source .venv/bin/activate
          '';
        };
      }
    );
}
