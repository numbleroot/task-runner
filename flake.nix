{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
    }:
    let
      overlays = [ (import rust-overlay) ];
      system = "x86_64-linux";
      pkgs = import nixpkgs {
        inherit system overlays;
      };
    in
    {
      formatter.${system} = nixpkgs.legacyPackages.${system}.nixfmt-tree;
      devShells.${system}.default =
        let
          rust-nightly = pkgs.rust-bin.selectLatestNightlyWith (
            toolchain:
            toolchain.default.override {
              extensions = [
                "rust-src"
                "rust-analyzer"
                "clippy"
                "rustfmt"
                "llvm-tools-preview"
                "cargo"
              ];
            }
          );
        in
        pkgs.mkShell {
          buildInputs = with pkgs; [
            pkg-config
            openssl
            gcc
            gnumake
            rust-nightly
            cargo-deny
          ];
          shellHook = ''
            umask 0077
          '';
          RUST_BACKTRACE = "full";
          RUST_SRC_PATH = "${rust-nightly}/lib/rustlib/src/rust/library";
        };
    };
}
