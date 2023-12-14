rec {
  description = "Summarize distribution of file sizes";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }: let
    inherit (nixpkgs) lib;
    inherit (lib) substring;
    eachSystem = lib.genAttrs lib.systems.flakeExposed;
  in {
    packages = eachSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};

      mtime = self.lastModifiedDate;
      date = "${substring 0 4 mtime}-${substring 4 2 mtime}-${substring 6 2 mtime}";
      rev = self.rev or (throw "Git changes are not committed");
    in rec {
      default = histodu;

      histodu = pkgs.rustPlatform.buildRustPackage rec {
        pname = "histodu";
        version = "unstable-${date}";
        src = self;

        cargoLock.lockFile = ./Cargo.lock;

        buildFeatures = [ "completion" ];

        CFG_RELEASE = "git-${rev}";

        nativeBuildInputs = [ pkgs.installShellFiles ];

        postInstall = ''
          installShellCompletion \
            --bash completions/bash/${pname}.bash \
            --fish completions/fish/${pname}.fish \
            --zsh completions/zsh/_${pname}
        '';

        meta = {
          inherit description;
          homepage = "https://github.com/oxalica/histodu";
          license = with lib.licenses; [ mit asl20 ];
          mainProgram = "histodu";
        };
      };
    });
  };
}
