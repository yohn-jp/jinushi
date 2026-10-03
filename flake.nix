{
  description = "Jinushi: Go-native local execution substrate";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          jinushi = pkgs.buildGo126Module {
            pname = "jinushi";
            version = self.shortRev or self.dirtyShortRev or "dev";
            src = self;
            subPackages = [ "cmd/jinushi" ];
            vendorHash = "sha256-4H7mQu8z9bIwtkj2jHjfhDNCNCcqqpSYxN1atAJw6l8=";
            meta = {
              description = "Go-native local execution substrate";
              homepage = "https://github.com/yohn-jp/jinushi";
              platforms = systems;
              mainProgram = "jinushi";
            };
          };
        in
        {
          inherit jinushi;
          default = jinushi;
        }
      );

      apps = forAllSystems (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/jinushi";
          meta.description = "Run the Jinushi CLI";
        };
      });
    };
}
