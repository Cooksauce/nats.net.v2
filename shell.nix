{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  name = "Nats-Proxy Shell";
  buildInputs = with pkgs; [
    natscli
    nats-server
    nats-top
    parallel
    man
  ];

  shellHook = ''
    echo "Nats-Proxy shell started."
    export RESPONSE='{ "Body": "Message {{Count}} @ {{Time}}" }'
  '';
}
