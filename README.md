# Rebecca-node

## Quick install
Install Rebecca-node with Docker using this script:
```bash
sudo bash -c "$(curl -sL https://raw.githubusercontent.com/rebeccapanel/Rebecca/dev/scripts/rebecca/rebecca-node.sh)" @ install
```
Install Rebecca-node with Docker and a custom name:
```bash
sudo bash -c "$(curl -sL https://raw.githubusercontent.com/rebeccapanel/Rebecca/dev/scripts/rebecca/rebecca-node.sh)" @ install --name rebecca-node2
```
Install Rebecca-node as a native binary service:
```bash
sudo bash -c "$(curl -sL https://raw.githubusercontent.com/rebeccapanel/Rebecca/dev/scripts/rebecca/rebecca-node-binary.sh)" @ install
```
Install the binary dev channel:
```bash
sudo bash -c "$(curl -sL https://raw.githubusercontent.com/rebeccapanel/Rebecca/dev/scripts/rebecca/rebecca-node-binary.sh)" @ install --dev
```
Install the Docker dev channel:
```bash
sudo bash -c "$(curl -sL https://raw.githubusercontent.com/rebeccapanel/Rebecca/dev/scripts/rebecca/rebecca-node.sh)" @ install --dev
```
Or you can only install the Docker node script by using this command:
```bash
sudo bash -c "$(curl -sL https://raw.githubusercontent.com/rebeccapanel/Rebecca/dev/scripts/rebecca/rebecca-node.sh)" @ install-script
```

The Docker and binary installers are intentionally separate. Use the matching script for update, restart, core update, and script update operations.

OpenVPN runtime support is binary-only. Docker nodes reject OpenVPN runtime payloads
instead of installing host VPN prerequisites inside the container.

WireGuard runtime support is binary-only for the same reason. On first use the node installs
`wireguard-tools`, `nftables`, loads the kernel module, and reconciles one kernel interface per
inbound. By default each WireGuard inbound sends TCP/UDP traffic through nftables TPROXY to its
Xray tunnel port, so Rebecca routing rules still apply. Direct MASQUERADE/NAT is available only
when the runtime explicitly sets `nat_enabled=true`. The master pushes WireGuard state inside the
shared runtime envelope (`ov_runtime_json`) under `wg_inbounds`, each with a server `private_key`,
`address_pool`, `listen_port`, `tunnel_port` and a `peers` list (`public_key`, optional
`preshared_key`, `address`). Per-peer rx+tx is read from the kernel and reported to the master as
`wg:<user_id>` usage deltas.

Use `help` to view all commands:
```Rebecca-node help```


## Manual install
Read the setup guide here: Soon!

## Runtime
Rebecca-node is implemented in Go and ships one host-level binary:

- `rebecca-node`: the TLS REST API used by the Rebecca master to control Xray and schedule safe on-host restart/update commands

## Binary builds
Linux `amd64` and Windows `amd64` binaries are built for:

- every push to `dev`
- every pull request
- every published or prereleased GitHub release

Release assets are uploaded as raw executables for all supported release targets:

- `rebecca-node-<version>-linux-386`
- `rebecca-node-<version>-linux-amd64`
- `rebecca-node-<version>-linux-arm64`
- `rebecca-node-<version>-linux-armv5`
- `rebecca-node-<version>-linux-armv6`
- `rebecca-node-<version>-linux-armv7`
- `rebecca-node-<version>-linux-s390x`
- `rebecca-node-<version>-windows-amd64.exe`

You can also reproduce the CI packaging flow locally:

```bash
go run ./tools/build
go run ./tools/smoke
```

The Go packages can be checked directly with:

```bash
go test ./...
```
