# Run the Penumbra ⇄ Noble IBC Relayer Yourself

> Rotko Networks' Hermes fork with an embedded Penumbra view server.
> This guide gets you from zero to a running relayer on Linux, macOS, or Windows (via WSL2).

[TOC]

---

## What this is

Hermes is an IBC relayer: it watches two chains and carries packets (token
transfers, acks, timeouts) between them. This fork adds first-class **Penumbra**
support — it runs an **embedded view server** that syncs a local, verifiable view
of the shielded Penumbra chain so it can build and sign Penumbra transactions.

You can run it to help relay the **Penumbra ⇄ Noble (USDC)** corridor — the more
independent relayers, the more robust the bridge.

:::info
**Relaying costs gas.** The relayer signs transactions on each chain and pays fees
from its own accounts. You need a small balance of **UM** (Penumbra) and **USDC**
(Noble) on the relayer's keys. There is no built-in reward — relayers run for
robustness/altruism unless a channel has incentivization.
:::

---

## Supported platforms

| Platform | Status | How |
| --- | --- | --- |
| **Linux x86_64** | ✅ Native binary | Download the release, or build from source |
| **Linux aarch64** (ARM64) | ✅ Native binary | Download the release |
| **macOS Apple Silicon** (M1–M4) | ✅ Native binary | Download the release |
| **macOS Intel** | 🔧 Build from source | No prebuilt binary — Intel-Mac runners are scarce; `cargo build --release` |
| **Windows** | ⚠️ via **WSL2** | Run the Linux x86_64 binary inside WSL2 |

### Why no native Windows build?

The embedded view server pulls in native C/C++ dependencies — **RocksDB**
(`librocksdb-sys`) and **OpenSSL** (`openssl-sys`). Building those against the
Windows MSVC toolchain is impractical and untested. Since a relayer is a
long-running background service anyway, Windows users run it under **WSL2**, where
it's just the normal Linux binary. See [Windows (WSL2)](#windows-wsl2) below.

---

## 1. Install

### Option A — download a release binary (recommended)

Grab the archive for your platform from the
[Releases page](https://github.com/rotkonetworks/hermes/releases):

| You have | Download |
| --- | --- |
| Linux, Intel/AMD | `hermes-<tag>-x86_64-unknown-linux-gnu.tar.gz` |
| Linux, ARM64 | `hermes-<tag>-aarch64-unknown-linux-gnu.tar.gz` |
| macOS, Apple Silicon | `hermes-<tag>-aarch64-apple-darwin.tar.gz` |
| macOS, Intel | *(build from source — see Option B)* |

```bash
# example (Linux x86_64)
tar xzf hermes-*-x86_64-unknown-linux-gnu.tar.gz
sudo install -m 0755 hermes /usr/local/bin/hermes
hermes version
```

:::warning
**macOS Gatekeeper**: unsigned binaries are quarantined. If macOS refuses to run it:
`xattr -d com.apple.quarantine ./hermes`
:::

### Option B — build from source

You need the Rust toolchain plus the native build deps.

```bash
# Linux (Debian/Ubuntu)
sudo apt-get update
sudo apt-get install -y build-essential clang libclang-dev cmake pkg-config libssl-dev protobuf-compiler

# macOS
brew install cmake protobuf   # clang/openssl come with Xcode CLT

# then, from a checkout of this repo:
cargo build --release --bin hermes
sudo install -m 0755 target/release/hermes /usr/local/bin/hermes
```

:::info
The first source build compiles RocksDB and can take 15–40 min. Subsequent builds
are incremental.
:::

### Windows (WSL2)

```powershell
# in an elevated PowerShell, once:
wsl --install -d Ubuntu
```

Then open the Ubuntu shell and follow the **Linux** instructions above (download
the `x86_64-unknown-linux-gnu` release, or build from source). Everything else in
this guide is identical inside WSL2.

---

## 2. Configure

Start from the public example config shipped in the repo:
[`config.example.toml`](../config.example.toml).

```bash
mkdir -p ~/.hermes
cp config.example.toml ~/.hermes/config.toml
```

It's preconfigured with **public** Penumbra and Noble endpoints (Polkachu), so you
can try relaying without running your own nodes:

```toml
[[chains]]
id = "penumbra-1"
type = "Penumbra"
rpc_addr  = "https://penumbra-rpc.polkachu.com"
grpc_addr = "https://penumbra-grpc.polkachu.com:443"
...

[[chains]]
id = "noble-1"
type = "CosmosSdk"
rpc_addr  = "https://noble-rpc.polkachu.com"
grpc_addr = "https://noble-grpc.polkachu.com:443"
```

:::warning
**Public endpoints are for trying it out, not for 24/7 relaying.** They are
rate-limited and often prune `/block_results`, which the `pull` event source needs
— you'll see periodic `failed to collect events` errors. For reliable relaying,
run your own Penumbra + Noble full nodes and point the addresses at `localhost`
(see [Production](#production-run-your-own-nodes)).
:::

### Add signing keys

**Noble** (a normal Cosmos key — fund it with a little USDC for gas):

```bash
hermes keys add --chain noble-1 --key-file noble.json
# or from a mnemonic:
hermes keys add --chain noble-1 --mnemonic-file noble.mnemonic
```

**Penumbra** is signed by the **spend key** in `kms_config`, not the keyring. Put
your own spend key in `~/.hermes/config.toml`:

```toml
kms_config = { spend_key = "penumbraspendkey1......your-own-key......" }
```

:::danger
Never share or commit a real Penumbra spend key. Anyone with it controls the funds
on that account. The value in the example file is a non-functional placeholder.
:::

---

## 3. Verify, then run

```bash
hermes health-check          # checks endpoints, keys, and balances
hermes start                 # begin relaying
```

On the **first** start the Penumbra view server does a full view sync — this can
take a while. Progress is persisted under `view_service_storage_dir`, so if you
stop and restart, it resumes rather than starting over.

Healthy output looks like a steady stream of `packet` / `client` events with no
`inter-thread channel` timeouts.

### Run it as a service (Linux/WSL2)

`/etc/systemd/system/hermes.service`:

```ini
[Unit]
Description=Hermes IBC relayer (Penumbra <-> Noble)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/hermes start
Restart=always
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hermes
journalctl -u hermes -f
```

---

## 4. Production: run your own nodes

For dependable relaying, replace the public endpoints with your own nodes so you
control uptime, rate limits, and `/block_results` availability:

```toml
# penumbra-1
rpc_addr  = "http://127.0.0.1:26657"
grpc_addr = "http://127.0.0.1:8080"

# noble-1
rpc_addr  = "http://127.0.0.1:26657"
grpc_addr = "http://127.0.0.1:9090"
```

- Penumbra node: run `pd` (>= 2.0.6) + a CometBFT sidecar.
- Noble node: a standard Cosmos SDK full node (unpruned enough to serve
  `/block_results`).

---

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `failed to collect events … block_results` | The RPC node pruned block results. Use an archive/full node, or your own node. |
| `timeout when waiting for response over inter-thread channel` | View worker stalled. `systemctl restart hermes` resumes from persisted progress; if it recurs, you're likely on a stale binary — use the latest release. |
| `VIEW_DB_CORRUPTED … Wiping` on startup | Normal self-heal: the view server detected an inconsistent local tree and is rebuilding it. Let it finish. |
| Penumbra view sync is very slow | Expected on first sync, especially against a public gRPC. A local node is much faster. |
| `insufficient funds` on Noble | Fund the Noble relayer account with USDC for gas. |
| macOS "cannot be opened" | `xattr -d com.apple.quarantine ./hermes` |

---

## Links

- Releases: <https://github.com/rotkonetworks/hermes/releases>
- Example config: [`config.example.toml`](../config.example.toml)
- Rotko Networks: <https://rotko.net>
