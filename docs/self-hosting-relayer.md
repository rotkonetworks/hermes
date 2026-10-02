# Running a Penumbra IBC relayer

Hermes fork with Penumbra support, maintained by Rotko Networks. This covers
building it, keys and funding, the config we run in production, and the
failure modes we have hit.

More independent relayers means a channel keeps working when one operator
goes down. Relaying is permissionless: anyone with funded keys can relay any
open channel. There is no fee reward on these channels.

## What hermes does on Penumbra

- Keeps the light clients on both sides of each channel updated.
- Relays inbound packets (counterparty -> Penumbra) and the acknowledgements
  and timeouts for outbound ones.

It does not create outbound transfers. A Penumbra withdrawal is a native
`Ics20Withdrawal` action built by a wallet (`pcli tx withdraw`, Prax, Veil).
`hermes tx ft-transfer --src-chain penumbra-1` is rejected by pd with
`unknown IBC message type: /ibc.applications.transfer.v1.MsgTransfer`. That
is expected.

To sign Penumbra transactions hermes runs an embedded view server that syncs
the relayer's own shielded wallet into a local SQLite file.

## Channels

| Counterparty | Chain id | Penumbra channel | Counterparty channel | Gas denom |
| --- | --- | --- | --- | --- |
| Noble | `noble-1` | `channel-2` | `channel-89` | `uusdc` |
| Injective | `injective-1` | `channel-18` | `channel-494` | `inj` |
| Osmosis | `osmosis-1` | `channel-20` | `channel-111093` | `uosmo` |
| Kava | `kava_2222-10` | `channel-21` | `channel-162` | `ukava` |
| Cosmos Hub | `cosmoshub-4` | `channel-22` | `channel-1934` | `uatom` |
| Celestia | `celestia` | `channel-23` | `channel-701` | `utia` |
| Axelar | `axelar-dojo-1` | `channel-24` | `channel-198` | `uaxl` |

Other Penumbra channels (0, 1, 3, 4, 5, 7, 8, 10, 13, 15, 17, 19) are dead or
unused. Do not relay them.

## Build

Build from `main`. The GitHub releases (`v1.8.2-rotko.1`/`.2`, July 2026) are
behind and lack fixes we depend on, among them 7b143b30d (Cosmos Hub
acknowledgements) and the memory fixes merged in 74ebf3588.

Debian/Ubuntu:

```sh
sudo apt-get install -y build-essential clang libclang-dev cmake pkg-config \
  libssl-dev protobuf-compiler
git clone https://github.com/rotkonetworks/hermes
cd hermes
cargo build --release --bin hermes
sudo install -m 0755 target/release/hermes /usr/local/bin/hermes
hermes version
```

macOS: `brew install cmake protobuf`, then the same `cargo build`.

The first build compiles RocksDB and takes 15-40 min. Linux x86_64 and
aarch64 and macOS work. Windows only works under WSL2: the RocksDB and OpenSSL
native deps do not build against MSVC.

Build on the same distro (or an older one) as the one you run on. A binary
built on a newer glibc will not start on an older host.

## Keys and funding

The examples below run hermes as a dedicated `hermes` user whose home is
`/var/lib/hermes`:

```sh
sudo useradd --system --home-dir /var/lib/hermes --create-home --shell /usr/sbin/nologin hermes
```

The cosmos keyring lives in `$HOME/.hermes/keys` of whoever runs the command;
`--config` does not move it. Run `hermes keys add` as the same user as the
service (`sudo -u hermes hermes ...`), or the daemon will not find the keys.

Use your own keys for every chain. If two relayers share a cosmos key they
race on the account sequence and waste retries.

### Penumbra

Hermes reads the spend key straight from `kms_config` in the config file, not
from the hermes keyring. Use a dedicated wallet for the relayer:

```sh
pcli --home ~/.relayer-pcli init --grpc-url https://penumbra.rotko.net soft-kms generate
grep spend_key ~/.relayer-pcli/config.toml    # goes into kms_config
pcli --home ~/.relayer-pcli view address 0    # fund this address
```

Send it a few UM from another wallet. Fees are around 0.0013 UM per tx, so a
few UM covers thousands of relays. Without UM every inbound relay fails with
`ran out of notes to spend while planning transaction`.

Do not spend from the relayer wallet with pcli while hermes runs: both would
pick the same notes. `hermes keys balance --chain penumbra-1` is not
implemented and panics. Check the balance with
`pcli --home ~/.relayer-pcli view balance`.

The spend key controls the funds. Keep the config file `0600` and never
commit it.

### Cosmos chains

Prefix each of these with `sudo -u hermes`:

```sh
hermes keys add --chain noble-1      --key-name noble-relayer     --mnemonic-file noble.mnemonic
hermes keys add --chain osmosis-1    --key-name osmosis-relayer   --mnemonic-file osmo.mnemonic
hermes keys add --chain kava_2222-10 --key-name kava-relayer      --mnemonic-file kava.mnemonic
hermes keys add --chain cosmoshub-4  --key-name cosmos-relayer    --mnemonic-file cosmos.mnemonic
hermes keys add --chain celestia     --key-name celestia-relayer  --mnemonic-file tia.mnemonic
hermes keys add --chain axelar-dojo-1 --key-name axelar-relayer   --mnemonic-file axl.mnemonic

# injective is ethsecp256k1, coin type 60
hermes keys add --chain injective-1 --key-name injective-relayer \
  --mnemonic-file inj.mnemonic --hd-path "m/44'/60'/0'/0/0"
```

`--key-name` must match `key_name` in the chain's config block. Fund each
account with the gas denom from the table. A few units of each is plenty;
Noble pays gas in USDC, so a few USDC.

## Configure

[`config.example.toml`](../config.example.toml) is our production config with
the keys removed. Edit four things before running: `kms_config.spend_key`,
`view_service_storage_dir`, `memo_prefix` and the `key_name`s. Drop the
chain blocks you don't want and remove their channel from the `penumbra-1`
`packet_filter`.

```sh
sudo mkdir -p /etc/hermes
sudo -u hermes mkdir -p /var/lib/hermes/view
sudo install -m 600 -o hermes -g hermes config.example.toml /etc/hermes/config.toml
# edit, then:
sudo -u hermes hermes --config /etc/hermes/config.toml config validate
sudo -u hermes hermes --config /etc/hermes/config.toml health-check
```

`config validate` fails until `spend_key` holds a real key; the error
(`mixed-case strings not allowed`) points at an unrelated line.

`view_service_storage_dir` must be absolute. `~` is not expanded, so
`~/.hermes` would create a directory literally named `~` in the working
directory.

### Endpoints

The example uses public endpoints that worked for us:

- Penumbra: `https://penumbra.rotko.net` (CometBFT RPC and pd gRPC on 443).
  We run against a local pd + cometbft (`http://127.0.0.1:26657` and
  `http://127.0.0.1:8080`) and recommend that for anything long-running.
- Cosmos chains: Keplr or Polkachu RPC with Polkachu plaintext gRPC on its
  per-chain ports.

Lessons from picking them:

- Hermes needs gRPC (simulate, broadcast, queries). Keplr endpoints are RPC
  and LCD only.
- Avoid gRPC behind Cloudflare. A 524 HTML page comes back mid-stream and
  hermes reports `invalid compression flag: 101`.
- The `pull` event source needs `/block_results`. Endpoints that prune it
  give `failed to collect events`.
- Probe a gRPC endpoint before using it: `grpcurl host:port list`. A bad
  `grpc_addr` stops hermes at startup.

## Run

The first start syncs the relayer wallet's view from genesis, which takes a
while. Progress is kept in `view_service_storage_dir`, so restarts resume.

`/etc/systemd/system/hermes.service` (what we run, with a dedicated
user and an explicit config path):

```ini
[Unit]
Description=Hermes IBC relayer
After=network-online.target
Wants=network-online.target
StartLimitBurst=50
StartLimitIntervalSec=600

[Service]
Type=simple
User=hermes
Environment="HOME=/var/lib/hermes"
WorkingDirectory=/var/lib/hermes
ExecStart=/usr/local/bin/hermes --config /etc/hermes/config.toml start
Environment="RUST_LOG=info,ibc_relayer=debug,tendermint_light_client=info"
Restart=always
RestartSec=10
LimitNOFILE=65535
MemoryHigh=4500M
MemoryMax=6G

[Install]
WantedBy=multi-user.target
```

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now hermes
journalctl -u hermes -f
```

Only one hermes process can open the view database. A second hermes process
gets no view, so anything that builds a Penumbra transaction (`hermes tx ...`
with penumbra-1 as destination, `create client/connection/channel`) fails
while the daemon runs. Read-only queries still work. Stop the daemon before
running those.

## Keep clients alive

A client not updated within its trusting period expires, and its channel is
dead. Penumbra 2.0.x has no client recovery, so a dead channel means
opening a new one, with a new and non-fungible asset denom. That is how
most of the old channels died.

The tightest clocks are on the counterparty side. The clients that track
penumbra-1 have a 4.67-day trusting period on Osmosis, Kava, Cosmos Hub and
Celestia, and 100 hours on Injective. `client_refresh_rate` on a chain block
applies to clients that track that chain, so these are refreshed per the
`penumbra-1` block's `1/4` (about once a day). Don't loosen it. The `1/3`
and `1/4` on the counterparty blocks govern the clients hosted on Penumbra
(9-14 day periods). All of it only holds while hermes is up and funded on
both sides.

Watch for:

- `hermes query client state --chain <host-chain> --client <client-id>`
  (latest height and timestamp)
- Prometheus metrics on `127.0.0.1:3001/metrics`, notably
  `client_updates_submitted_total` and `wallet_balance`
- Relayer balances on every chain, including UM

Alert well before expiry, not on it.

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| `ran out of notes to spend while planning transaction` | Relayer Penumbra wallet has no UM. Fund `pcli view address 0` of the relayer key. |
| `insufficient funds` on a cosmos chain | Fund that chain's relayer account with its gas denom. |
| `invalid compression flag: 101` | gRPC endpoint is behind Cloudflare and returned an HTML error page. Use another gRPC. |
| `failed to collect events ... block_results` | RPC node prunes block results. Use another RPC or your own node. |
| `timeout when waiting for response over inter-thread channel` | View worker stalled. Restart hermes; it resumes. If it keeps recurring, check you are on current `main`. |
| `VIEW_DB_CORRUPTED ... Wiping` at startup | View server found an inconsistent local tree and is rebuilding it. Let it finish. |
| Penumbra tx command fails while daemon runs | View DB is locked by the daemon. Stop the daemon first. |
| `unknown IBC message type ... MsgTransfer` | Outbound transfers from Penumbra are wallet operations. Use `pcli tx withdraw`. |
| `hermes query channels --chain osmosis-1` fails on decode size | The response is ~40 MB, above `max_grpc_decoding_size`. Query the specific channel or client instead. |
| Injective `compat_mode` error | Must be `'0.37'`; `'0.38'` is not supported by this fork. |
| Hermes rejects a `previous channel identifier` | Don't resume a half-open channel with `tx chan-open-try`. Run `create channel` again for a fresh id. |
| Packet stuck on Noble side | Check the Noble node itself is not halted on a missed upgrade before blaming the relayer. |

## Links

- Source: <https://github.com/rotkonetworks/hermes>
- Example config: [`config.example.toml`](../config.example.toml)
- Live relay status: <https://ibc.rotko.net>
