# Reopening Cosmos Hub and Osmosis to Penumbra

The original channels are dead (expired clients; penumbra 2.0.x has no client
recovery). This opens NEW ones. Nothing here needs governance: creating
clients, connections and channels is permissionless.

## Before

1. Keys with a little gas on each chain (hermes pays the handshakes and every
   client update / packet afterwards):
   - `hermes keys add --chain cosmoshub-4 --key-name cosmoshub-relayer --mnemonic-file ...` + some ATOM
   - `hermes keys add --chain osmosis-1 --key-name osmosis-relayer --mnemonic-file ...` + some OSMO
   - the existing penumbra-1 relayer key (already funded for injective)
2. Append `config-blocks.toml` to `~/.hermes/config.toml`, probe each grpc
   endpoint, `hermes health-check`.

## Open the channels

```sh
hermes create channel --a-chain penumbra-1 --b-chain cosmoshub-4 \
  --a-port transfer --b-port transfer --new-client-connection --yes
hermes create channel --a-chain penumbra-1 --b-chain osmosis-1 \
  --a-port transfer --b-port transfer --new-client-connection --yes
```

Each prints the channel id on both sides. Put them in the packet filters
(`channel-HUB`, `channel-OSMO`, and the two penumbra-side ids), restart hermes,
and send a tiny round-trip transfer each way before announcing anything.

## Keep them alive

A client that isn't updated within its trusting period expires, and 2.0.x
can't recover it - that's how the old channels died. Hermes updates clients
while it relays; add the new clients to monitoring (gatus config in
`ibc-relay/status-penumbra`, same shape as the injective client check) so a
stalled relayer alerts long before expiry.

## After (outside this repo)

1. **Registry** (penumbrafi/registry): the two ibcConnections with the new
   channel ids, and the new assets - `transfer/<new penumbra channel>/uatom`,
   `.../uosmo` (different assets from the old stuck ATOM/OSMO).
2. **Zafu** (`apps/extension/src/config/networks.ts`): `cosmoshub` / `osmosis`
   `launched: true` with `ibcChainId`; channel ids in
   `packages/wallet/src/networks/cosmos/chains.ts` (`penumbraChannel` = the
   chain-side channel). Both are coin-118, so the existing cosmos deposit UI
   covers them - no new wallet code.
3. **Veil**: the same channel ids in its deposit / withdraw config.
4. Optional parameter change: add the new ATOM / OSMO as routing candidates or
   fee assets (they need a liquid pair against UM first).

Old ATOM / OSMO already on Penumbra came over the dead channels: a new channel
is a different asset, so those can't exit through it - only swap on the DEX.
