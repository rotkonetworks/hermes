# Recovering your timed-out USDC unshield (penumbra-1)

## What happened
Your USDC unshield (penumbra -> noble) was submitted just before the 2026-09-02
chain halt and never relayed to noble before its timeout expired. It sat in the
IBC escrow: not delivered to noble, not yet refunded.

We have now relayed the timeout. The escrow is released and the **2040 USDC is
refunded to your penumbra address** (the return address on the original
withdrawal). Nothing more is required on-chain.

Reference (public, on-chain):
- withdrawal tx: `1b20b77b6f4608aa0ad3372f6f803f6d303ee8a4c813f8ad2eaf902c58828145` (height 12580284)
- packet: channel-2, sequence 7693, 2,040,000,000 uusdc
- refund (timeout) tx: `CFC5D9AA64073753CBA0FFBEFDF20F045FDCBA979459DADB42E849131F2F8EBA` (height 12763226)

## Why you don't see it in your wallet yet
Your wallet keeps a local "view" that it builds by scanning every block with
your viewing key. That local sync was built before the restart and is stuck at
the boundary, so it shows stale balances. Two things fix it: point the wallet at
a node that actually restarted, and make it re-scan across the boundary.

A wallet still pointed at an RPC endpoint whose node never restarted will never
sync past the halt height.

## Steps (Prax / Zafu or any Penumbra wallet)

1. Point the wallet at a current, restarted-chain RPC endpoint. In Prax:
   settings -> the RPC / gRPC endpoint -> set a current one, for example
   `https://penumbra.rotko.net` (or any listed endpoint that is on 2.0.9).

2. Trigger a re-sync so it re-scans across the restart. In Prax this is the
   resync / clear-cache option under advanced settings.

3. Let it finish syncing (the first scan can take a while), then check the
   balance. The 2040 USDC should be restored, shown as the noble-bridged USDC
   asset.

4. If it will not sync or still shows the wrong balance, re-import the wallet
   from your recovery phrase against that RPC endpoint. This is safe: your keys
   and address derive from the seed and the funds are already on-chain, so this
   only forces a clean full scan. (Never share your recovery phrase or full
   viewing key with anyone, including us -- we don't need them.)

## Advanced (pcli)
If you use the `pcli` CLI instead of a wallet: use a current `pcli` (the restart
moved the chain to 2.0.9; an older client cannot read a view built against it),
then:
```
pcli --grpc-url https://penumbra.rotko.net view reset
pcli --grpc-url https://penumbra.rotko.net balance
```

## Verify on-chain (optional)
The refund is the `timeout_packet` event in tx `CFC5D9AA...` above. It credits
the packet's return address, which is your address.

## If it still doesn't show
Send us:
- your `pcli --version`
- the gRPC endpoint you synced against
- the output of `pcli view reset` followed by `pcli balance`

No viewing key is needed for any of this — do not share it.
