# cosmoshub acks failed: hermes read `packet_ack`, ibc-go v10 only emits `packet_ack_hex`

RESOLVED — fixed on `fix/packet-ack-hex`. Kept as a record because the symptom pointed
nowhere near the cause.

## Symptom

Relaying an acknowledgement from `cosmoshub-4` to `penumbra-1` was rejected by `pd`:

```
1: failed to execute MsgAcknowledgement
2: packet ack proof verification failed
3: merkle proof verification failed
```

Reproducible across two independent packets. Delivery worked in both directions; only the
ack could not be proven, so penumbra kept the packet commitment forever.

## Cause

ibc-go v10 dropped the deprecated non-hex event attributes. It emits `packet_ack_hex` and
no longer emits `packet_ack`. Our fork (hermes 1.8.2) read only `packet_ack`:

- `crates/relayer-types/src/core/ics04_channel/events.rs` — `PKT_ACK_ATTRIBUTE_KEY = "packet_ack"`
- `crates/relayer/src/event.rs` — the match arm for that key; no arm for the hex form

An unmatched attribute left `write_ack` **empty with no error**, so hermes submitted
`MsgAcknowledgement` with no ack bytes. The merkle proof was valid — it just proved a
different value than the one claimed:

```
sha256('{"result":"AQ=="}') = CPdVftUYJv4Y2EUSvyTsdQAe268hI6R333KgqfNkCnw=   on-chain ack
sha256("")                  = 47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=   what we sent
```

The first hash is byte-identical to cosmoshub's `packet_acks/1`, which is what closed the
diagnosis. penumbra's verifier was correct throughout
(`component/ibc/src/component/proof_verification.rs`, `commit_acknowledgement` = sha256).

Only the ack leg broke because `packet_data_hex` was already the key hermes used for
packet data, so recv/connection/channel proofs were unaffected — which is why three other
proof types from the same chain verified on the same client and sent the investigation
chasing client staleness and proof heights first.

## Why only cosmoshub

| chain | sdk | ibc-go | attributes emitted | ack |
|---|---|---|---|---|
| cosmoshub-4 | v0.53.4 | v10.7.0 | `packet_ack_hex` only | failed |
| kava_2222-10 | v0.47.15 | v7.7.0 | both | ok |
| noble-1 | v0.50.14 | v8 | both | ok |
| osmosis-1, celestia | v0.50.14 / v0.52.8 | v8 lineage | both | ok |

## Fix

Upstream fixed this in PR #3922 (2024-04-02), after our 1.8.2 base — which is why we
lacked it. Unrelated to the open informalsystems/hermes#4356 ibc-go-v10 umbrella task.

Our version accepts **both** keys and lets hex win, rather than upstream's straight rename,
because penumbra's own ibc-types emits `packet_ack_hex` first and `packet_ack` only when the
ack is valid UTF-8 (`ibc-types-core-channel-0.15.1/src/events/packet.rs`) — a naive
last-match-wins would have regressed the penumbra->cosmos leg while fixing cosmoshub.

Patched both parsing paths:
- `crates/relayer/src/event.rs` — tx-query path (`hermes tx packet-ack`, the drain script)
- `crates/relayer/src/chain/cosmos/types/events/channel.rs` — websocket path (ct1102's daemon)

## Wider point

This bug was fixed upstream 18 months ago. Our fork is 1.8.2 against upstream 1.13.x, and
counterparties will keep upgrading to SDK v0.53 / ibc-go v10. Expect more failures of this
shape — presenting as something obscure rather than as a version problem.
