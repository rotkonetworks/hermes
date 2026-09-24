# penumbra-1 restart: crossing Noble client 07-tendermint-109 over the boundary

## Facts (verified)
- Noble consensus state @12598600 next_validators_hash
  = 3FAB62A1536C9FEB1820D34BFA5BD52B47D67CB90A0C330B60ADC015F681B65C
  = header@12598600.validators_hash = header@12598600.next_validators_hash
  = locally computed hash of the saved 16-validator sets at both 12598600 and 12598601.
- New set = 14 validators, all 14 present in the old 16 with IDENTICAL voting power.
  Old total 7,089,521,028,265; new total 3,908,449,679,831.
  Any CometBFT-valid commit on the new chain carries >= 2/3 of 3.908T = 2.606T
  = 36.76% of old power > 33.33% required by trust_level 1/3. Guaranteed, not average.
- Client trusting period 403200s; expiry 2026-09-07T11:37:07Z.

## Mechanism
Height 12598601 is application-only (no CometBFT block/commit); migrated nodes serve
nothing <= 12598600. `adjust_headers` needs a light block at trusted_height+1 purely for
its `.validators`. `penumbra-archive-shim` (127.0.0.1:26670 in CT1101) answers `commit`
at 12598601 with the saved 12598600 commit, so tendermint-rs builds a LightBlock whose
`.validators` is the real 16-set with the real proposer, hashing to 3FAB62... exactly
what Noble requires. NOTE: serving 12598601 IS the mechanism -- do not "fix" the shim to
error there.

Hermes needs no code change: the deployed `hermes 1.8.2+599e4690` already has
`--archive-address` / `--restart-height` (upstream genesis-restart support, RestartAwareIo).

## Automatic path (already installed, nothing to do)
noble-refresh-on-restart.service waits for local height >= 12598605, then runs
/usr/local/bin/hermes-client-refresh-109.sh (6 attempts, 60s apart), which now uses
HERMES=/usr/local/bin/hermes-restart. That wrapper detects Noble's client still sitting
at 12598600 and adds:
    --trusted-height 12598600 --height <tip-2> \
    --archive-address http://127.0.0.1:26670 --restart-height 12598601
Once Noble has a post-restart consensus state it stops adding them and execs stock hermes.

## Manual path (run in CT1101 on bkk06)
    pct exec 1101 -- systemctl status penumbra-archive-shim   # must be active
    pct exec 1101 -- curl -s 'http://127.0.0.1:26670/commit?height=12598601' | head -c 80
    pct exec 1101 -- /usr/local/bin/hermes-restart update client \
        --host-chain noble-1 --client 07-tendermint-109
Fully explicit equivalent (take the drain flock first if the drain loop is running):
    pct exec 1101 -- flock /var/run/hermes-drain-pending.lock \
      /root/.cargo/bin/hermes update client \
        --host-chain noble-1 --client 07-tendermint-109 \
        --trusted-height 12598600 --height <first post-restart height, tip-2> \
        --archive-address http://127.0.0.1:26670 --restart-height 12598601

## Verify on Noble
    curl -s https://noble-api.polkachu.com/ibc/core/client/v1/client_status/07-tendermint-109
    curl -s https://noble-api.polkachu.com/ibc/core/client/v1/client_states/07-tendermint-109 \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["client_state"]["latest_height"])'
Success = latest_height revision_height > 12598600 and status Active.

## Swap back
Automatic (the wrapper falls through). Optional cleanup afterwards:
    pct exec 1101 -- systemctl disable --now penumbra-archive-shim
    # restore HERMES= line: /usr/local/bin/hermes-client-refresh-109.sh.bak-prerestart

## Fallback if the update cannot be landed before 11:37Z
1. After the chain has blocks: create a fresh client
       hermes create client --host-chain noble-1 --reference-chain penumbra-1 \
           --trusting-period 100hours
2. Ask Noble's maintenance authority for MsgRecoverClient
   { subject_client_id: "07-tendermint-109", substitute_client_id: "<new>" }
   so connection-104 / channel-89 keep working.

## Test evidence (2026-09-07 ~02:15Z, rehearsal chain penumbra-restart-drill-1)
Ran the real client 07-tendermint-109 against the drill chain (which crossed the
identical boundary from the same halt-src state) with the shim, on bkk07:

    adjusting headers with 0 supporting headers trusted=1-12598600 target=12616409
    building a MsgUpdateAnyClient from trusted height 1-12598600 to target height 1-12616409

Shim log for that run -- exactly the three boundary fetches, nothing for the target:

    serving commit     height=12598601
    serving commit     height=12598601
    serving validators height=12598600
    serving validators height=12598601

Terminal error was the deliberately unfunded throwaway Noble key failing the fee
ante at simulate ("spendable balance 0uusdc"), i.e. AFTER header assembly and
without broadcasting. Noble's 07-tendermint-109 latest_height stayed 12598600
and status Active throughout.
