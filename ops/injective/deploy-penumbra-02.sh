#!/bin/bash
# Make penumbra-02 (ct1102 on bkk07) a SECOND injective relayer (backup to the
# primary drain rig on penumbra-01/ct1101).
#
# penumbra-02 is a plain `hermes start` daemon (currently penumbra-1 + noble-1).
# This adds injective-1 to its config so the daemon also refreshes the inj
# client and relays straightforward inj<->penumbra packets. It is a LIGHTER
# backup: penumbra-01's flock-drain rig remains the robust primary (it handles
# the injective ack quirks - hermes #4387 - and born-expired packets that a
# bare daemon does not).
#
# ---------------------------------------------------------------------------
# PREREQUISITES (only you can do these - secret key material + a funded balance):
#
#   1. Give ct1102 its OWN inj key (do NOT reuse penumbra-01's inj key - two
#      relayers signing from one inj account race on the account sequence and
#      one tx always fails):
#        ssh bkk07 "pct exec 1102 -- hermes keys add --chain injective-1 \
#          --mnemonic-file /root/inj-relayer.mnemonic --hd-path \"m/44'/60'/0'/0/0\""
#        ssh bkk07 "pct exec 1102 -- hermes keys list --chain injective-1"   # note the inj1 addr
#   2. Fund that inj1 address with INJ for gas (exchange withdrawal, or sweep -
#      packages/wallet/scripts/inj-sweep.mts). A financial action - you do it.
#
# Only once the key exists AND is funded, run this script. Until then the daemon
# would just error every relay attempt on injective.
# ---------------------------------------------------------------------------
set -euo pipefail

HOST=bkk07
CT=1102
BLOCK=/home/alice/rotko/hermes/ops/injective/config-injective-block.toml

# 0. refuse if the inj key is not present (avoids a config that can't sign)
if ! ssh -o BatchMode=yes "$HOST" "pct exec $CT -- hermes keys list --chain injective-1" >/dev/null 2>&1; then
  # keys list errors when the chain is absent from config too, so check the key file directly
  if ! ssh -o BatchMode=yes "$HOST" "pct exec $CT -- test -s /root/.hermes/keys/injective-1/keyring-test/*.json" 2>/dev/null; then
    echo "ABORT: no injective-1 key on ct1102 yet. Do the PREREQUISITES first." >&2
    exit 1
  fi
fi

# 1. back up the current config
ssh -o BatchMode=yes "$HOST" "pct exec $CT -- cp /root/.hermes/config.toml /root/.hermes/config.toml.bak.\$(date +%s)"

# 2. append the injective-1 chain block (strip the leading comment header; the
#    [[chains]] block starts at the first '[[chains]]' line of the drafted file)
sed -n '/^\[\[chains\]\]/,/^# ===/p' "$BLOCK" | sed '/^# ===/d' \
  | ssh -o BatchMode=yes "$HOST" "pct exec $CT -- tee -a /root/.hermes/config.toml >/dev/null"

# 3. add channel-18 to the penumbra-1 packet_filter allow-list (MANUAL - the
#    filter is inside the existing penumbra-1 [[chains]] block; do this by hand
#    to avoid clobbering the noble entry):
echo
echo ">>> MANUAL STEP: edit /root/.hermes/config.toml on ct1102 and add"
echo "        ['transfer', 'channel-18']   # penumbra-1 <-> injective-1"
echo "    to the penumbra-1 [chains.packet_filter] list (next to channel-4/noble)."
echo "    Then press enter to validate + restart, or Ctrl-C to stop."
read -r _

# 4. validate config, then restart the daemon
ssh -o BatchMode=yes "$HOST" "pct exec $CT -- hermes config validate"
ssh -o BatchMode=yes "$HOST" "pct exec $CT -- systemctl restart hermes"
sleep 6
ssh -o BatchMode=yes "$HOST" "pct exec $CT -- bash -lc '
  systemctl is-active hermes
  journalctl -u hermes -n 20 --no-pager | grep -iE \"injective|channel-494|channel-18|error\" | tail -10
'"
echo
echo "Done. Verify no packets get stuck on either host:"
echo "  ssh bkk06jump \"pct exec 1101 -- hermes query packet pending --chain penumbra-1 --port transfer --channel channel-18\""
