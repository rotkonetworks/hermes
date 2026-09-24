#!/bin/bash
cd /root/hermes-drilltest
pgrep -f hold-view-lock >/dev/null || setsid python3 hold-view-lock.py view/relayer-view.sqlite.lock </dev/null > lock.log 2>&1 &
sleep 1
setsid env ARCHIVE_DATA_DIR=/root/hermes-drilltest/data ARCHIVE_PORT=26670 python3 penumbra-archive-shim.py </dev/null > shim.log 2>&1 &
sleep 3
curl -s -m5 "http://127.0.0.1:26670/commit?height=12598601" >/dev/null && echo shim-up
tip=$(curl -s -m5 http://127.0.0.1:26957/status | python3 -c 'import sys,json;print(json.load(sys.stdin)["result"]["sync_info"]["latest_block_height"])')
target=$((tip-2)); echo "drill tip=$tip target=$target"
timeout 300 ./hermes --config config-drilltest.toml update client \
  --host-chain noble-1 --client 07-tendermint-109 \
  --trusted-height 12598600 --height "$target" \
  --archive-address http://127.0.0.1:26670 --restart-height 12598601 > run2.log 2>&1
echo "rc=$?"
echo "=== shim served ==="; grep serving shim.log
echo "=== assembly ==="; grep -oE "adjusting headers with [0-9]+ supporting headers trusted=[^ ]+ target=[0-9]+|building a MsgUpdateAnyClient from trusted height [^ ]+ to target height [^ ]+" run2.log
echo "=== terminal ==="; tail -1 run2.log | cut -c1-240
