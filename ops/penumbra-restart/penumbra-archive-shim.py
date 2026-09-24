#!/usr/bin/env python3
"""
penumbra-archive-shim: a minimal CometBFT JSON-RPC endpoint that replays the
pre-restart light-client data of penumbra-1 (halted at 12,598,600).

Why this exists
---------------
penumbra-1 was restarted with a checkpoint genesis at initial_height 12598602.
Height 12598601 is an application-only synthetic block: there is NO CometBFT
block and NO commit for it anywhere, and migrated nodes wiped their blockstore
so they no longer serve heights <= 12598600 either.

Hermes' `adjust_headers` needs `fetch(trusted_height + 1).validators` to fill
the MsgUpdateClient `trusted_validator_set`. That is a light-block fetch at
12598601, which nothing can serve.

Key fact that makes this safe: in header 12598600,
    validators_hash == next_validators_hash == 3FAB62A1536C9FEB1820D34BFA5BD52B47D67CB90A0C330B60ADC015F681B65C
and the /validators sets at 12598600 and 12598601 are identical (same 16
members, same voting powers; only proposer_priority differs). So the light
block at 12598600 already carries exactly the validator set Noble's consensus
state expects in `trusted_validator_set`.

Therefore this shim answers `commit` at 12598601 with the *12598600* signed
header. tendermint-rs' ProdIo::fetch_light_block takes the height from the
returned header, so it then asks for validators at 12598600 (with proposer) and
12598601 (next), both of which we serve verbatim from disk. The resulting
LightBlock's `.validators` hashes to 3FAB62..., which is what
`adjust_headers` uses and all that Noble's ibc-go checks
(`checkTrustedHeader`: TrustedValidators.Hash() == consState.NextValidatorsHash).

Use with:
  hermes update client --host-chain noble-1 --client 07-tendermint-109 \
      --trusted-height 12598600 \
      --archive-address http://127.0.0.1:26670 --restart-height 12598601
(`--restart-height 12598601` so that RestartAwareIo routes heights <= 12598601
here and everything above to the live, restarted node.)
"""

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

DATA_DIR = os.environ.get("ARCHIVE_DATA_DIR", "/root/prerestart-light")
BIND = os.environ.get("ARCHIVE_BIND", "127.0.0.1")
PORT = int(os.environ.get("ARCHIVE_PORT", "26670"))

RESTART_LAST = 12598600      # last real CometBFT height of penumbra-1
SYNTHETIC = 12598601         # app-only block, no commit exists


def _load(name):
    with open(os.path.join(DATA_DIR, name)) as f:
        return json.load(f)["result"]


COMMIT_600 = _load("commit_height_12598600.json")
VALS = {
    RESTART_LAST: _load("validators_height_12598600_per_page_100.json"),
    SYNTHETIC: _load("validators_height_12598601_per_page_100.json"),
}

# Sanity: the served commit must be the pre-restart tip of penumbra-1.
_h = COMMIT_600["signed_header"]["header"]
assert _h["chain_id"] == "penumbra-1", _h["chain_id"]
assert int(_h["height"]) == RESTART_LAST, _h["height"]
assert _h["validators_hash"] == _h["next_validators_hash"], "validator set changed at the boundary"


def ok(rid, result):
    return {"jsonrpc": "2.0", "id": rid, "result": result}


def err(rid, msg, code=-32603):
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": "Internal error", "data": msg}}


def height_of(params):
    if not params:
        return None
    h = params.get("height")
    if h in (None, "", "0", 0):
        return None
    return int(h)


def dispatch(method, params, rid):
    # Log every call: during the restart this log is the record of exactly which
    # heights hermes asked the archive for.
    sys.stderr.write("serving %s height=%s\n" % (method, (params or {}).get("height")))
    sys.stderr.flush()
    if method == "commit":
        h = height_of(params)
        # `latest` (height absent/0) is deliberately unsupported: this shim must
        # never be mistaken for a live node.
        if h in (RESTART_LAST, SYNTHETIC):
            return ok(rid, COMMIT_600)
        return err(rid, f"archive shim serves commit only at {RESTART_LAST}/{SYNTHETIC}, got {h}")

    if method == "validators":
        h = height_of(params)
        if h in VALS:
            return ok(rid, VALS[h])
        return err(rid, f"archive shim serves validators only at {RESTART_LAST}/{SYNTHETIC}, got {h}")

    if method == "status":
        # Enough shape for a probe; deliberately reports the halted tip.
        return ok(rid, {
            "node_info": {
                "protocol_version": {"p2p": "8", "block": "11", "app": "0"},
                "id": "0000000000000000000000000000000000000000",
                "listen_addr": "tcp://0.0.0.0:26656",
                "network": "penumbra-1",
                "version": "0.37.15",
                "channels": "40",
                "moniker": "penumbra-archive-shim",
                "other": {"tx_index": "off", "rpc_address": f"tcp://{BIND}:{PORT}"},
            },
            "sync_info": {
                "latest_block_hash": COMMIT_600["signed_header"]["commit"]["block_id"]["hash"],
                "latest_app_hash": _h["app_hash"],
                "latest_block_height": str(RESTART_LAST),
                "latest_block_time": _h["time"],
                "catching_up": False,
            },
            "validator_info": {
                "address": _h["proposer_address"],
                "pub_key": {"type": "tendermint/PubKeyEd25519", "value": ""},
                "voting_power": "0",
            },
        })

    return err(rid, f"archive shim does not implement method {method!r}", code=-32601)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _respond(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n)
        try:
            req = json.loads(raw)
        except Exception as e:
            return self._respond(err(None, f"bad json: {e}", code=-32700))
        if isinstance(req, list):
            return self._respond([dispatch(r.get("method"), r.get("params"), r.get("id")) for r in req])
        self._respond(dispatch(req.get("method"), req.get("params"), req.get("id")))

    def do_GET(self):
        # URI-style requests, for curl-based checks: /commit?height=12598601
        u = urlparse(self.path)
        method = u.path.lstrip("/") or "status"
        params = {k: v[0] for k, v in parse_qs(u.query).items()}
        self._respond(dispatch(method, params, -1))

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))


if __name__ == "__main__":
    srv = ThreadingHTTPServer((BIND, PORT), Handler)
    print(f"penumbra-archive-shim on {BIND}:{PORT} serving heights {RESTART_LAST}/{SYNTHETIC} from {DATA_DIR}", flush=True)
    srv.serve_forever()
