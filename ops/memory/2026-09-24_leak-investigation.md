# hermes memory investigation, 2026-09-24

Supersedes the conclusions of `LEAK_ANALYSIS.md` (May 2026) and
`LEAK_ANALYSIS_ADDENDUM_simulate_errors.md`. Both were written against a
different lineage and their central file reference does not exist on master.

## What was wrong in the earlier analysis

- `create_grpc_client` (`crates/relayer/src/util.rs`) does not exist on
  master. `util.rs` is 16 lines of `mod` declarations; `git log -S` finds
  nothing. It lives only on `harden/*`/`cherry` via upstream `f2d4e7f81`.
  Master instead calls the tonic codegen `ServiceClient::connect(uri)`
  inline at ~27 sites.
- A client refresh does NOT issue "3 gRPC queries per attempt".
  `query_client_state`, `query_consensus_state` and
  `query_application_status` are ABCI over the tendermint RPC client and
  create zero gRPC channels. A refresh is 2 gRPC channels steady-state
  (`chain_grpc_status` on src, `send_tx_simulate` on dst), 3 on the first
  refresh after startup.
- Unpooled channel creation cannot account for multi-GB growth. Measured
  against the pinned versions (tonic 0.10.2 / hyper 0.14.32 / h2 0.3.27):
  ~28 kB per connection including tonic overhead, and fds return to
  baseline within 2s of drop. 4 GB would require ~143,000 retained
  connections, or ~480 conn/sec sustained at 100% retention. Observed rate
  is ~24 simulate attempts per supervisor scan.

## What is actually suspected now

1. **RSS may not have been measuring a leak.** glibc malloc does not
   return freed heap to the OS. In the harness, dropping 1000 connections
   moved RSS not at all, yet a second round of 1000 grew RSS by 15 MB
   rather than 47 MB — two thirds of the heap was reused. A transient
   burst and a permanent leak are indistinguishable by RSS alone. Sample
   `VmHWM` alongside `VmRSS`, and retest under
   `MALLOC_TRIM_THRESHOLD_=131072 MALLOC_ARENA_MAX=2`.
2. **Per-in-flight-request buffers.** `max_grpc_decoding_size` is 32 MiB
   (`crates/relayer/src/config.rs:215`); with h2 windows and the
   prost-decoded copy that is ~60-70 MB peak per large concurrent
   response. 4 GB is ~60-70 concurrent large responses. Bait is a full
   supervisor enumeration (`supervisor/scan.rs:334`), not a failing
   simulate.
3. **h2 0.3.27 is missing the connection-shutdown race fix** shipped in
   h2 0.4.5 (`be12983`): the last stream reference dropping between
   `maybe_close_connection_if_no_streams` and `inner.poll()` can leave a
   connection dangling forever. Present in 0.4.14
   (`src/client.rs:1437-1450`), absent in 0.3.27 (`:1422-1424`). The 0.3.x
   branch is EOL; the only escape is moving ibc-proto to tonic 0.12.
4. **Penumbra-side, the old 60 GB was most likely a crash-resync loop.**
   `trigger_view_db_recovery_if_corrupted` (`chain/penumbra/chain.rs:2091-2143`)
   landed before the `tx_build_lock` fix: a `Note commitment missing`
   deleted the view sqlite and `exit(1)`'d, and the restart resynced from
   genesis through an `mpsc::channel(1000)` of compact blocks decoded at up
   to 12 MiB each (`worker.rs:220,41,204`) — count-bounded, not
   byte-bounded. `599e46905` removes the trigger.

## Not the leak

Penumbra gRPC clients are long-lived struct fields cloned per query
(`chain/penumbra/chain.rs:168-170,823-830`); view and custody clients are
in-memory tower services with no socket. Proving keys are `Lazy`/`OnceCell`
process statics, ~100 MB compiled in, ~29 MB touched for an IBC tx — loaded
once, not per proof. Concurrent proving is bounded to 1 per process
(`parallel` feature off, single-threaded `ChainRuntime`, exclusive
`tx_build_lock`), and across processes by the view-db flock whenever
`view_service_storage_dir` is set — which it is on ct1101.

## Latent bugs found, not fixed

- `view_server_cache()` (`chain/penumbra/chain.rs:114-117`) is never
  evicted. The cached `ViewServer` clone keeps `sync_height_rx` alive and
  the sync worker only exits when all receivers drop
  (`worker.rs:455-457`), so the worker and its in-memory SCT cannot shut
  down for the life of the process.
- Three gRPC sites set no max-decoding-message-size at all:
  `chain/cosmos.rs:696`, `chain/cosmos/query/query.rs:131`,
  `chain/cosmos.rs:2377`.
- No connect_timeout, request timeout, or h2 keepalive is set anywhere.
  `Endpoint::from(Uri)` defaults every field to `None` except
  `tcp_nodelay`. An unresponsive gRPC endpoint blocks a chain-runtime
  thread inside `block_on` until the OS gives up (~15 min).
- RUSTSEC-2026-0258 (unbounded queued empty DATA frames) affects h2
  < 0.4.16. The penumbra stack pins h2 0.4.14 and is affected.

## What this branch changes

Two fixes, neither of which is claimed to be "the" leak fix:

- refresh worker short-circuits on expired/frozen clients. Note the scope:
  `spawn_refresh_client` already skips clients that are expired at spawn
  time (`worker/client.rs:25`), so this only affects clients that expire
  DURING a run — which is the cosmoshub-4 case from May.
- `simulate_errors` telemetry label cardinality is bounded. This is a
  confirmed unbounded-growth bug and worth fixing on its own merits.

## Open: channel pooling

Worth doing for latency, CPU and fd churn, not for RSS. Design at
`scratchpad/design-grpc-channel-pool.md`. Recommendation is a per-endpoint
field on `CosmosSdkChain` (as Astria and Penumbra already do), NOT a global
URI-keyed pool: a tonic `Channel` is a `tower::Buffer` whose worker is
`tokio::spawn`ed at creation, so a pool seeded from a throwaway runtime is
poisoned for the process. A prior attempt exists on another lineage
(`ee23c0d02`, global `RwLock<HashMap<String, Channel>>`), never
forward-ported and never reverted — find out why before redoing it.
Stage 0 is metrics only: a `grpc_dials_total` counter on the existing path.
