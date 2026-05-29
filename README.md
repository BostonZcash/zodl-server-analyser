# zodl-server-analyser

A small Linux utility that runs the Zodl wallet's "Choose a Server" test
locally and prints the per-server metrics. It connects directly to each
lightwalletd endpoint that the wallet ships with and reproduces the ranking
the wallet would produce on-device.

## What it tests

Mirrors `FastestServerFetcher` from
[`zcash-android-wallet-sdk`](https://github.com/Electric-Coin-Company/zcash-android-wallet-sdk)
(`sdk-lib/src/main/java/cash/z/ecc/android/sdk/internal/FastestServerFetcher.kt`).

**Phase 1 — parallel, per server**

- `GetLightdInfo` — measured, **20 s hard deadline**, with a 5 s "SDK
  budget" line (the wallet's own 5 s timeout). Calls slower than 5 s are
  still measured up to 20 s but flagged with `!` and excluded from the
  SDK ranking.
- `GetLatestBlock` — same treatment as `GetLightdInfo`.
- Validation gates (any fail => server disqualified by SDK):
  - `chainName` matches the selected network (`main` / `test`).
  - `saplingActivationHeight` matches the network's well-known constant.
  - `estimatedHeight - blockHeight < 288` (server is in sync).
  - `consensusBranchId` matches the majority across responding servers.
  - `info_ms <= 5 s` and `latest_ms <= 5 s` (SDK budget).
- Mean RPC latency = average of the two call durations.
- Survivors: top 3 by mean latency, plus anyone <= 300 ms.

**Phase 2 — sequential, in latency order**

- `GetBlockRange` for the last 100 blocks, 60 s timeout.
- Runs against **every reachable server** so the full table shows each
  one's fetch latency, not just the SDK survivors. (The wallet itself
  would only run it on survivors and stop after 3 successes — that's
  still how the App ranking is computed, but the table reports the
  numbers for everyone.)
- The first 3 survivors to succeed become the final App ranking.

## Usage

```sh
./run.sh                       # mainnet, prompts for single vs sustained
./run.sh --testnet             # testnet
./run.sh --single              # skip the prompt, run once
./run.sh --sustained           # 10 Phase 1 runs averaged + one Phase 2
./run.sh --sustained --runs 5  # custom run count
```

When stdin is a TTY and no mode flag is passed, the tool asks whether to
run a single sweep or a sustained (10-run averaged) sweep.

`run.sh` creates a venv on first run, installs `grpcio` / `grpcio-tools`,
generates the gRPC stubs from `proto/`, and runs `server_test.py`.

## Output

### Single mode

1. **App ranking** — the top 3 servers the wallet would suggest.
2. **All servers** — a colored table with per-server metrics:
   - `info`, `latest` — RPC latencies. Failed calls show their observed
     duration; calls slower than the 5 s SDK budget are shown in magenta
     with a trailing `!` (still measured up to the 20 s hard deadline).
   - `mean` — the SDK's ranking metric.
   - `tip` — block height reported by `GetLatestBlock`.
   - `sync gap` — `estimatedHeight - blockHeight` from `GetLightdInfo`.
   - `branch` — `consensusBranchId` (green if it matches the majority).
   - `fetch 100blk` — duration of the `GetBlockRange` test.
   - `verdict` — `TOP #n`, `disqualified: <reasons>` (including
     `info>5s` / `latest>5s` when the SDK budget was overshot), or
     per-RPC error codes (e.g. `GetLightdInfo:DEADLINE_EXCEEDED`).

### Sustained mode

Runs Phase 1 `N` times back-to-back (default 10), aggregates per-server,
then runs Phase 2 once against the averaged ranking. Per-server columns:

- `succ` — `X/N` runs where both `GetLightdInfo` and `GetLatestBlock`
  responded (within the 20 s hard deadline).
- `info avg`, `latest avg` — mean RPC latencies across successful runs;
  flagged with `!` if the average overshoots the 5 s SDK budget.
- `mean ±std` — mean of per-run mean latencies, with standard deviation.
- `min-max` — overall fastest and slowest single-call observation.
- `>5s` — count of individual calls (across all runs) that overshot the
  SDK budget but still completed within 20 s.
- `verdict` — `TOP #n`, `disqualified: <gate>` (gate fails on every
  responding run), `unreliable: >5s ×N` / `… err ×N` (intermittent), or
  `all failed`.

## Notes

- Endpoints are hardcoded to match the wallet's
  `LightWalletEndpointProvider`. Update them in `server_test.py` if the
  wallet's list changes.
- The consensus branch ID check uses the majority across responding
  servers rather than a hardcoded network-upgrade activation table (the
  SDK delegates that computation to native Rust). A disagreement with the
  majority will flag the outlier.
- The `proto/` directory contains `service.proto` and
  `compact_formats.proto` copied verbatim from the SDK's
  `lightwallet-client-lib/src/main/proto/`.

## Layout

```
.
├── proto/                # gRPC schema (copied from the SDK)
├── generated/            # stubs produced by grpc_tools.protoc (gitignored)
├── .venv/                # virtualenv (gitignored)
├── requirements.txt
├── run.sh
└── server_test.py
```
