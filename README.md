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

- `GetLightdInfo` — measured, 5 s timeout.
- `GetLatestBlock` — measured, 5 s timeout.
- Validation gates (any fail => server disqualified):
  - `chainName` matches the selected network (`main` / `test`).
  - `saplingActivationHeight` matches the network's well-known constant.
  - `estimatedHeight - blockHeight < 288` (server is in sync).
  - `consensusBranchId` matches the majority across responding servers.
- Mean RPC latency = average of the two call durations.
- Survivors: top 3 by mean latency, plus anyone <= 300 ms.

**Phase 2 — sequential, in latency order**

- `GetBlockRange` for the last 100 blocks, 60 s timeout.
- First 3 servers that succeed become the final ranking.

## Usage

```sh
./run.sh             # mainnet
./run.sh --testnet   # testnet
```

`run.sh` creates a venv on first run, installs `grpcio` / `grpcio-tools`,
generates the gRPC stubs from `proto/`, and runs `server_test.py`.

## Output

Three sections:

1. **App ranking** — the top 3 servers the wallet would suggest.
2. **All servers** — a colored table with per-server metrics:
   - `info`, `latest` — RPC latencies (failed calls show their observed
     duration, e.g. `5002 ms` for a 5 s timeout).
   - `mean` — the SDK's ranking metric.
   - `tip` — block height reported by `GetLatestBlock`.
   - `sync gap` — `estimatedHeight - blockHeight` from `GetLightdInfo`.
   - `branch` — `consensusBranchId` (green if it matches the majority).
   - `fetch 100blk` — duration of the `GetBlockRange` test.
   - `verdict` — `TOP #n`, `disqualified: <reasons>`, or per-RPC error
     codes (e.g. `GetLightdInfo:DEADLINE_EXCEEDED`).

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
