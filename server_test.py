#!/usr/bin/env python3
"""
Local reproduction of the Zcash Android wallet's 'Choose a Server' test.

Mirrors FastestServerFetcher from zcash-android-wallet-sdk:
  Phase 1 (parallel): validate each server + measure GetLightdInfo and
                      GetLatestBlock latencies. Sort by mean of those two.
                      Keep top K=3 OR anything <=300ms mean latency.
  Phase 2 (sequential, in latency order): fetch the last 100 blocks via
                      GetBlockRange with a 60s timeout. First K survivors
                      win.

Validation gates (any one fails => server dropped):
  - GetLightdInfo reachable within 5s
  - chainName matches network (main / test)
  - saplingActivationHeight matches the well-known constant
  - consensusBranchId matches majority across responding servers
  - estimatedHeight - blockHeight < 288 (server is in sync)
  - GetLatestBlock reachable within 5s
"""

import argparse
import concurrent.futures
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Optional

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "generated"))

import grpc  # noqa: E402
import service_pb2  # noqa: E402
import service_pb2_grpc  # noqa: E402

# Endpoints copied from
# ui-lib/src/main/java/co/electriccoin/zcash/ui/common/provider/LightWalletEndpointProvider.kt
MAINNET_ENDPOINTS = [
    ("us.zec.stardust.rest", 443),
    ("eu.zec.stardust.rest", 443),
    ("eu2.zec.stardust.rest", 443),
    ("jp.zec.stardust.rest", 443),
    ("zec.rocks", 443),
    ("na.zec.rocks", 443),
    ("sa.zec.rocks", 443),
    ("eu.zec.rocks", 443),
    ("ap.zec.rocks", 443),
]
TESTNET_ENDPOINTS = [("testnet.zec.rocks", 443)]

# Constants mirrored from FastestServerFetcher.kt
K = 3
N = 100
LATENCY_THRESHOLD_MS = 300
FETCH_THRESHOLD_S = 60
SYNCED_THRESHOLD_BLOCKS = 288
INFO_TIMEOUT_S = 5

EXPECTED_NETWORK_NAME = {"mainnet": "main", "testnet": "test"}
EXPECTED_SAPLING_ACTIVATION = {"main": 419200, "test": 280000}

# ANSI
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"


@dataclass
class Result:
    host: str
    port: int
    reachable: bool = False
    network_name: Optional[str] = None
    sapling_activation: Optional[int] = None
    consensus_branch_id: Optional[str] = None
    block_height: Optional[int] = None
    estimated_height: Optional[int] = None
    latest_height: Optional[int] = None
    info_ms: Optional[float] = None
    latest_ms: Optional[float] = None
    block_range_ms: Optional[float] = None
    block_range_ok: Optional[bool] = None
    rule_out: list = field(default_factory=list)
    info_error: Optional[str] = None
    latest_error: Optional[str] = None

    @property
    def mean_ms(self) -> Optional[float]:
        # Only successful calls contribute to the SDK's ranking metric.
        if (
            self.info_ms is not None
            and self.latest_ms is not None
            and self.info_error is None
            and self.latest_error is None
        ):
            return (self.info_ms + self.latest_ms) / 2.0
        return None


def _new_stub(host: str, port: int):
    creds = grpc.ssl_channel_credentials()
    channel = grpc.secure_channel(f"{host}:{port}", creds)
    return channel, service_pb2_grpc.CompactTxStreamerStub(channel)


def _short_grpc_error(e: grpc.RpcError) -> str:
    try:
        return e.code().name
    except Exception:
        return str(e)


def measure_endpoint(host: str, port: int, network: str) -> Result:
    r = Result(host=host, port=port)
    expected_net = EXPECTED_NETWORK_NAME[network]
    expected_sapling = EXPECTED_SAPLING_ACTIVATION[expected_net]

    channel = None
    try:
        channel, stub = _new_stub(host, port)

        # GetLightdInfo (5s timeout, matching the wallet).
        t0 = time.perf_counter()
        try:
            info = stub.GetLightdInfo(service_pb2.Empty(), timeout=INFO_TIMEOUT_S)
            r.info_ms = (time.perf_counter() - t0) * 1000.0
            r.reachable = True
            r.network_name = info.chainName
            r.sapling_activation = info.saplingActivationHeight
            r.consensus_branch_id = info.consensusBranchId
            r.block_height = info.blockHeight
            r.estimated_height = info.estimatedHeight

            if info.chainName != expected_net:
                r.rule_out.append(f"network={info.chainName!r}")
            if info.saplingActivationHeight != expected_sapling:
                r.rule_out.append(
                    f"saplingActivation={info.saplingActivationHeight}"
                )
            gap = info.estimatedHeight - info.blockHeight
            if gap >= SYNCED_THRESHOLD_BLOCKS:
                r.rule_out.append(f"unsynced(gap={gap})")
        except grpc.RpcError as e:
            r.info_ms = (time.perf_counter() - t0) * 1000.0
            r.info_error = _short_grpc_error(e)
        except Exception as e:
            r.info_error = f"{type(e).__name__}: {e}"

        # GetLatestBlock (5s timeout, matching the wallet). Attempted
        # independently so we still see what the server reports as the tip,
        # even when GetLightdInfo fails.
        t0 = time.perf_counter()
        try:
            latest = stub.GetLatestBlock(
                service_pb2.ChainSpec(), timeout=INFO_TIMEOUT_S
            )
            r.latest_ms = (time.perf_counter() - t0) * 1000.0
            r.latest_height = latest.height
            r.reachable = True
        except grpc.RpcError as e:
            r.latest_ms = (time.perf_counter() - t0) * 1000.0
            r.latest_error = _short_grpc_error(e)
        except Exception as e:
            r.latest_error = f"{type(e).__name__}: {e}"
    finally:
        if channel is not None:
            channel.close()
    return r


def measure_block_range(r: Result) -> None:
    if r.block_height is None:
        return
    channel = None
    try:
        channel, stub = _new_stub(r.host, r.port)
        to_h = r.block_height
        from_h = max(0, to_h - N)
        req = service_pb2.BlockRange(
            start=service_pb2.BlockID(height=from_h),
            end=service_pb2.BlockID(height=to_h),
        )
        t0 = time.perf_counter()
        try:
            for _ in stub.GetBlockRange(req, timeout=FETCH_THRESHOLD_S):
                pass
            r.block_range_ms = (time.perf_counter() - t0) * 1000.0
            r.block_range_ok = True
        except grpc.RpcError:
            r.block_range_ok = False
    except Exception:
        r.block_range_ok = False
    finally:
        if channel is not None:
            channel.close()


def cell(text: str, width: int, color: str = "", align: str = "<") -> str:
    if align == ">":
        padded = text.rjust(width)
    elif align == "^":
        padded = text.center(width)
    else:
        padded = text.ljust(width)
    return f"{color}{padded}{RESET}" if color else padded


def latency_color(ms: Optional[float]) -> str:
    if ms is None:
        return DIM
    if ms <= LATENCY_THRESHOLD_MS:
        return GREEN
    if ms <= 1000:
        return YELLOW
    return RED


def latency_cell(ms: Optional[float], width: int = 10) -> str:
    if ms is None:
        return cell("n/a", width, DIM, ">")
    return cell(f"{ms:.1f}ms", width, latency_color(ms), ">")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reproduce the Zcash Android wallet's 'Choose a Server' test."
    )
    parser.add_argument(
        "--testnet",
        action="store_true",
        help="Test the testnet endpoint instead of mainnet.",
    )
    args = parser.parse_args()
    network = "testnet" if args.testnet else "mainnet"
    endpoints = TESTNET_ENDPOINTS if args.testnet else MAINNET_ENDPOINTS

    print(f"{BOLD}Zcash lightwalletd server test ({network}){RESET}")
    print(
        f"{DIM}Mirrors FastestServerFetcher.kt from zcash-android-wallet-sdk.{RESET}\n"
    )

    print(
        f"{BOLD}[Phase 1]{RESET} Measuring GetLightdInfo + GetLatestBlock in parallel "
        f"({len(endpoints)} servers, {INFO_TIMEOUT_S}s timeout)..."
    )
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, len(endpoints))
    ) as pool:
        results = list(
            pool.map(
                lambda ep: measure_endpoint(ep[0], ep[1], network),
                endpoints,
            )
        )

    # Majority consensus branch id across responding servers.
    branch_ids = [r.consensus_branch_id for r in results if r.consensus_branch_id]
    majority_branch = (
        Counter(branch_ids).most_common(1)[0][0] if branch_ids else None
    )
    for r in results:
        if (
            r.consensus_branch_id
            and majority_branch
            and r.consensus_branch_id != majority_branch
        ):
            r.rule_out.append(f"branchId={r.consensus_branch_id}")

    # SDK trimming: sort valid responders by mean latency, keep top K or <=300ms.
    valid = [
        r for r in results if r.mean_ms is not None and not r.rule_out
    ]
    valid.sort(key=lambda r: r.mean_ms)
    survivors = [
        r for i, r in enumerate(valid)
        if i < K or r.mean_ms <= LATENCY_THRESHOLD_MS
    ]

    print(
        f"{BOLD}[Phase 2]{RESET} Fetching last {N} blocks from {len(survivors)} candidate(s) "
        f"({FETCH_THRESHOLD_S}s timeout each)..."
    )
    final_top: list = []
    for r in survivors:
        measure_block_range(r)
        if r.block_range_ok and len(final_top) < K:
            final_top.append(r)
    print()

    # ---- SDK-style ranking (what the app would show at the top) ----
    print(f"{BOLD}=== App ranking (what the wallet would suggest) ==={RESET}")
    if not final_top:
        print(f"  {RED}No servers passed both phases.{RESET}")
    else:
        for i, r in enumerate(final_top, 1):
            badge = f"{GREEN}{BOLD}#{i}{RESET}"
            print(
                f"  {badge}  {BOLD}{r.host}{RESET}  "
                f"mean {latency_cell(r.mean_ms)}  "
                f"100-block fetch {latency_cell(r.block_range_ms)}"
            )
    print()

    # ---- Full table ----
    def sort_key(r: Result):
        if not r.reachable:
            return (4, 0.0)
        if r.info_error and r.latest_error:
            return (3, 0.0)
        if r.rule_out or r.info_error or r.latest_error:
            best = r.mean_ms or r.info_ms or r.latest_ms or float("inf")
            return (2, best)
        if r.block_range_ok is False:
            return (1, r.mean_ms or float("inf"))
        return (0, r.mean_ms or float("inf"))

    all_sorted = sorted(results, key=sort_key)
    final_set = {(r.host, r.port) for r in final_top}

    widths = {
        "n": 2, "host": 22, "info": 10, "latest": 10, "mean": 10,
        "tip": 9, "gap": 9, "branch": 8, "fetch": 12, "verdict": 0,
    }
    header_cells = [
        cell("#", widths["n"], BOLD, ">"),
        cell("host", widths["host"], BOLD),
        cell("info", widths["info"], BOLD, ">"),
        cell("latest", widths["latest"], BOLD, ">"),
        cell("mean", widths["mean"], BOLD, ">"),
        cell("tip", widths["tip"], BOLD, ">"),
        cell("sync gap", widths["gap"], BOLD, ">"),
        cell("branch", widths["branch"], BOLD),
        cell("fetch 100blk", widths["fetch"], BOLD, ">"),
        cell("verdict", 0, BOLD),
    ]
    print(f"{BOLD}=== All servers ==={RESET}")
    print("  " + "  ".join(header_cells))
    print("  " + DIM + "-" * 120 + RESET)

    for i, r in enumerate(all_sorted, 1):
        info_c = latency_cell(r.info_ms, widths["info"])
        latest_c = latency_cell(r.latest_ms, widths["latest"])
        mean_c = latency_cell(r.mean_ms, widths["mean"])

        if r.latest_height is not None:
            tip_c = cell(str(r.latest_height), widths["tip"], GREEN, ">")
        elif r.block_height is not None:
            # Fall back to the LightdInfo tip if GetLatestBlock failed.
            tip_c = cell(str(r.block_height), widths["tip"], DIM, ">")
        else:
            tip_c = cell("n/a", widths["tip"], DIM, ">")

        if r.block_height is not None and r.estimated_height is not None:
            gap = r.estimated_height - r.block_height
            gap_color = GREEN if gap < SYNCED_THRESHOLD_BLOCKS else RED
            gap_c = cell(str(gap), widths["gap"], gap_color, ">")
        else:
            gap_c = cell("n/a", widths["gap"], DIM, ">")

        if r.consensus_branch_id:
            branch_text = r.consensus_branch_id[: widths["branch"]]
            if majority_branch and r.consensus_branch_id == majority_branch:
                branch_c = cell(branch_text, widths["branch"], GREEN)
            else:
                branch_c = cell(branch_text, widths["branch"], RED)
        else:
            branch_c = cell("-", widths["branch"], DIM)

        if r.block_range_ok:
            fetch_c = latency_cell(r.block_range_ms, widths["fetch"])
        elif r.block_range_ok is False:
            fetch_c = cell("FAIL", widths["fetch"], RED, ">")
        else:
            fetch_c = cell("skip", widths["fetch"], DIM, ">")

        rpc_errs = []
        if r.info_error:
            rpc_errs.append(f"GetLightdInfo:{r.info_error}")
        if r.latest_error:
            rpc_errs.append(f"GetLatestBlock:{r.latest_error}")

        if rpc_errs and not r.rule_out:
            verdict = f"{RED}{', '.join(rpc_errs)}{RESET}"
        elif r.rule_out or rpc_errs:
            parts = list(r.rule_out) + rpc_errs
            verdict = f"{YELLOW}disqualified: {', '.join(parts)}{RESET}"
        elif r.block_range_ok is False:
            verdict = f"{RED}fetch failed{RESET}"
        elif (r.host, r.port) in final_set:
            rank = next(
                idx for idx, x in enumerate(final_top, 1)
                if (x.host, x.port) == (r.host, r.port)
            )
            verdict = f"{GREEN}{BOLD}TOP #{rank}{RESET}"
        elif r in survivors:
            verdict = f"{CYAN}passed phase 1, not in top {K}{RESET}"
        else:
            verdict = f"{DIM}slower than top {K}{RESET}"

        row = [
            cell(str(i), widths["n"], align=">"),
            cell(r.host, widths["host"]),
            info_c, latest_c, mean_c, tip_c, gap_c, branch_c, fetch_c, verdict,
        ]
        print("  " + "  ".join(row))

    print()
    print(
        f"{DIM}Thresholds: mean RPC latency cap {LATENCY_THRESHOLD_MS}ms, "
        f"sync gap < {SYNCED_THRESHOLD_BLOCKS} blocks, "
        f"last-{N}-block fetch < {FETCH_THRESHOLD_S}s. "
        f"Branch-id reference = majority across responding servers.{RESET}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
