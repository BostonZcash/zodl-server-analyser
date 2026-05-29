#!/usr/bin/env python3
"""
Local reproduction of the Zcash Android wallet's 'Choose a Server' test,
extended to keep measuring past the SDK's 5s budget so we can collect an
objective picture of server performance.

Two modes:
  - single (default): one Phase 1 sweep + Phase 2, like the wallet.
  - sustained: N repeated Phase 1 sweeps (default 10), then Phase 2 once
    against the averaged ranking. Per-server stats include success rate,
    mean / stddev / min / max, and how often each server overshot the
    SDK's 5s budget.

The SDK's 5s budget is preserved as the "would the wallet have accepted
this" signal: calls slower than 5s are still measured (up to a 20s hard
deadline) but flagged as over-budget and excluded from the SDK ranking.

Mirrors FastestServerFetcher from zcash-android-wallet-sdk.
"""

import argparse
import concurrent.futures
import os
import statistics
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

# SDK budget — calls slower than this would have been dropped by the wallet.
INFO_TIMEOUT_S = 5
# Hard gRPC deadline — we keep trying past the SDK budget to learn the real
# latency (so a server that's "just" 7s is distinguishable from one that's
# genuinely unreachable).
INFO_HARD_TIMEOUT_S = 20
DEFAULT_RUNS = 10

EXPECTED_NETWORK_NAME = {"mainnet": "main", "testnet": "test"}
EXPECTED_SAPLING_ACTIVATION = {"main": 419200, "test": 280000}

# ANSI
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"
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
    info_over_budget: bool = False
    latest_over_budget: bool = False
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
    budget_ms = INFO_TIMEOUT_S * 1000

    channel = None
    try:
        channel, stub = _new_stub(host, port)

        # GetLightdInfo — hard 20s deadline. SDK budget (5s) is checked
        # against the measured latency afterwards.
        t0 = time.perf_counter()
        try:
            info = stub.GetLightdInfo(
                service_pb2.Empty(), timeout=INFO_HARD_TIMEOUT_S
            )
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
            if r.info_ms > budget_ms:
                r.info_over_budget = True
                r.rule_out.append(f"info>{INFO_TIMEOUT_S}s")
        except grpc.RpcError as e:
            r.info_ms = (time.perf_counter() - t0) * 1000.0
            r.info_error = _short_grpc_error(e)
        except Exception as e:
            r.info_error = f"{type(e).__name__}: {e}"

        # GetLatestBlock — same treatment. Attempted independently so we
        # still observe the server's reported tip even when GetLightdInfo
        # failed.
        t0 = time.perf_counter()
        try:
            latest = stub.GetLatestBlock(
                service_pb2.ChainSpec(), timeout=INFO_HARD_TIMEOUT_S
            )
            r.latest_ms = (time.perf_counter() - t0) * 1000.0
            r.latest_height = latest.height
            r.reachable = True
            if r.latest_ms > budget_ms:
                r.latest_over_budget = True
                r.rule_out.append(f"latest>{INFO_TIMEOUT_S}s")
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


def apply_branch_consensus(results) -> Optional[str]:
    """Flag any server whose consensusBranchId disagrees with the majority."""
    branch_ids = [r.consensus_branch_id for r in results if r.consensus_branch_id]
    majority = (
        Counter(branch_ids).most_common(1)[0][0] if branch_ids else None
    )
    for r in results:
        if (
            r.consensus_branch_id
            and majority
            and r.consensus_branch_id != majority
        ):
            r.rule_out.append(f"branchId={r.consensus_branch_id}")
    return majority


def run_phase1(endpoints, network):
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, len(endpoints))
    ) as pool:
        return list(
            pool.map(
                lambda ep: measure_endpoint(ep[0], ep[1], network),
                endpoints,
            )
        )


def pick_survivors(results):
    valid = [r for r in results if r.mean_ms is not None and not r.rule_out]
    valid.sort(key=lambda r: r.mean_ms)
    return [
        r for i, r in enumerate(valid)
        if i < K or r.mean_ms <= LATENCY_THRESHOLD_MS
    ]


def run_phase2(results, survivors):
    """Measure GetBlockRange on every reachable server.

    App ranking is decided from `survivors` in the order given (first K
    successes). Non-survivors are still measured so the full table shows
    their fetch latency for comparison.
    """
    final_top = []
    measured = set()
    for r in survivors:
        if r.block_height is None:
            continue
        measure_block_range(r)
        measured.add((r.host, r.port))
        if r.block_range_ok and len(final_top) < K:
            final_top.append(r)
    for r in results:
        if r.block_height is None:
            continue
        if (r.host, r.port) in measured:
            continue
        measure_block_range(r)
    return final_top


# ---------- Sustained-mode aggregation ----------

@dataclass
class Sustained:
    host: str
    port: int
    runs: list  # list[Result]

    @property
    def n_runs(self) -> int:
        return len(self.runs)

    @property
    def info_times(self):
        return [
            r.info_ms for r in self.runs
            if r.info_ms is not None and r.info_error is None
        ]

    @property
    def latest_times(self):
        return [
            r.latest_ms for r in self.runs
            if r.latest_ms is not None and r.latest_error is None
        ]

    @property
    def both_success_count(self) -> int:
        return sum(1 for r in self.runs if r.mean_ms is not None)

    @property
    def info_avg(self):
        ts = self.info_times
        return statistics.mean(ts) if ts else None

    @property
    def latest_avg(self):
        ts = self.latest_times
        return statistics.mean(ts) if ts else None

    @property
    def mean_avg(self):
        ms = [r.mean_ms for r in self.runs if r.mean_ms is not None]
        return statistics.mean(ms) if ms else None

    @property
    def mean_stddev(self) -> float:
        ms = [r.mean_ms for r in self.runs if r.mean_ms is not None]
        return statistics.stdev(ms) if len(ms) >= 2 else 0.0

    @property
    def overall_min(self):
        all_t = self.info_times + self.latest_times
        return min(all_t) if all_t else None

    @property
    def overall_max(self):
        all_t = self.info_times + self.latest_times
        return max(all_t) if all_t else None

    @property
    def over_budget_count(self) -> int:
        return sum(
            (1 if r.info_over_budget else 0)
            + (1 if r.latest_over_budget else 0)
            for r in self.runs
        )

    @property
    def info_error_count(self) -> int:
        return sum(1 for r in self.runs if r.info_error)

    @property
    def latest_error_count(self) -> int:
        return sum(1 for r in self.runs if r.latest_error)

    @property
    def consensus_branch_id(self):
        ids = [r.consensus_branch_id for r in self.runs if r.consensus_branch_id]
        return Counter(ids).most_common(1)[0][0] if ids else None

    @property
    def consistent_gates(self):
        """Non-budget rule_out reasons that appear in every responding run."""
        responding = [r for r in self.runs if r.reachable]
        if not responding:
            return []
        budget_tags = {f"info>{INFO_TIMEOUT_S}s", f"latest>{INFO_TIMEOUT_S}s"}
        sets = [
            {x for x in r.rule_out if x not in budget_tags}
            for r in responding
        ]
        common = sets[0]
        for s in sets[1:]:
            common &= s
        return sorted(common)

    @property
    def representative(self):
        """Last run with a usable block_height, for Phase 2."""
        for r in reversed(self.runs):
            if r.block_height is not None and r.info_error is None:
                return r
        return None


def aggregate(per_run_results, endpoints):
    by_endpoint = {(h, p): [] for h, p in endpoints}
    for run in per_run_results:
        for r in run:
            by_endpoint[(r.host, r.port)].append(r)
    return [
        Sustained(host=h, port=p, runs=by_endpoint[(h, p)])
        for h, p in endpoints
    ]


# ---------- Output helpers ----------

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
    if ms <= INFO_TIMEOUT_S * 1000:
        return RED
    return MAGENTA  # over SDK budget


def latency_cell(
    ms: Optional[float], width: int = 10, over_budget: bool = False
) -> str:
    if ms is None:
        return cell("n/a", width, DIM, ">")
    mark = "!" if over_budget else ""
    return cell(f"{ms:.1f}ms{mark}", width, latency_color(ms), ">")


# ---------- Single-mode rendering (unchanged shape) ----------

def print_single(results, network, survivors, final_top, majority_branch):
    print()
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
        "n": 2, "host": 22, "info": 11, "latest": 11, "mean": 10,
        "tip": 9, "gap": 9, "branch": 8, "fetch": 12,
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
        info_c = latency_cell(r.info_ms, widths["info"], r.info_over_budget)
        latest_c = latency_cell(r.latest_ms, widths["latest"], r.latest_over_budget)
        mean_c = latency_cell(r.mean_ms, widths["mean"])

        if r.latest_height is not None:
            tip_c = cell(str(r.latest_height), widths["tip"], GREEN, ">")
        elif r.block_height is not None:
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


# ---------- Sustained-mode rendering ----------

def print_sustained(sustained, n_runs, final_top, majority_branch):
    final_set = {(s.host, s.port) for s in final_top}

    def sort_key(s: Sustained):
        # Order: phase-2 survivors first (by rank), then by SDK-acceptable
        # mean, then by success rate, then by failure mode.
        if (s.host, s.port) in final_set:
            rank = next(
                i for i, x in enumerate(final_top, 1)
                if (x.host, x.port) == (s.host, s.port)
            )
            return (0, rank, 0.0)
        if s.both_success_count == 0:
            return (4, 0, 0.0)
        if s.consistent_gates:
            return (3, 0, s.mean_avg or float("inf"))
        if s.over_budget_count > 0:
            return (2, 0, s.mean_avg or float("inf"))
        return (1, 0, s.mean_avg or float("inf"))

    print()
    print(
        f"{BOLD}=== App ranking (averaged over {n_runs} runs) ==={RESET}"
    )
    if not final_top:
        print(f"  {RED}No servers passed both phases.{RESET}")
    else:
        for i, s in enumerate(final_top, 1):
            badge = f"{GREEN}{BOLD}#{i}{RESET}"
            stddev_part = (
                f" ±{s.mean_stddev:.0f}ms" if s.mean_stddev > 0 else ""
            )
            fetch_r = s.representative
            fetch_str = (
                f"{fetch_r.block_range_ms:.1f}ms"
                if fetch_r and fetch_r.block_range_ms is not None
                else "n/a"
            )
            print(
                f"  {badge}  {BOLD}{s.host}{RESET}  "
                f"mean {s.mean_avg:.1f}ms{stddev_part}  "
                f"({s.both_success_count}/{n_runs} ok)  "
                f"100-block fetch {fetch_str}"
            )
    print()

    all_sorted = sorted(sustained, key=sort_key)

    widths = {
        "n": 2, "host": 22, "succ": 5, "info": 11, "latest": 11,
        "mean": 16, "range": 16, "ovr": 4, "branch": 8, "fetch": 12,
    }
    header_cells = [
        cell("#", widths["n"], BOLD, ">"),
        cell("host", widths["host"], BOLD),
        cell("succ", widths["succ"], BOLD, ">"),
        cell("info avg", widths["info"], BOLD, ">"),
        cell("latest avg", widths["latest"], BOLD, ">"),
        cell("mean ±std", widths["mean"], BOLD, ">"),
        cell("min-max", widths["range"], BOLD, ">"),
        cell(">5s", widths["ovr"], BOLD, ">"),
        cell("branch", widths["branch"], BOLD),
        cell("fetch 100blk", widths["fetch"], BOLD, ">"),
        cell("verdict", 0, BOLD),
    ]
    print(f"{BOLD}=== All servers (averaged over {n_runs} runs) ==={RESET}")
    print("  " + "  ".join(header_cells))
    print("  " + DIM + "-" * 140 + RESET)

    budget_ms = INFO_TIMEOUT_S * 1000

    for i, s in enumerate(all_sorted, 1):
        succ_color = (
            GREEN if s.both_success_count == n_runs
            else (YELLOW if s.both_success_count > 0 else RED)
        )
        succ_c = cell(
            f"{s.both_success_count}/{n_runs}",
            widths["succ"], succ_color, ">",
        )

        info_over = s.info_avg is not None and s.info_avg > budget_ms
        latest_over = s.latest_avg is not None and s.latest_avg > budget_ms
        info_c = latency_cell(s.info_avg, widths["info"], info_over)
        latest_c = latency_cell(s.latest_avg, widths["latest"], latest_over)

        if s.mean_avg is not None:
            stddev_part = (
                f" ±{s.mean_stddev:.0f}" if s.mean_stddev > 0 else ""
            )
            mean_text = f"{s.mean_avg:.1f}ms{stddev_part}"
            mean_c = cell(
                mean_text, widths["mean"], latency_color(s.mean_avg), ">"
            )
        else:
            mean_c = cell("n/a", widths["mean"], DIM, ">")

        if s.overall_min is not None and s.overall_max is not None:
            range_text = f"{s.overall_min:.0f}-{s.overall_max:.0f}ms"
            range_c = cell(range_text, widths["range"], "", ">")
        else:
            range_c = cell("n/a", widths["range"], DIM, ">")

        ovr_count = s.over_budget_count
        if ovr_count == 0:
            ovr_c = cell("0", widths["ovr"], GREEN, ">")
        else:
            ovr_c = cell(str(ovr_count), widths["ovr"], MAGENTA, ">")

        if s.consensus_branch_id:
            branch_text = s.consensus_branch_id[: widths["branch"]]
            if majority_branch and s.consensus_branch_id == majority_branch:
                branch_c = cell(branch_text, widths["branch"], GREEN)
            else:
                branch_c = cell(branch_text, widths["branch"], RED)
        else:
            branch_c = cell("-", widths["branch"], DIM)

        rep = s.representative
        if rep is not None and rep.block_range_ok:
            fetch_c = latency_cell(rep.block_range_ms, widths["fetch"])
        elif rep is not None and rep.block_range_ok is False:
            fetch_c = cell("FAIL", widths["fetch"], RED, ">")
        else:
            fetch_c = cell("skip", widths["fetch"], DIM, ">")

        # Verdict
        gates = s.consistent_gates
        if (s.host, s.port) in final_set:
            rank = next(
                idx for idx, x in enumerate(final_top, 1)
                if (x.host, x.port) == (s.host, s.port)
            )
            verdict = f"{GREEN}{BOLD}TOP #{rank}{RESET}"
        elif s.both_success_count == 0:
            errs = []
            if s.info_error_count:
                errs.append(f"info err ×{s.info_error_count}")
            if s.latest_error_count:
                errs.append(f"latest err ×{s.latest_error_count}")
            verdict = f"{RED}all failed ({', '.join(errs) or 'n/a'}){RESET}"
        elif gates:
            verdict = f"{YELLOW}disqualified: {', '.join(gates)}{RESET}"
        elif ovr_count > 0 or s.info_error_count or s.latest_error_count:
            parts = []
            if ovr_count > 0:
                parts.append(f">5s ×{ovr_count}")
            if s.info_error_count:
                parts.append(f"info err ×{s.info_error_count}")
            if s.latest_error_count:
                parts.append(f"latest err ×{s.latest_error_count}")
            verdict = f"{YELLOW}unreliable: {', '.join(parts)}{RESET}"
        elif rep is not None and rep.block_range_ok is False:
            verdict = f"{RED}fetch failed{RESET}"
        else:
            verdict = f"{DIM}slower than top {K}{RESET}"

        row = [
            cell(str(i), widths["n"], align=">"),
            cell(s.host, widths["host"]),
            succ_c, info_c, latest_c, mean_c, range_c, ovr_c,
            branch_c, fetch_c, verdict,
        ]
        print("  " + "  ".join(row))


# ---------- Main flow ----------

def prompt_mode() -> str:
    if not sys.stdin.isatty():
        return "single"
    print(f"{BOLD}Test mode{RESET}")
    print(f"  {BOLD}s{RESET}  single run (one Phase 1 sweep + Phase 2)")
    print(
        f"  {BOLD}t{RESET}  sustained "
        f"({DEFAULT_RUNS} Phase 1 runs averaged + one Phase 2)"
    )
    while True:
        try:
            choice = input("Choose [s/t] (default s): ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            return "single"
        if choice in ("", "s", "single"):
            return "single"
        if choice in ("t", "sustained"):
            return "sustained"
        print(f"{YELLOW}Please enter 's' or 't'.{RESET}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Reproduce the Zcash Android wallet's 'Choose a Server' test, "
            "with extended timeouts and optional sustained averaging."
        )
    )
    parser.add_argument(
        "--testnet", action="store_true",
        help="Test the testnet endpoint instead of mainnet.",
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--single", action="store_true",
        help="Run a single Phase 1 + Phase 2 sweep (default if non-interactive).",
    )
    mode_group.add_argument(
        "--sustained", action="store_true",
        help=f"Average {DEFAULT_RUNS} Phase 1 runs, then Phase 2 once.",
    )
    parser.add_argument(
        "--runs", type=int, default=None,
        help=(
            "Number of Phase 1 runs for sustained mode "
            f"(default {DEFAULT_RUNS}). Implies --sustained when >1."
        ),
    )
    args = parser.parse_args()

    network = "testnet" if args.testnet else "mainnet"
    endpoints = TESTNET_ENDPOINTS if args.testnet else MAINNET_ENDPOINTS

    if args.runs is not None and args.runs > 1:
        mode = "sustained"
    elif args.sustained:
        mode = "sustained"
    elif args.single:
        mode = "single"
    else:
        mode = prompt_mode()

    n_runs = args.runs if args.runs is not None else DEFAULT_RUNS
    if n_runs < 1:
        n_runs = 1
    if mode == "single":
        n_runs = 1

    print(
        f"\n{BOLD}Zcash lightwalletd server test "
        f"({network}, {mode}{f' x{n_runs}' if mode == 'sustained' else ''})"
        f"{RESET}"
    )
    print(
        f"{DIM}SDK budget {INFO_TIMEOUT_S}s, hard deadline "
        f"{INFO_HARD_TIMEOUT_S}s. Calls slower than {INFO_TIMEOUT_S}s "
        f"are flagged with '!' and would be dropped by the wallet, "
        f"but their actual latency is still measured.{RESET}\n"
    )

    if mode == "single":
        print(
            f"{BOLD}[Phase 1]{RESET} Measuring GetLightdInfo + GetLatestBlock "
            f"in parallel ({len(endpoints)} servers, "
            f"{INFO_HARD_TIMEOUT_S}s hard timeout)..."
        )
        results = run_phase1(endpoints, network)
        majority_branch = apply_branch_consensus(results)
        survivors = pick_survivors(results)
        reachable = sum(1 for r in results if r.block_height is not None)
        print(
            f"{BOLD}[Phase 2]{RESET} Fetching last {N} blocks from "
            f"{reachable} reachable server(s) "
            f"({FETCH_THRESHOLD_S}s timeout each; "
            f"App ranking = first {K} of {len(survivors)} SDK survivor(s) "
            f"to succeed)..."
        )
        final_top = run_phase2(results, survivors)
        print_single(results, network, survivors, final_top, majority_branch)
    else:
        per_run = []
        total_start = time.perf_counter()
        for i in range(1, n_runs + 1):
            run_start = time.perf_counter()
            print(
                f"{BOLD}[Phase 1, run {i}/{n_runs}]{RESET} starting...",
                end="", flush=True,
            )
            results = run_phase1(endpoints, network)
            apply_branch_consensus(results)
            ok = sum(1 for r in results if r.mean_ms is not None)
            elapsed = time.perf_counter() - run_start
            print(
                f" done in {elapsed:.1f}s "
                f"({ok}/{len(endpoints)} servers responded within budget)"
            )
            per_run.append(results)
        total_elapsed = time.perf_counter() - total_start
        print(
            f"{DIM}Phase 1 total: {total_elapsed:.1f}s across "
            f"{n_runs} runs.{RESET}"
        )

        sustained = aggregate(per_run, endpoints)

        # Cross-run majority branch is computed across all responses.
        all_branches = [
            r.consensus_branch_id
            for run in per_run for r in run if r.consensus_branch_id
        ]
        majority_branch = (
            Counter(all_branches).most_common(1)[0][0]
            if all_branches else None
        )

        # Pick survivors using the averaged metrics. A server passes if it
        # had at least one fully-successful run, no consistent gate
        # failures, no over-budget overshoots, and an averaged mean_ms.
        candidates = []
        for s in sustained:
            if s.mean_avg is None:
                continue
            if s.consistent_gates:
                continue
            if s.over_budget_count > 0:
                continue
            if s.info_error_count or s.latest_error_count:
                continue
            if (
                s.consensus_branch_id
                and majority_branch
                and s.consensus_branch_id != majority_branch
            ):
                continue
            candidates.append(s)
        candidates.sort(key=lambda s: s.mean_avg)
        survivors_s = [
            s for i, s in enumerate(candidates)
            if i < K or s.mean_avg <= LATENCY_THRESHOLD_MS
        ]

        reachable = sum(1 for s in sustained if s.representative is not None)
        print(
            f"{BOLD}[Phase 2]{RESET} Fetching last {N} blocks from "
            f"{reachable} reachable server(s) "
            f"({FETCH_THRESHOLD_S}s timeout each, single run; "
            f"App ranking = first {K} of {len(survivors_s)} SDK survivor(s) "
            f"to succeed)..."
        )
        final_top_s = []
        measured = set()
        for s in survivors_s:
            rep = s.representative
            if rep is None:
                continue
            measure_block_range(rep)
            measured.add((s.host, s.port))
            if rep.block_range_ok and len(final_top_s) < K:
                final_top_s.append(s)
        for s in sustained:
            if (s.host, s.port) in measured:
                continue
            rep = s.representative
            if rep is None:
                continue
            measure_block_range(rep)

        print_sustained(sustained, n_runs, final_top_s, majority_branch)

    print()
    print(
        f"{DIM}Thresholds: SDK mean RPC latency cap {LATENCY_THRESHOLD_MS}ms, "
        f"SDK timeout budget {INFO_TIMEOUT_S}s, hard deadline "
        f"{INFO_HARD_TIMEOUT_S}s, sync gap < {SYNCED_THRESHOLD_BLOCKS} "
        f"blocks, last-{N}-block fetch < {FETCH_THRESHOLD_S}s. "
        f"Branch-id reference = majority across responding servers.{RESET}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
