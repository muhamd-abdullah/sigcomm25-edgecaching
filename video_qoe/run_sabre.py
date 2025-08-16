#!/usr/bin/env python3
"""Parallel Sabre runner.

Modes:
  1) Batch mode: read an input Parquet with `miss_indices` and `latency_list`,
     run Sabre in parallel across rows, and write an output Parquet.
  2) Direct mode: provide `--miss-indices` and `--latency-list` on CLI to run
     a single invocation and print QoE (when --verbose is enabled).
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
import pandas as pd
import subprocess

DEFAULT_NETWORK_JSON = "./config/my_network.json"
DEFAULT_VIDEO_MANIFEST_JSON = "./config/bbb4k.json"

@dataclass(frozen=True)
class RunConfig:
    network_json: str
    manifest_json: str
    input_parquet: Optional[Path]
    output_parquet: Optional[Path]
    bandwidth: int
    miss_latency: Any
    buffer_size: int
    max_workers: int
    abr: str = "dynamic"
    verbose: bool = False
    # Direct mode
    miss_indices: Optional[List[int]] = None
    latency_list: Optional[List[int]] = None


def _json_list(s: str) -> List[int]:
    try:
        v = json.loads(s)
        if isinstance(v, list):
            return [int(x) for x in v]
    except Exception:
        pass
    raise ValueError(f"Invalid list for argument: {s!r}. Use JSON like [2,1].")


def _to_list_safe(obj) -> List:
    try:
        if isinstance(obj, list):
            return obj
        if getattr(obj, "tolist", None):
            return list(obj.tolist())
        return list(obj)
    except Exception:
        return [obj]


def _build_cmd(cfg: RunConfig, normalized_miss_json: str) -> List[str]:
    return [
        sys.executable,
        str(Path("./src/sabre.py")),
        "-v",
        "-n", cfg.network_json,
        "-m", cfg.manifest_json,
        "-b", str(cfg.buffer_size),
        "-nm", str(cfg.bandwidth),
        "-a", cfg.abr,
        "-nmd", normalized_miss_json,
    ]


def _parse_qoe(stdout: str) -> Optional[Dict[str, Any]]:
    for line in (stdout or "").splitlines():
        line = line.strip()
        if not line or not line.startswith("{") or '"buffer_size"' not in line:
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


def _run_once(row: Dict[str, Any], cfg: RunConfig) -> Optional[Dict[str, Any]]:
    miss_indices = _to_list_safe(row.get("miss_indices", []))
    latency_list = _to_list_safe(row.get("latency_list", []))
    payload = {
        "miss_indices": miss_indices,
        "miss_latency": cfg.miss_latency,
        "latency_list": latency_list,
    }
    payload_json = json.dumps(payload)

    cmd = _build_cmd(cfg, payload_json)
    if cfg.verbose:
        logging.debug("CMD: %s", cmd)

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        if cfg.verbose:
            logging.debug("\n%s", proc.stdout)
    except subprocess.CalledProcessError as e:
        logging.error("Sabre failed (returncode=%s): %s", e.returncode, e.stderr or e.stdout)
        return None

    qoe = _parse_qoe(proc.stdout)
    if not qoe:
        logging.warning("No QoE JSON found in sabre output.")
        if cfg.verbose:
            logging.debug("stdout was:\n%s", proc.stdout)
        return None

    # Keep only essential QoE metrics (remove verbose internals)
    drop = [
        "time_average_played_utility", "total_played_utility", "buffer_size", "num_chunks",
        "over_estimate_count", "over_estimate", "leq_estimate_count", "leq_estimate", "estimate",
        "total_reaction_time", "total_log_bitrate_change", "time_average_log_bitrate_change",
    ]
    for k in drop:
        qoe.pop(k, None)

    sabre_config = {
        "sabre_miss_latency": cfg.miss_latency,
        "sabre_bandwidth": cfg.bandwidth,
        "sabre_buffer": cfg.buffer_size,
        "sabre_abr": cfg.abr,
    }

    qoe.update(sabre_config)

    merged = dict(row)
    merged.update(qoe)

    if cfg.verbose:
        logging.info("sabre_config: %s", json.dumps(sabre_config, indent=2))
    return merged


def run_batch(cfg: RunConfig) -> pd.DataFrame:
    assert cfg.input_parquet is not None
    logging.info("Reading input parquet: %s", cfg.input_parquet)
    df = pd.read_parquet(cfg.input_parquet)
    total = len(df)
    logging.info("total videos: %d", total)

    rows = df.to_dict(orient="records")
    results: List[Dict[str, Any]] = []
    done = 0

    def pct(n): return (n / total) * 100 if total else 100.0

    with ThreadPoolExecutor(max_workers=max(1, cfg.max_workers)) as pool:
        futures = {pool.submit(_run_once, row, cfg): row for row in rows}
        for fut in as_completed(futures):
            row = futures[fut]
            try:
                res = fut.result()
                if res is not None:
                    results.append(res)
            except Exception as e:
                logging.exception("Unhandled error for row: %s", e)
            finally:
                done += 1
                logging.info("Progress: %.2f%% (%d/%d) [bw:%s mbps, miss:%s ms]",
                             pct(done), done, total, cfg.bandwidth, cfg.miss_latency)

    return pd.DataFrame(results)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run Sabre with normalized MISS latency (batch or direct mode).")
    p.add_argument("--network-json", default=DEFAULT_NETWORK_JSON, help="Network JSON (default: ./config/my_network.json)")
    p.add_argument("--manifest-json", default=DEFAULT_VIDEO_MANIFEST_JSON, help="manifest JSON (default: ./config/bbb4k.json)")

    # Batch mode
    p.add_argument("--input-parquet", help="Input Parquet with miss_indices and latency_list")
    p.add_argument("--output-parquet", help="Output Parquet path")

    # Parameters
    p.add_argument("--bandwidth", type=float, default=25, help="Bandwidth in Mbps (e.g., 2,8,25,100)")
    p.add_argument("--miss-latency", type=float, default=62.0, help="MISS latency in ms (e.g., 62,370,910)")
    p.add_argument("--buffer-size", type=int, default=30, help="Player buffer size in seconds")
    p.add_argument( "--abr", type=str, default="dynamic", help="ABR algorithm to use (default: dynamic)")
    p.add_argument("--max-workers", type=int, default=25, help="Parallel threads for batch mode")

    # Direct mode
    p.add_argument("--miss-indices", type=str, help="JSON list of miss indices, e.g. '[2,1]'")
    p.add_argument("--latency-list", type=str, help="JSON list of latencies (ms), e.g. '[25,625,700,21,19]'")

    p.add_argument("--verbose", action="store_true", help="Enable verbose logging (prints QoE in direct mode)")
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Force verbose logging in direct mode
    if args.miss_indices and args.latency_list:
        args.verbose = True

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    # Determine mode
    direct_mode = bool(args.miss_indices) or bool(args.latency_list)
    if direct_mode and (not args.miss_indices or not args.latency_list):
        parser.error("Both --miss-indices and --latency-list are required for direct mode.")

    cfg = RunConfig(
        network_json=args.network_json,
        manifest_json=args.manifest_json,
        input_parquet=Path(args.input_parquet).expanduser().resolve() if args.input_parquet else None,
        output_parquet=Path(args.output_parquet).expanduser().resolve() if args.output_parquet else None,
        bandwidth=float(args.bandwidth),
        miss_latency=float(args.miss_latency),
        buffer_size=max(1, int(args.buffer_size)),
        abr = args.abr,
        max_workers=max(1, int(args.max_workers)),
        verbose=bool(args.verbose),
        miss_indices=_json_list(args.miss_indices) if args.miss_indices else None,
        latency_list=_json_list(args.latency_list) if args.latency_list else None,
    )

    # Direct mode: single invocation
    if direct_mode:
        row = {"miss_indices": cfg.miss_indices or [], "latency_list": cfg.latency_list or []}
        res = _run_once(row, cfg)
        if res is None:
            logging.error("Direct run produced no result.")
        return

    # Batch mode requires parquet paths
    if not cfg.input_parquet or not cfg.output_parquet:
        parser.error("--input-parquet and --output-parquet are required for batch mode.")

    try:
        df = run_batch(cfg)
        if df.empty:
            logging.warning("No results produced; output parquet will be empty.")
        out = cfg.output_parquet
        out.parent.mkdir(parents=True, exist_ok=True)
        logging.info("Writing results to %s (video=%d)", out, len(df))
        df.to_parquet(out, index=False)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
    except Exception:
        logging.exception("Fatal error")


if __name__ == "__main__":
    raise SystemExit(main())