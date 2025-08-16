#!/usr/bin/env python3
"""
Processing script for raw CDN measurements.

- Scans result folders under CSV_FOLDER_PATH.
- Reads all CSVs, tagging each row with location and inferred CDN.
- Processes and enriches raw data (cache trace, edges location, origin, timing).
- Aggregates per-video data and writes to a Parquet file.
- Prints hit-rate summaries for content providers under each CDN.

"""

from __future__ import annotations
import argparse
from pathlib import Path
import asyncio
import ast
import json
import os
import time
from typing import Dict, List, Tuple
import pandas as pd

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

CSV_FOLDER_PATH = "./basic_measurement/results"
OUTPUT_DIR = "./data"
OUTPUT_PARQUET_PATH = "./basic_measurement/results/all_results.parquet"

# Streaming services under each CDN (NOTE: manual mapping; update as needed)
AKAMAI_CONTENT = ["vimeo", "dw", "nobudge", "waterbear", "channel5", "zdf"]
AMAZON_CONTENT = ["prime", "magellantv", "pbs", "dailymotion"]
EDGIO_CONTENT = ["rakuten", "fawesome"]
FASTLY_CONTENT = ["plex"]

# ------------------------------------------------------------------------------
# Utilities: filesystem
# ------------------------------------------------------------------------------

def list_folders(directory: str) -> List[str]:
    """List non-test subfolders under `directory`, returning joined paths."""
    try:
        entries = os.listdir(directory)
    except FileNotFoundError:
        return []
    folders = [
        os.path.join(directory, name)
        for name in entries
        if os.path.isdir(os.path.join(directory, name))
    ]
    return [f for f in folders if "test" not in os.path.basename(f).lower()]


def ensure_output_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Folder '{path}' created successfully.")


def ensure_parent_dir(filepath: str) -> None:
    parent = os.path.dirname(os.path.abspath(filepath))
    if parent and not os.path.exists(parent):
        os.makedirs(parent)
        print(f"Folder '{parent}' created successfully.")


# ------------------------------------------------------------------------------
# Utilities: header parsing & cache classification
# ------------------------------------------------------------------------------

def _lower_keys(d: Dict[str, str]) -> Dict[str, str]:
    return {str(k).lower(): v for k, v in d.items()}


def assign_origin(header_dict: Dict[str, str]) -> str:
    """Common origin extraction (generic)."""
    return header_dict.get("server", "NaN")


# -------------------------- Amazon (CloudFront) --------------------------------

def assign_hit_miss_amazon(header_dict: Dict[str, str]) -> str:
    if "x-cache" in header_dict:
        xcache_val = str(header_dict["x-cache"]).lower()
        if "from cloudfront" in xcache_val:
            if "miss" in xcache_val or "refresh" in xcache_val:
                return "miss"
            if "hit" in xcache_val:
                return "hit"
            return xcache_val.split()[0]
    return "NaN"


def assign_edge_amazon(header_dict: Dict[str, str]) -> Tuple[str, str]:
    if "x-amz-cf-pop" in header_dict:
        return header_dict["x-amz-cf-pop"], "NaN"
    return "NaN", "NaN"


def extract_headers_amazon(header_str: str) -> pd.Series:
    try:
        header_dict = _lower_keys(ast.literal_eval(header_str))
    except Exception:
        return pd.Series({"cache_trace": None, "edge_l1": None, "edge_l2": None, "origin": None})

    edge_l1, edge_l2 = assign_edge_amazon(header_dict)
    hit_miss = assign_hit_miss_amazon(header_dict)
    origin = assign_origin(header_dict)
    return pd.Series({"cache_trace": hit_miss, "edge_l1": edge_l1, "edge_l2": edge_l2, "origin": origin})


# ------------------------------ Akamai -----------------------------------------

def assign_hit_miss_akamai(header_dict: Dict[str, str]) -> str:
    x_cache = None
    x_cache_remote = None

    if "x-cache" in header_dict and "akamai" in str(header_dict["x-cache"]).lower():
        x_cache = str(header_dict["x-cache"]).split()[0].lower()
        if "x-cache-remote" in header_dict and "akamai" in str(header_dict["x-cache-remote"]).lower():
            x_cache_remote = str(header_dict["x-cache-remote"]).split()[0].lower()

    if not x_cache and "akamai-cache-status" in header_dict:
        cache_status = str(header_dict["akamai-cache-status"]).split(", ")
        x_cache = cache_status[0].lower()
        if len(cache_status) > 1:
            x_cache_remote = cache_status[1].lower()

    if x_cache and "hit" in x_cache:
        return "l1"
    if x_cache and "miss" in x_cache:
        if x_cache_remote and "hit" in x_cache_remote:
            return "l2"
        return "miss"
    return "NaN"


def assign_edge_akamai(header_dict: Dict[str, str]) -> Tuple[str, str]:
    if "akamai-request-bc" in header_dict:
        breadcrumbs = str(header_dict["akamai-request-bc"])
        blocks = breadcrumbs.strip("[]").split("],[")
        edges: List[str] = []
        for block in blocks:
            for pair in block.split(","):
                key, _, value = pair.partition("=")
                if key.strip() == "n":
                    edges.append(value)
        if len(edges) > 1:
            return edges[0], edges[1]
        if edges:
            return edges[0], "NaN"
    return "NaN", "NaN"


def extract_headers_akamai(header_str: str) -> pd.Series:
    try:
        header_dict = _lower_keys(ast.literal_eval(header_str))
    except Exception:
        return pd.Series({"cache_trace": None, "edge_l1": None, "edge_l2": None, "origin": None})

    edge_l1, edge_l2 = assign_edge_akamai(header_dict)
    hit_miss = assign_hit_miss_akamai(header_dict)
    origin = assign_origin(header_dict)
    return pd.Series({"cache_trace": hit_miss, "edge_l1": edge_l1, "edge_l2": edge_l2, "origin": origin})


# -------------------------------- Edgio -----------------------------------------

def assign_hit_miss_edgio(header_dict: Dict[str, str]) -> str:
    x_cache = header_dict.get("x-ec-cache")
    x_cache_remote = header_dict.get("x-ec-cache-remote")

    if x_cache:
        x_cache_l = str(x_cache).lower()
        x_cache_remote_l = str(x_cache_remote).lower() if x_cache_remote else None

        if "expire" in x_cache_l:
            return "miss"
        if "hit" in x_cache_l:
            return "l1"
        if "miss" in x_cache_l:
            if x_cache_remote_l and "hit" in x_cache_remote_l:
                return "l2"
            return "miss"
    return "NaN"


def assign_edge_edgio(header_dict: Dict[str, str]) -> Tuple[str, str]:
    edge_l1, edge_l2 = "NaN", "NaN"
    if "x-ec-cache" in header_dict:
        last = str(header_dict["x-ec-cache"]).split()[-1].lower().replace(")", "").replace("(", "")
        edge_l1 = last.split("/")[0]
        if "x-ec-cache-remote" in header_dict:
            last_r = str(header_dict["x-ec-cache-remote"]).split()[-1].lower().replace(")", "").replace("(", "")
            edge_l2 = last_r.split("/")[0]
    return edge_l1, edge_l2


def assign_origin_edgio(header_dict: Dict[str, str]) -> str:
    if "server" in header_dict:
        return str(header_dict["server"]).replace(")", "").replace("(", "").split("/")[0]
    return "NaN"


def extract_headers_edgio(header_str: str) -> pd.Series:
    try:
        header_dict = _lower_keys(ast.literal_eval(header_str))
    except Exception:
        return pd.Series({"cache_trace": None, "edge_l1": None, "edge_l2": None, "origin": None})

    edge_l1, edge_l2 = assign_edge_edgio(header_dict)
    hit_miss = assign_hit_miss_edgio(header_dict)
    origin = assign_origin_edgio(header_dict)
    return pd.Series({"cache_trace": hit_miss, "edge_l1": edge_l1, "edge_l2": edge_l2, "origin": origin})


# -------------------------------- Fastly ----------------------------------------

def assign_hit_miss_fastly(header_dict: Dict[str, str]) -> str:
    if "x-cache" in header_dict:
        x = str(header_dict["x-cache"]).lower()
        if x == "hit, miss":
            return "l2"
        if "hit" in x:
            return "l1"
        return "miss"
    return "NaN"


def assign_edge_fastly(header_dict: Dict[str, str]) -> Tuple[str, str]:
    if "x-served-by" in header_dict:
        path = str(header_dict["x-served-by"])
        blocks = path.split(", ")[::-1]  # reverse: L1 first
        edges = [b.split("-")[-1] for b in blocks]
        if len(edges) > 1:
            return edges[0], edges[1]
        if edges:
            return edges[0], "NaN"
    return "NaN", "NaN"


def extract_headers_fastly(header_str: str) -> pd.Series:
    try:
        header_dict = _lower_keys(ast.literal_eval(header_str))
    except Exception:
        return pd.Series({"cache_trace": None, "edge_l1": None, "edge_l2": None, "origin": None})

    edge_l1, edge_l2 = assign_edge_fastly(header_dict)
    hit_miss = assign_hit_miss_fastly(header_dict)
    origin = assign_origin(header_dict)
    return pd.Series({"cache_trace": hit_miss, "edge_l1": edge_l1, "edge_l2": edge_l2, "origin": origin})


# ------------------------------------------------------------------------------
# Utilities: timing parsing
# ------------------------------------------------------------------------------

def extract_timing(timing_dict_str: str) -> pd.Series:
    """
    Parse a timing-info string to dns/tcp/ssl (ms).

    NOTE (logic preserved): 'ssl(ms)' is intentionally set from 'dns'.
    """
    timing_dict = json.loads(str(timing_dict_str).replace("'", '"'))
    dns = round(float(timing_dict["dns"]), 1)
    tcp = round(float(timing_dict["tcp"]), 1)
    ssl = round(float(timing_dict["dns"]), 1)  # intentional
    return pd.Series({"dns(ms)": dns, "tcp(ms)": tcp, "ssl(ms)": ssl})


# ------------------------------------------------------------------------------
# DataFrame processing pipeline
# ------------------------------------------------------------------------------

def process_raw_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process raw per-chunk data and enrich with cache/edge/origin/timing info.
    """
    df = df[df["resp_code"].isin([200, 206])]
    df = df.drop(columns=["url", "responseIP", "resp_code"])

    df["content"] = df["content"].apply(lambda x: str(x).split("_")[0])
    df = df.rename(columns={"timestamp(dd-mm-yyyy hh:mm:ss:ms)": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%d-%m-%Y_%H:%M:%S:%f").dt.strftime("%d-%m-%Y_%H:%M")
    df["latency(ms)"] = pd.to_numeric(df["latency(ms)"], errors="coerce").round(1)

    # Akamai
    df_akamai = df[(df["content"].isin(AKAMAI_CONTENT)) | (df["cdn"] == "akamai")].copy()
    df_akamai.loc[:, ["cache_trace", "edge_l1", "edge_l2", "origin"]] = df_akamai["responseHeaders"].apply(
        extract_headers_akamai
    )
    df_akamai = df_akamai[df_akamai["cache_trace"].isin(["l1", "l2", "miss"])]
    df_akamai.loc[:, "cdn"] = "akamai"

    # Amazon/CloudFront
    df_amazon = df[(df["content"].isin(AMAZON_CONTENT)) | (df["cdn"] == "cloudfront")].copy()
    df_amazon.loc[:, ["cache_trace", "edge_l1", "edge_l2", "origin"]] = df_amazon["responseHeaders"].apply(
        extract_headers_amazon
    )
    df_amazon = df_amazon[df_amazon["cache_trace"].isin(["hit", "miss", "refreshhit"])]
    df_amazon.loc[:, "cdn"] = "cloudfront"

    # Edgio
    df_edgio = df[(df["content"].isin(EDGIO_CONTENT)) | (df["cdn"] == "edgio")].copy()
    df_edgio.loc[:, ["cache_trace", "edge_l1", "edge_l2", "origin"]] = df_edgio["responseHeaders"].apply(
        extract_headers_edgio
    )
    df_edgio.loc[:, "cdn"] = "edgio"

    # Fastly
    df_fastly = df[(df["content"].isin(FASTLY_CONTENT)) | (df["cdn"] == "fastly")].copy()
    df_fastly.loc[:, ["cache_trace", "edge_l1", "edge_l2", "origin"]] = df_fastly["responseHeaders"].apply(
        extract_headers_fastly
    )
    df_fastly = df_fastly[df_fastly["cache_trace"].isin(["l1", "l2", "miss"])]
    df_fastly.loc[:, "cdn"] = "fastly"

    frames = [df_akamai, df_amazon, df_edgio, df_fastly]
    if all(getattr(f, "empty", True) for f in frames):
        print("ERROR: No valid data (HTTP 200) found after parsing.")
        return

    # Combine and shape
    df_out = pd.concat([df_akamai, df_amazon, df_edgio, df_fastly], ignore_index=True)
    df_out = df_out[
        [
            "timestamp",
            "location",
            "content",
            "name",
            "quality",
            "latency(ms)",
            "cache_trace",
            "edge_l1",
            "edge_l2",
            "origin",
            "cdn",
            "timing_info",
        ]
    ].reset_index(drop=True)

    df_out["name"] = df_out["name"].astype(str)
    df_out["quality"] = df_out["quality"].astype(str)

    df_out.loc[:, ["dns(ms)", "tcp(ms)", "ssl(ms)"]] = df_out["timing_info"].apply(extract_timing)
    df_out = df_out[
        [
            "timestamp",
            "location",
            "content",
            "name",
            "quality",
            "latency(ms)",
            "cache_trace",
            "edge_l1",
            "edge_l2",
            "origin",
            "dns(ms)",
            "tcp(ms)",
            "ssl(ms)",
            "cdn",
        ]
    ]

    return df_out


# ------------------------------------------------------------------------------
# Async CSV ingestion
# ------------------------------------------------------------------------------

async def read_csv_and_tag_async(file_path: str, semaphore: asyncio.Semaphore) -> pd.DataFrame:
    """Read a CSV and add 'location' and 'cdn' columns inferred from path and filename."""
    loop = asyncio.get_running_loop()
    async with semaphore:
        with open(file_path, "rb") as f:
            df = await loop.run_in_executor(None, pd.read_csv, f)

    norm_path = file_path.replace("//", "/")
    parts = norm_path.split("/")

    city_name = parts[-3].split("_")[-1] if len(parts) >= 3 else "NaN"

    cdn = "NaN"
    fname = os.path.basename(norm_path).lower()
    if "_akamai_" in fname:
        cdn = "akamai"
    elif "_cloudfront_" in fname:
        cdn = "cloudfront"
    elif "_fastly_" in fname:
        cdn = "fastly"
    elif "_edgio_" in fname:
        cdn = "edgio"

    df["location"] = city_name
    df["cdn"] = cdn
    return df


async def process_folder_async(root: str, files: List[str], semaphore: asyncio.Semaphore, sink: List[pd.DataFrame]) -> None:
    if "test/" in root:
        return
    file_paths = [os.path.join(root, f) for f in files if f.endswith(".csv")]
    if not file_paths:
        return
    dfs = await asyncio.gather(*(read_csv_and_tag_async(p, semaphore) for p in file_paths))
    if dfs:
        sink.append(pd.concat(dfs, ignore_index=True))


async def process_tree_async(base_folder: str, sink: List[pd.DataFrame]) -> None:
    semaphore = asyncio.Semaphore(800)  # preserved cap
    tasks = []
    for root, _, files in os.walk(base_folder):
        tasks.append(process_folder_async(root, files, semaphore, sink))
    if tasks:
        await asyncio.gather(*tasks)


# ------------------------------------------------------------------------------
# Summaries & aggregation
# ------------------------------------------------------------------------------

def summarize_hitrate(per_segment_df: pd.DataFrame) -> None:
    """Print per-CDN/content hit-rate breakdown in a clean table."""
    for cdn in per_segment_df["cdn"].unique():
        print(f"****** {cdn.upper()} ******")
        rows = []
        df_cdn = per_segment_df[per_segment_df["cdn"] == cdn]

        for content in df_cdn["content"].unique():
            stats = df_cdn[df_cdn["content"] == content]["cache_trace"].value_counts()
            hits_l1 = stats.get("l1", 0)
            hits_l2 = stats.get("l2", 0)
            hits_any = stats.get("hit", 0)  # CloudFront “hit”
            hits = hits_l1 + hits_l2 + hits_any
            denom = sum(int(v) for v in stats.values) or 1

            rows.append({
                "Content": content,
                "Requests": denom,
                "Hits": hits,
                "L1 Hits": hits_l1,
                "L2 Hits": hits_l2,
                "Hitrate (%)": f"{hits / denom * 100:.1f}",
                "L1 Hitrate (%)": f"{hits_l1 / denom * 100:.1f}",
                "L2 Hitrate (%)": f"{hits_l2 / denom * 100:.1f}",
            })

        table = pd.DataFrame(rows)
        print(table.to_string(index=False))


def aggregate_per_video(per_segment_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate first-50-chunk stats per (date, location, cdn, content, name, quality)."""
    per_video_df = per_segment_df.copy()
    per_video_df["latency_list"] = per_video_df["latency(ms)"]
    per_video_df["date"] = per_video_df["timestamp"].apply(lambda x: str(x).split("_")[0])
    per_video_df["quality"] = per_video_df["quality"].apply(lambda x: str(x).split("_")[0])

    agg_dict = {
        "latency(ms)": lambda x: x.iloc[:50].mean(),
        "latency_list": lambda x: list(x)[:50],
        "dns(ms)": "max",
        "tcp(ms)": "mean",
        "ssl(ms)": "mean",
        "origin": "first",
        "cache_trace": lambda x: list(x)[:50],
    }

    per_video_df = (
        per_video_df.groupby(["date", "location", "cdn", "content", "name", "quality"])
        .agg(agg_dict)
        .reset_index()
    )

    per_video_df["miss_indices"] = per_video_df["cache_trace"].apply(
        lambda xs: [i for i, v in enumerate(xs) if v == "miss"]
    )
    per_video_df["num_chunks"] = per_video_df["cache_trace"].apply(len)

    return per_video_df


# ------------------------------------------------------------------------------
# CLI / Orchestration
# ------------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Process CDN measurement results.")
    p.add_argument("csv_folder", type=Path, help="Root folder with CSV results")
    p.add_argument("output_parquet", type=Path, help="Path to write the Parquet")
    return p.parse_args(argv)


def run(csv_folder_path: str, output_parquet_path: str) -> None:
    # Discover folders
    folders = list_folders(csv_folder_path)
    folders.sort(reverse=True)

    # Ensure output dirs
    ensure_output_dir(OUTPUT_DIR)
    ensure_parent_dir(output_parquet_path)

    start_time = time.time()
    per_segment_df: pd.DataFrame | None = None

    # Process each folder's CSVs
    print("List of folders in CSV results directory:")
    for i, folder in enumerate(folders, start=1):
        combined_dfs: List[pd.DataFrame] = []
        print(f"{i}/{len(folders)} {folder}")
        asyncio.run(process_tree_async(folder, combined_dfs))

        if not combined_dfs:
            print("No data to concatenate.")
            continue

        segment_df = pd.concat(combined_dfs, ignore_index=True)
        per_segment_df = process_raw_df(segment_df)

        elapsed = int(time.time() - start_time)
        print(f"elapsed time: {elapsed // 60} min , {elapsed % 60} sec\n")

    if per_segment_df is None or per_segment_df.empty:
        print("No processed data available.")
        return

    summarize_hitrate(per_segment_df)

    per_video_df = aggregate_per_video(per_segment_df)

    print(f"\nprocessed data saved at: {output_parquet_path}")
    per_video_df.to_parquet(output_parquet_path)


# ------------------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    run(csv_folder_path=args.csv_folder, output_parquet_path=args.output_parquet)
