"""
This script measures CDN edge timing for video segment URLs via HTTP HEAD requests.

It loads video segment URL from CSV files, performs concurrent measurements using
PycURL, and records results to structured CSV output files. Measurement includes
DNS resolution, TCP connect, SSL handshake, initial response, waiting time, download
time, and total latency. Response headers, IP addresses, and HTTP status codes are
also captured.

"""

import argparse
import concurrent.futures
import csv
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Sequence
import pycurl
from unidecode import unidecode

# --------------------------- Configuration & Defaults --------------------------- #

DEFAULT_URLS_DIR = "./video_streaming/measurement_script/urls"
DEFAULT_RESULTS_DIR = "./video_streaming/measurement_script/results"
DEFAULT_NUM_CHUNKS = 50
DEFAULT_USER_AGENT = "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
DEFAULT_WORKERS = 50
CONNECT_TIMEOUT_S = 10
TOTAL_TIMEOUT_S = 10

# CSV schema (corrected plus backward-compat field)
CSV_FIELDS = [
    "timestamp(dd-mm-yyyy hh:mm:ss:ms)",
    "responseIP",
    "resp_code",
    "latency(ms)",
    "timing_info",
    "responseHeaders",
    "name",
    "content",
    "quality",
    "url",
]


# --------------------------- Utility Functions --------------------------- #

def sanitize_name(name: str) -> str:
    """Return a filesystem-safe ASCII string.

    - Converts Unicode to ASCII via :func:`unidecode`.
    - Removes characters except ``[a-zA-Z0-9_ ]``.
    """
    pattern = r"[^a-zA-Z0-9_ ]"
    return re.sub(pattern, "", unidecode(name))


def make_debug_headers() -> List[str]:
    """Return HTTP debug headers for multiple CDN providers.

    This helper builds request headers intended to trigger verbose diagnostic
    information from supported CDN vendors. While these headers request additional
    metadata, not all CDNs will honor them depending on CDN customer's settings.
    """
    akamai = (
        "akamai-x-serial-no, akamai-x-cache-remote-on, akamai-x-request-trace, "
        "akamai-x-get-cache-key, akamai-x-meta-trace, akamai-x-get-ssl-client-session-id, "
        "akamai-x-get-extracted-values, akamai-x-get-nonces, akamai-x-check-cacheable, "
        "akamai-x-get-true-cache-key, akamai-x-get-request-id, akamai-x-cache-on"
    )
    akamai_pragma = [f"Pragma: {akamai}"]
    edgio = [
        "X-EC-Debug: x-ec-cache, x-ec-check-cacheable, x-ec-cache-key, x-ec-cache-state",
    ]
    fastly = [
        "Fastly-Debug: 1",
        "Fastly-Debug-Path: 1",
        "Fastly-Debug-TTL: 1",
        "Fastly-Debug-Digest: 1",
    ]
    return akamai_pragma + edgio + fastly


def is_valid_url(url: str) -> bool:
    return url.startswith("http://") or url.startswith("https://")


# --------------------------- Data Classes --------------------------- #

@dataclass
class ChunkRequest:
    url: str
    req_headers: str = ""  # e.g., "Range: bytes=0-500"


@dataclass
class TimingInfo:
    dns: float
    tcp: float
    ssl: float
    initial: float
    waiting: float
    download: float
    total: float


@dataclass
class MeasurementResult:
    response_ip: str
    response_headers: Mapping[str, str]
    response_code: int
    timing: TimingInfo


# --------------------------- Curl Helpers --------------------------- #

def _measure_chunk(req: ChunkRequest, user_agent: str) -> MeasurementResult:
    """Perform a HEAD request (optionally ranged) and collect timings and headers."""
    resp_headers: Dict[str, str] = {}
    debug_headers = make_debug_headers()

    def on_header(line: bytes) -> None:
        text = line.decode("iso-8859-1", errors="ignore")
        if ":" not in text:
            return
        name, value = text.split(":", 1)
        resp_headers[name.strip()] = value.strip()

    c = pycurl.Curl()
    try:
        c.setopt(c.HEADERFUNCTION, on_header)
        c.setopt(c.FOLLOWLOCATION, 1)
        c.setopt(c.CUSTOMREQUEST, "HEAD")
        c.setopt(c.NOBODY, 1)
        c.setopt(c.NOPROGRESS, 1)
        c.setopt(pycurl.CONNECTTIMEOUT, CONNECT_TIMEOUT_S)
        c.setopt(pycurl.TIMEOUT, TOTAL_TIMEOUT_S)
        c.setopt(pycurl.USERAGENT, user_agent)
        c.setopt(pycurl.HTTPHEADER, debug_headers)
        c.setopt(pycurl.URL, req.url)

        # Optional Range support (expects header value e.g., "bytes=0-500")
        if req.req_headers:
            # Allow either full header or just the byte expression
            rng = req.req_headers
            if rng.lower().startswith("range:"):
                rng = rng.split(":", 1)[1].strip()
            c.setopt(pycurl.RANGE, rng.replace("bytes=", ""))
            c.setopt(pycurl.HTTPHEADER, [f"Range: {rng}"] + debug_headers)
            logging.debug("Using Range header: %s", rng)

        c.perform()

        server_ip = c.getinfo(pycurl.PRIMARY_IP) or ""
        code = int(c.getinfo(pycurl.HTTP_CODE))

        # Timings in ms
        name_lookup = c.getinfo(pycurl.NAMELOOKUP_TIME) * 1000
        connect = c.getinfo(pycurl.CONNECT_TIME) * 1000
        app_connect = c.getinfo(pycurl.APPCONNECT_TIME) * 1000
        pretransfer = c.getinfo(pycurl.PRETRANSFER_TIME) * 1000
        start_transfer = c.getinfo(pycurl.STARTTRANSFER_TIME) * 1000
        total = c.getinfo(pycurl.TOTAL_TIME) * 1000

        tcp_ms = connect
        ssl_ms = max(app_connect - connect, 0.0)
        waiting_ms = max(start_transfer - pretransfer, 0.0)
        download_ms = max(total - start_transfer, 0.0)

        timing = TimingInfo(
            dns=name_lookup,
            tcp=tcp_ms,
            ssl=ssl_ms,
            initial=pretransfer,
            waiting=waiting_ms,
            download=download_ms,
            total=total,
        )

        return MeasurementResult(
            response_ip=server_ip,
            response_headers=resp_headers,
            response_code=code,
            timing=timing,
        )
    except pycurl.error as e:
        errno, errstr = e.args
        logging.error("PycURL error %d while performing request to %s: %s", errno, req.url, errstr)
        raise
    finally:
        c.close()


# --------------------------- CSV I/O --------------------------- #

def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _write_csv_row(csv_path: str, row: Mapping[str, object]) -> None:
    """Append a row to CSV, creating the file with header if needed."""
    _ensure_parent_dir(csv_path)
    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists or os.path.getsize(csv_path) == 0:
            writer.writeheader()
        writer.writerow(row)


# --------------------------- URL Source Parsing --------------------------- #

def _load_url_dicts(urls_dir: str) -> List[dict]:
    """Parse all *.csv files in urls_dir that contain a `quality_urls` JSON column.

    Returns a list of dicts, where each dict corresponds to a video with field  `segment_urls` 
    as a list of URLs or dicts with keys {url, req_headers}.
    """
    url_dicts: List[dict] = []
    for fname in sorted(os.listdir(urls_dir)):
        if not fname.endswith(".csv"):
            continue
        path = os.path.join(urls_dir, fname)
        logging.info("Reading URL manifest: %s", path)
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw = row.get("quality_urls", "")
                name = row.get("name", "")
                if not raw:
                    logging.warning("Missing quality_urls for entry '%s' in %s", name[:30], fname)
                    continue
                try:
                    qmap = json.loads(raw)
                except Exception as e:
                    logging.error("Failed to parse quality_urls for '%s' in %s: %s", name[:30], fname, e)
                    continue
                for quality, urls in qmap.items():
                    item = dict(row)
                    item["segment_urls"] = urls
                    item["quality"] = quality
                    item.pop("quality_urls", None)
                    url_dicts.append(item)
    return url_dicts


# --------------------------- Measurement Pipeline --------------------------- #

def _run_measurements(
    urls: Sequence[object],
    out_prefix: str,
    name: str,
    content: str,
    quality: str,
    *,
    user_agent: str,
) -> None:
    """Measure HEAD responses of up to DEFAULT_NUM_CHUNKS from the given URLs.

    Each measurement is appended as a row to `{out_prefix}.csv`. Errors are
    appended to `{out_prefix}.errors.txt`.
    """
    selected: List[object] = list(urls)[:DEFAULT_NUM_CHUNKS]
    if not selected:
        logging.error("No URLs to measure for %s", out_prefix)
        return

    errors_path = f"{out_prefix}.errors.txt"
    csv_path = f"{out_prefix}.csv"

    for i, entry in enumerate(selected, start=1):
        # Normalize to ChunkRequest
        if isinstance(entry, dict):
            url = entry.get("url", "")
            req_headers = entry.get("req_headers", "")
        else:
            url = str(entry)
            req_headers = ""
        
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.info("[%d/%d] %s", i, len(selected), url)

        if not is_valid_url(url):
            logging.warning("Skipping invalid URL: %s", url)
            continue

        try:
            ts = datetime.now().strftime("%d-%m-%Y_%H:%M:%S:%f")
            result = _measure_chunk(ChunkRequest(url=url, req_headers=req_headers), user_agent)

            # Build row
            link_plus_header = f"{url} {req_headers}".strip()
            row = {k: "" for k in CSV_FIELDS}
            row.update(
                {
                    "timestamp(dd-mm-yyyy hh:mm:ss:ms)": ts,
                    "responseIP": result.response_ip,
                    "resp_code": result.response_code,
                    "latency(ms)": result.timing.waiting,
                    "timing_info": json.dumps(result.timing.__dict__),
                    "responseHeaders": json.dumps(result.response_headers),
                    "name": name,
                    "content": content,
                    "quality": quality,
                    "url": link_plus_header,
                }
            )
            _write_csv_row(csv_path, row)
        except Exception as e:
            logging.exception("Measurement failed for %s", url)
            _ensure_parent_dir(errors_path)
            with open(errors_path, "a", encoding="utf-8") as ef:
                ef.write(f"{datetime.now().isoformat()} - {url} - {e}\n")


# --------------------------- Orchestration --------------------------- #

def run(
    urls_dir: str,
    results_dir: str,
    *,
    user_agent: str = DEFAULT_USER_AGENT,
    workers: int = DEFAULT_WORKERS,
) -> None:
    start = time.time()
    url_dicts = _load_url_dicts(urls_dir)
    timestamp = datetime.now().strftime("%d-%m-%Y_%Hhh_%Mmm")

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as tp:
        futures = []
        for i in range(0, len(url_dicts), workers):
            logging.info("Batch %d .. %d of %d URLs", i//workers, min(i + workers, len(url_dicts)), len(url_dicts))
            batch = url_dicts[i : i + workers]
            for entry in batch:
                urls = entry.get("segment_urls", [])
                name = sanitize_name(entry.get("name", ""))
                content = entry.get("content", "")
                quality = entry.get("quality", "")
                service = content.split("_")[0] if content else "service"
                out_dir = os.path.join(results_dir, timestamp, service)
                out_prefix = os.path.join(out_dir, f"{name}--{quality}_{content}")

                futures.append(
                    tp.submit(
                        _run_measurements,
                        urls,
                        out_prefix,
                        name,
                        content,
                        quality,
                        user_agent=user_agent,
                    )
                )
        concurrent.futures.wait(futures)

    elapsed_min = int((time.time() - start) // 60)
    logging.info("Results saved in %s (elapsed ~%d min)", os.path.join(results_dir, timestamp), elapsed_min)


# --------------------------- CLI --------------------------- #

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Measure CDN edge timing via PycURL HEAD requests.")
    p.add_argument("urls_dir", nargs="?", default=DEFAULT_URLS_DIR, help="Directory with URL manifests (*.csv)")
    p.add_argument("results_dir", nargs="?", default=DEFAULT_RESULTS_DIR, help="Directory to write results to")
    p.add_argument("--user-agent", default=DEFAULT_USER_AGENT, help="Override HTTP User-Agent")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Max concurrent measurement workers")
    p.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    try:
        run(
            urls_dir=args.urls_dir,
            results_dir=args.results_dir,
            user_agent=args.user_agent,
            workers=max(1, args.workers),
        )
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
    except Exception:
        logging.exception("Fatal error")
    return


if __name__ == "__main__":
    raise SystemExit(main())
