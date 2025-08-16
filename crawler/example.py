# vimeo_scraper.py
"""
Vimeo URL collector and manifest/segment extractor.

This tool uses Selenium to discover Vimeo watch pages, plays each video to capture
network activity, extracts the DASH playlist JSON, and collects up to N segment URLs per available quality. Results are saved to CSV.

Notes
-----
- Respect Vimeo's Terms of Service. Only use on content you're allowed to test.
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
import pandas as pd
import requests
import timeout_decorator
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from config import CHROME_EXECUTABLE_PATH, CHROME_PROFILE_PATH


# ------------------------------- Defaults -------------------------------- #

SERVICE_NAME = "vimeo"
MAIN_URL = "https://vimeo.com/categories/documentary"

DEFAULT_MAX_VIDEOS = 10
DEFAULT_MAX_SEGMENTS = 50
DEFAULT_DISABLE_IMAGES = True
DEFAULT_HEADLESS = False

# I/O
DEFAULT_OUT_DIR = os.path.join(os.getcwd(), f"urls/{SERVICE_NAME}")
CSV_FILENAME = os.path.join(DEFAULT_OUT_DIR, f"{SERVICE_NAME}_urls_manifest.csv")
TXT_FILENAME = os.path.join(DEFAULT_OUT_DIR, f"{SERVICE_NAME}_video_urls.txt")

# Selenium / waits
DEFAULT_PAGE_WAIT = 10  # seconds
DEFAULT_SCROLL_PAUSE = 2  # seconds
DEFAULT_SCROLL_INCREMENT = 600  # px


# ------------------------------- Data types ------------------------------- #

@dataclass(frozen=True)
class ScrapeConfig:
    max_videos: int
    max_segments: int
    disable_images: bool
    headless: bool
    page_wait: int
    scroll_pause: int
    scroll_increment: int
    chrome_binary: Optional[str]
    chrome_profile_root: Optional[str]
    out_dir: str
    main_url: str = MAIN_URL


# ------------------------------- Utilities -------------------------------- #

def ensure_dir(path: str) -> None:
    """Create directory `path` if it does not exist (idempotent)."""
    os.makedirs(path, exist_ok=True)


def read_unique_lines(path: str) -> List[str]:
    """Read unique non-empty lines from a text file; return [] if missing."""
    if not os.path.exists(path):
        return []
    items: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                items.append(s)
    # Preserve file order but dedupe
    seen = set()
    deduped = []
    for u in items:
        if u not in seen:
            seen.add(u)
            deduped.append(u)
    return deduped


def write_unique_lines(path: str, lines: Iterable[str]) -> None:
    """Write unique lines to `path`, replacing existing content."""
    ensure_dir(os.path.dirname(path))
    unique = []
    seen = set()
    for l in lines:
        if l not in seen:
            seen.add(l)
            unique.append(l)
    with open(path, "w", encoding="utf-8") as f:
        for l in unique:
            f.write(l + "\n")


def read_processed_urls(csv_path: str) -> List[str]:
    """Return list of previously processed `url_titlepage` values from CSV."""
    if not os.path.exists(csv_path):
        return []
    try:
        df = pd.read_csv(csv_path)
        if "url_titlepage" in df.columns and not df.empty:
            return df["url_titlepage"].dropna().astype(str).tolist()
    except Exception as e:
        logging.warning("Failed reading processed URLs from %s: %s", csv_path, e)
    return []


def append_result_row(csv_path: str, row: Dict[str, object]) -> None:
    """Append a single result row to CSV, creating header if missing."""
    ensure_dir(os.path.dirname(csv_path))
    header = [
        "name",
        "url_titlepage",
        "url_videopage",
        "quality_urls",         # JSON mapping: height -> [segment URLs]
        "content",
        "parent_manifest_url",
    ]
    file_exists = os.path.exists(csv_path)
    if not file_exists or os.path.getsize(csv_path) == 0:
        pd.DataFrame(columns=header).to_csv(csv_path, index=False)
    # Validate and append
    for key in header:
        row.setdefault(key, "")
    pd.DataFrame([row], columns=header).to_csv(csv_path, mode="a", index=False, header=False)


# ----------------------------- Selenium setup ----------------------------- #

def build_chrome_options(cfg: ScrapeConfig) -> ChromeOptions:
    """Build Chrome options and capabilities based on configuration."""
    opts = ChromeOptions()
    caps = DesiredCapabilities.CHROME.copy()
    caps["goog:loggingPrefs"] = {"performance": "ALL"}
    for k, v in caps.items():
        opts.set_capability(k, v)

    if cfg.headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--window-size=1920,1080")

    # Disable/enable images to control bandwidth and rendering overhead
    prefs = {
        "profile.managed_default_content_settings.images": 2 if cfg.disable_images else 1
    }
    opts.add_experimental_option("prefs", prefs)

    # Optional profile isolation per service
    if cfg.chrome_profile_root:
        opts.add_argument(f"user-data-dir={os.path.join(cfg.chrome_profile_root, SERVICE_NAME)}")

    if cfg.chrome_binary:
        opts.binary_location = cfg.chrome_binary

    # Stability flags
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-extensions")
    return opts


def launch_driver(cfg: ScrapeConfig) -> webdriver.Chrome:
    """Launch and return a Chrome WebDriver."""
    options = build_chrome_options(cfg)
    service = ChromeService(ChromeDriverManager().install())
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.maximize_window()
        return driver
    except WebDriverException as e:
        logging.error("Failed to start Chrome driver: %s", e)
        raise


# --------------------------- Vimeo-specific logic -------------------------- #

def extract_watch_urls(
    driver: webdriver.Chrome,
    url: str,
    *,
    max_videos: int,
    scroll_pause: int,
    scroll_increment: int,
    wait_seconds: int,
    sidecar_path: str,
) -> List[str]:
    """
    Scroll a Vimeo listing page and collect unique watch-page URLs.

    Returns a deduped list (merged with any pre-existing URLs in `sidecar_path`).
    """
    # Seed from sidecar to avoid losing earlier progress
    urls = set(read_unique_lines(sidecar_path))
    vimeo_re = re.compile(r"^https://vimeo\.com/\d+$")

    driver.get(url)
    WebDriverWait(driver, wait_seconds).until(
        EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/')]"))
    )
    last_height = driver.execute_script("return document.body.scrollHeight")

    while len(urls) < max_videos:
        anchors = driver.find_elements(By.XPATH, "//a[contains(@href, '/')]")
        for a in anchors:
            if len(urls) >= max_videos:
                break
            href = a.get_attribute("href")
            if vimeo_re.match(href):
                if href not in urls:
                    urls.add(href)
                    logging.info("Discovered %d/%d Videos", len(urls), max_videos)
                    write_unique_lines(sidecar_path, sorted(urls))

        driver.execute_script(f"window.scrollBy(0, {scroll_increment});")
        time.sleep(scroll_pause)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # Return up-to-date list from disk
    return read_unique_lines(sidecar_path)


def extract_video_name_from_url(video_url: str) -> str:
    """Derive a simple stable 'name' from a Vimeo watch URL."""
    try:
        tail = video_url.rstrip("/").split("/")[-1]
        return tail or "Unknown Title"
    except Exception:
        return "Unknown Title"


def parse_parent_manifest_urls_from_logs(perf_logs: List[dict]) -> List[str]:
    """
    Parse Chrome DevTools 'performance' logs to find parent manifest URLs.

    We look for Network.requestWillBeSent events containing 'player.vimeo'.
    """
    out: List[str] = []
    for entry in perf_logs:
        try:
            msg = json.loads(entry["message"])["message"]
            if msg.get("method") == "Network.requestWillBeSent":
                req = msg.get("params", {}).get("request", {})
                url = req.get("url", "")
                if "player.vimeo" in url:
                    out.append(url)
        except (KeyError, json.JSONDecodeError) as e:
            logging.debug("Skipping unparsable log entry: %s", e)
    return out


def fetch_manifest_url(parent_manifest_url: str, *, max_retries: int = 5, base_backoff: int = 2) -> Optional[str]:
    """
    Resolve Vimeo's player JSON to the DASH playlist JSON URL.

    Returns the URL ending with 'playlist.json' (vod-adaptive), or None.
    Retries with exponential backoff on HTTP 429.
    """
    for attempt in range(max_retries):
        try:
            r = requests.get(parent_manifest_url, timeout=15)
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError as e:
                    logging.warning("Non-JSON player response: %s", e)
                    return None
                cdns = data.get("request", {}).get("files", {}).get("dash", {}).get("cdns", {})
                for _cdn, details in cdns.items():
                    url = details.get("url", "")
                    if url and "playlist.json" in url and "vod-adaptive" in url:
                        return url
                logging.info("No suitable playlist.json found in player JSON.")
                return None
            if r.status_code == 429:
                sleep_s = base_backoff * (2 ** attempt)
                logging.warning("429 from player JSON; backing off %ss", sleep_s)
                time.sleep(sleep_s)
                continue
            logging.warning("Unexpected HTTP %s from %s", r.status_code, parent_manifest_url)
            return None
        except requests.RequestException as e:
            logging.error("Request error for %s: %s", parent_manifest_url, e)
            return None
    logging.error("Failed to extract manifest after %d retries", max_retries)
    return None


def build_quality_segment_urls(manifest_url: str, *, max_segments: int) -> Optional[Dict[str, List[str]]]:
    """
    Download the DASH manifest JSON and assemble up to `max_segments` segment URLs per quality.

    Returns a dict: { "height(str)": ["https://...seg1", "https://...seg2", ...], ... }
    """
    try:
        r = requests.get(manifest_url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as e:
        logging.error("Error fetching manifest %s: %s", manifest_url, e)
        return None
    except ValueError as e:
        logging.error("Invalid JSON from manifest %s: %s", manifest_url, e)
        return None

    # Build base URL prefix
    base_idx = manifest_url.find("/v2")
    if base_idx == -1:
        logging.warning("Unexpected manifest URL shape (no /v2): %s", manifest_url)
        return None
    manifest_base = manifest_url[: base_idx + 3] + "/"

    base_url = (data.get("base_url") or "").split("../")[-1]

    out: Dict[str, List[str]] = {}
    for video in data.get("video", []):
        height = video.get("height")
        vbase = video.get("base_url", "")
        segments = video.get("segments", [])
        if not (height and isinstance(segments, list)):
            continue
        quality = str(height)
        urls: List[str] = []
        for seg in segments:
            su = seg.get("url")
            if not su:
                continue
            full = f"{manifest_base}{base_url}{vbase}{su}"
            urls.append(full)
            if len(urls) >= max_segments:
                break
        out[quality] = urls
    return out


@timeout_decorator.timeout(60, use_signals=False)
def play_and_extract(
    driver: webdriver.Chrome,
    watch_url: str,
    *,
    wait_seconds: int,
    check_interval: int = 2,
) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, List[str]]], Optional[str]]:
    """
    Open a Vimeo watch page, start playback, capture the parent manifest, and derive quality URLs.

    Returns:
        (url_titlepage, url_videopage, quality_urls_dict, parent_manifest_url)
    """
    driver.get(watch_url)
    url_titlepage = driver.current_url
    try:
        # Click play
        play_btn = WebDriverWait(driver, wait_seconds).until(
            EC.element_to_be_clickable((By.XPATH, "//button[@data-play-button='true']"))
        )
        ActionChains(driver).move_to_element(play_btn).click(play_btn).perform()
        logging.debug("Playback started")
        url_videopage = driver.current_url

        # Poll performance logs for parent manifest
        start = time.time()
        parent_urls: List[str] = []
        while time.time() - start < wait_seconds:
            logs = driver.get_log("performance")
            parent_urls = parse_parent_manifest_urls_from_logs(logs)
            if parent_urls:
                break
            time.sleep(check_interval)
        if not parent_urls:
            logging.warning("No parent manifest observed for %s", watch_url)
            return url_titlepage, url_videopage, None, None

        parent_manifest_url = parent_urls[0]
        manifest_url = fetch_manifest_url(parent_manifest_url)
        if not manifest_url:
            return url_titlepage, url_videopage, None, parent_manifest_url

        qmap = build_quality_segment_urls(manifest_url, max_segments=DEFAULT_MAX_SEGMENTS)
        return url_titlepage, url_videopage, qmap, parent_manifest_url
    except TimeoutException:
        logging.warning("Timed out waiting for player on %s", watch_url)
        return url_titlepage, None, None, None
    except Exception as e:
        logging.error("Error extracting from %s: %s", watch_url, e)
        return url_titlepage, None, None, None


# --------------------------------- CLI flow -------------------------------- #

def run(cfg: ScrapeConfig) -> None:
    """Main orchestration: gather watch URLs, play/extract, and write CSV rows."""
    ensure_dir(cfg.out_dir)
    csv_path = os.path.join(cfg.out_dir, f"{SERVICE_NAME}_urls_manifest.csv")
    txt_path = os.path.join(cfg.out_dir, f"{SERVICE_NAME}_video_urls.txt")

    # Previously found and processed URLs
    existing_watch_urls = read_unique_lines(txt_path)
    processed_urls = set(read_processed_urls(csv_path))

    driver: Optional[webdriver.Chrome] = None
    try:
        logging.info("%s - Launching driver...", SERVICE_NAME)
        driver = launch_driver(cfg)

        # Discover new watch URLs
        watch_urls = extract_watch_urls(
            driver,
            cfg.main_url,
            max_videos=cfg.max_videos,
            scroll_pause=cfg.scroll_pause,
            scroll_increment=cfg.scroll_increment,
            wait_seconds=cfg.page_wait,
            sidecar_path=txt_path,
        )
        logging.info("%s - Watch URLs discovered: %d", SERVICE_NAME, len(watch_urls))

        # Filter out already processed
        todo = [u for u in watch_urls if u not in processed_urls]
        if not todo:
            logging.info("Nothing new to process.")
            return

        results_count = len(processed_urls)
        for watch_url in todo:
            if results_count >= cfg.max_videos:
                break
            logging.info("Processing %s (%d/%d)", watch_url, results_count + 1, cfg.max_videos)
            name = extract_video_name_from_url(watch_url)

            try:
                url_titlepage, url_videopage, qmap, parent_manifest_url = play_and_extract(
                    driver, watch_url, wait_seconds=cfg.page_wait
                )
            except Exception as e:
                logging.error("Extraction failed for %s: %s", watch_url, e)
                qmap = None
                url_titlepage = watch_url
                url_videopage = None
                parent_manifest_url = None

            if qmap:
                content = "vimeo_video"
                row = {
                    "name": name,
                    "url_titlepage": url_titlepage or "",
                    "url_videopage": url_videopage or "",
                    "quality_urls": json.dumps(qmap),
                    "content": content,
                    "parent_manifest_url": parent_manifest_url or "",
                }
                append_result_row(csv_path, row)
                results_count += 1
                logging.info("Saved segment URLs for video %d to %s", results_count, csv_path)
            else:
                logging.info("No quality URLs for %s", watch_url)

    finally:
        if driver:
            driver.quit()


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser."""
    p = argparse.ArgumentParser(description="Collect Vimeo watch URLs and extract DASH video segment URLs.")
    p.add_argument("--max-videos", type=int, default=DEFAULT_MAX_VIDEOS, help="Max watch pages to process")
    p.add_argument("--max-segments", type=int, default=DEFAULT_MAX_SEGMENTS, help="Max segments per quality")
    p.add_argument("--disable-images", action="store_true", default=DEFAULT_DISABLE_IMAGES, help="Disable images for faster loads")
    p.add_argument("--headless", action="store_true", default=DEFAULT_HEADLESS, help="Run Chrome in headless mode")
    p.add_argument("--page-wait", type=int, default=DEFAULT_PAGE_WAIT, help="Max seconds to wait for page/player")
    p.add_argument("--scroll-pause", type=int, default=DEFAULT_SCROLL_PAUSE, help="Seconds between scroll steps")
    p.add_argument("--scroll-increment", type=int, default=DEFAULT_SCROLL_INCREMENT, help="Pixels to scroll per step")
    p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Output directory for CSV/TXT")
    p.add_argument("--main-url", default=MAIN_URL, help="Seed page to discover watch URLs")
    p.add_argument("--chrome-binary", default=CHROME_EXECUTABLE_PATH, help="Path to Chrome binary (optional)")
    p.add_argument("--chrome-profile-root", default=CHROME_PROFILE_PATH, help="Root dir for Chrome user-data-dir (optional)")
    p.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    cfg = ScrapeConfig(
        max_videos=max(1, args.max_videos),
        max_segments=max(1, args.max_segments),
        disable_images=bool(args.disable_images),
        headless=bool(args.headless),
        page_wait=max(1, args.page_wait),
        scroll_pause=max(1, args.scroll_pause),
        scroll_increment=max(1, args.scroll_increment),
        chrome_binary=args.chrome_binary,
        chrome_profile_root=args.chrome_profile_root,
        out_dir=args.out_dir,
        main_url=args.main_url,
    )

    try:
        run(cfg)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
    except Exception:
        logging.exception("Fatal error")
    finally:
        return


if __name__ == "__main__":
    raise SystemExit(main())
