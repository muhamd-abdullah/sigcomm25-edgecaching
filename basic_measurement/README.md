# CDN Hitrate Measurement Script

Measure CDN edge latency and hitrate for video segments or web objects using lightweight HTTP **HEAD** requests. The tool loads URL manifests from CSV files, executes concurrent measurements with PycURL, and writes structured CSV results (including timing breakdowns, response headers, status codes, and serving edge IPs).

---

## Contents
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [URL Manifests](#url-manifests)
- [Output Files & Schema](#output-files--schema)
- [Command‑Line Options](#command-line-options)
- [Examples](#examples)
- [Performance Tuning](#performance-tuning)
- [Notes & Caveats](#notes--caveats)

---

## Features
- Parses URL manifests (CSV) containing media segment or web object URLs grouped by **name**, **content**, and **quality** (bitrate or resolution for videos).
- Issues **HEAD** requests (optionally with `Range`) to avoid full downloads.
- Collects detailed timings per request: DNS, TCP connect, TLS handshake, initial response, waiting time, download time, and total latency.
- Captures response headers containing information regarding cache hit/miss, HTTP status code, and serving edge IP.
- Concurrent execution with a configurable worker pool.

> **CDN diagnostics**: The script sends vendor‑specific debug request headers for various CDNs to request additional metadata (best‑effort; may be ignored). These debug headers reveal extra information such as whether the video segment was served from cache (cache hit or miss), the location of the edge cache, and other CDN-specific diagnostics. We use this information in the hit rate analysis presented in `Section 3 (Basic Measurements)` of our paper.  

---

## Installation
### Requirements
- Python 3.9+
- System libcurl with SSL support
- Python packages:
  - `pycurl`
  - `Unidecode`

### Installing Requirements
Install all required Python packages using:
```bash
pip install -r requirements.txt
```

If `pycurl` install fails, ensure libcurl and development headers are present on your system.

---

## Quick Start
```bash
python main.py <path_to_url_manifests> <output_path>       
```

---

## URL Manifests (CSV)
Place one or more CSV files under your **url manifests directory**. Each row corresponds to a single video (for streaming services) or a webpage (for social sites) and should include:

| column         | description |
|----------------|-------------|
| `name`         | Human‑readable title for the video or webpage |
| `content`      | Content identifier (e.g., `plex_godfather`, `reddit_mainfeed`) |
| `quality_urls` | JSON object mapping quality → list of video segment or object URLs |

`quality_urls` accepts either a list of strings (URLs) or a list of objects with optional request headers, e.g.:

Option-1:
```json
{
  "1080p": [
    "https://cdn.example.com/seg1.ts",
    "https://cdn.example.com/seg2.ts"],
    
  "720p": [
    "https://cdn.example.com/seg1.ts",
    "https://cdn.example.com/seg2.ts"],
}
```
Option-2:
```json
{
  "1080p": [
    { "url": "https://cdn.example.com/video.mp4", "req_headers": "Range: bytes=0-5000" },
    { "url": "https://cdn.example.com/video.mp4", "req_headers": "Range: bytes=5001-10000" }
  ],
  "720p": [
    { "url": "https://cdn.example.com/video.mp4", "req_headers": "Range: bytes=0-1000" },
    { "url": "https://cdn.example.com/video.mp4", "req_headers": "Range: bytes=1001-2000" }
  ],
}
```

If you are measuring web objects from social sites, your `quality_urls` can look like this:

```json
{
  "objects": [
    "https://cdn.example.com/img1.jpeg",
    "https://cdn.example.com/layout.xml"
    "https://cdn.example.com/script1.js"],
}
```

---

## Output Files & Schema
Results are written under the **results directory** (default: `video_streaming/measurement_script/results`) in a structure similar to:
```
results/
  13-08-2025_14hh_27mm/
    {content}/
      {name}--{quality}_{content}.csv
      {name}--{quality}_{content}.errors.txt
```

### CSV Columns
- `timestamp(dd-mm-yyyy hh:mm:ss:ms)`
- `responseIP`
- `resp_code`
- `latency(ms)` — waiting time between request and first byte i-e time to first byte (TTFB)
- `timing_info` — JSON dict with `{dns, tcp, ssl, initial, waiting, download, total}` (milliseconds)
- `responseHeaders` — JSON object of response headers
- `name`
- `content`
- `quality` — video segment bitrate or resolution
- `url` — URL of the video segment or web object plus any applied Range header

### Error Log
`*.errors.txt` contains one line per failure: ISO timestamp, URL, and error message.

---

## Command‑Line Options
```text
usage: main.py [urls_dir] [results_dir] [--user-agent UA] [--workers N]
                     [--verbose] [--strict-headers]
```
- `urls_dir` (positional): Directory containing URL CSVs.
- `results_dir` (positional): Directory where results are written.
- `--user-agent` : Override HTTP User‑Agent string.
- `--workers` : Max concurrent measurements (default: 50).
- `--verbose` : Enable detailed logging.

---

## Examples
**Measure example CDN-hosted video segments with 10 workers and verbose logging**
```bash
python main.py ./urls/example ./results --workers 10 --verbose
```

> **Note:** The example URLs included with this script are intentionally expired/invalid to avoid directing unnecessary traffic. The measurements will still execute and produce CSV results containing response headers and timing info — but the HTTP response code will not be `200`.

---

## Performance Tuning
- Increase `--workers` cautiously to avoid saturating your network or being rate‑limited by the CDN.
- Keep timeouts conservative for wide‑area measurements; raise them when probing high‑latency regions.

---

## Notes & Caveats
- **CDN debug headers** are best‑effort and may be ignored or stripped by providers depending on configuration.
- Respect robots/terms of service and avoid probing endpoints you’re not authorized to test.

---
