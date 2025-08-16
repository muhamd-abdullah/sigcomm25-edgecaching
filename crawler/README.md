# Example Web Crawler for Vimeo

## Overview
`example.py` is a **sample crawler** for the video streaming service **Vimeo**.  
It discovers Vimeo watch pages, plays each video to capture the player’s network activity, extracts DASH playlist JSON to generate a set of segment URLs per available quality. Results are saved to a CSV for downstream measurement and analysis.

> **Maintenance note:** Streaming service front-ends change rapidly. This script was **last tested in August 2025** and confirmed working. It may require updates to XPath selectors, player element IDs, or manifest parsing logic if Vimeo changes its site structure.  
> The code serves as a **baseline** and can be adapted to other video streaming services with similar playback and manifest structures.

## Requirements
- **Python** 3.9+
- **Google Chrome** (or Chromium) installed
- **Python packages** (see `requirements.txt`):
  - `selenium`
  - `webdriver-manager`
  - `pandas`
  - `requests`
  - `timeout-decorator`

Install all dependencies:
```bash
pip install -r requirements.txt
```

## Setup
Before running, update the provided `config.py` file with your system's Chrome executable and profile paths:

```python
CHROME_EXECUTABLE_PATH = "/usr/bin/google-chrome-stable"
CHROME_PROFILE_PATH = "~/google-chrome/Default/"
```

- **`CHROME_EXECUTABLE_PATH`** — Full path to your Chrome or Chromium binary.  
- **`CHROME_PROFILE_PATH`** — Path to your Chrome profile root folder. The script will append `/vimeo` internally to isolate session data for this service.  

⚠ **Make sure to update these paths** to match your environment. If left unset or incorrect, the script will fall back to Chrome’s default installation and a temporary profile.

You can also override these paths via CLI flags `--chrome-binary` and `--chrome-profile-root`.

## Usage
```bash
python example.py  --max-videos 10  --max-segments 50  --disable-images  --out-dir ./urls/vimeo  --verbose
```

### Important options
- `--max-videos` (default: 10) — maximum watch pages to process.
- `--max-segments` (default: 50) — maximum segment URLs per quality.
- `--disable-images` — speeds up loads by turning images off.
- `--page-wait` — max seconds to wait for UI/player readiness.
- `--out-dir` — where `vimeo_urls_manifest.csv` and `vimeo_video_urls.txt` are written.
- `--main-url` — seed video listing page (default: `https://vimeo.com/categories/documentary`).
- `--verbose` — detailed logs including debug messages.

## Output
The script writes:
- `vimeo_video_urls.txt` — deduped list of discovered watch-page URLs
- `vimeo_urls_manifest.csv` — append-only table with columns:
  - `name` — derived from the watch URL tail
  - `url_titlepage` — canonical URL for the watch page before playback
  - `url_videopage` — URL after playback (may change due to navigation)
  - `quality_urls` — JSON mapping: `{ "1080": ["…seg1", "…seg2", …], "720": [...] }`
  - `content` — static tag (`vimeo_video`)
  - `parent_manifest_url` — player JSON URL used to resolve `playlist.json`

## Tips
- If player controls don’t appear, try increasing `--page-wait`.
- If you see HTTP 429 on the player JSON, the script backs off and retries automatically.

## Compliance
Only scrape content you have rights to access and measure. Ensure your usage complies with Vimeo’s Terms of Service and applicable laws/regulations.